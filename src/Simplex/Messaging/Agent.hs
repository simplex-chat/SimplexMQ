{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Simplex.Messaging.Agent
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- This module defines SMP protocol agent with SQLite persistence.
--
-- See https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md
module Simplex.Messaging.Agent
  ( runSMPAgent,
    runSMPAgentBlocking,
    getSMPAgentClient,
    runSMPAgentClient,
  )
where

import Control.Concurrent.STM (stateTVar)
import Control.Logger.Simple (logInfo, showText)
import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Reader
import Crypto.Random (MonadRandom)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock
import Database.SQLite.Simple (SQLError)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite (SQLiteStore, connectSQLiteStore)
import Simplex.Messaging.Client (SMPServerTransmission)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (CorrId (..), MsgBody, SenderPublicKey)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Transport (putLn, runTCPServer)
import Simplex.Messaging.Util (bshow)
import System.IO (Handle)
import System.Random (randomR)
import UnliftIO.Async (race_)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

-- | Runs an SMP agent as a TCP service using passed configuration.
--
-- See a full agent executable here: https://github.com/simplex-chat/simplexmq/blob/master/apps/smp-agent/Main.hs
runSMPAgent :: (MonadRandom m, MonadUnliftIO m) => AgentConfig -> m ()
runSMPAgent cfg = newEmptyTMVarIO >>= (`runSMPAgentBlocking` cfg)

-- | Runs an SMP agent as a TCP service using passed configuration with signalling.
--
-- This function uses passed TMVar to signal when the server is ready to accept TCP requests (True)
-- and when it is disconnected from the TCP socket once the server thread is killed (False).
runSMPAgentBlocking :: (MonadRandom m, MonadUnliftIO m) => TMVar Bool -> AgentConfig -> m ()
runSMPAgentBlocking started cfg@AgentConfig {tcpPort} = runReaderT smpAgent =<< newSMPAgentEnv cfg
  where
    smpAgent :: (MonadUnliftIO m', MonadReader Env m') => m' ()
    smpAgent = runTCPServer started tcpPort $ \h -> do
      liftIO $ putLn h "Welcome to SMP v0.3.0 agent"
      c <- getSMPAgentClient
      logConnection c True
      race_ (connectClient h c) (runSMPAgentClient c)
        `E.finally` (closeSMPServerClients c >> logConnection c False)

-- | Creates an SMP agent instance that receives commands and sends responses via 'TBQueue's.
getSMPAgentClient :: (MonadUnliftIO m, MonadReader Env m) => m AgentClient
getSMPAgentClient = do
  n <- asks clientCounter
  cfg <- asks config
  atomically $ newAgentClient n cfg

connectClient :: MonadUnliftIO m => Handle -> AgentClient -> m ()
connectClient h c = race_ (send h c) (receive h c)

logConnection :: MonadUnliftIO m => AgentClient -> Bool -> m ()
logConnection c connected =
  let event = if connected then "connected to" else "disconnected from"
   in logInfo $ T.unwords ["client", showText (clientId c), event, "Agent"]

-- | Runs an SMP agent instance that receives commands and sends responses via 'TBQueue's.
runSMPAgentClient :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
runSMPAgentClient c = do
  db <- asks $ dbFile . config
  s1 <- connectSQLiteStore db
  s2 <- connectSQLiteStore db
  race_ (subscriber c s1) (client c s2)

receive :: forall m. MonadUnliftIO m => Handle -> AgentClient -> m ()
receive h c@AgentClient {rcvQ, sndQ} = forever $ do
  (corrId, cAlias, cmdOrErr) <- tGet SClient h
  case cmdOrErr of
    Right cmd -> write rcvQ (corrId, cAlias, cmd)
    Left e -> write sndQ (corrId, cAlias, ERR e)
  where
    write :: TBQueue (ATransmission p) -> ATransmission p -> m ()
    write q t = do
      logClient c "-->" t
      atomically $ writeTBQueue q t

send :: MonadUnliftIO m => Handle -> AgentClient -> m ()
send h c@AgentClient {sndQ} = forever $ do
  t <- atomically $ readTBQueue sndQ
  tPut h t
  logClient c "<--" t

logClient :: MonadUnliftIO m => AgentClient -> ByteString -> ATransmission a -> m ()
logClient AgentClient {clientId} dir (CorrId corrId, cAlias, cmd) = do
  logInfo . decodeUtf8 $ B.unwords [bshow clientId, dir, "A :", corrId, cAlias, B.takeWhile (/= ' ') $ serializeCommand cmd]

client :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> SQLiteStore -> m ()
client c@AgentClient {rcvQ, sndQ} st = forever $ do
  t@(corrId, cAlias, _) <- atomically $ readTBQueue rcvQ
  runExceptT (processCommand c st t) >>= \case
    Left e -> atomically $ writeTBQueue sndQ (corrId, cAlias, ERR e)
    Right _ -> return ()

withStore ::
  AgentMonad m =>
  (forall m'. (MonadUnliftIO m', MonadError StoreError m') => m' a) ->
  m a
withStore action = do
  runExceptT (action `E.catch` handleInternal) >>= \case
    Right c -> return c
    Left e -> throwError $ storeError e
  where
    handleInternal :: (MonadError StoreError m') => SQLError -> m' a
    handleInternal e = throwError . SEInternal $ bshow e
    storeError :: StoreError -> AgentErrorType
    storeError = \case
      SEConnNotFound -> CONN UNKNOWN
      SEConnDuplicate -> CONN DUPLICATE
      e -> INTERNAL $ show e

processCommand :: forall m. AgentMonad m => AgentClient -> SQLiteStore -> ATransmission 'Client -> m ()
processCommand c@AgentClient {sndQ} st (corrId, connAlias, cmd) =
  case cmd of
    NEW -> createNewConnection
    JOIN smpQueueInfo replyMode -> joinConnection smpQueueInfo replyMode
    SUB -> subscribeConnection connAlias
    SUBALL -> subscribeAll
    SEND msgBody -> sendMessage msgBody
    OFF -> suspendConnection
    DEL -> deleteConnection
  where
    createNewConnection :: m ()
    createNewConnection = do
      -- TODO create connection alias if not passed
      -- make connAlias Maybe?
      srv <- getSMPServer
      (rq, qInfo) <- newReceiveQueue c srv connAlias
      withStore $ createRcvConn st rq
      respond $ INV qInfo

    getSMPServer :: m SMPServer
    getSMPServer =
      asks (smpServers . config) >>= \case
        srv :| [] -> pure srv
        servers -> do
          gen <- asks randomServer
          i <- atomically . stateTVar gen $ randomR (0, L.length servers - 1)
          pure $ servers L.!! i

    joinConnection :: SMPQueueInfo -> ReplyMode -> m ()
    joinConnection qInfo (ReplyMode replyMode) = do
      -- TODO create connection alias if not passed
      -- make connAlias Maybe?
      (sq, senderKey, verifyKey) <- newSendQueue qInfo connAlias
      withStore $ createSndConn st sq
      connectToSendQueue c st sq senderKey verifyKey
      when (replyMode == On) $ createReplyQueue sq
    -- TODO this response is disabled to avoid two responses in terminal client (OK + CON),
    -- respond OK

    subscribeConnection :: ConnAlias -> m ()
    subscribeConnection cAlias =
      withStore (getConn st cAlias) >>= \case
        SomeConn _ (DuplexConnection _ rq _) -> subscribe rq
        SomeConn _ (RcvConnection _ rq) -> subscribe rq
        _ -> throwError $ CONN SIMPLEX
      where
        subscribe rq = subscribeQueue c rq cAlias >> respond' cAlias OK

    -- TODO remove - hack for subscribing to all; respond' and parameterization of subscribeConnection are byproduct
    subscribeAll :: m ()
    subscribeAll = withStore (getAllConnAliases st) >>= mapM_ subscribeConnection

    sendMessage :: MsgBody -> m ()
    sendMessage msgBody =
      withStore (getConn st connAlias) >>= \case
        SomeConn _ (DuplexConnection _ _ sq) -> sendMsg sq
        SomeConn _ (SndConnection _ sq) -> sendMsg sq
        _ -> throwError $ CONN SIMPLEX
      where
        sendMsg sq = do
          internalTs <- liftIO getCurrentTime
          (internalId, internalSndId, previousMsgHash) <- withStore $ updateSndIds st sq
          let msgStr =
                serializeSMPMessage
                  SMPMessage
                    { senderMsgId = unSndId internalSndId,
                      senderTimestamp = internalTs,
                      previousMsgHash,
                      agentMessage = A_MSG msgBody
                    }
              msgHash = C.sha256Hash msgStr
          withStore $
            createSndMsg st sq $
              SndMsgData {internalId, internalSndId, internalTs, msgBody, internalHash = msgHash}
          sendAgentMessage c sq msgStr
          respond $ SENT (unId internalId)

    suspendConnection :: m ()
    suspendConnection =
      withStore (getConn st connAlias) >>= \case
        SomeConn _ (DuplexConnection _ rq _) -> suspend rq
        SomeConn _ (RcvConnection _ rq) -> suspend rq
        _ -> throwError $ CONN SIMPLEX
      where
        suspend rq = suspendQueue c rq >> respond OK

    deleteConnection :: m ()
    deleteConnection =
      withStore (getConn st connAlias) >>= \case
        SomeConn _ (DuplexConnection _ rq _) -> delete rq
        SomeConn _ (RcvConnection _ rq) -> delete rq
        _ -> delConn
      where
        delConn = withStore (deleteConn st connAlias) >> respond OK
        delete rq = do
          deleteQueue c rq
          removeSubscription c connAlias
          delConn

    createReplyQueue :: SndQueue -> m ()
    createReplyQueue sq = do
      srv <- getSMPServer
      (rq, qInfo) <- newReceiveQueue c srv connAlias
      withStore $ upgradeSndConnToDuplex st connAlias rq
      senderTimestamp <- liftIO getCurrentTime
      sendAgentMessage c sq . serializeSMPMessage $
        SMPMessage
          { senderMsgId = 0,
            senderTimestamp,
            previousMsgHash = "",
            agentMessage = REPLY qInfo
          }

    respond :: ACommand 'Agent -> m ()
    respond = respond' connAlias

    respond' :: ConnAlias -> ACommand 'Agent -> m ()
    respond' cAlias resp = atomically $ writeTBQueue sndQ (corrId, cAlias, resp)

subscriber :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> SQLiteStore -> m ()
subscriber c@AgentClient {msgQ} st = forever $ do
  -- TODO this will only process messages and notifications
  t <- atomically $ readTBQueue msgQ
  runExceptT (processSMPTransmission c st t) >>= \case
    Left e -> liftIO $ print e
    Right _ -> return ()

processSMPTransmission :: forall m. AgentMonad m => AgentClient -> SQLiteStore -> SMPServerTransmission -> m ()
processSMPTransmission c@AgentClient {sndQ} st (srv, rId, cmd) = do
  withStore (getRcvConn st srv rId) >>= \case
    SomeConn SCDuplex (DuplexConnection _ rq _) -> processSMP SCDuplex rq
    SomeConn SCRcv (RcvConnection _ rq) -> processSMP SCRcv rq
    _ -> atomically $ writeTBQueue sndQ ("", "", ERR $ CONN SIMPLEX)
  where
    processSMP :: SConnType c -> RcvQueue -> m ()
    processSMP cType rq@RcvQueue {connAlias, status} =
      case cmd of
        SMP.MSG srvMsgId srvTs msgBody -> do
          -- TODO deduplicate with previously received
          msg <- decryptAndVerify rq msgBody
          let msgHash = C.sha256Hash msg
          agentMsg <- liftEither $ parseSMPMessage msg
          case agentMsg of
            SMPConfirmation senderKey -> smpConfirmation senderKey
            SMPMessage {agentMessage, senderMsgId, senderTimestamp, previousMsgHash} ->
              case agentMessage of
                HELLO verifyKey _ -> helloMsg verifyKey msgBody
                REPLY qInfo -> replyMsg qInfo
                A_MSG body -> agentClientMsg previousMsgHash (senderMsgId, senderTimestamp) (srvMsgId, srvTs) body msgHash
          sendAck c rq
          return ()
        SMP.END -> do
          removeSubscription c connAlias
          logServer "<--" c srv rId "END"
          notify END
        _ -> do
          logServer "<--" c srv rId $ "unexpected: " <> bshow cmd
          notify . ERR $ BROKER UNEXPECTED
      where
        notify :: ACommand 'Agent -> m ()
        notify msg = atomically $ writeTBQueue sndQ ("", connAlias, msg)

        prohibited :: m ()
        prohibited = notify . ERR $ AGENT A_PROHIBITED

        smpConfirmation :: SenderPublicKey -> m ()
        smpConfirmation senderKey = do
          logServer "<--" c srv rId "MSG <KEY>"
          case status of
            New -> do
              -- TODO currently it automatically allows whoever sends the confirmation
              -- Commands CONF and LET are not supported in v0.2
              withStore $ setRcvQueueStatus st rq Confirmed
              -- TODO update sender key in the store?
              secureQueue c rq senderKey
              withStore $ setRcvQueueStatus st rq Secured
            _ -> prohibited

        helloMsg :: SenderPublicKey -> ByteString -> m ()
        helloMsg verifyKey msgBody = do
          logServer "<--" c srv rId "MSG <HELLO>"
          case status of
            Active -> prohibited
            _ -> do
              void $ verifyMessage (Just verifyKey) msgBody
              withStore $ setRcvQueueActive st rq verifyKey
              case cType of
                SCDuplex -> notify CON
                _ -> pure ()

        replyMsg :: SMPQueueInfo -> m ()
        replyMsg qInfo = do
          logServer "<--" c srv rId "MSG <REPLY>"
          case cType of
            SCRcv -> do
              (sq, senderKey, verifyKey) <- newSendQueue qInfo connAlias
              withStore $ upgradeRcvConnToDuplex st connAlias sq
              connectToSendQueue c st sq senderKey verifyKey
              notify CON
            _ -> prohibited

        agentClientMsg :: PrevRcvMsgHash -> (ExternalSndId, ExternalSndTs) -> (BrokerId, BrokerTs) -> MsgBody -> MsgHash -> m ()
        agentClientMsg receivedPrevMsgHash senderMeta brokerMeta msgBody msgHash = do
          logServer "<--" c srv rId "MSG <MSG>"
          case status of
            Active -> do
              internalTs <- liftIO getCurrentTime
              (internalId, internalRcvId, prevExtSndId, prevRcvMsgHash) <- withStore $ updateRcvIds st rq
              let msgIntegrity = checkMsgIntegrity prevExtSndId (fst senderMeta) prevRcvMsgHash receivedPrevMsgHash
              withStore $
                createRcvMsg st rq $
                  RcvMsgData
                    { internalId,
                      internalRcvId,
                      internalTs,
                      senderMeta,
                      brokerMeta,
                      msgBody,
                      internalHash = msgHash,
                      externalPrevSndHash = receivedPrevMsgHash,
                      msgIntegrity
                    }
              notify
                MSG
                  { recipientMeta = (unId internalId, internalTs),
                    senderMeta,
                    brokerMeta,
                    msgBody,
                    msgIntegrity
                  }
            _ -> prohibited

        checkMsgIntegrity :: PrevExternalSndId -> ExternalSndId -> PrevRcvMsgHash -> ByteString -> MsgIntegrity
        checkMsgIntegrity prevExtSndId extSndId internalPrevMsgHash receivedPrevMsgHash
          | extSndId == prevExtSndId + 1 && internalPrevMsgHash == receivedPrevMsgHash = MsgOk
          | extSndId < prevExtSndId = MsgError $ MsgBadId extSndId
          | extSndId == prevExtSndId = MsgError MsgDuplicate -- ? deduplicate
          | extSndId > prevExtSndId + 1 = MsgError $ MsgSkipped (prevExtSndId + 1) (extSndId - 1)
          | internalPrevMsgHash /= receivedPrevMsgHash = MsgError MsgBadHash
          | otherwise = MsgError MsgDuplicate -- this case is not possible

connectToSendQueue :: AgentMonad m => AgentClient -> SQLiteStore -> SndQueue -> SenderPublicKey -> VerificationKey -> m ()
connectToSendQueue c st sq senderKey verifyKey = do
  sendConfirmation c sq senderKey
  withStore $ setSndQueueStatus st sq Confirmed
  sendHello c sq verifyKey
  withStore $ setSndQueueStatus st sq Active

newSendQueue ::
  (MonadUnliftIO m, MonadReader Env m) => SMPQueueInfo -> ConnAlias -> m (SndQueue, SenderPublicKey, VerificationKey)
newSendQueue (SMPQueueInfo smpServer senderId encryptKey) connAlias = do
  size <- asks $ rsaKeySize . config
  (senderKey, sndPrivateKey) <- liftIO $ C.generateKeyPair size
  (verifyKey, signKey) <- liftIO $ C.generateKeyPair size
  let sndQueue =
        SndQueue
          { server = smpServer,
            sndId = senderId,
            connAlias,
            sndPrivateKey,
            encryptKey,
            signKey,
            status = New
          }
  return (sndQueue, senderKey, verifyKey)
