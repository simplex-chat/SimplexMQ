{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

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
  ( -- * SMP agent over TCP
    runSMPAgent,
    runSMPAgentBlocking,

    -- * queue-based SMP agent
    getAgentClient,
    runAgentClient,

    -- * SMP agent functional API
    AgentClient (..),
    AgentMonad,
    AgentErrorMonad,
    getSMPAgentClient,
    createConnection,
    joinConnection,
    sendIntroduction,
    acceptInvitation,
    subscribeConnection,
    sendMessage,
    suspendConnection,
    deleteConnection,
    createConnection',
    joinConnection',
    sendIntroduction',
    acceptInvitation',
    subscribeConnection',
    sendMessage',
    suspendConnection',
    deleteConnection',
  )
where

import Control.Concurrent.STM (stateTVar)
import Control.Logger.Simple (logInfo, showText)
import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Reader
import Crypto.Random (MonadRandom)
import Data.Bifunctor (second)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Composition ((.:), (.:.))
import Data.Functor (($>))
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock
import Database.SQLite.Simple (SQLError)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite (SQLiteStore)
import Simplex.Messaging.Client (SMPServerTransmission)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (MsgBody, SenderPublicKey)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Transport (ATransport (..), TProxy, Transport (..), runTransportServer)
import Simplex.Messaging.Util (bshow)
import System.Random (randomR)
import UnliftIO.Async (async, race_)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

-- | Runs an SMP agent as a TCP service using passed configuration.
--
-- See a full agent executable here: https://github.com/simplex-chat/simplexmq/blob/master/apps/smp-agent/Main.hs
runSMPAgent :: (MonadRandom m, MonadUnliftIO m) => ATransport -> AgentConfig -> m ()
runSMPAgent t cfg = do
  started <- newEmptyTMVarIO
  runSMPAgentBlocking t started cfg

-- | Runs an SMP agent as a TCP service using passed configuration with signalling.
--
-- This function uses passed TMVar to signal when the server is ready to accept TCP requests (True)
-- and when it is disconnected from the TCP socket once the server thread is killed (False).
runSMPAgentBlocking :: (MonadRandom m, MonadUnliftIO m) => ATransport -> TMVar Bool -> AgentConfig -> m ()
runSMPAgentBlocking (ATransport t) started cfg@AgentConfig {tcpPort} = runReaderT (smpAgent t) =<< newSMPAgentEnv cfg
  where
    smpAgent :: forall c m'. (Transport c, MonadUnliftIO m', MonadReader Env m') => TProxy c -> m' ()
    smpAgent _ = runTransportServer started tcpPort $ \(h :: c) -> do
      liftIO $ putLn h "Welcome to SMP v0.3.2 agent"
      c <- getAgentClient
      logConnection c True
      race_ (connectClient h c) (runAgentClient c)
        `E.finally` disconnectServers c

-- | Creates an SMP agent client instance
getSMPAgentClient :: (MonadRandom m, MonadUnliftIO m) => AgentConfig -> m AgentClient
getSMPAgentClient cfg = newSMPAgentEnv cfg >>= runReaderT runAgent
  where
    runAgent = do
      c <- getAgentClient
      action <- async $ subscriber c `E.finally` disconnectServers c
      pure c {smpSubscriber = action}

disconnectServers :: MonadUnliftIO m => AgentClient -> m ()
disconnectServers c = closeSMPServerClients c >> logConnection c False

-- |
type AgentErrorMonad m = (MonadUnliftIO m, MonadError AgentErrorType m)

-- | Create SMP agent connection (NEW command) in Reader monad
createConnection' :: AgentMonad m => AgentClient -> Maybe ConnId -> m (ConnId, SMPQueueInfo)
createConnection' c connId = newConn c (fromMaybe "" connId) Nothing 0

-- | Create SMP agent connection (NEW command)
createConnection :: AgentErrorMonad m => AgentClient -> Maybe ConnId -> m (ConnId, SMPQueueInfo)
createConnection c = (`runReaderT` agentEnv c) . createConnection' c

-- | Join SMP agent connection (JOIN command) in Reader monad
joinConnection' :: AgentMonad m => AgentClient -> Maybe ConnId -> SMPQueueInfo -> m ConnId
joinConnection' c connId qInfo = joinConn c (fromMaybe "" connId) qInfo (ReplyMode On) Nothing 0

-- | Join SMP agent connection (JOIN command)
joinConnection :: AgentErrorMonad m => AgentClient -> Maybe ConnId -> SMPQueueInfo -> m ConnId
joinConnection c = (`runReaderT` agentEnv c) .: joinConnection' c

-- | Accept invitation (ACPT command) in Reader monad
acceptInvitation' :: AgentMonad m => AgentClient -> InvitationId -> ConnInfo -> m ConnId
acceptInvitation' c = acceptInv c ""

-- | Accept invitation (ACPT command)
acceptInvitation :: AgentErrorMonad m => AgentClient -> InvitationId -> ConnInfo -> m ConnId
acceptInvitation c = (`runReaderT` agentEnv c) .: acceptInvitation c

-- | Send introduction of the second connection the first (INTRO command)
sendIntroduction :: AgentErrorMonad m => AgentClient -> ConnId -> ConnId -> ConnInfo -> m ()
sendIntroduction c = (`runReaderT` agentEnv c) .:. sendIntroduction' c

-- | Subscribe to receive connection messages (SUB command)
subscribeConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
subscribeConnection c = (`runReaderT` agentEnv c) . subscribeConnection' c

-- | Send message to the connection (SEND command)
sendMessage :: AgentErrorMonad m => AgentClient -> ConnId -> MsgBody -> m InternalId
sendMessage c = (`runReaderT` agentEnv c) .: sendMessage' c

-- | Suspend SMP agent connection (OFF command)
suspendConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
suspendConnection c = (`runReaderT` agentEnv c) . suspendConnection' c

-- | Delete SMP agent connection (DEL command)
deleteConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
deleteConnection c = (`runReaderT` agentEnv c) . deleteConnection' c

-- | Creates an SMP agent client instance that receives commands and sends responses via 'TBQueue's.
getAgentClient :: (MonadUnliftIO m, MonadReader Env m) => m AgentClient
getAgentClient = ask >>= atomically . newAgentClient

connectClient :: Transport c => MonadUnliftIO m => c -> AgentClient -> m ()
connectClient h c = race_ (send h c) (receive h c)

logConnection :: MonadUnliftIO m => AgentClient -> Bool -> m ()
logConnection c connected =
  let event = if connected then "connected to" else "disconnected from"
   in logInfo $ T.unwords ["client", showText (clientId c), event, "Agent"]

-- | Runs an SMP agent instance that receives commands and sends responses via 'TBQueue's.
runAgentClient :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
runAgentClient c = race_ (subscriber c) (client c)

receive :: forall c m. (Transport c, MonadUnliftIO m) => c -> AgentClient -> m ()
receive h c@AgentClient {rcvQ, subQ} = forever $ do
  (corrId, connId, cmdOrErr) <- tGet SClient h
  case cmdOrErr of
    Right cmd -> write rcvQ (corrId, connId, cmd)
    Left e -> write subQ (corrId, connId, ERR e)
  where
    write :: TBQueue (ATransmission p) -> ATransmission p -> m ()
    write q t = do
      logClient c "-->" t
      atomically $ writeTBQueue q t

send :: (Transport c, MonadUnliftIO m) => c -> AgentClient -> m ()
send h c@AgentClient {subQ} = forever $ do
  t <- atomically $ readTBQueue subQ
  tPut h t
  logClient c "<--" t

logClient :: MonadUnliftIO m => AgentClient -> ByteString -> ATransmission a -> m ()
logClient AgentClient {clientId} dir (corrId, connId, cmd) = do
  logInfo . decodeUtf8 $ B.unwords [bshow clientId, dir, "A :", corrId, connId, B.takeWhile (/= ' ') $ serializeCommand cmd]

client :: forall m. (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
client c@AgentClient {rcvQ, subQ} = forever $ do
  (corrId, connId, cmd) <- atomically $ readTBQueue rcvQ
  runExceptT (processCommand c (connId, cmd))
    >>= atomically . writeTBQueue subQ . \case
      Left e -> (corrId, connId, ERR e)
      Right (connId', resp) -> (corrId, connId', resp)

withStore ::
  AgentMonad m =>
  (forall m'. (MonadUnliftIO m', MonadError StoreError m') => SQLiteStore -> m' a) ->
  m a
withStore action = do
  st <- asks store'
  runExceptT (action st `E.catch` handleInternal) >>= \case
    Right c -> return c
    Left e -> throwError $ storeError e
  where
    -- TODO when parsing exception happens in store, the agent hangs;
    -- changing SQLError to SomeException does not help
    handleInternal :: (MonadError StoreError m') => SQLError -> m' a
    handleInternal e = throwError . SEInternal $ bshow e
    storeError :: StoreError -> AgentErrorType
    storeError = \case
      SEConnNotFound -> CONN NOT_FOUND
      SEConnDuplicate -> CONN DUPLICATE
      SEBadConnType CRcv -> CONN SIMPLEX
      SEBadConnType CSnd -> CONN SIMPLEX
      e -> INTERNAL $ show e

-- | execute any SMP agent command
processCommand :: forall m. AgentMonad m => AgentClient -> (ConnId, ACommand 'Client) -> m (ConnId, ACommand 'Agent)
processCommand c (connId, cmd) = case cmd of
  NEW -> second INV <$> newConn c connId Nothing 0
  JOIN smpQueueInfo replyMode -> (,OK) <$> joinConn c connId smpQueueInfo replyMode Nothing 0
  INTRO reConnId reInfo -> sendIntroduction' c connId reConnId reInfo $> (connId, OK)
  ACPT invId connInfo -> (,OK) <$> acceptInv c connId invId connInfo
  SUB -> subscribeConnection' c connId $> (connId, OK)
  SEND msgBody -> (connId,) . SENT . unId <$> sendMessage' c connId msgBody
  OFF -> suspendConnection' c connId $> (connId, OK)
  DEL -> deleteConnection' c connId $> (connId, OK)

newConn :: AgentMonad m => AgentClient -> ConnId -> Maybe InvitationId -> Int -> m (ConnId, SMPQueueInfo)
newConn c connId viaInv connLevel = do
  srv <- getSMPServer
  (rq, qInfo) <- newReceiveQueue c srv
  g <- asks idsDrg
  let cData = ConnData {connId, viaInv, connLevel}
  connId' <- withStore $ \st -> createRcvConn st g cData rq
  addSubscription c rq connId'
  pure (connId', qInfo)

joinConn :: forall m. AgentMonad m => AgentClient -> ConnId -> SMPQueueInfo -> ReplyMode -> Maybe InvitationId -> Int -> m ConnId
joinConn c connId qInfo (ReplyMode replyMode) viaInv connLevel = do
  (sq, senderKey, verifyKey) <- newSendQueue qInfo
  g <- asks idsDrg
  let cData = ConnData {connId, viaInv, connLevel}
  connId' <- withStore $ \st -> createSndConn st g cData sq
  connectToSendQueue c sq senderKey verifyKey
  when (replyMode == On) $ createReplyQueue connId' sq
  pure connId'
  where
    createReplyQueue :: ConnId -> SndQueue -> m ()
    createReplyQueue cId sq = do
      srv <- getSMPServer
      (rq, qInfo') <- newReceiveQueue c srv
      addSubscription c rq cId
      withStore $ \st -> upgradeSndConnToDuplex st cId rq
      sendControlMessage c sq $ REPLY qInfo'

-- | Send introduction of the second connection the first (INTRO command) in Reader monad
sendIntroduction' :: AgentMonad m => AgentClient -> ConnId -> ConnId -> ConnInfo -> m ()
sendIntroduction' c toConn reConn reInfo =
  withStore (\st -> (,) <$> getConn st toConn <*> getConn st reConn) >>= \case
    (SomeConn _ (DuplexConnection _ _ sq), SomeConn _ DuplexConnection {}) -> do
      g <- asks idsDrg
      introId <- withStore $ \st -> createIntro st g NewIntroduction {toConn, reConn, reInfo}
      sendControlMessage c sq $ A_INTRO introId reInfo
    _ -> throwError $ CONN SIMPLEX

acceptInv :: AgentMonad m => AgentClient -> ConnId -> InvitationId -> ConnInfo -> m ConnId
acceptInv c connId invId connInfo =
  withStore (`getInvitation` invId) >>= \case
    Invitation {viaConn, qInfo, externalIntroId, status = InvNew} ->
      withStore (`getConn` viaConn) >>= \case
        SomeConn _ (DuplexConnection ConnData {connLevel} _ sq) -> case qInfo of
          Nothing -> do
            (connId', qInfo') <- newConn c connId (Just invId) (connLevel + 1)
            withStore $ \st -> addInvitationConn st invId connId'
            sendControlMessage c sq $ A_INV externalIntroId qInfo' connInfo
            pure connId'
          Just qInfo' -> do
            connId' <- joinConn c connId qInfo' (ReplyMode On) (Just invId) (connLevel + 1)
            withStore $ \st -> addInvitationConn st invId connId'
            pure connId'
        _ -> throwError $ CONN SIMPLEX
    _ -> throwError $ CMD PROHIBITED

-- | Subscribe to receive connection messages (SUB command) in Reader monad
subscribeConnection' :: AgentMonad m => AgentClient -> ConnId -> m ()
subscribeConnection' c connId =
  withStore (`getConn` connId) >>= \case
    SomeConn _ (DuplexConnection _ rq _) -> subscribeQueue c rq connId
    SomeConn _ (RcvConnection _ rq) -> subscribeQueue c rq connId
    _ -> throwError $ CONN SIMPLEX

-- | Send message to the connection (SEND command) in Reader monad
sendMessage' :: forall m. AgentMonad m => AgentClient -> ConnId -> MsgBody -> m InternalId
sendMessage' c connId msgBody =
  withStore (`getConn` connId) >>= \case
    SomeConn _ (DuplexConnection _ _ sq) -> sendMsg_ sq
    SomeConn _ (SndConnection _ sq) -> sendMsg_ sq
    _ -> throwError $ CONN SIMPLEX
  where
    sendMsg_ :: SndQueue -> m InternalId
    sendMsg_ sq = do
      internalTs <- liftIO getCurrentTime
      (internalId, internalSndId, previousMsgHash) <- withStore (`updateSndIds` connId)
      let msgStr =
            serializeSMPMessage
              SMPMessage
                { senderMsgId = unSndId internalSndId,
                  senderTimestamp = internalTs,
                  previousMsgHash,
                  agentMessage = A_MSG msgBody
                }
          msgHash = C.sha256Hash msgStr
      withStore $ \st ->
        createSndMsg st connId $
          SndMsgData {internalId, internalSndId, internalTs, msgBody, internalHash = msgHash}
      sendAgentMessage c sq msgStr
      pure internalId

-- | Suspend SMP agent connection (OFF command) in Reader monad
suspendConnection' :: AgentMonad m => AgentClient -> ConnId -> m ()
suspendConnection' c connId =
  withStore (`getConn` connId) >>= \case
    SomeConn _ (DuplexConnection _ rq _) -> suspendQueue c rq
    SomeConn _ (RcvConnection _ rq) -> suspendQueue c rq
    _ -> throwError $ CONN SIMPLEX

-- | Delete SMP agent connection (DEL command) in Reader monad
deleteConnection' :: forall m. AgentMonad m => AgentClient -> ConnId -> m ()
deleteConnection' c connId =
  withStore (`getConn` connId) >>= \case
    SomeConn _ (DuplexConnection _ rq _) -> delete rq
    SomeConn _ (RcvConnection _ rq) -> delete rq
    _ -> withStore (`deleteConn` connId)
  where
    delete :: RcvQueue -> m ()
    delete rq = do
      deleteQueue c rq
      removeSubscription c connId
      withStore (`deleteConn` connId)

getSMPServer :: AgentMonad m => m SMPServer
getSMPServer =
  asks (smpServers . config) >>= \case
    srv :| [] -> pure srv
    servers -> do
      gen <- asks randomServer
      i <- atomically . stateTVar gen $ randomR (0, L.length servers - 1)
      pure $ servers L.!! i

sendControlMessage :: AgentMonad m => AgentClient -> SndQueue -> AMessage -> m ()
sendControlMessage c sq agentMessage = do
  senderTimestamp <- liftIO getCurrentTime
  sendAgentMessage c sq . serializeSMPMessage $
    SMPMessage
      { senderMsgId = 0,
        senderTimestamp,
        previousMsgHash = "",
        agentMessage
      }

subscriber :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
subscriber c@AgentClient {msgQ} = forever $ do
  t <- atomically $ readTBQueue msgQ
  runExceptT (processSMPTransmission c t) >>= \case
    Left e -> liftIO $ print e
    Right _ -> return ()

processSMPTransmission :: forall m. AgentMonad m => AgentClient -> SMPServerTransmission -> m ()
processSMPTransmission c@AgentClient {subQ} (srv, rId, cmd) = do
  withStore (\st -> getRcvConn st srv rId) >>= \case
    SomeConn SCDuplex (DuplexConnection cData rq _) -> processSMP SCDuplex cData rq
    SomeConn SCRcv (RcvConnection cData rq) -> processSMP SCRcv cData rq
    _ -> atomically $ writeTBQueue subQ ("", "", ERR $ CONN NOT_FOUND)
  where
    processSMP :: SConnType c -> ConnData -> RcvQueue -> m ()
    processSMP cType ConnData {connId} rq@RcvQueue {status} =
      case cmd of
        SMP.MSG srvMsgId srvTs msgBody -> do
          -- TODO deduplicate with previously received
          msg <- decryptAndVerify rq msgBody
          let msgHash = C.sha256Hash msg
          case parseSMPMessage msg of
            Left e -> notify $ ERR e
            Right (SMPConfirmation senderKey) -> smpConfirmation senderKey
            Right SMPMessage {agentMessage, senderMsgId, senderTimestamp, previousMsgHash} ->
              case agentMessage of
                HELLO verifyKey _ -> helloMsg verifyKey msgBody
                REPLY qInfo -> replyMsg qInfo
                A_MSG body -> agentClientMsg previousMsgHash (senderMsgId, senderTimestamp) (srvMsgId, srvTs) body msgHash
                A_INTRO introId cInfo -> introMsg introId cInfo
                A_INV introId qInfo cInfo -> invMsg introId qInfo cInfo
                A_REQ introId qInfo cInfo -> reqMsg introId qInfo cInfo
                A_CON introId -> conMsg introId
          sendAck c rq
          return ()
        SMP.END -> do
          removeSubscription c connId
          logServer "<--" c srv rId "END"
          notify END
        _ -> do
          logServer "<--" c srv rId $ "unexpected: " <> bshow cmd
          notify . ERR $ BROKER UNEXPECTED
      where
        notify :: ACommand 'Agent -> m ()
        notify msg = atomically $ writeTBQueue subQ ("", connId, msg)

        prohibited :: m ()
        prohibited = notify . ERR $ AGENT A_PROHIBITED

        smpConfirmation :: SenderPublicKey -> m ()
        smpConfirmation senderKey = do
          logServer "<--" c srv rId "MSG <KEY>"
          case status of
            New -> do
              -- TODO currently it automatically allows whoever sends the confirmation
              -- TODO create invitation and send REQ
              withStore $ \st -> setRcvQueueStatus st rq Confirmed
              -- TODO update sender key in the store?
              secureQueue c rq senderKey
              withStore $ \st -> setRcvQueueStatus st rq Secured
            _ -> prohibited

        helloMsg :: SenderPublicKey -> ByteString -> m ()
        helloMsg verifyKey msgBody = do
          logServer "<--" c srv rId "MSG <HELLO>"
          case status of
            Active -> prohibited
            _ -> do
              void $ verifyMessage (Just verifyKey) msgBody
              withStore $ \st -> setRcvQueueActive st rq verifyKey
              case cType of
                SCDuplex -> connected
                _ -> pure ()

        replyMsg :: SMPQueueInfo -> m ()
        replyMsg qInfo = do
          logServer "<--" c srv rId "MSG <REPLY>"
          case cType of
            SCRcv -> do
              (sq, senderKey, verifyKey) <- newSendQueue qInfo
              withStore $ \st -> upgradeRcvConnToDuplex st connId sq
              connectToSendQueue c sq senderKey verifyKey
              connected
            _ -> prohibited

        connected :: m ()
        connected = do
          withStore (`getConnInvitation` connId) >>= \case
            Just (Invitation {invId, externalIntroId}, DuplexConnection _ _ sq) -> do
              withStore $ \st -> setInvitationStatus st invId InvCon
              sendControlMessage c sq $ A_CON externalIntroId
            _ -> pure ()
          notify CON

        introMsg :: IntroId -> ConnInfo -> m ()
        introMsg introId reInfo = do
          logServer "<--" c srv rId "MSG <INTRO>"
          case cType of
            SCDuplex -> createInv introId Nothing reInfo
            _ -> prohibited

        invMsg :: IntroId -> SMPQueueInfo -> ConnInfo -> m ()
        invMsg introId qInfo toInfo = do
          logServer "<--" c srv rId "MSG <INV>"
          case cType of
            SCDuplex ->
              withStore (`getIntro` introId) >>= \case
                Introduction {toConn, toStatus = IntroNew, reConn, reStatus = IntroNew}
                  | toConn /= connId -> prohibited
                  | otherwise ->
                    withStore (\st -> addIntroInvitation st introId toInfo qInfo >> getConn st reConn) >>= \case
                      SomeConn _ (DuplexConnection _ _ sq) -> do
                        sendControlMessage c sq $ A_REQ introId qInfo toInfo
                        withStore $ \st -> setIntroReStatus st introId IntroInv
                      _ -> prohibited
                _ -> prohibited
            _ -> prohibited

        reqMsg :: IntroId -> SMPQueueInfo -> ConnInfo -> m ()
        reqMsg introId qInfo connInfo = do
          logServer "<--" c srv rId "MSG <REQ>"
          case cType of
            SCDuplex -> createInv introId (Just qInfo) connInfo
            _ -> prohibited

        createInv :: IntroId -> Maybe SMPQueueInfo -> ConnInfo -> m ()
        createInv externalIntroId qInfo connInfo = do
          g <- asks idsDrg
          let newInv = NewInvitation {viaConn = connId, externalIntroId, connInfo, qInfo}
          invId <- withStore $ \st -> createInvitation st g newInv
          notify $ REQ invId connInfo

        conMsg :: IntroId -> m ()
        conMsg introId = do
          logServer "<--" c srv rId "MSG <CON>"
          withStore (`getIntro` introId) >>= \case
            Introduction {toConn, toStatus, reConn, reStatus}
              | toConn == connId && toStatus == IntroInv -> do
                withStore $ \st -> setIntroToStatus st introId IntroCon
                when (reStatus == IntroCon) $ sendConMsg toConn reConn
              | reConn == connId && reStatus == IntroInv -> do
                withStore $ \st -> setIntroReStatus st introId IntroCon
                when (toStatus == IntroCon) $ sendConMsg toConn reConn
              | otherwise -> prohibited
          where
            sendConMsg :: ConnId -> ConnId -> m ()
            sendConMsg toConn reConn = atomically $ writeTBQueue subQ ("", toConn, ICON reConn)

        agentClientMsg :: PrevRcvMsgHash -> (ExternalSndId, ExternalSndTs) -> (BrokerId, BrokerTs) -> MsgBody -> MsgHash -> m ()
        agentClientMsg receivedPrevMsgHash senderMeta brokerMeta msgBody msgHash = do
          logServer "<--" c srv rId "MSG <MSG>"
          case status of
            Active -> do
              internalTs <- liftIO getCurrentTime
              (internalId, internalRcvId, prevExtSndId, prevRcvMsgHash) <- withStore (`updateRcvIds` connId)
              let msgIntegrity = checkMsgIntegrity prevExtSndId (fst senderMeta) prevRcvMsgHash receivedPrevMsgHash
              withStore $ \st ->
                createRcvMsg st connId $
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

connectToSendQueue :: AgentMonad m => AgentClient -> SndQueue -> SenderPublicKey -> VerificationKey -> m ()
connectToSendQueue c sq senderKey verifyKey = do
  sendConfirmation c sq senderKey
  withStore $ \st -> setSndQueueStatus st sq Confirmed
  sendHello c sq verifyKey
  withStore $ \st -> setSndQueueStatus st sq Active

newSendQueue ::
  (MonadUnliftIO m, MonadReader Env m) => SMPQueueInfo -> m (SndQueue, SenderPublicKey, VerificationKey)
newSendQueue (SMPQueueInfo smpServer senderId encryptKey) = do
  size <- asks $ rsaKeySize . config
  (senderKey, sndPrivateKey) <- liftIO $ C.generateKeyPair size
  (verifyKey, signKey) <- liftIO $ C.generateKeyPair size
  let sndQueue =
        SndQueue
          { server = smpServer,
            sndId = senderId,
            sndPrivateKey,
            encryptKey,
            signKey,
            status = New
          }
  return (sndQueue, senderKey, verifyKey)
