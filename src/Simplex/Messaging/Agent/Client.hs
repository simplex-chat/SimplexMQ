{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Agent.Client
  ( AgentClient (..),
    newAgentClient,
    AgentMonad,
    getSMPServerClient,
    closeSMPServerClients,
    newReceiveQueue,
    subscribeQueue,
    sendConfirmation,
    sendHello,
    secureQueue,
    sendAgentMessage,
    decryptAndVerify,
    verifyMessage,
    sendAck,
    suspendQueue,
    deleteQueue,
    logServer,
    removeSubscription,
    cryptoError,
  )
where

import Control.Logger.Simple
import Control.Monad.Except
import Control.Monad.IO.Unlift
import Control.Monad.Reader
import Control.Monad.Trans.Except
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text.Encoding
import Data.Time.Clock
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Transmission
import Simplex.Messaging.Client
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (ErrorType (AUTH), MsgBody, QueueId, SenderPublicKey)
import Simplex.Messaging.Util (bshow, liftEitherError, liftError)
import UnliftIO.Concurrent
import UnliftIO.Exception (IOException)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

data AgentClient = AgentClient
  { rcvQ :: TBQueue (ATransmission 'Client),
    sndQ :: TBQueue (ATransmission 'Agent),
    msgQ :: TBQueue SMPServerTransmission,
    smpClients :: TVar (Map SMPServer SMPClient),
    subscrSrvrs :: TVar (Map SMPServer (Set ConnAlias)),
    subscrConns :: TVar (Map ConnAlias SMPServer),
    clientId :: Int
  }

newAgentClient :: TVar Int -> AgentConfig -> STM AgentClient
newAgentClient cc AgentConfig {tbqSize} = do
  rcvQ <- newTBQueue tbqSize
  sndQ <- newTBQueue tbqSize
  msgQ <- newTBQueue tbqSize
  smpClients <- newTVar M.empty
  subscrSrvrs <- newTVar M.empty
  subscrConns <- newTVar M.empty
  clientId <- (+ 1) <$> readTVar cc
  writeTVar cc clientId
  return AgentClient {rcvQ, sndQ, msgQ, smpClients, subscrSrvrs, subscrConns, clientId}

type AgentMonad m = (MonadUnliftIO m, MonadReader Env m, MonadError AgentErrorType m)

getSMPServerClient :: forall m. AgentMonad m => AgentClient -> SMPServer -> m SMPClient
getSMPServerClient c@AgentClient {smpClients, msgQ} srv =
  readTVarIO smpClients
    >>= maybe newSMPClient return . M.lookup srv
  where
    newSMPClient :: m SMPClient
    newSMPClient = do
      smp <- connectClient
      logInfo . decodeUtf8 $ "Agent connected to " <> showServer srv
      atomically . modifyTVar smpClients $ M.insert srv smp
      return smp

    connectClient :: m SMPClient
    connectClient = do
      cfg <- asks $ smpCfg . config
      liftEitherError smpClientError (getSMPClient srv cfg msgQ clientDisconnected)
        `E.catch` internalError
      where
        internalError :: IOException -> m SMPClient
        internalError = throwError . INTERNAL . show

    clientDisconnected :: IO ()
    clientDisconnected = do
      removeSubs >>= mapM_ (mapM_ notifySub)
      logInfo . decodeUtf8 $ "Agent disconnected from " <> showServer srv

    removeSubs :: IO (Maybe (Set ConnAlias))
    removeSubs = atomically $ do
      modifyTVar smpClients $ M.delete srv
      cs <- M.lookup srv <$> readTVar (subscrSrvrs c)
      modifyTVar (subscrSrvrs c) $ M.delete srv
      modifyTVar (subscrConns c) $ maybe id deleteKeys cs
      return cs
      where
        deleteKeys :: Ord k => Set k -> Map k a -> Map k a
        deleteKeys ks m = S.foldr' M.delete m ks

    notifySub :: ConnAlias -> IO ()
    notifySub connAlias = atomically $ writeTBQueue (sndQ c) ("", connAlias, END)

closeSMPServerClients :: MonadUnliftIO m => AgentClient -> m ()
closeSMPServerClients c = liftIO $ readTVarIO (smpClients c) >>= mapM_ closeSMPClient

withSMP_ :: forall a m. AgentMonad m => AgentClient -> SMPServer -> (SMPClient -> m a) -> m a
withSMP_ c srv action =
  (getSMPServerClient c srv >>= action) `catchError` logServerError
  where
    logServerError :: AgentErrorType -> m a
    logServerError e = do
      logServer "<--" c srv "" $ bshow e
      throwError e

withLogSMP_ :: AgentMonad m => AgentClient -> SMPServer -> QueueId -> ByteString -> (SMPClient -> m a) -> m a
withLogSMP_ c srv qId cmdStr action = do
  logServer "-->" c srv qId cmdStr
  res <- withSMP_ c srv action
  logServer "<--" c srv qId "OK"
  return res

withSMP :: AgentMonad m => AgentClient -> SMPServer -> (SMPClient -> ExceptT SMPClientError IO a) -> m a
withSMP c srv action = withSMP_ c srv $ liftSMP . action

withLogSMP :: AgentMonad m => AgentClient -> SMPServer -> QueueId -> ByteString -> (SMPClient -> ExceptT SMPClientError IO a) -> m a
withLogSMP c srv qId cmdStr action = withLogSMP_ c srv qId cmdStr $ liftSMP . action

liftSMP :: AgentMonad m => ExceptT SMPClientError IO a -> m a
liftSMP = liftError smpClientError

smpClientError :: SMPClientError -> AgentErrorType
smpClientError = \case
  SMPServerError e -> SMP e
  SMPResponseError e -> BROKER $ RESPONSE e
  SMPUnexpectedResponse -> BROKER UNEXPECTED
  SMPResponseTimeout -> BROKER TIMEOUT
  SMPNetworkError -> BROKER NETWORK
  SMPTransportError e -> BROKER $ TRANSPORT e
  e -> INTERNAL $ show e

newReceiveQueue :: AgentMonad m => AgentClient -> SMPServer -> ConnAlias -> m (RcvQueue, SMPQueueInfo)
newReceiveQueue c srv connAlias = do
  size <- asks $ rsaKeySize . config
  (recipientKey, rcvPrivateKey) <- liftIO $ C.generateKeyPair size
  logServer "-->" c srv "" "NEW"
  (rcvId, sId) <- withSMP c srv $ \smp -> createSMPQueue smp rcvPrivateKey recipientKey
  logServer "<--" c srv "" $ B.unwords ["IDS", logSecret rcvId, logSecret sId]
  (encryptKey, decryptKey) <- liftIO $ C.generateKeyPair size
  let rq =
        RcvQueue
          { server = srv,
            rcvId,
            connAlias,
            rcvPrivateKey,
            sndId = Just sId,
            sndKey = Nothing,
            decryptKey,
            verifyKey = Nothing,
            status = New
          }
  addSubscription c rq connAlias
  return (rq, SMPQueueInfo srv sId encryptKey)

subscribeQueue :: AgentMonad m => AgentClient -> RcvQueue -> ConnAlias -> m ()
subscribeQueue c rq@RcvQueue {server, rcvPrivateKey, rcvId} connAlias = do
  withLogSMP c server rcvId "SUB" $ \smp ->
    subscribeSMPQueue smp rcvPrivateKey rcvId
  addSubscription c rq connAlias

addSubscription :: MonadUnliftIO m => AgentClient -> RcvQueue -> ConnAlias -> m ()
addSubscription c RcvQueue {server} connAlias = atomically $ do
  modifyTVar (subscrConns c) $ M.insert connAlias server
  modifyTVar (subscrSrvrs c) $ M.alter (Just . addSub) server
  where
    addSub :: Maybe (Set ConnAlias) -> Set ConnAlias
    addSub (Just cs) = S.insert connAlias cs
    addSub _ = S.singleton connAlias

removeSubscription :: AgentMonad m => AgentClient -> ConnAlias -> m ()
removeSubscription AgentClient {subscrConns, subscrSrvrs} connAlias = atomically $ do
  cs <- readTVar subscrConns
  writeTVar subscrConns $ M.delete connAlias cs
  mapM_
    (modifyTVar subscrSrvrs . M.alter (>>= delSub))
    (M.lookup connAlias cs)
  where
    delSub :: Set ConnAlias -> Maybe (Set ConnAlias)
    delSub cs =
      let cs' = S.delete connAlias cs
       in if S.null cs' then Nothing else Just cs'

logServer :: AgentMonad m => ByteString -> AgentClient -> SMPServer -> QueueId -> ByteString -> m ()
logServer dir AgentClient {clientId} srv qId cmdStr =
  logInfo . decodeUtf8 $ B.unwords ["A", "(" <> bshow clientId <> ")", dir, showServer srv, ":", logSecret qId, cmdStr]

showServer :: SMPServer -> ByteString
showServer srv = B.pack $ host srv <> maybe "" (":" <>) (port srv)

logSecret :: ByteString -> ByteString
logSecret bs = encode $ B.take 3 bs

sendConfirmation :: forall m. AgentMonad m => AgentClient -> SndQueue -> SenderPublicKey -> m ()
sendConfirmation c sq@SndQueue {server, sndId} senderKey =
  withLogSMP_ c server sndId "SEND <KEY>" $ \smp -> do
    msg <- mkConfirmation smp
    liftSMP $ sendSMPMessage smp Nothing sndId msg
  where
    mkConfirmation :: SMPClient -> m MsgBody
    mkConfirmation smp = encryptAndSign smp sq $ SMPConfirmation senderKey

sendHello :: forall m. AgentMonad m => AgentClient -> SndQueue -> VerificationKey -> m ()
sendHello c sq@SndQueue {server, sndId, sndPrivateKey} verifyKey =
  withLogSMP_ c server sndId "SEND <HELLO> (retrying)" $ \smp -> do
    msg <- mkHello smp $ AckMode On
    liftSMP $ send 8 100000 msg smp
  where
    mkHello :: SMPClient -> AckMode -> m ByteString
    mkHello smp ackMode = do
      senderTs <- liftIO getCurrentTime
      mkAgentMessage smp sq senderTs $ HELLO verifyKey ackMode

    send :: Int -> Int -> ByteString -> SMPClient -> ExceptT SMPClientError IO ()
    send 0 _ _ _ = throwE $ SMPServerError AUTH
    send retry delay msg smp =
      sendSMPMessage smp (Just sndPrivateKey) sndId msg `catchE` \case
        SMPServerError AUTH -> do
          threadDelay delay
          send (retry - 1) (delay * 3 `div` 2) msg smp
        e -> throwE e

secureQueue :: AgentMonad m => AgentClient -> RcvQueue -> SenderPublicKey -> m ()
secureQueue c RcvQueue {server, rcvId, rcvPrivateKey} senderKey =
  withLogSMP c server rcvId "KEY <key>" $ \smp ->
    secureSMPQueue smp rcvPrivateKey rcvId senderKey

sendAck :: AgentMonad m => AgentClient -> RcvQueue -> m ()
sendAck c RcvQueue {server, rcvId, rcvPrivateKey} =
  withLogSMP c server rcvId "ACK" $ \smp ->
    ackSMPMessage smp rcvPrivateKey rcvId

suspendQueue :: AgentMonad m => AgentClient -> RcvQueue -> m ()
suspendQueue c RcvQueue {server, rcvId, rcvPrivateKey} =
  withLogSMP c server rcvId "OFF" $ \smp ->
    suspendSMPQueue smp rcvPrivateKey rcvId

deleteQueue :: AgentMonad m => AgentClient -> RcvQueue -> m ()
deleteQueue c RcvQueue {server, rcvId, rcvPrivateKey} =
  withLogSMP c server rcvId "DEL" $ \smp ->
    deleteSMPQueue smp rcvPrivateKey rcvId

sendAgentMessage :: AgentMonad m => AgentClient -> SndQueue -> SenderTimestamp -> AMessage -> m ()
sendAgentMessage c sq@SndQueue {server, sndId, sndPrivateKey} senderTs agentMsg =
  withLogSMP_ c server sndId "SEND <message>" $ \smp -> do
    msg <- mkAgentMessage smp sq senderTs agentMsg
    liftSMP $ sendSMPMessage smp (Just sndPrivateKey) sndId msg

mkAgentMessage :: AgentMonad m => SMPClient -> SndQueue -> SenderTimestamp -> AMessage -> m ByteString
mkAgentMessage smp sq senderTs agentMessage = do
  encryptAndSign smp sq $
    SMPMessage
      { senderMsgId = 0,
        senderTimestamp = senderTs,
        previousMsgHash = "1234", -- TODO hash of the previous message
        agentMessage
      }

encryptAndSign :: AgentMonad m => SMPClient -> SndQueue -> SMPMessage -> m ByteString
encryptAndSign smp SndQueue {encryptKey, signKey} msg = do
  paddedSize <- asks $ (blockSize smp -) . reservedMsgSize
  liftError cryptoError $ do
    enc <- C.encrypt encryptKey paddedSize $ serializeSMPMessage msg
    C.Signature sig <- C.sign signKey enc
    pure $ sig <> enc

decryptAndVerify :: AgentMonad m => RcvQueue -> ByteString -> m ByteString
decryptAndVerify RcvQueue {decryptKey, verifyKey} msg =
  verifyMessage verifyKey msg
    >>= liftError cryptoError . C.decrypt decryptKey

verifyMessage :: AgentMonad m => Maybe VerificationKey -> ByteString -> m ByteString
verifyMessage verifyKey msg = do
  size <- asks $ rsaKeySize . config
  let (sig, enc) = B.splitAt size msg
  case verifyKey of
    Nothing -> pure enc
    Just k
      | C.verify k (C.Signature sig) enc -> pure enc
      | otherwise -> throwError $ AGENT A_SIGNATURE

cryptoError :: C.CryptoError -> AgentErrorType
cryptoError = \case
  C.CryptoLargeMsgError -> CMD LARGE
  C.RSADecryptError _ -> AGENT A_ENCRYPTION
  C.CryptoHeaderError _ -> AGENT A_ENCRYPTION
  C.AESDecryptError -> AGENT A_ENCRYPTION
  e -> INTERNAL $ show e
