{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Simplex.Messaging.Agent.Client
  ( AgentClient (..),
    ProtocolTestFailure (..),
    ProtocolTestStep (..),
    newAgentClient,
    withConnLock,
    withConnLocks,
    withInvLock,
    closeAgentClient,
    closeProtocolServerClients,
    closeXFTPServerClient,
    runSMPServerTest,
    runXFTPServerTest,
    getXFTPWorkPath,
    newRcvQueue,
    subscribeQueues,
    getQueueMessage,
    decryptSMPMessage,
    addSubscription,
    getSubscriptions,
    sendConfirmation,
    sendInvitation,
    temporaryAgentError,
    temporaryOrHostError,
    secureQueue,
    enableQueueNotifications,
    enableQueuesNtfs,
    disableQueueNotifications,
    disableQueuesNtfs,
    sendAgentMessage,
    agentNtfRegisterToken,
    agentNtfVerifyToken,
    agentNtfCheckToken,
    agentNtfReplaceToken,
    agentNtfDeleteToken,
    agentNtfEnableCron,
    agentNtfCreateSubscription,
    agentNtfCheckSubscription,
    agentNtfDeleteSubscription,
    agentXFTPDownloadChunk,
    agentXFTPNewChunk,
    agentXFTPUploadChunk,
    agentXFTPAddRecipients,
    agentXFTPDeleteChunk,
    agentCbEncrypt,
    agentCbDecrypt,
    cryptoError,
    sendAck,
    suspendQueue,
    deleteQueue,
    deleteQueues,
    logServer,
    logSecret,
    removeSubscription,
    hasActiveSubscription,
    hasGetLock,
    agentClientStore,
    agentDRG,
    getAgentSubscriptions,
    Worker (..),
    SessionVar (..),
    SubscriptionsInfo (..),
    SubInfo (..),
    AgentOperation (..),
    AgentOpState (..),
    AgentState (..),
    AgentLocks (..),
    AgentStatsKey (..),
    mkSMPTransportSession,
    getAgentWorker,
    getAgentWorker',
    cancelWorker,
    waitForWork,
    hasWorkToDo,
    hasWorkToDo',
    withWork,
    agentOperations,
    agentOperationBracket,
    waitUntilActive,
    throwWhenInactive,
    throwWhenNoDelivery,
    beginAgentOperation,
    endAgentOperation,
    waitUntilForeground,
    suspendSendingAndDatabase,
    suspendOperation,
    notifySuspended,
    whenSuspending,
    withStore,
    withStore',
    withStoreCtx,
    withStoreCtx',
    withStoreBatch,
    withStoreBatch',
    storeError,
    userServers,
    pickServer,
    getNextServer,
    withUserServers,
    withNextSrv,
    AgentWorkersDetails (..),
    getAgentWorkersDetails,
    AgentWorkersSummary (..),
    getAgentWorkersSummary,
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async (Async, uninterruptibleCancel)
import Control.Concurrent.STM (retry, throwSTM)
import Control.Exception (AsyncException (..))
import Control.Logger.Simple
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Unlift
import Control.Monad.Reader
import Crypto.Random (ChaChaDRG)
import qualified Data.Aeson.TH as J
import Data.Bifunctor (bimap, first, second)
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Either (lefts, partitionEithers)
import Data.Functor (($>))
import Data.List (deleteFirstsBy, foldl', partition, (\\))
import Data.List.NonEmpty (NonEmpty (..), (<|))
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (isNothing, listToMaybe)
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import Data.Text.Encoding
import Data.Time (UTCTime, defaultTimeLocale, formatTime, getCurrentTime)
import Data.Time.Clock.System (getSystemTime)
import Data.Word (Word16)
import Network.Socket (HostName)
import Simplex.FileTransfer.Client (XFTPChunkSpec (..), XFTPClient, XFTPClientConfig (..), XFTPClientError)
import qualified Simplex.FileTransfer.Client as X
import Simplex.FileTransfer.Description (ChunkReplicaId (..), FileDigest (..), kb)
import Simplex.FileTransfer.Protocol (FileInfo (..), FileResponse, XFTPErrorType (DIGEST))
import Simplex.FileTransfer.Transport (XFTPRcvChunkSpec (..))
import Simplex.FileTransfer.Types (DeletedSndChunkReplica (..), NewSndChunkReplica (..), RcvFileChunkReplica (..), SndFileChunk (..), SndFileChunkReplica (..))
import Simplex.FileTransfer.Util (uniqueCombine)
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Lock
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite (SQLiteStore (..), withTransaction)
import qualified Simplex.Messaging.Agent.Store.SQLite.DB as DB
import Simplex.Messaging.Agent.TAsyncs
import Simplex.Messaging.Agent.TRcvQueues (TRcvQueues (getRcvQueues))
import qualified Simplex.Messaging.Agent.TRcvQueues as RQ
import Simplex.Messaging.Client
import Simplex.Messaging.Client.Agent ()
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Notifications.Client
import Simplex.Messaging.Notifications.Protocol
import Simplex.Messaging.Notifications.Types
import Simplex.Messaging.Parsers (defaultJSON, dropPrefix, enumJSON, parse)
import Simplex.Messaging.Protocol
  ( AProtocolType (..),
    BrokerMsg,
    EntityId,
    ErrorType,
    MsgFlags (..),
    MsgId,
    NtfServer,
    ProtoServer,
    ProtoServerWithAuth (..),
    Protocol (..),
    ProtocolServer (..),
    ProtocolTypeI (..),
    QueueId,
    QueueIdsKeys (..),
    RcvMessage (..),
    RcvNtfPublicDhKey,
    SMPMsgMeta (..),
    SProtocolType (..),
    SndPublicVerifyKey,
    SubscriptionMode (..),
    UserProtocol,
    XFTPServer,
    XFTPServerWithAuth,
    sameSrvAddr',
  )
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport.Client (TransportHost)
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import System.Random (randomR)
import UnliftIO (mapConcurrently, timeout)
import UnliftIO.Async (async)
import UnliftIO.Directory (getTemporaryDirectory)
import UnliftIO.Exception (bracket)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

data SessionVar a = SessionVar
  { sessionVar :: TMVar a,
    sessionVarId :: Int
  }

type ClientVar msg = SessionVar (Either AgentErrorType (Client msg))

type SMPClientVar = ClientVar SMP.BrokerMsg

type NtfClientVar = ClientVar NtfResponse

type XFTPClientVar = ClientVar FileResponse

type NtfTransportSession = TransportSession NtfResponse

type XFTPTransportSession = TransportSession FileResponse

data AgentClient = AgentClient
  { active :: TVar Bool,
    rcvQ :: TBQueue (ATransmission 'Client),
    subQ :: TBQueue (ATransmission 'Agent),
    msgQ :: TBQueue (ServerTransmission BrokerMsg),
    smpServers :: TMap UserId (NonEmpty SMPServerWithAuth),
    smpClients :: TMap SMPTransportSession SMPClientVar,
    ntfServers :: TVar [NtfServer],
    ntfClients :: TMap NtfTransportSession NtfClientVar,
    xftpServers :: TMap UserId (NonEmpty XFTPServerWithAuth),
    xftpClients :: TMap XFTPTransportSession XFTPClientVar,
    useNetworkConfig :: TVar NetworkConfig,
    subscrConns :: TVar (Set ConnId),
    activeSubs :: TRcvQueues,
    pendingSubs :: TRcvQueues,
    removedSubs :: TMap (UserId, SMPServer, SMP.RecipientId) SMPClientError,
    workerSeq :: TVar Int,
    smpDeliveryWorkers :: TMap SMPTransportSession Worker,
    asyncCmdWorkers :: TMap (Maybe SMPServer) Worker,
    connCmdsQueued :: TMap ConnId Bool,
    ntfNetworkOp :: TVar AgentOpState,
    rcvNetworkOp :: TVar AgentOpState,
    msgDeliveryOp :: TVar AgentOpState,
    sndNetworkOp :: TVar AgentOpState,
    databaseOp :: TVar AgentOpState,
    agentState :: TVar AgentState,
    getMsgLocks :: TMap (SMPServer, SMP.RecipientId) (TMVar ()),
    -- locks to prevent concurrent operations with connection
    connLocks :: TMap ConnId Lock,
    -- locks to prevent concurrent operations with connection request invitations
    invLocks :: TMap ByteString Lock,
    -- lock to prevent concurrency between periodic and async connection deletions
    deleteLock :: Lock,
    -- smpSubWorkers for SMP servers sessions
    smpSubWorkers :: TMap SMPTransportSession (SessionVar (Async ())),
    asyncClients :: TAsyncs,
    agentStats :: TMap AgentStatsKey (TVar Int),
    clientId :: Int,
    agentEnv :: Env
  }

getAgentWorker :: (AgentMonad' m, Ord k, Show k) => String -> Bool -> AgentClient -> k -> TMap k Worker -> (Worker -> ExceptT AgentErrorType m ()) -> m Worker
getAgentWorker = getAgentWorker' id pure

getAgentWorker' :: forall a k m. (AgentMonad' m, Ord k, Show k) => (a -> Worker) -> (Worker -> STM a) -> String -> Bool -> AgentClient -> k -> TMap k a -> (a -> ExceptT AgentErrorType m ()) -> m a
getAgentWorker' toW fromW name hasWork c key ws work = do
  atomically (getWorker >>= maybe createWorker whenExists) >>= \w -> runWorker w $> w
  where
    getWorker = TM.lookup key ws
    createWorker = do
      w <- fromW =<< newWorker c
      TM.insert key w ws
      pure w
    whenExists w
      | hasWork = hasWorkToDo (toW w) $> w
      | otherwise = pure w
    runWorker w = runWorkerAsync (toW w) runWork
      where
        runWork :: m ()
        runWork = tryAgentError' (work w) >>= restartOrDelete
        restartOrDelete :: Either AgentErrorType () -> m ()
        restartOrDelete e_ = do
          t <- liftIO getSystemTime
          maxRestarts <- asks $ maxWorkerRestartsPerMin . config
          -- worker may terminate because it was deleted from the map (getWorker returns Nothing), then it won't restart
          restart <- atomically $ getWorker >>= maybe (pure False) (shouldRestart e_ (toW w) t maxRestarts)
          when restart runWork
        shouldRestart e_ Worker {workerId = wId, doWork, action, restarts} t maxRestarts w'
          | wId == workerId (toW w') =
              checkRestarts . updateRestartCount t =<< readTVar restarts
          | otherwise =
              pure False -- there is a new worker in the map, no action
          where
            checkRestarts rc
              | restartCount rc < maxRestarts = do
                  writeTVar restarts rc
                  hasWorkToDo' doWork
                  void $ tryPutTMVar action Nothing
                  notifyErr INTERNAL
                  pure True
              | otherwise = do
                  TM.delete key ws
                  notifyErr $ CRITICAL True
                  pure False
              where
                notifyErr err = do
                  let e = either ((", error: " <>) . show) (\_ -> ", no error") e_
                      msg = "Worker " <> name <> " for " <> show key <> " terminated " <> show (restartCount rc) <> " times" <> e
                  writeTBQueue (subQ c) ("", "", APC SAEConn $ ERR $ err msg)

newWorker :: AgentClient -> STM Worker
newWorker c = do
  workerId <- stateTVar (workerSeq c) $ \next -> (next, next + 1)
  doWork <- newTMVar ()
  action <- newTMVar Nothing
  restarts <- newTVar $ RestartCount 0 0
  pure Worker {workerId, doWork, action, restarts}

runWorkerAsync :: AgentMonad' m => Worker -> m () -> m ()
runWorkerAsync Worker {action} work =
  bracket
    (atomically $ takeTMVar action) -- get current action, locking to avoid race conditions
    (atomically . tryPutTMVar action) -- if it was running (or if start crashes), put it back and unlock (don't lock if it was just started)
    (\a -> when (isNothing a) start) -- start worker if it's not running
  where
    start = atomically . putTMVar action . Just =<< async work

data AgentOperation = AONtfNetwork | AORcvNetwork | AOMsgDelivery | AOSndNetwork | AODatabase
  deriving (Eq, Show)

agentOpSel :: AgentOperation -> (AgentClient -> TVar AgentOpState)
agentOpSel = \case
  AONtfNetwork -> ntfNetworkOp
  AORcvNetwork -> rcvNetworkOp
  AOMsgDelivery -> msgDeliveryOp
  AOSndNetwork -> sndNetworkOp
  AODatabase -> databaseOp

agentOperations :: [AgentClient -> TVar AgentOpState]
agentOperations = [ntfNetworkOp, rcvNetworkOp, msgDeliveryOp, sndNetworkOp, databaseOp]

data AgentOpState = AgentOpState {opSuspended :: !Bool, opsInProgress :: !Int}

data AgentState = ASForeground | ASSuspending | ASSuspended
  deriving (Eq, Show)

data AgentLocks = AgentLocks
  { connLocks :: Map String String,
    invLocks :: Map String String,
    delLock :: Maybe String
  }
  deriving (Show)

data AgentStatsKey = AgentStatsKey
  { userId :: UserId,
    host :: ByteString,
    clientTs :: ByteString,
    cmd :: ByteString,
    res :: ByteString
  }
  deriving (Eq, Ord, Show)

newAgentClient :: Int -> InitialAgentServers -> Env -> STM AgentClient
newAgentClient clientId InitialAgentServers {smp, ntf, xftp, netCfg} agentEnv = do
  let qSize = tbqSize $ config agentEnv
  active <- newTVar True
  rcvQ <- newTBQueue qSize
  subQ <- newTBQueue qSize
  msgQ <- newTBQueue qSize
  smpServers <- newTVar smp
  smpClients <- TM.empty
  ntfServers <- newTVar ntf
  ntfClients <- TM.empty
  xftpServers <- newTVar xftp
  xftpClients <- TM.empty
  useNetworkConfig <- newTVar netCfg
  subscrConns <- newTVar S.empty
  activeSubs <- RQ.empty
  pendingSubs <- RQ.empty
  removedSubs <- TM.empty
  workerSeq <- newTVar 0
  smpDeliveryWorkers <- TM.empty
  asyncCmdWorkers <- TM.empty
  connCmdsQueued <- TM.empty
  ntfNetworkOp <- newTVar $ AgentOpState False 0
  rcvNetworkOp <- newTVar $ AgentOpState False 0
  msgDeliveryOp <- newTVar $ AgentOpState False 0
  sndNetworkOp <- newTVar $ AgentOpState False 0
  databaseOp <- newTVar $ AgentOpState False 0
  agentState <- newTVar ASForeground
  getMsgLocks <- TM.empty
  connLocks <- TM.empty
  invLocks <- TM.empty
  deleteLock <- createLock
  smpSubWorkers <- TM.empty
  asyncClients <- newTAsyncs
  agentStats <- TM.empty
  return
    AgentClient
      { active,
        rcvQ,
        subQ,
        msgQ,
        smpServers,
        smpClients,
        ntfServers,
        ntfClients,
        xftpServers,
        xftpClients,
        useNetworkConfig,
        subscrConns,
        activeSubs,
        pendingSubs,
        removedSubs,
        workerSeq,
        smpDeliveryWorkers,
        asyncCmdWorkers,
        connCmdsQueued,
        ntfNetworkOp,
        rcvNetworkOp,
        msgDeliveryOp,
        sndNetworkOp,
        databaseOp,
        agentState,
        getMsgLocks,
        connLocks,
        invLocks,
        deleteLock,
        smpSubWorkers,
        asyncClients,
        agentStats,
        clientId,
        agentEnv
      }

agentClientStore :: AgentClient -> SQLiteStore
agentClientStore AgentClient {agentEnv = Env {store}} = store

agentDRG :: AgentClient -> TVar ChaChaDRG
agentDRG AgentClient {agentEnv = Env {random}} = random

class (Encoding err, Show err) => ProtocolServerClient err msg | msg -> err where
  type Client msg = c | c -> msg
  getProtocolServerClient :: AgentMonad m => AgentClient -> TransportSession msg -> m (Client msg)
  clientProtocolError :: err -> AgentErrorType
  closeProtocolServerClient :: Client msg -> IO ()
  clientServer :: Client msg -> String
  clientTransportHost :: Client msg -> TransportHost
  clientSessionTs :: Client msg -> UTCTime

instance ProtocolServerClient ErrorType BrokerMsg where
  type Client BrokerMsg = ProtocolClient ErrorType BrokerMsg
  getProtocolServerClient = getSMPServerClient
  clientProtocolError = SMP
  closeProtocolServerClient = closeProtocolClient
  clientServer = protocolClientServer
  clientTransportHost = transportHost'
  clientSessionTs = sessionTs

instance ProtocolServerClient ErrorType NtfResponse where
  type Client NtfResponse = ProtocolClient ErrorType NtfResponse
  getProtocolServerClient = getNtfServerClient
  clientProtocolError = NTF
  closeProtocolServerClient = closeProtocolClient
  clientServer = protocolClientServer
  clientTransportHost = transportHost'
  clientSessionTs = sessionTs

instance ProtocolServerClient XFTPErrorType FileResponse where
  type Client FileResponse = XFTPClient
  getProtocolServerClient = getXFTPServerClient
  clientProtocolError = XFTP
  closeProtocolServerClient = X.closeXFTPClient
  clientServer = X.xftpClientServer
  clientTransportHost = X.xftpTransportHost
  clientSessionTs = X.xftpSessionTs

getSMPServerClient :: forall m. AgentMonad m => AgentClient -> SMPTransportSession -> m SMPClient
getSMPServerClient c@AgentClient {active, smpClients, msgQ} tSess@(userId, srv, _) = do
  unlessM (readTVarIO active) . throwError $ INACTIVE
  atomically (getTSessVar c tSess smpClients)
    >>= either newClient (waitForProtocolClient c tSess)
  where
    newClient = newProtocolClient c tSess smpClients connectClient resubscribeSMPSession
    connectClient :: SMPClientVar -> m SMPClient
    connectClient v = do
      cfg <- getClientConfig c smpCfg
      u <- askUnliftIO
      liftEitherError (protocolClientError SMP $ B.unpack $ strEncode srv) (getProtocolClient tSess cfg (Just msgQ) $ clientDisconnected u v)

    clientDisconnected :: UnliftIO m -> SMPClientVar -> SMPClient -> IO ()
    clientDisconnected u v client = do
      removeClientAndSubs >>= serverDown
      logInfo . decodeUtf8 $ "Agent disconnected from " <> showServer srv
      where
        removeClientAndSubs :: IO ([RcvQueue], [ConnId])
        removeClientAndSubs = atomically $ do
          removeTSessVar v tSess smpClients
          qs <- RQ.getDelSessQueues tSess $ activeSubs c
          mapM_ (`RQ.addQueue` pendingSubs c) qs
          let cs = S.fromList $ map qConnId qs
          cs' <- RQ.getConns $ activeSubs c
          pure (qs, S.toList $ cs `S.difference` cs')

        serverDown :: ([RcvQueue], [ConnId]) -> IO ()
        serverDown (qs, conns) = whenM (readTVarIO active) $ do
          incClientStat c userId client "DISCONNECT" ""
          notifySub "" $ hostEvent DISCONNECT client
          unless (null conns) $ notifySub "" $ DOWN srv conns
          unless (null qs) $ do
            atomically $ mapM_ (releaseGetLock c) qs
            unliftIO u $ resubscribeSMPSession c tSess

        notifySub :: forall e. AEntityI e => ConnId -> ACommand 'Agent e -> IO ()
        notifySub connId cmd = atomically $ writeTBQueue (subQ c) ("", connId, APC (sAEntity @e) cmd)

resubscribeSMPSession :: AgentMonad' m => AgentClient -> SMPTransportSession -> m ()
resubscribeSMPSession c@AgentClient {smpSubWorkers} tSess =
  atomically (getTSessVar c tSess smpSubWorkers) >>= either newSubWorker (\_ -> pure ())
  where
    newSubWorker v = do
      a <- async $ void (E.tryAny runSubWorker) >> atomically (cleanup v)
      atomically $ putTMVar (sessionVar v) a
    runSubWorker = do
      ri <- asks $ reconnectInterval . config
      timeoutCounts <- newTVarIO 0
      withRetryInterval ri $ \_ loop -> do
        pending <- atomically . RQ.getSessQueues tSess $ pendingSubs c
        forM_ (L.nonEmpty pending) $ \qs -> do
          void . tryAgentError' $ reconnectSMPClient timeoutCounts c tSess qs
          loop
    cleanup :: SessionVar (Async ()) -> STM ()
    cleanup v = do
      -- Here we wait until TMVar is not empty to prevent worker cleanup happening before worker is added to TMVar.
      -- Not waiting may result in terminated worker remaining in the map.
      whenM (isEmptyTMVar $ sessionVar v) retry
      removeTSessVar v tSess smpSubWorkers

reconnectSMPClient :: forall m. AgentMonad m => TVar Int -> AgentClient -> SMPTransportSession -> NonEmpty RcvQueue -> m ()
reconnectSMPClient tc c tSess@(_, srv, _) qs = do
  NetworkConfig {tcpTimeout} <- readTVarIO $ useNetworkConfig c
  -- this allows 3x of timeout per batch of subscription (90 queues per batch empirically)
  let t = (length qs `div` 90 + 1) * tcpTimeout * 3
  t `timeout` resubscribe >>= \case
    Just _ -> atomically $ writeTVar tc 0
    Nothing -> do
      tc' <- atomically $ stateTVar tc $ \i -> (i + 1, i + 1)
      maxTC <- asks $ maxSubscriptionTimeouts . config
      let err = if tc' >= maxTC then CRITICAL True else INTERNAL
          msg = show tc' <> " consecutive subscription timeouts: " <> show (length qs) <> " queues, transport session: " <> show tSess
      atomically $ writeTBQueue (subQ c) ("", "", APC SAEConn $ ERR $ err msg)
  where
    resubscribe :: m ()
    resubscribe = do
      cs <- atomically . RQ.getConns $ activeSubs c
      rs <- subscribeQueues c $ L.toList qs
      let (errs, okConns) = partitionEithers $ map (\(RcvQueue {connId}, r) -> bimap (connId,) (const connId) r) rs
      liftIO $ do
        let conns = S.toList $ S.fromList okConns `S.difference` cs
        unless (null conns) $ notifySub "" $ UP srv conns
      let (tempErrs, finalErrs) = partition (temporaryAgentError . snd) errs
      liftIO $ mapM_ (\(connId, e) -> notifySub connId $ ERR e) finalErrs
      forM_ (listToMaybe tempErrs) $ \(_, err) -> do
        when (null okConns && S.null cs && null finalErrs) . liftIO $
          closeClient c smpClients tSess
        throwError err
    notifySub :: forall e. AEntityI e => ConnId -> ACommand 'Agent e -> IO ()
    notifySub connId cmd = atomically $ writeTBQueue (subQ c) ("", connId, APC (sAEntity @e) cmd)

getNtfServerClient :: forall m. AgentMonad m => AgentClient -> NtfTransportSession -> m NtfClient
getNtfServerClient c@AgentClient {active, ntfClients} tSess@(userId, srv, _) = do
  unlessM (readTVarIO active) . throwError $ INACTIVE
  atomically (getTSessVar c tSess ntfClients)
    >>= either
      (newProtocolClient c tSess ntfClients connectClient $ \_ _ -> pure ())
      (waitForProtocolClient c tSess)
  where
    connectClient :: NtfClientVar -> m NtfClient
    connectClient v = do
      cfg <- getClientConfig c ntfCfg
      liftEitherError (protocolClientError NTF $ B.unpack $ strEncode srv) (getProtocolClient tSess cfg Nothing $ clientDisconnected v)

    clientDisconnected :: NtfClientVar -> NtfClient -> IO ()
    clientDisconnected v client = do
      atomically $ removeTSessVar v tSess ntfClients
      incClientStat c userId client "DISCONNECT" ""
      atomically $ writeTBQueue (subQ c) ("", "", APC SAENone $ hostEvent DISCONNECT client)
      logInfo . decodeUtf8 $ "Agent disconnected from " <> showServer srv

getXFTPServerClient :: forall m. AgentMonad m => AgentClient -> XFTPTransportSession -> m XFTPClient
getXFTPServerClient c@AgentClient {active, xftpClients, useNetworkConfig} tSess@(userId, srv, _) = do
  unlessM (readTVarIO active) . throwError $ INACTIVE
  atomically (getTSessVar c tSess xftpClients)
    >>= either
      (newProtocolClient c tSess xftpClients connectClient $ \_ _ -> pure ())
      (waitForProtocolClient c tSess)
  where
    connectClient :: XFTPClientVar -> m XFTPClient
    connectClient v = do
      cfg <- asks $ xftpCfg . config
      xftpNetworkConfig <- readTVarIO useNetworkConfig
      liftEitherError (protocolClientError XFTP $ B.unpack $ strEncode srv) (X.getXFTPClient tSess cfg {xftpNetworkConfig} $ clientDisconnected v)

    clientDisconnected :: XFTPClientVar -> XFTPClient -> IO ()
    clientDisconnected v client = do
      atomically $ removeTSessVar v tSess xftpClients
      incClientStat c userId client "DISCONNECT" ""
      atomically $ writeTBQueue (subQ c) ("", "", APC SAENone $ hostEvent DISCONNECT client)
      logInfo . decodeUtf8 $ "Agent disconnected from " <> showServer srv

getTSessVar :: forall a s. AgentClient -> TransportSession s -> TMap (TransportSession s) (SessionVar a) -> STM (Either (SessionVar a) (SessionVar a))
getTSessVar c tSess vs = maybe (Left <$> newSessionVar) (pure . Right) =<< TM.lookup tSess vs
  where
    newSessionVar :: STM (SessionVar a)
    newSessionVar = do
      sessionVar <- newEmptyTMVar
      sessionVarId <- stateTVar (workerSeq c) $ \next -> (next, next + 1)
      let v = SessionVar {sessionVar, sessionVarId}
      TM.insert tSess v vs
      pure v

removeTSessVar :: SessionVar a -> TransportSession msg -> TMap (TransportSession msg) (SessionVar a) -> STM ()
removeTSessVar v tSess vs =
  TM.lookup tSess vs
    >>= mapM_ (\v' -> when (sessionVarId v == sessionVarId v') $ TM.delete tSess vs)

waitForProtocolClient :: (AgentMonad m, ProtocolTypeI (ProtoType msg)) => AgentClient -> TransportSession msg -> ClientVar msg -> m (Client msg)
waitForProtocolClient c (_, srv, _) v = do
  NetworkConfig {tcpConnectTimeout} <- readTVarIO $ useNetworkConfig c
  client_ <- liftIO $ tcpConnectTimeout `timeout` atomically (readTMVar $ sessionVar v)
  liftEither $ case client_ of
    Just (Right smpClient) -> Right smpClient
    Just (Left e) -> Left e
    Nothing -> Left $ BROKER (B.unpack $ strEncode srv) TIMEOUT

-- clientConnected arg is only passed for SMP server
newProtocolClient ::
  forall err msg m.
  (AgentMonad m, ProtocolTypeI (ProtoType msg), ProtocolServerClient err msg) =>
  AgentClient ->
  TransportSession msg ->
  TMap (TransportSession msg) (ClientVar msg) ->
  (ClientVar msg -> m (Client msg)) ->
  (AgentClient -> TransportSession msg -> m ()) ->
  ClientVar msg ->
  m (Client msg)
newProtocolClient c tSess@(userId, srv, entityId_) clients connectClient clientConnected v = tryConnectClient pure tryConnectAsync
  where
    tryConnectClient :: (Client msg -> m a) -> m () -> m a
    tryConnectClient successAction retryAction =
      tryAgentError (connectClient v) >>= \case
        Right client -> do
          logInfo . decodeUtf8 $ "Agent connected to " <> showServer srv <> " (user " <> bshow userId <> maybe "" (" for entity " <>) entityId_ <> ")"
          atomically $ putTMVar (sessionVar v) (Right client)
          liftIO $ incClientStat c userId client "CLIENT" "OK"
          atomically $ writeTBQueue (subQ c) ("", "", APC SAENone $ hostEvent CONNECT client)
          successAction client
        Left e -> do
          liftIO $ incServerStat c userId srv "CLIENT" $ strEncode e
          if temporaryAgentError e
            then retryAction
            else atomically $ do
              putTMVar (sessionVar v) (Left e)
              removeTSessVar v tSess clients
          throwError e
    tryConnectAsync :: m ()
    tryConnectAsync = newAsyncAction connectAsync $ asyncClients c
    connectAsync :: Int -> m ()
    connectAsync aId = do
      ri <- asks $ reconnectInterval . config
      withRetryInterval ri $ \_ loop -> void $ tryConnectClient (const $ clientConnected c tSess) loop
      atomically . removeAsyncAction aId $ asyncClients c

hostEvent :: forall err msg. (ProtocolTypeI (ProtoType msg), ProtocolServerClient err msg) => (AProtocolType -> TransportHost -> ACommand 'Agent 'AENone) -> Client msg -> ACommand 'Agent 'AENone
hostEvent event = event (AProtocolType $ protocolTypeI @(ProtoType msg)) . clientTransportHost

getClientConfig :: AgentMonad' m => AgentClient -> (AgentConfig -> ProtocolClientConfig) -> m ProtocolClientConfig
getClientConfig AgentClient {useNetworkConfig} cfgSel = do
  cfg <- asks $ cfgSel . config
  networkConfig <- readTVarIO useNetworkConfig
  pure cfg {networkConfig}

closeAgentClient :: MonadIO m => AgentClient -> m ()
closeAgentClient c = liftIO $ do
  atomically $ writeTVar (active c) False
  closeProtocolServerClients c smpClients
  closeProtocolServerClients c ntfClients
  closeProtocolServerClients c xftpClients
  atomically (swapTVar (smpSubWorkers c) M.empty) >>= mapM_ cancelReconnect
  cancelActions . actions $ asyncClients c
  clearWorkers smpDeliveryWorkers >>= mapM_ cancelWorker
  clearWorkers asyncCmdWorkers >>= mapM_ cancelWorker
  clear connCmdsQueued
  atomically . RQ.clear $ activeSubs c
  atomically . RQ.clear $ pendingSubs c
  clear subscrConns
  clear getMsgLocks
  where
    clearWorkers :: Ord k => (AgentClient -> TMap k a) -> IO (Map k a)
    clearWorkers workers = atomically $ swapTVar (workers c) mempty
    clear :: Monoid m => (AgentClient -> TVar m) -> IO ()
    clear sel = atomically $ writeTVar (sel c) mempty
    cancelReconnect :: SessionVar (Async ()) -> IO ()
    cancelReconnect v = void . forkIO $ atomically (readTMVar $ sessionVar v) >>= uninterruptibleCancel

cancelWorker :: Worker -> IO ()
cancelWorker Worker {doWork, action} = do
  noWorkToDo doWork
  atomically (tryTakeTMVar action) >>= mapM_ (mapM_ uninterruptibleCancel)

waitUntilActive :: AgentClient -> STM ()
waitUntilActive c = unlessM (readTVar $ active c) retry

throwWhenInactive :: AgentClient -> STM ()
throwWhenInactive c = unlessM (readTVar $ active c) $ throwSTM ThreadKilled

-- this function is used to remove workers once delivery is complete, not when it is removed from the map
throwWhenNoDelivery :: AgentClient -> SMPTransportSession -> STM ()
throwWhenNoDelivery c tSess =
  unlessM (TM.member tSess $ smpDeliveryWorkers c) $
    throwSTM ThreadKilled

closeProtocolServerClients :: ProtocolServerClient err msg => AgentClient -> (AgentClient -> TMap (TransportSession msg) (ClientVar msg)) -> IO ()
closeProtocolServerClients c clientsSel =
  atomically (clientsSel c `swapTVar` M.empty) >>= mapM_ (forkIO . closeClient_ c)

closeClient :: ProtocolServerClient err msg => AgentClient -> (AgentClient -> TMap (TransportSession msg) (ClientVar msg)) -> TransportSession msg -> IO ()
closeClient c clientSel tSess =
  atomically (TM.lookupDelete tSess $ clientSel c) >>= mapM_ (closeClient_ c)

closeClient_ :: ProtocolServerClient err msg => AgentClient -> ClientVar msg -> IO ()
closeClient_ c v = do
  NetworkConfig {tcpConnectTimeout} <- readTVarIO $ useNetworkConfig c
  tcpConnectTimeout `timeout` atomically (readTMVar $ sessionVar v) >>= \case
    Just (Right client) -> closeProtocolServerClient client `catchAll_` pure ()
    _ -> pure ()

closeXFTPServerClient :: AgentMonad' m => AgentClient -> UserId -> XFTPServer -> FileDigest -> m ()
closeXFTPServerClient c userId server (FileDigest chunkDigest) =
  mkTransportSession c userId server chunkDigest >>= liftIO . closeClient c xftpClients

cancelActions :: (Foldable f, Monoid (f (Async ()))) => TVar (f (Async ())) -> IO ()
cancelActions as = atomically (swapTVar as mempty) >>= mapM_ (forkIO . uninterruptibleCancel)

withConnLock :: MonadUnliftIO m => AgentClient -> ConnId -> String -> m a -> m a
withConnLock _ "" _ = id
withConnLock AgentClient {connLocks} connId name = withLockMap_ connLocks connId name

withInvLock :: MonadUnliftIO m => AgentClient -> ByteString -> String -> m a -> m a
withInvLock AgentClient {invLocks} = withLockMap_ invLocks

withConnLocks :: MonadUnliftIO m => AgentClient -> [ConnId] -> String -> m a -> m a
withConnLocks AgentClient {connLocks} = withLocksMap_ connLocks . filter (not . B.null)

withLockMap_ :: (Ord k, MonadUnliftIO m) => TMap k Lock -> k -> String -> m a -> m a
withLockMap_ = withGetLock . getMapLock

withLocksMap_ :: (Ord k, MonadUnliftIO m) => TMap k Lock -> [k] -> String -> m a -> m a
withLocksMap_ = withGetLocks . getMapLock

getMapLock :: Ord k => TMap k Lock -> k -> STM Lock
getMapLock locks key = TM.lookup key locks >>= maybe newLock pure
  where
    newLock = createLock >>= \l -> TM.insert key l locks $> l

withClient_ :: forall a m err msg. (AgentMonad m, ProtocolServerClient err msg) => AgentClient -> TransportSession msg -> ByteString -> (Client msg -> m a) -> m a
withClient_ c tSess@(userId, srv, _) statCmd action = do
  cl <- getProtocolServerClient c tSess
  (action cl <* stat cl "OK") `catchAgentError` logServerError cl
  where
    stat cl = liftIO . incClientStat c userId cl statCmd
    logServerError :: Client msg -> AgentErrorType -> m a
    logServerError cl e = do
      logServer "<--" c srv "" $ strEncode e
      stat cl $ strEncode e
      throwError e

withLogClient_ :: (AgentMonad m, ProtocolServerClient err msg) => AgentClient -> TransportSession msg -> EntityId -> ByteString -> (Client msg -> m a) -> m a
withLogClient_ c tSess@(_, srv, _) entId cmdStr action = do
  logServer "-->" c srv entId cmdStr
  res <- withClient_ c tSess cmdStr action
  logServer "<--" c srv entId "OK"
  return res

withClient :: forall m err msg a. (AgentMonad m, ProtocolServerClient err msg) => AgentClient -> TransportSession msg -> ByteString -> (Client msg -> ExceptT (ProtocolClientError err) IO a) -> m a
withClient c tSess statKey action = withClient_ c tSess statKey $ \client -> liftClient (clientProtocolError @err @msg) (clientServer client) $ action client

withLogClient :: forall m err msg a. (AgentMonad m, ProtocolServerClient err msg) => AgentClient -> TransportSession msg -> EntityId -> ByteString -> (Client msg -> ExceptT (ProtocolClientError err) IO a) -> m a
withLogClient c tSess entId cmdStr action = withLogClient_ c tSess entId cmdStr $ \client -> liftClient (clientProtocolError @err @msg) (clientServer client) $ action client

withSMPClient :: (AgentMonad m, SMPQueueRec q) => AgentClient -> q -> ByteString -> (SMPClient -> ExceptT SMPClientError IO a) -> m a
withSMPClient c q cmdStr action = do
  tSess <- mkSMPTransportSession c q
  withLogClient c tSess (queueId q) cmdStr action

withSMPClient_ :: (AgentMonad m, SMPQueueRec q) => AgentClient -> q -> ByteString -> (SMPClient -> m a) -> m a
withSMPClient_ c q cmdStr action = do
  tSess <- mkSMPTransportSession c q
  withLogClient_ c tSess (queueId q) cmdStr action

withNtfClient :: forall m a. AgentMonad m => AgentClient -> NtfServer -> EntityId -> ByteString -> (NtfClient -> ExceptT NtfClientError IO a) -> m a
withNtfClient c srv = withLogClient c (0, srv, Nothing)

withXFTPClient ::
  (AgentMonad m, ProtocolServerClient err msg) =>
  AgentClient ->
  (UserId, ProtoServer msg, EntityId) ->
  ByteString ->
  (Client msg -> ExceptT (ProtocolClientError err) IO b) ->
  m b
withXFTPClient c (userId, srv, entityId) cmdStr action = do
  tSess <- mkTransportSession c userId srv entityId
  withLogClient c tSess entityId cmdStr action

liftClient :: (AgentMonad m, Show err, Encoding err) => (err -> AgentErrorType) -> HostName -> ExceptT (ProtocolClientError err) IO a -> m a
liftClient protocolError_ = liftError . protocolClientError protocolError_

protocolClientError :: (Show err, Encoding err) => (err -> AgentErrorType) -> HostName -> ProtocolClientError err -> AgentErrorType
protocolClientError protocolError_ host = \case
  PCEProtocolError e -> protocolError_ e
  PCEResponseError e -> BROKER host $ RESPONSE $ B.unpack $ smpEncode e
  PCEUnexpectedResponse _ -> BROKER host UNEXPECTED
  PCEResponseTimeout -> BROKER host TIMEOUT
  PCENetworkError -> BROKER host NETWORK
  PCEIncompatibleHost -> BROKER host HOST
  PCETransportError e -> BROKER host $ TRANSPORT e
  e@PCECryptoError {} -> INTERNAL $ show e
  PCEIOError {} -> BROKER host NETWORK

data ProtocolTestStep
  = TSConnect
  | TSDisconnect
  | TSCreateQueue
  | TSSecureQueue
  | TSDeleteQueue
  | TSCreateFile
  | TSUploadFile
  | TSDownloadFile
  | TSCompareFile
  | TSDeleteFile
  deriving (Eq, Show)

data ProtocolTestFailure = ProtocolTestFailure
  { testStep :: ProtocolTestStep,
    testError :: AgentErrorType
  }
  deriving (Eq, Show)

runSMPServerTest :: AgentMonad m => AgentClient -> UserId -> SMPServerWithAuth -> m (Maybe ProtocolTestFailure)
runSMPServerTest c userId (ProtoServerWithAuth srv auth) = do
  cfg <- getClientConfig c smpCfg
  C.SignAlg a <- asks $ cmdSignAlg . config
  g <- asks random
  liftIO $ do
    let tSess = (userId, srv, Nothing)
    getProtocolClient tSess cfg Nothing (\_ -> pure ()) >>= \case
      Right smp -> do
        (rKey, rpKey) <- atomically $ C.generateSignatureKeyPair a g
        (sKey, _) <- atomically $ C.generateSignatureKeyPair a g
        (dhKey, _) <- atomically $ C.generateKeyPair g
        r <- runExceptT $ do
          SMP.QIK {rcvId} <- liftError (testErr TSCreateQueue) $ createSMPQueue smp rpKey rKey dhKey auth SMSubscribe
          liftError (testErr TSSecureQueue) $ secureSMPQueue smp rpKey rcvId sKey
          liftError (testErr TSDeleteQueue) $ deleteSMPQueue smp rpKey rcvId
        ok <- tcpTimeout (networkConfig cfg) `timeout` closeProtocolClient smp
        incClientStat c userId smp "SMP_TEST" "OK"
        pure $ either Just (const Nothing) r <|> maybe (Just (ProtocolTestFailure TSDisconnect $ BROKER addr TIMEOUT)) (const Nothing) ok
      Left e -> pure (Just $ testErr TSConnect e)
  where
    addr = B.unpack $ strEncode srv
    testErr :: ProtocolTestStep -> SMPClientError -> ProtocolTestFailure
    testErr step = ProtocolTestFailure step . protocolClientError SMP addr

runXFTPServerTest :: forall m. AgentMonad m => AgentClient -> UserId -> XFTPServerWithAuth -> m (Maybe ProtocolTestFailure)
runXFTPServerTest c userId (ProtoServerWithAuth srv auth) = do
  cfg <- asks $ xftpCfg . config
  g <- asks random
  xftpNetworkConfig <- readTVarIO $ useNetworkConfig c
  workDir <- getXFTPWorkPath
  filePath <- getTempFilePath workDir
  rcvPath <- getTempFilePath workDir
  liftIO $ do
    let tSess = (userId, srv, Nothing)
    X.getXFTPClient tSess cfg {xftpNetworkConfig} (\_ -> pure ()) >>= \case
      Right xftp -> do
        (sndKey, spKey) <- atomically $ C.generateSignatureKeyPair C.SEd25519 g
        (rcvKey, rpKey) <- atomically $ C.generateSignatureKeyPair C.SEd25519 g
        createTestChunk filePath
        digest <- liftIO $ C.sha256Hash <$> B.readFile filePath
        let file = FileInfo {sndKey, size = chSize, digest}
            chunkSpec = X.XFTPChunkSpec {filePath, chunkOffset = 0, chunkSize = chSize}
        r <- runExceptT $ do
          (sId, [rId]) <- liftError (testErr TSCreateFile) $ X.createXFTPChunk xftp spKey file [rcvKey] auth
          liftError (testErr TSUploadFile) $ X.uploadXFTPChunk xftp spKey sId chunkSpec
          liftError (testErr TSDownloadFile) $ X.downloadXFTPChunk g xftp rpKey rId $ XFTPRcvChunkSpec rcvPath chSize digest
          rcvDigest <- liftIO $ C.sha256Hash <$> B.readFile rcvPath
          unless (digest == rcvDigest) $ throwError $ ProtocolTestFailure TSCompareFile $ XFTP DIGEST
          liftError (testErr TSDeleteFile) $ X.deleteXFTPChunk xftp spKey sId
        ok <- tcpTimeout xftpNetworkConfig `timeout` X.closeXFTPClient xftp
        incClientStat c userId xftp "XFTP_TEST" "OK"
        pure $ either Just (const Nothing) r <|> maybe (Just (ProtocolTestFailure TSDisconnect $ BROKER addr TIMEOUT)) (const Nothing) ok
      Left e -> pure (Just $ testErr TSConnect e)
  where
    addr = B.unpack $ strEncode srv
    testErr :: ProtocolTestStep -> XFTPClientError -> ProtocolTestFailure
    testErr step = ProtocolTestFailure step . protocolClientError XFTP addr
    chSize :: Integral a => a
    chSize = kb 256
    getTempFilePath :: FilePath -> m FilePath
    getTempFilePath workPath = do
      ts <- liftIO getCurrentTime
      let isoTime = formatTime defaultTimeLocale "%Y-%m-%dT%H%M%S.%6q" ts
      uniqueCombine workPath isoTime
    -- this creates a new DRG on purpose to avoid blocking the one used in the agent
    createTestChunk :: FilePath -> IO ()
    createTestChunk fp = B.writeFile fp =<< atomically . C.randomBytes chSize =<< C.newRandom

getXFTPWorkPath :: AgentMonad m => m FilePath
getXFTPWorkPath = do
  workDir <- readTVarIO =<< asks (xftpWorkDir . xftpAgent)
  maybe getTemporaryDirectory pure workDir

mkTransportSession :: AgentMonad' m => AgentClient -> UserId -> ProtoServer msg -> EntityId -> m (TransportSession msg)
mkTransportSession c userId srv entityId = mkTSession userId srv entityId <$> getSessionMode c

mkTSession :: UserId -> ProtoServer msg -> EntityId -> TransportSessionMode -> TransportSession msg
mkTSession userId srv entityId mode = (userId, srv, if mode == TSMEntity then Just entityId else Nothing)

mkSMPTransportSession :: (AgentMonad' m, SMPQueueRec q) => AgentClient -> q -> m SMPTransportSession
mkSMPTransportSession c q = mkSMPTSession q <$> getSessionMode c

mkSMPTSession :: SMPQueueRec q => q -> TransportSessionMode -> SMPTransportSession
mkSMPTSession q = mkTSession (qUserId q) (qServer q) (qConnId q)

getSessionMode :: AgentMonad' m => AgentClient -> m TransportSessionMode
getSessionMode = fmap sessionMode . readTVarIO . useNetworkConfig

newRcvQueue :: AgentMonad m => AgentClient -> UserId -> ConnId -> SMPServerWithAuth -> VersionRange -> SubscriptionMode -> m (NewRcvQueue, SMPQueueUri)
newRcvQueue c userId connId (ProtoServerWithAuth srv auth) vRange subMode = do
  C.SignAlg a <- asks (cmdSignAlg . config)
  g <- asks random
  (recipientKey, rcvPrivateKey) <- atomically $ C.generateSignatureKeyPair a g
  (dhKey, privDhKey) <- atomically $ C.generateKeyPair g
  (e2eDhKey, e2ePrivKey) <- atomically $ C.generateKeyPair g
  logServer "-->" c srv "" "NEW"
  tSess <- mkTransportSession c userId srv connId
  QIK {rcvId, sndId, rcvPublicDhKey} <-
    withClient c tSess "NEW" $ \smp -> createSMPQueue smp rcvPrivateKey recipientKey dhKey auth subMode
  logServer "<--" c srv "" $ B.unwords ["IDS", logSecret rcvId, logSecret sndId]
  let rq =
        RcvQueue
          { userId,
            connId,
            server = srv,
            rcvId,
            rcvPrivateKey,
            rcvDhSecret = C.dh' rcvPublicDhKey privDhKey,
            e2ePrivKey,
            e2eDhSecret = Nothing,
            sndId,
            status = New,
            dbQueueId = DBNewQueue,
            primary = True,
            dbReplaceQueueId = Nothing,
            rcvSwchStatus = Nothing,
            smpClientVersion = maxVersion vRange,
            clientNtfCreds = Nothing,
            deleteErrors = 0
          }
  pure (rq, SMPQueueUri vRange $ SMPQueueAddress srv sndId e2eDhKey)

processSubResult :: AgentClient -> RcvQueue -> Either SMPClientError () -> IO (Either SMPClientError ())
processSubResult c rq r = do
  case r of
    Left e ->
      unless (temporaryClientError e) . atomically $ do
        RQ.deleteQueue rq (pendingSubs c)
        TM.insert (RQ.qKey rq) e (removedSubs c)
    _ -> addSubscription c rq
  pure r

temporaryAgentError :: AgentErrorType -> Bool
temporaryAgentError = \case
  BROKER _ NETWORK -> True
  BROKER _ TIMEOUT -> True
  INACTIVE -> True
  _ -> False

temporaryOrHostError :: AgentErrorType -> Bool
temporaryOrHostError = \case
  BROKER _ HOST -> True
  e -> temporaryAgentError e

-- | Subscribe to queues. The list of results can have a different order.
subscribeQueues :: forall m. AgentMonad' m => AgentClient -> [RcvQueue] -> m [(RcvQueue, Either AgentErrorType ())]
subscribeQueues c qs = do
  (errs, qs') <- partitionEithers <$> mapM checkQueue qs
  forM_ qs' $ \rq@RcvQueue {connId} -> atomically $ do
    modifyTVar (subscrConns c) $ S.insert connId
    RQ.addQueue rq $ pendingSubs c
  u <- askUnliftIO
  -- only "checked" queues are subscribed
  (errs <>) <$> sendTSessionBatches "SUB" 90 id (subscribeQueues_ u) c qs'
  where
    checkQueue rq = do
      prohibited <- atomically $ hasGetLock c rq
      pure $ if prohibited then Left (rq, Left $ CMD PROHIBITED) else Right rq
    subscribeQueues_ :: UnliftIO m -> SMPClient -> NonEmpty RcvQueue -> IO (BatchResponses SMPClientError ())
    subscribeQueues_ u smp qs' = do
      rs <- sendBatch subscribeSMPQueues smp qs'
      mapM_ (uncurry $ processSubResult c) rs
      when (any temporaryClientError . lefts . map snd $ L.toList rs) . unliftIO u $
        resubscribeSMPSession c (transportSession' smp)
      pure rs

type BatchResponses e r = (NonEmpty (RcvQueue, Either e r))

-- statBatchSize is not used to batch the commands, only for traffic statistics
sendTSessionBatches :: forall m q r. AgentMonad' m => ByteString -> Int -> (q -> RcvQueue) -> (SMPClient -> NonEmpty q -> IO (BatchResponses SMPClientError r)) -> AgentClient -> [q] -> m [(RcvQueue, Either AgentErrorType r)]
sendTSessionBatches statCmd statBatchSize toRQ action c qs =
  concatMap L.toList <$> (mapConcurrently sendClientBatch =<< batchQueues)
  where
    batchQueues :: m [(SMPTransportSession, NonEmpty q)]
    batchQueues = do
      mode <- sessionMode <$> readTVarIO (useNetworkConfig c)
      pure . M.assocs $ foldl' (batch mode) M.empty qs
      where
        batch mode m q =
          let tSess = mkSMPTSession (toRQ q) mode
           in M.alter (Just . maybe [q] (q <|)) tSess m
    sendClientBatch :: (SMPTransportSession, NonEmpty q) -> m (BatchResponses AgentErrorType r)
    sendClientBatch (tSess@(userId, srv, _), qs') =
      tryAgentError' (getSMPServerClient c tSess) >>= \case
        Left e -> pure $ L.map ((,Left e) . toRQ) qs'
        Right smp -> liftIO $ do
          logServer "-->" c srv (bshow (length qs') <> " queues") statCmd
          rs <- L.map agentError <$> action smp qs'
          statBatch
          pure rs
          where
            agentError = second . first $ protocolClientError SMP $ clientServer smp
            statBatch =
              let n = (length qs - 1) `div` statBatchSize + 1
               in incClientStatN c userId smp n statCmd "OK"

sendBatch :: (SMPClient -> NonEmpty (SMP.RcvPrivateSignKey, SMP.RecipientId) -> IO (NonEmpty (Either SMPClientError ()))) -> SMPClient -> NonEmpty RcvQueue -> IO (BatchResponses SMPClientError ())
sendBatch smpCmdFunc smp qs = L.zip qs <$> smpCmdFunc smp (L.map queueCreds qs)
  where
    queueCreds RcvQueue {rcvPrivateKey, rcvId} = (rcvPrivateKey, rcvId)

addSubscription :: MonadIO m => AgentClient -> RcvQueue -> m ()
addSubscription c rq@RcvQueue {connId} = atomically $ do
  modifyTVar' (subscrConns c) $ S.insert connId
  RQ.addQueue rq $ activeSubs c
  RQ.deleteQueue rq $ pendingSubs c

hasActiveSubscription :: AgentClient -> ConnId -> STM Bool
hasActiveSubscription c connId = RQ.hasConn connId $ activeSubs c

removeSubscription :: AgentClient -> ConnId -> STM ()
removeSubscription c connId = do
  modifyTVar' (subscrConns c) $ S.delete connId
  RQ.deleteConn connId $ activeSubs c
  RQ.deleteConn connId $ pendingSubs c

getSubscriptions :: AgentClient -> STM (Set ConnId)
getSubscriptions = readTVar . subscrConns

logServer :: MonadIO m => ByteString -> AgentClient -> ProtocolServer s -> QueueId -> ByteString -> m ()
logServer dir AgentClient {clientId} srv qId cmdStr =
  logInfo . decodeUtf8 $ B.unwords ["A", "(" <> bshow clientId <> ")", dir, showServer srv, ":", logSecret qId, cmdStr]

showServer :: ProtocolServer s -> ByteString
showServer ProtocolServer {host, port} =
  strEncode host <> B.pack (if null port then "" else ':' : port)

logSecret :: ByteString -> ByteString
logSecret bs = encode $ B.take 3 bs

sendConfirmation :: forall m. AgentMonad m => AgentClient -> SndQueue -> ByteString -> m ()
sendConfirmation c sq@SndQueue {sndId, sndPublicKey = Just sndPublicKey, e2ePubKey = e2ePubKey@Just {}} agentConfirmation =
  withSMPClient_ c sq "SEND <CONF>" $ \smp -> do
    let clientMsg = SMP.ClientMessage (SMP.PHConfirmation sndPublicKey) agentConfirmation
    msg <- agentCbEncrypt sq e2ePubKey $ smpEncode clientMsg
    liftClient SMP (clientServer smp) $ sendSMPMessage smp Nothing sndId (SMP.MsgFlags {notification = True}) msg
sendConfirmation _ _ _ = throwError $ INTERNAL "sendConfirmation called without snd_queue public key(s) in the database"

sendInvitation :: forall m. AgentMonad m => AgentClient -> UserId -> Compatible SMPQueueInfo -> Compatible Version -> ConnectionRequestUri 'CMInvitation -> ConnInfo -> m ()
sendInvitation c userId (Compatible (SMPQueueInfo v SMPQueueAddress {smpServer, senderId, dhPublicKey})) (Compatible agentVersion) connReq connInfo = do
  tSess <- mkTransportSession c userId smpServer senderId
  withLogClient_ c tSess senderId "SEND <INV>" $ \smp -> do
    msg <- mkInvitation
    liftClient SMP (clientServer smp) $ sendSMPMessage smp Nothing senderId MsgFlags {notification = True} msg
  where
    mkInvitation :: m ByteString
    -- this is only encrypted with per-queue E2E, not with double ratchet
    mkInvitation = do
      let agentEnvelope = AgentInvitation {agentVersion, connReq, connInfo}
      agentCbEncryptOnce v dhPublicKey . smpEncode $
        SMP.ClientMessage SMP.PHEmpty (smpEncode agentEnvelope)

getQueueMessage :: AgentMonad m => AgentClient -> RcvQueue -> m (Maybe SMPMsgMeta)
getQueueMessage c rq@RcvQueue {server, rcvId, rcvPrivateKey} = do
  atomically createTakeGetLock
  (v, msg_) <- withSMPClient c rq "GET" $ \smp ->
    (thVersion smp,) <$> getSMPMessage smp rcvPrivateKey rcvId
  mapM (decryptMeta v) msg_
  where
    decryptMeta v msg@SMP.RcvMessage {msgId} = SMP.rcvMessageMeta msgId <$> decryptSMPMessage v rq msg
    createTakeGetLock = TM.alterF takeLock (server, rcvId) $ getMsgLocks c
      where
        takeLock l_ = do
          l <- maybe (newTMVar ()) pure l_
          takeTMVar l
          pure $ Just l

decryptSMPMessage :: AgentMonad m => Version -> RcvQueue -> SMP.RcvMessage -> m SMP.ClientRcvMsgBody
decryptSMPMessage v rq SMP.RcvMessage {msgId, msgTs, msgFlags, msgBody = SMP.EncRcvMsgBody body}
  | v == 1 || v == 2 = SMP.ClientRcvMsgBody msgTs msgFlags <$> decrypt body
  | otherwise = liftEither . parse SMP.clientRcvMsgBodyP (AGENT A_MESSAGE) =<< decrypt body
  where
    decrypt = agentCbDecrypt (rcvDhSecret rq) (C.cbNonce msgId)

secureQueue :: AgentMonad m => AgentClient -> RcvQueue -> SndPublicVerifyKey -> m ()
secureQueue c rq@RcvQueue {rcvId, rcvPrivateKey} senderKey =
  withSMPClient c rq "KEY <key>" $ \smp ->
    secureSMPQueue smp rcvPrivateKey rcvId senderKey

enableQueueNotifications :: AgentMonad m => AgentClient -> RcvQueue -> SMP.NtfPublicVerifyKey -> SMP.RcvNtfPublicDhKey -> m (SMP.NotifierId, SMP.RcvNtfPublicDhKey)
enableQueueNotifications c rq@RcvQueue {rcvId, rcvPrivateKey} notifierKey rcvNtfPublicDhKey =
  withSMPClient c rq "NKEY <nkey>" $ \smp ->
    enableSMPQueueNotifications smp rcvPrivateKey rcvId notifierKey rcvNtfPublicDhKey

enableQueuesNtfs :: forall m. AgentMonad' m => AgentClient -> [(RcvQueue, SMP.NtfPublicVerifyKey, SMP.RcvNtfPublicDhKey)] -> m [(RcvQueue, Either AgentErrorType (SMP.NotifierId, SMP.RcvNtfPublicDhKey))]
enableQueuesNtfs = sendTSessionBatches "NKEY" 90 fst3 enableQueues_
  where
    fst3 (x, _, _) = x
    enableQueues_ :: SMPClient -> NonEmpty (RcvQueue, SMP.NtfPublicVerifyKey, SMP.RcvNtfPublicDhKey) -> IO (NonEmpty (RcvQueue, Either (ProtocolClientError ErrorType) (SMP.NotifierId, RcvNtfPublicDhKey)))
    enableQueues_ smp qs' = L.zipWith ((,) . fst3) qs' <$> enableSMPQueuesNtfs smp (L.map queueCreds qs')
    queueCreds :: (RcvQueue, SMP.NtfPublicVerifyKey, SMP.RcvNtfPublicDhKey) -> (SMP.RcvPrivateSignKey, SMP.RecipientId, SMP.NtfPublicVerifyKey, SMP.RcvNtfPublicDhKey)
    queueCreds (RcvQueue {rcvPrivateKey, rcvId}, notifierKey, rcvNtfPublicDhKey) = (rcvPrivateKey, rcvId, notifierKey, rcvNtfPublicDhKey)

disableQueueNotifications :: AgentMonad m => AgentClient -> RcvQueue -> m ()
disableQueueNotifications c rq@RcvQueue {rcvId, rcvPrivateKey} =
  withSMPClient c rq "NDEL" $ \smp ->
    disableSMPQueueNotifications smp rcvPrivateKey rcvId

disableQueuesNtfs :: forall m. AgentMonad' m => AgentClient -> [RcvQueue] -> m [(RcvQueue, Either AgentErrorType ())]
disableQueuesNtfs = sendTSessionBatches "NDEL" 90 id $ sendBatch disableSMPQueuesNtfs

sendAck :: AgentMonad m => AgentClient -> RcvQueue -> MsgId -> m ()
sendAck c rq@RcvQueue {rcvId, rcvPrivateKey} msgId = do
  withSMPClient c rq ("ACK:" <> logSecret msgId) $ \smp ->
    ackSMPMessage smp rcvPrivateKey rcvId msgId
  atomically $ releaseGetLock c rq

hasGetLock :: AgentClient -> RcvQueue -> STM Bool
hasGetLock c RcvQueue {server, rcvId} =
  TM.member (server, rcvId) $ getMsgLocks c

releaseGetLock :: AgentClient -> RcvQueue -> STM ()
releaseGetLock c RcvQueue {server, rcvId} =
  TM.lookup (server, rcvId) (getMsgLocks c) >>= mapM_ (`tryPutTMVar` ())

suspendQueue :: AgentMonad m => AgentClient -> RcvQueue -> m ()
suspendQueue c rq@RcvQueue {rcvId, rcvPrivateKey} =
  withSMPClient c rq "OFF" $ \smp ->
    suspendSMPQueue smp rcvPrivateKey rcvId

deleteQueue :: AgentMonad m => AgentClient -> RcvQueue -> m ()
deleteQueue c rq@RcvQueue {rcvId, rcvPrivateKey} = do
  withSMPClient c rq "DEL" $ \smp ->
    deleteSMPQueue smp rcvPrivateKey rcvId

deleteQueues :: forall m. AgentMonad' m => AgentClient -> [RcvQueue] -> m [(RcvQueue, Either AgentErrorType ())]
deleteQueues = sendTSessionBatches "DEL" 90 id $ sendBatch deleteSMPQueues

sendAgentMessage :: AgentMonad m => AgentClient -> SndQueue -> MsgFlags -> ByteString -> m ()
sendAgentMessage c sq@SndQueue {sndId, sndPrivateKey} msgFlags agentMsg =
  withSMPClient_ c sq "SEND <MSG>" $ \smp -> do
    let clientMsg = SMP.ClientMessage SMP.PHEmpty agentMsg
    msg <- agentCbEncrypt sq Nothing $ smpEncode clientMsg
    liftClient SMP (clientServer smp) $ sendSMPMessage smp (Just sndPrivateKey) sndId msgFlags msg

agentNtfRegisterToken :: AgentMonad m => AgentClient -> NtfToken -> C.APublicVerifyKey -> C.PublicKeyX25519 -> m (NtfTokenId, C.PublicKeyX25519)
agentNtfRegisterToken c NtfToken {deviceToken, ntfServer, ntfPrivKey} ntfPubKey pubDhKey =
  withClient c (0, ntfServer, Nothing) "TNEW" $ \ntf -> ntfRegisterToken ntf ntfPrivKey (NewNtfTkn deviceToken ntfPubKey pubDhKey)

agentNtfVerifyToken :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> NtfRegCode -> m ()
agentNtfVerifyToken c tknId NtfToken {ntfServer, ntfPrivKey} code =
  withNtfClient c ntfServer tknId "TVFY" $ \ntf -> ntfVerifyToken ntf ntfPrivKey tknId code

agentNtfCheckToken :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> m NtfTknStatus
agentNtfCheckToken c tknId NtfToken {ntfServer, ntfPrivKey} =
  withNtfClient c ntfServer tknId "TCHK" $ \ntf -> ntfCheckToken ntf ntfPrivKey tknId

agentNtfReplaceToken :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> DeviceToken -> m ()
agentNtfReplaceToken c tknId NtfToken {ntfServer, ntfPrivKey} token =
  withNtfClient c ntfServer tknId "TRPL" $ \ntf -> ntfReplaceToken ntf ntfPrivKey tknId token

agentNtfDeleteToken :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> m ()
agentNtfDeleteToken c tknId NtfToken {ntfServer, ntfPrivKey} =
  withNtfClient c ntfServer tknId "TDEL" $ \ntf -> ntfDeleteToken ntf ntfPrivKey tknId

agentNtfEnableCron :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> Word16 -> m ()
agentNtfEnableCron c tknId NtfToken {ntfServer, ntfPrivKey} interval =
  withNtfClient c ntfServer tknId "TCRN" $ \ntf -> ntfEnableCron ntf ntfPrivKey tknId interval

agentNtfCreateSubscription :: AgentMonad m => AgentClient -> NtfTokenId -> NtfToken -> SMPQueueNtf -> SMP.NtfPrivateSignKey -> m NtfSubscriptionId
agentNtfCreateSubscription c tknId NtfToken {ntfServer, ntfPrivKey} smpQueue nKey =
  withNtfClient c ntfServer tknId "SNEW" $ \ntf -> ntfCreateSubscription ntf ntfPrivKey (NewNtfSub tknId smpQueue nKey)

agentNtfCheckSubscription :: AgentMonad m => AgentClient -> NtfSubscriptionId -> NtfToken -> m NtfSubStatus
agentNtfCheckSubscription c subId NtfToken {ntfServer, ntfPrivKey} =
  withNtfClient c ntfServer subId "SCHK" $ \ntf -> ntfCheckSubscription ntf ntfPrivKey subId

agentNtfDeleteSubscription :: AgentMonad m => AgentClient -> NtfSubscriptionId -> NtfToken -> m ()
agentNtfDeleteSubscription c subId NtfToken {ntfServer, ntfPrivKey} =
  withNtfClient c ntfServer subId "SDEL" $ \ntf -> ntfDeleteSubscription ntf ntfPrivKey subId

agentXFTPDownloadChunk :: AgentMonad m => AgentClient -> UserId -> FileDigest -> RcvFileChunkReplica -> XFTPRcvChunkSpec -> m ()
agentXFTPDownloadChunk c userId (FileDigest chunkDigest) RcvFileChunkReplica {server, replicaId = ChunkReplicaId fId, replicaKey} chunkSpec = do
  g <- asks random
  withXFTPClient c (userId, server, chunkDigest) "FGET" $ \xftp -> X.downloadXFTPChunk g xftp replicaKey fId chunkSpec

agentXFTPNewChunk :: AgentMonad m => AgentClient -> SndFileChunk -> Int -> XFTPServerWithAuth -> m NewSndChunkReplica
agentXFTPNewChunk c SndFileChunk {userId, chunkSpec = XFTPChunkSpec {chunkSize}, digest = FileDigest chunkDigest} n (ProtoServerWithAuth srv auth) = do
  rKeys <- xftpRcvKeys n
  (sndKey, replicaKey) <- atomically . C.generateSignatureKeyPair C.SEd25519 =<< asks random
  let fileInfo = FileInfo {sndKey, size = fromIntegral chunkSize, digest = chunkDigest}
  logServer "-->" c srv "" "FNEW"
  tSess <- mkTransportSession c userId srv chunkDigest
  (sndId, rIds) <- withClient c tSess "FNEW" $ \xftp -> X.createXFTPChunk xftp replicaKey fileInfo (L.map fst rKeys) auth
  logServer "<--" c srv "" $ B.unwords ["SIDS", logSecret sndId]
  pure NewSndChunkReplica {server = srv, replicaId = ChunkReplicaId sndId, replicaKey, rcvIdsKeys = L.toList $ xftpRcvIdsKeys rIds rKeys}

agentXFTPUploadChunk :: AgentMonad m => AgentClient -> UserId -> FileDigest -> SndFileChunkReplica -> XFTPChunkSpec -> m ()
agentXFTPUploadChunk c userId (FileDigest chunkDigest) SndFileChunkReplica {server, replicaId = ChunkReplicaId fId, replicaKey} chunkSpec =
  withXFTPClient c (userId, server, chunkDigest) "FPUT" $ \xftp -> X.uploadXFTPChunk xftp replicaKey fId chunkSpec

agentXFTPAddRecipients :: AgentMonad m => AgentClient -> UserId -> FileDigest -> SndFileChunkReplica -> Int -> m (NonEmpty (ChunkReplicaId, C.APrivateSignKey))
agentXFTPAddRecipients c userId (FileDigest chunkDigest) SndFileChunkReplica {server, replicaId = ChunkReplicaId fId, replicaKey} n = do
  rKeys <- xftpRcvKeys n
  rIds <- withXFTPClient c (userId, server, chunkDigest) "FADD" $ \xftp -> X.addXFTPRecipients xftp replicaKey fId (L.map fst rKeys)
  pure $ xftpRcvIdsKeys rIds rKeys

agentXFTPDeleteChunk :: AgentMonad m => AgentClient -> UserId -> DeletedSndChunkReplica -> m ()
agentXFTPDeleteChunk c userId DeletedSndChunkReplica {server, replicaId = ChunkReplicaId fId, replicaKey, chunkDigest = FileDigest chunkDigest} =
  withXFTPClient c (userId, server, chunkDigest) "FDEL" $ \xftp -> X.deleteXFTPChunk xftp replicaKey fId

xftpRcvKeys :: AgentMonad m => Int -> m (NonEmpty C.ASignatureKeyPair)
xftpRcvKeys n = do
  rKeys <- atomically . replicateM n . C.generateSignatureKeyPair C.SEd25519 =<< asks random
  case L.nonEmpty rKeys of
    Just rKeys' -> pure rKeys'
    _ -> throwError $ INTERNAL "non-positive number of recipients"

xftpRcvIdsKeys :: NonEmpty ByteString -> NonEmpty C.ASignatureKeyPair -> NonEmpty (ChunkReplicaId, C.APrivateSignKey)
xftpRcvIdsKeys rIds rKeys = L.map ChunkReplicaId rIds `L.zip` L.map snd rKeys

agentCbEncrypt :: AgentMonad m => SndQueue -> Maybe C.PublicKeyX25519 -> ByteString -> m ByteString
agentCbEncrypt SndQueue {e2eDhSecret, smpClientVersion} e2ePubKey msg = do
  cmNonce <- atomically . C.randomCbNonce =<< asks random
  let paddedLen = maybe SMP.e2eEncMessageLength (const SMP.e2eEncConfirmationLength) e2ePubKey
  cmEncBody <-
    liftEither . first cryptoError $
      C.cbEncrypt e2eDhSecret cmNonce msg paddedLen
  let cmHeader = SMP.PubHeader smpClientVersion e2ePubKey
  pure $ smpEncode SMP.ClientMsgEnvelope {cmHeader, cmNonce, cmEncBody}

-- add encoding as AgentInvitation'?
agentCbEncryptOnce :: AgentMonad m => Version -> C.PublicKeyX25519 -> ByteString -> m ByteString
agentCbEncryptOnce clientVersion dhRcvPubKey msg = do
  g <- asks random
  (dhSndPubKey, dhSndPrivKey) <- atomically $ C.generateKeyPair g
  let e2eDhSecret = C.dh' dhRcvPubKey dhSndPrivKey
  cmNonce <- atomically $ C.randomCbNonce g
  cmEncBody <-
    liftEither . first cryptoError $
      C.cbEncrypt e2eDhSecret cmNonce msg SMP.e2eEncConfirmationLength
  let cmHeader = SMP.PubHeader clientVersion (Just dhSndPubKey)
  pure $ smpEncode SMP.ClientMsgEnvelope {cmHeader, cmNonce, cmEncBody}

-- | NaCl crypto-box decrypt - both for messages received from the server
-- and per-queue E2E encrypted messages from the sender that were inside.
agentCbDecrypt :: AgentMonad m => C.DhSecretX25519 -> C.CbNonce -> ByteString -> m ByteString
agentCbDecrypt dhSecret nonce msg =
  liftEither . first cryptoError $
    C.cbDecrypt dhSecret nonce msg

cryptoError :: C.CryptoError -> AgentErrorType
cryptoError = \case
  C.CryptoLargeMsgError -> CMD LARGE
  C.CryptoHeaderError _ -> AGENT A_MESSAGE -- parsing error
  C.CERatchetDuplicateMessage -> AGENT A_DUPLICATE
  C.AESDecryptError -> c DECRYPT_AES
  C.CBDecryptError -> c DECRYPT_CB
  C.CERatchetHeader -> c RATCHET_HEADER
  C.CERatchetTooManySkipped n -> c $ RATCHET_SKIPPED n
  C.CERatchetEarlierMessage n -> c $ RATCHET_EARLIER n
  e -> INTERNAL $ show e
  where
    c = AGENT . A_CRYPTO

waitForWork :: AgentMonad' m => TMVar () -> m ()
waitForWork = void . atomically . readTMVar

withWork :: AgentMonad m => AgentClient -> TMVar () -> (DB.Connection -> IO (Either StoreError (Maybe a))) -> (a -> m ()) -> m ()
withWork c doWork getWork action =
  withStore' c getWork >>= \case
    Right (Just r) -> action r
    Right Nothing -> noWork
    Left e@SEWorkItemError {} -> noWork >> notifyErr (CRITICAL False) e
    Left e -> notifyErr INTERNAL e
  where
    noWork = liftIO $ noWorkToDo doWork
    notifyErr err e = atomically $ writeTBQueue (subQ c) ("", "", APC SAEConn $ ERR $ err $ show e)

noWorkToDo :: TMVar () -> IO ()
noWorkToDo = void . atomically . tryTakeTMVar

hasWorkToDo :: Worker -> STM ()
hasWorkToDo = hasWorkToDo' . doWork

hasWorkToDo' :: TMVar () -> STM ()
hasWorkToDo' = void . (`tryPutTMVar` ())

endAgentOperation :: AgentClient -> AgentOperation -> STM ()
endAgentOperation c op = endOperation c op $ case op of
  AONtfNetwork -> pure ()
  AORcvNetwork ->
    suspendOperation c AOMsgDelivery $
      suspendSendingAndDatabase c
  AOMsgDelivery ->
    suspendSendingAndDatabase c
  AOSndNetwork ->
    suspendOperation c AODatabase $
      notifySuspended c
  AODatabase ->
    notifySuspended c

suspendSendingAndDatabase :: AgentClient -> STM ()
suspendSendingAndDatabase c =
  suspendOperation c AOSndNetwork $
    suspendOperation c AODatabase $
      notifySuspended c

suspendOperation :: AgentClient -> AgentOperation -> STM () -> STM ()
suspendOperation c op endedAction = do
  n <- stateTVar (agentOpSel op c) $ \s -> (opsInProgress s, s {opSuspended = True})
  -- unsafeIOToSTM $ putStrLn $ "suspendOperation_ " <> show op <> " " <> show n
  when (n == 0) $ whenSuspending c endedAction

notifySuspended :: AgentClient -> STM ()
notifySuspended c = do
  -- unsafeIOToSTM $ putStrLn "notifySuspended"
  writeTBQueue (subQ c) ("", "", APC SAENone SUSPENDED)
  writeTVar (agentState c) ASSuspended

endOperation :: AgentClient -> AgentOperation -> STM () -> STM ()
endOperation c op endedAction = do
  (suspended, n) <- stateTVar (agentOpSel op c) $ \s ->
    let n = max 0 (opsInProgress s - 1)
     in ((opSuspended s, n), s {opsInProgress = n})
  -- unsafeIOToSTM $ putStrLn $ "endOperation: " <> show op <> " " <> show suspended <> " " <> show n
  when (suspended && n == 0) $ whenSuspending c endedAction

whenSuspending :: AgentClient -> STM () -> STM ()
whenSuspending c = whenM ((== ASSuspending) <$> readTVar (agentState c))

beginAgentOperation :: AgentClient -> AgentOperation -> STM ()
beginAgentOperation c op = do
  let opVar = agentOpSel op c
  s <- readTVar opVar
  -- unsafeIOToSTM $ putStrLn $ "beginOperation? " <> show op <> " " <> show (opsInProgress s)
  when (opSuspended s) retry
  -- unsafeIOToSTM $ putStrLn $ "beginOperation! " <> show op <> " " <> show (opsInProgress s + 1)
  writeTVar opVar $! s {opsInProgress = opsInProgress s + 1}

agentOperationBracket :: MonadUnliftIO m => AgentClient -> AgentOperation -> (AgentClient -> STM ()) -> m a -> m a
agentOperationBracket c op check action =
  E.bracket
    (atomically (check c) >> atomically (beginAgentOperation c op))
    (\_ -> atomically $ endAgentOperation c op)
    (const action)

waitUntilForeground :: AgentClient -> STM ()
waitUntilForeground c = unlessM ((ASForeground ==) <$> readTVar (agentState c)) retry

withStore' :: AgentMonad m => AgentClient -> (DB.Connection -> IO a) -> m a
withStore' = withStoreCtx_' Nothing

withStore :: AgentMonad m => AgentClient -> (DB.Connection -> IO (Either StoreError a)) -> m a
withStore = withStoreCtx_ Nothing

withStoreCtx' :: AgentMonad m => String -> AgentClient -> (DB.Connection -> IO a) -> m a
withStoreCtx' = withStoreCtx_' . Just

withStoreCtx :: AgentMonad m => String -> AgentClient -> (DB.Connection -> IO (Either StoreError a)) -> m a
withStoreCtx = withStoreCtx_ . Just

withStoreCtx_' :: AgentMonad m => Maybe String -> AgentClient -> (DB.Connection -> IO a) -> m a
withStoreCtx_' ctx_ c action = withStoreCtx_ ctx_ c $ fmap Right . action

withStoreCtx_ :: AgentMonad m => Maybe String -> AgentClient -> (DB.Connection -> IO (Either StoreError a)) -> m a
withStoreCtx_ ctx_ c action = do
  st <- asks store
  liftEitherError storeError . agentOperationBracket c AODatabase (\_ -> pure ()) $ case ctx_ of
    Nothing -> withTransaction st action `E.catch` handleInternal ""
    -- uncomment to debug store performance
    -- Just ctx -> do
    --   t1 <- liftIO getCurrentTime
    --   putStrLn $ "agent withStoreCtx start       :: " <> show t1 <> " :: " <> ctx
    --   r <- withTransaction st action `E.catch` handleInternal (" (" <> ctx <> ")")
    --   t2 <- liftIO getCurrentTime
    --   putStrLn $ "agent withStoreCtx end         :: " <> show t2 <> " :: " <> ctx <> " :: duration=" <> show (diffToMilliseconds $ diffUTCTime t2 t1)
    --   pure r
    Just _ -> withTransaction st action `E.catch` handleInternal ""
  where
    handleInternal :: String -> E.SomeException -> IO (Either StoreError a)
    handleInternal ctxStr e = pure . Left . SEInternal . B.pack $ show e <> ctxStr

withStoreBatch :: (AgentMonad' m, Traversable t) => AgentClient -> (DB.Connection -> t (IO (Either AgentErrorType a))) -> m (t (Either AgentErrorType a))
withStoreBatch c actions = do
  st <- asks store
  liftIO . agentOperationBracket c AODatabase (\_ -> pure ()) $
    withTransaction st $
      mapM (`E.catch` handleInternal) . actions
  where
    handleInternal :: E.SomeException -> IO (Either AgentErrorType a)
    handleInternal = pure . Left . INTERNAL . show

withStoreBatch' :: (AgentMonad' m, Traversable t) => AgentClient -> (DB.Connection -> t (IO a)) -> m (t (Either AgentErrorType a))
withStoreBatch' c actions = withStoreBatch c (fmap (fmap Right) . actions)

storeError :: StoreError -> AgentErrorType
storeError = \case
  SEConnNotFound -> CONN NOT_FOUND
  SERatchetNotFound -> CONN NOT_FOUND
  SEConnDuplicate -> CONN DUPLICATE
  SEBadConnType CRcv -> CONN SIMPLEX
  SEBadConnType CSnd -> CONN SIMPLEX
  SEInvitationNotFound -> CMD PROHIBITED
  -- this error is never reported as store error,
  -- it is used to wrap agent operations when "transaction-like" store access is needed
  -- NOTE: network IO should NOT be used inside AgentStoreMonad
  SEAgentError e -> e
  e -> INTERNAL $ show e

incStat :: AgentClient -> Int -> AgentStatsKey -> STM ()
incStat AgentClient {agentStats} n k = do
  TM.lookup k agentStats >>= \case
    Just v -> modifyTVar' v (+ n)
    _ -> newTVar n >>= \v -> TM.insert k v agentStats

incClientStat :: ProtocolServerClient err msg => AgentClient -> UserId -> Client msg -> ByteString -> ByteString -> IO ()
incClientStat c userId pc = incClientStatN c userId pc 1

incServerStat :: AgentClient -> UserId -> ProtocolServer p -> ByteString -> ByteString -> IO ()
incServerStat c userId ProtocolServer {host} cmd res = do
  threadDelay 100000
  atomically $ incStat c 1 statsKey
  where
    statsKey = AgentStatsKey {userId, host = strEncode $ L.head host, clientTs = "", cmd, res}

incClientStatN :: ProtocolServerClient err msg => AgentClient -> UserId -> Client msg -> Int -> ByteString -> ByteString -> IO ()
incClientStatN c userId pc n cmd res = do
  atomically $ incStat c n statsKey
  where
    statsKey = AgentStatsKey {userId, host = strEncode $ clientTransportHost pc, clientTs = strEncode $ clientSessionTs pc, cmd, res}

userServers :: forall p. (ProtocolTypeI p, UserProtocol p) => AgentClient -> TMap UserId (NonEmpty (ProtoServerWithAuth p))
userServers c = case protocolTypeI @p of
  SPSMP -> smpServers c
  SPXFTP -> xftpServers c

pickServer :: forall p m. AgentMonad' m => NonEmpty (ProtoServerWithAuth p) -> m (ProtoServerWithAuth p)
pickServer = \case
  srv :| [] -> pure srv
  servers -> do
    gen <- asks randomServer
    atomically $ (servers L.!!) <$> stateTVar gen (randomR (0, L.length servers - 1))

getNextServer :: forall p m. (ProtocolTypeI p, UserProtocol p, AgentMonad m) => AgentClient -> UserId -> [ProtocolServer p] -> m (ProtoServerWithAuth p)
getNextServer c userId usedSrvs = withUserServers c userId $ \srvs ->
  case L.nonEmpty $ deleteFirstsBy sameSrvAddr' (L.toList srvs) (map noAuthSrv usedSrvs) of
    Just srvs' -> pickServer srvs'
    _ -> pickServer srvs

withUserServers :: forall p m a. (ProtocolTypeI p, UserProtocol p, AgentMonad m) => AgentClient -> UserId -> (NonEmpty (ProtoServerWithAuth p) -> m a) -> m a
withUserServers c userId action =
  atomically (TM.lookup userId $ userServers c) >>= \case
    Just srvs -> action srvs
    _ -> throwError $ INTERNAL "unknown userId - no user servers"

withNextSrv :: forall p m a. (ProtocolTypeI p, UserProtocol p, AgentMonad m) => AgentClient -> UserId -> TVar [ProtocolServer p] -> [ProtocolServer p] -> (ProtoServerWithAuth p -> m a) -> m a
withNextSrv c userId usedSrvs initUsed action = do
  used <- readTVarIO usedSrvs
  srvAuth@(ProtoServerWithAuth srv _) <- getNextServer c userId used
  atomically $ do
    srvs_ <- TM.lookup userId $ userServers c
    let unused = maybe [] ((\\ used) . map protoServer . L.toList) srvs_
        used' = if null unused then initUsed else srv : used
    writeTVar usedSrvs $! used'
  action srvAuth

data SubInfo = SubInfo {userId :: UserId, server :: Text, rcvId :: Text, subError :: Maybe String}
  deriving (Show)

data SubscriptionsInfo = SubscriptionsInfo
  { activeSubscriptions :: [SubInfo],
    pendingSubscriptions :: [SubInfo],
    removedSubscriptions :: [SubInfo]
  }
  deriving (Show)

getAgentSubscriptions :: MonadIO m => AgentClient -> m SubscriptionsInfo
getAgentSubscriptions c = do
  activeSubscriptions <- getSubs activeSubs
  pendingSubscriptions <- getSubs pendingSubs
  removedSubscriptions <- getRemovedSubs
  pure $ SubscriptionsInfo {activeSubscriptions, pendingSubscriptions, removedSubscriptions}
  where
    getSubs sel = map (`subInfo` Nothing) . M.keys <$> readTVarIO (getRcvQueues $ sel c)
    getRemovedSubs = map (uncurry subInfo . second Just) . M.assocs <$> readTVarIO (removedSubs c)
    subInfo :: (UserId, SMPServer, SMP.RecipientId) -> Maybe SMPClientError -> SubInfo
    subInfo (uId, srv, rId) err = SubInfo {userId = uId, server = enc srv, rcvId = enc rId, subError = show <$> err}
    enc :: StrEncoding a => a -> Text
    enc = decodeLatin1 . strEncode

data AgentWorkersDetails = AgentWorkersDetails
  { smpClients_ :: [Text],
    ntfClients_ :: [Text],
    xftpClients_ :: [Text],
    smpDeliveryWorkers_ :: Map Text Int,
    asyncCmdWorkers_ :: Map Text Int,
    smpSubWorkers_ :: [Text],
    asyncCients_ :: [Int],
    ntfWorkers_ :: Map Text Int,
    ntfSMPWorkers_ :: Map Text Int,
    xftpRcvWorkers_ :: Map Text Int,
    xftpSndWorkers_ :: Map Text Int,
    xftpDelWorkers_ :: Map Text Int
  }
  deriving (Show)

getAgentWorkersDetails :: MonadIO m => AgentClient -> m AgentWorkersDetails
getAgentWorkersDetails AgentClient {smpClients, ntfClients, xftpClients, smpDeliveryWorkers, asyncCmdWorkers, smpSubWorkers, asyncClients = TAsyncs {actions}, agentEnv} = do
  smpClients_ <- textKeys <$> readTVarIO smpClients
  ntfClients_ <- textKeys <$> readTVarIO ntfClients
  xftpClients_ <- textKeys <$> readTVarIO xftpClients
  smpDeliveryWorkers_ <- workerStats =<< readTVarIO smpDeliveryWorkers
  asyncCmdWorkers_ <- workerStats =<< readTVarIO asyncCmdWorkers
  smpSubWorkers_ <- textKeys <$> readTVarIO smpSubWorkers
  asyncCients_ <- M.keys <$> readTVarIO actions
  ntfWorkers_ <- workerStats =<< readTVarIO ntfWorkers
  ntfSMPWorkers_ <- workerStats =<< readTVarIO ntfSMPWorkers
  xftpRcvWorkers_ <- workerStats =<< readTVarIO xftpRcvWorkers
  xftpSndWorkers_ <- workerStats =<< readTVarIO xftpSndWorkers
  xftpDelWorkers_ <- workerStats =<< readTVarIO xftpDelWorkers
  pure
    AgentWorkersDetails
      { smpClients_,
        ntfClients_,
        xftpClients_,
        smpDeliveryWorkers_,
        asyncCmdWorkers_,
        smpSubWorkers_,
        asyncCients_,
        ntfWorkers_,
        ntfSMPWorkers_,
        xftpRcvWorkers_,
        xftpSndWorkers_,
        xftpDelWorkers_
      }
  where
    textKeys :: StrEncoding k => Map k v -> [Text]
    textKeys = map textKey . M.keys
    textKey :: StrEncoding k => k -> Text
    textKey = decodeASCII . strEncode
    workerStats :: (StrEncoding k, MonadIO m) => Map k Worker -> m (Map Text Int)
    workerStats ws = fmap M.fromList . forM (M.toList ws) $ \(qa, Worker {restarts}) -> do
      RestartCount {restartCount} <- readTVarIO restarts
      pure (textKey qa, restartCount)
    Env {ntfSupervisor, xftpAgent} = agentEnv
    NtfSupervisor {ntfWorkers, ntfSMPWorkers} = ntfSupervisor
    XFTPAgent {xftpRcvWorkers, xftpSndWorkers, xftpDelWorkers} = xftpAgent

data AgentWorkersSummary = AgentWorkersSummary
  { smpClientsCount :: Int,
    ntfClientsCount :: Int,
    xftpClientsCount :: Int,
    smpDeliveryWorkersCount :: Int,
    asyncCmdWorkersCount :: Int,
    smpSubWorkersCount :: Int,
    asyncCientsCount :: Int,
    ntfWorkersCount :: Int,
    ntfSMPWorkersCount :: Int,
    xftpRcvWorkersCount :: Int,
    xftpSndWorkersCount :: Int,
    xftpDelWorkersCount :: Int
  }
  deriving (Show)

getAgentWorkersSummary :: MonadIO m => AgentClient -> m AgentWorkersSummary
getAgentWorkersSummary AgentClient {smpClients, ntfClients, xftpClients, smpDeliveryWorkers, asyncCmdWorkers, smpSubWorkers, asyncClients = TAsyncs {actions}, agentEnv} = do
  smpClientsCount <- M.size <$> readTVarIO smpClients
  ntfClientsCount <- M.size <$> readTVarIO ntfClients
  xftpClientsCount <- M.size <$> readTVarIO xftpClients
  smpDeliveryWorkersCount <- M.size <$> readTVarIO smpDeliveryWorkers
  asyncCmdWorkersCount <- M.size <$> readTVarIO asyncCmdWorkers
  smpSubWorkersCount <- M.size <$> readTVarIO smpSubWorkers
  asyncCientsCount <- M.size <$> readTVarIO actions
  ntfWorkersCount <- M.size <$> readTVarIO ntfWorkers
  ntfSMPWorkersCount <- M.size <$> readTVarIO ntfSMPWorkers
  xftpRcvWorkersCount <- M.size <$> readTVarIO xftpRcvWorkers
  xftpSndWorkersCount <- M.size <$> readTVarIO xftpSndWorkers
  xftpDelWorkersCount <- M.size <$> readTVarIO xftpDelWorkers
  pure
    AgentWorkersSummary
      { smpClientsCount,
        ntfClientsCount,
        xftpClientsCount,
        smpDeliveryWorkersCount,
        asyncCmdWorkersCount,
        smpSubWorkersCount,
        asyncCientsCount,
        ntfWorkersCount,
        ntfSMPWorkersCount,
        xftpRcvWorkersCount,
        xftpSndWorkersCount,
        xftpDelWorkersCount
      }
  where
    Env {ntfSupervisor, xftpAgent} = agentEnv
    NtfSupervisor {ntfWorkers, ntfSMPWorkers} = ntfSupervisor
    XFTPAgent {xftpRcvWorkers, xftpSndWorkers, xftpDelWorkers} = xftpAgent

$(J.deriveJSON defaultJSON ''AgentLocks)

$(J.deriveJSON (enumJSON $ dropPrefix "TS") ''ProtocolTestStep)

$(J.deriveJSON defaultJSON ''ProtocolTestFailure)

$(J.deriveJSON defaultJSON ''SubInfo)

$(J.deriveJSON defaultJSON ''SubscriptionsInfo)

$(J.deriveJSON defaultJSON {J.fieldLabelModifier = takeWhile (/= '_')} ''AgentWorkersDetails)

$(J.deriveJSON defaultJSON ''AgentWorkersSummary)
