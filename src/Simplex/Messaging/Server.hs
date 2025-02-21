{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Simplex.Messaging.Server
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- This module defines SMP protocol server with in-memory persistence
-- and optional append only log of SMP queue records.
--
-- See https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md
module Simplex.Messaging.Server
  ( runSMPServer,
    runSMPServerBlocking,
    importMessages,
    exportMessages,
    printMessageStats,
    disconnectTransport,
    verifyCmdAuthorization,
    dummyVerifyCmd,
    randomId,
    AttachHTTP,
    MessageStats (..),
  )
where

import Control.Concurrent.STM (throwSTM)
import Control.Logger.Simple
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Unlift
import Control.Monad.Reader
import Control.Monad.Trans.Except
import Control.Monad.STM (retry)
import Data.Bifunctor (first)
import Data.ByteString.Base64 (encode)
import qualified Data.ByteString.Builder as BLD
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Dynamic (toDyn)
import Data.Either (fromRight, partitionEithers)
import Data.Functor (($>))
import Data.IORef
import Data.Int (Int64)
import qualified Data.IntMap.Strict as IM
import qualified Data.IntSet as IS
import Data.List (foldl', intercalate, mapAccumR)
import Data.List.NonEmpty (NonEmpty (..), (<|))
import qualified Data.List.NonEmpty as L
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
import Data.Semigroup (Sum (..))
import qualified Data.Text as T
import Data.Text.Encoding (decodeLatin1)
import qualified Data.Text.IO as T
import Data.Time.Clock (UTCTime (..), diffTimeToPicoseconds, getCurrentTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Data.Time.Format.ISO8601 (iso8601Show)
import Data.Type.Equality
import Data.Typeable (cast)
import GHC.Conc.Signal
import GHC.IORef (atomicSwapIORef)
import GHC.Stats (getRTSStats)
import GHC.TypeLits (KnownNat)
import Network.Socket (ServiceName, Socket, socketToHandle)
import qualified Network.TLS as TLS
import Numeric.Natural (Natural)
import Simplex.Messaging.Agent.Lock
import Simplex.Messaging.Client (ProtocolClient (thParams), ProtocolClientError (..), SMPClient, SMPClientError, forwardSMPTransmission, smpProxyError, temporaryClientError)
import Simplex.Messaging.Client.Agent (OwnServer, SMPClientAgent (..), SMPClientAgentEvent (..), closeSMPClientAgent, getSMPServerClient'', isOwnServer, lookupSMPServerClient, getConnectedSMPServerClient)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.Control
import Simplex.Messaging.Server.Env.STM as Env
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Server.MsgStore
import Simplex.Messaging.Server.MsgStore.Journal (JournalMsgStore, JournalQueue, closeMsgQueue)
import Simplex.Messaging.Server.MsgStore.STM
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.NtfStore
import Simplex.Messaging.Server.Prometheus
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.QueueInfo
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Server.Stats
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport
import Simplex.Messaging.Transport.Buffer (trimCR)
import Simplex.Messaging.Transport.Server
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import System.Exit (exitFailure)
import System.IO (hPrint, hPutStrLn, hSetNewlineMode, universalNewlineMode)
import System.Mem.Weak (deRefWeak)
import UnliftIO (timeout)
import UnliftIO.Concurrent
import UnliftIO.Directory (doesFileExist, renameFile)
import UnliftIO.Exception
import UnliftIO.IO
import UnliftIO.STM
#if MIN_VERSION_base(4,18,0)
import Data.List (sort)
import GHC.Conc (listThreads, threadStatus)
import GHC.Conc.Sync (threadLabel)
#endif

-- | Runs an SMP server using passed configuration.
--
-- See a full server here: https://github.com/simplex-chat/simplexmq/blob/master/apps/smp-server/Main.hs
runSMPServer :: ServerConfig -> Maybe AttachHTTP -> IO ()
runSMPServer cfg attachHTTP_ = do
  started <- newEmptyTMVarIO
  runSMPServerBlocking started cfg attachHTTP_

-- | Runs an SMP server using passed configuration with signalling.
--
-- This function uses passed TMVar to signal when the server is ready to accept TCP requests (True)
-- and when it is disconnected from the TCP socket once the server thread is killed (False).
runSMPServerBlocking :: TMVar Bool -> ServerConfig -> Maybe AttachHTTP -> IO ()
runSMPServerBlocking started cfg attachHTTP_ = newEnv cfg >>= runReaderT (smpServer started cfg attachHTTP_)

type M a = ReaderT Env IO a
type AttachHTTP = Socket -> TLS.Context -> IO ()

data MessageStats = MessageStats
  { storedMsgsCount :: Int,
    expiredMsgsCount :: Int,
    storedQueues :: Int
  }

instance Monoid MessageStats where
  mempty = MessageStats 0 0 0
  {-# INLINE mempty #-}

instance Semigroup MessageStats where
  MessageStats a b c <> MessageStats x y z = MessageStats (a + x) (b + y) (c + z)
  {-# INLINE (<>) #-}

newMessageStats :: MessageStats
newMessageStats = MessageStats 0 0 0

smpServer :: TMVar Bool -> ServerConfig -> Maybe AttachHTTP -> M ()
smpServer started cfg@ServerConfig {transports, transportConfig = tCfg} attachHTTP_ = do
  s <- asks server
  pa <- asks proxyAgent
  msgStats_ <- processServerMessages
  ntfStats <- restoreServerNtfs
  liftIO $ mapM_ (printMessageStats "messages") msgStats_
  liftIO $ printMessageStats "notifications" ntfStats
  restoreServerStats msgStats_ ntfStats
  raceAny_
    ( serverThread s "server subscribedQ" subscribedQ subscribers subClients pendingSubEvents subscriptions cancelSub
        : serverThread s "server ntfSubscribedQ" ntfSubscribedQ Env.notifiers ntfSubClients pendingNtfSubEvents ntfSubscriptions (\_ -> pure ())
        : deliverNtfsThread s
        : sendPendingEvtsThread s
        : receiveFromProxyAgent pa
        : expireNtfsThread cfg
        : sigIntHandlerThread
        : map runServer transports
            <> expireMessagesThread_ cfg
            <> serverStatsThread_ cfg
            <> prometheusMetricsThread_ cfg
            <> controlPortThread_ cfg
    )
    `finally` stopServer s
  where
    runServer :: (ServiceName, ATransport, AddHTTP) -> M ()
    runServer (tcpPort, ATransport t, addHTTP) = do
      smpCreds <- asks tlsServerCreds
      httpCreds_ <- asks httpServerCreds
      ss <- liftIO newSocketState
      asks sockets >>= atomically . (`modifyTVar'` ((tcpPort, ss) :))
      serverSignKey <- either fail pure $ fromTLSCredentials smpCreds
      env <- ask
      liftIO $ case (httpCreds_, attachHTTP_) of
        (Just httpCreds, Just attachHTTP) | addHTTP ->
          runTransportServerState_ ss started tcpPort defaultSupportedParamsHTTPS chooseCreds (Just combinedALPNs) tCfg $ \s h ->
            case cast h of
              Just TLS {tlsContext} | maybe False (`elem` httpALPN) (getSessionALPN h) -> labelMyThread "https client" >> attachHTTP s tlsContext
              _ -> runClient serverSignKey t h `runReaderT` env
          where
            chooseCreds = maybe smpCreds (\_host -> httpCreds)
            combinedALPNs = supportedSMPHandshakes <> httpALPN
            httpALPN :: [ALPN]
            httpALPN = ["h2", "http/1.1"]
        _ ->
          runTransportServerState ss started tcpPort defaultSupportedParams smpCreds (Just supportedSMPHandshakes) tCfg $ \h -> runClient serverSignKey t h `runReaderT` env
    fromTLSCredentials (_, pk) = C.x509ToPrivate (pk, []) >>= C.privKey

    sigIntHandlerThread :: M ()
    sigIntHandlerThread = do
      flagINT <- newEmptyTMVarIO
      let sigINT = 2 -- CONST_SIGINT value
          sigIntAction = \_ptr -> atomically $ void $ tryPutTMVar flagINT ()
          sigIntHandler = Just (sigIntAction, toDyn ())
      void $ liftIO $ setHandler sigINT sigIntHandler
      atomically $ readTMVar flagINT
      logInfo "Received SIGINT, stopping server..."

    stopServer :: Server -> M ()
    stopServer s = do
      asks serverActive >>= atomically . (`writeTVar` False)
      logInfo "Saving server state..."
      withLock' (savingLock s) "final" $ saveServer True >> closeServer
      logInfo "Server stopped"

    saveServer :: Bool -> M ()
    saveServer drainMsgs = do
      ams@(AMS _ _ ms) <- asks msgStore
      liftIO $ saveServerMessages drainMsgs ams >> closeMsgStore ms
      saveServerNtfs
      saveServerStats

    closeServer :: M ()
    closeServer = asks (smpAgent . proxyAgent) >>= liftIO . closeSMPClientAgent

    serverThread ::
      forall s.
      Server ->
      String ->
      (Server -> TQueue (QueueId, ClientId, Subscribed)) ->
      (Server -> TMap QueueId (TVar AClient)) ->
      (Server -> TVar (IM.IntMap AClient)) ->
      (Server -> TVar (IM.IntMap (NonEmpty (QueueId, Subscribed)))) ->
      (forall st. Client st -> TMap QueueId s) ->
      (s -> IO ()) ->
      M ()
    serverThread s label subQ subs subClnts pendingEvts clientSubs unsub = do
      labelMyThread label
      cls <- asks clients
      liftIO . forever $
        (atomically (readTQueue $ subQ s) >>= atomically . updateSubscribers cls)
          $>>= endPreviousSubscriptions
          >>= mapM_ unsub
      where
        updateSubscribers :: TVar (IM.IntMap (Maybe AClient)) -> (QueueId, ClientId, Subscribed) -> STM (Maybe ((QueueId, Subscribed), AClient))
        updateSubscribers cls (qId, clntId, subscribed) =
          -- Client lookup by ID is in the same STM transaction.
          -- In case client disconnects during the transaction,
          -- it will be re-evaluated, and the client won't be stored as subscribed.
          (readTVar cls >>= updateSub . IM.lookup clntId)
            $>>= clientToBeNotified
          where
            ss = subs s
            updateSub = \case
              Just (Just clnt)
                | subscribed -> do
                    modifyTVar' (subClnts s) $ IM.insert clntId clnt -- add client to server's subscribed cients
                    TM.lookup qId ss >>= -- insert subscribed and current client
                      maybe
                        (newTVar clnt >>= \cv -> TM.insert qId cv ss $> Nothing)
                        (\cv -> Just <$> swapTVar cv clnt)
                | otherwise -> do
                    removeWhenNoSubs clnt
                    TM.lookupDelete qId ss >>= mapM readTVar
              -- This case catches Just Nothing - it cannot happen here.
              -- Nothing is there only before client thread is started.
              _ -> TM.lookup qId ss >>= mapM readTVar -- do not insert client if it is already disconnected, but send END to any other client
            clientToBeNotified ac@(AClient _ _ c')
              | clntId == clientId c' = pure Nothing
              | otherwise = (\yes -> if yes then Just ((qId, subscribed), ac) else Nothing) <$> readTVar (connected c')
        endPreviousSubscriptions :: ((QueueId, Subscribed), AClient) -> IO (Maybe s)
        endPreviousSubscriptions (qEvt@(qId, _), ac@(AClient _ _ c)) = do
          atomically $ modifyTVar' (pendingEvts s) $ IM.alter (Just . maybe [qEvt] (qEvt <|)) (clientId c)
          atomically $ do
            sub <- TM.lookupDelete qId (clientSubs c)
            removeWhenNoSubs ac $> sub
        -- remove client from server's subscribed cients
        removeWhenNoSubs (AClient _ _ c) = whenM (null <$> readTVar (clientSubs c)) $ modifyTVar' (subClnts s) $ IM.delete (clientId c)

    deliverNtfsThread :: Server -> M ()
    deliverNtfsThread Server {ntfSubClients} = do
      ntfInt <- asks $ ntfDeliveryInterval . config
      NtfStore ns <- asks ntfStore
      stats <- asks serverStats
      liftIO $ forever $ do
        threadDelay ntfInt
        readTVarIO ntfSubClients >>= mapM_ (deliverNtfs ns stats)
      where
        deliverNtfs ns stats (AClient _ _ Client {clientId, ntfSubscriptions, sndQ, connected}) =
          whenM (currentClient readTVarIO) $ do
            subs <- readTVarIO ntfSubscriptions
            ntfQs <- M.assocs . M.filterWithKey (\nId _ -> M.member nId subs) <$> readTVarIO ns
            tryAny (atomically $ flushSubscribedNtfs ntfQs) >>= \case
              Right len -> updateNtfStats len
              Left e -> logDebug $ "NOTIFICATIONS: cancelled for client #" <> tshow clientId <> ", reason: " <> tshow e
          where
            flushSubscribedNtfs :: [(NotifierId, TVar [MsgNtf])] -> STM Int
            flushSubscribedNtfs ntfQs = do
              ts_ <- foldM addNtfs [] ntfQs
              forM_ (L.nonEmpty ts_) $ \ts -> do
                let cancelNtfs s = throwSTM $ userError $ s <> ", " <> show (length ts_) <> " ntfs kept"
                unlessM (currentClient readTVar) $ cancelNtfs "not current client"
                whenM (isFullTBQueue sndQ) $ cancelNtfs "sending queue full"
                writeTBQueue sndQ ts
              pure $ length ts_
            currentClient :: Monad m => (forall a. TVar a -> m a) -> m Bool
            currentClient rd = (&&) <$> rd connected <*> (IM.member clientId <$> rd ntfSubClients)
            addNtfs :: [Transmission BrokerMsg] -> (NotifierId, TVar [MsgNtf]) -> STM [Transmission BrokerMsg]
            addNtfs acc (nId, v) =
              readTVar v >>= \case
                [] -> pure acc
                ntfs -> do
                  writeTVar v []
                  pure $ foldl' (\acc' ntf -> nmsg nId ntf : acc') acc ntfs -- reverses, to order by time
            nmsg nId MsgNtf {ntfNonce, ntfEncMeta} = (CorrId "", nId, NMSG ntfNonce ntfEncMeta)
            updateNtfStats 0 = pure ()
            updateNtfStats len = liftIO $ do
              atomicModifyIORef'_ (ntfCount stats) (subtract len)
              atomicModifyIORef'_ (msgNtfs stats) (+ len)
              atomicModifyIORef'_ (msgNtfsB stats) (+ (len `div` 80 + 1)) -- up to 80 NMSG in the batch

    sendPendingEvtsThread :: Server -> M ()
    sendPendingEvtsThread s = do
      endInt <- asks $ pendingENDInterval . config
      cls <- asks clients
      forever $ do
        threadDelay endInt
        sendPending cls $ pendingSubEvents s
        sendPending cls $ pendingNtfSubEvents s
      where
        sendPending cls ref = do
          ends <- atomically $ swapTVar ref IM.empty
          unless (null ends) $ forM_ (IM.assocs ends) $ \(cId, qEvts) ->
            mapM_ (queueEvts qEvts) . join . IM.lookup cId =<< readTVarIO cls
        queueEvts qEvts (AClient _ _ c@Client {connected, sndQ = q}) =
          whenM (readTVarIO connected) $ do
            sent <- atomically $ ifM (isFullTBQueue q) (pure False) (writeTBQueue q ts $> True)
            if sent
              then updateEndStats
              else -- if queue is full it can block
                forkClient c ("sendPendingEvtsThread.queueEvts") $
                  atomically (writeTBQueue q ts) >> updateEndStats
          where
            ts = L.map (\(qId, subscribed) -> (CorrId "", qId, evt subscribed)) qEvts
            evt True = END
            evt False = DELD
            -- this accounts for both END and DELD events
            updateEndStats = do
              stats <- asks serverStats
              let len = L.length qEvts
              when (len > 0) $ liftIO $ do
                atomicModifyIORef'_ (qSubEnd stats) (+ len)
                atomicModifyIORef'_ (qSubEndB stats) (+ (len `div` 255 + 1)) -- up to 255 ENDs or DELDs in the batch

    receiveFromProxyAgent :: ProxyAgent -> M ()
    receiveFromProxyAgent ProxyAgent {smpAgent = SMPClientAgent {agentQ}} =
      forever $
        atomically (readTBQueue agentQ) >>= \case
          CAConnected srv -> logInfo $ "SMP server connected " <> showServer' srv
          CADisconnected srv [] -> logInfo $ "SMP server disconnected " <> showServer' srv
          CADisconnected srv subs -> logError $ "SMP server disconnected " <> showServer' srv <> " / subscriptions: " <> tshow (length subs)
          CASubscribed srv _ subs -> logError $ "SMP server subscribed " <> showServer' srv <> " / subscriptions: " <> tshow (length subs)
          CASubError srv _ errs -> logError $ "SMP server subscription errors " <> showServer' srv <> " / errors: " <> tshow (length errs)
      where
        showServer' = decodeLatin1 . strEncode . host

    expireMessagesThread_ :: ServerConfig -> [M ()]
    expireMessagesThread_ ServerConfig {messageExpiration = Just msgExp} = [expireMessagesThread msgExp]
    expireMessagesThread_ _ = []

    expireMessagesThread :: ExpirationConfig -> M ()
    expireMessagesThread expCfg = do
      AMS _ _ ms <- asks msgStore
      let interval = checkInterval expCfg * 1000000
      stats <- asks serverStats
      labelMyThread "expireMessagesThread"
      liftIO $ forever $ do
        threadDelay' interval
        old <- expireBeforeEpoch expCfg
        now <- systemSeconds <$> getSystemTime
        msgStats@MessageStats {storedMsgsCount = stored, expiredMsgsCount = expired} <-
          withActiveMsgQueues ms $ expireQueueMsgs now ms old
        atomicWriteIORef (msgCount stats) stored
        atomicModifyIORef'_ (msgExpired stats) (+ expired)
        printMessageStats "STORE: messages" msgStats
      where
        expireQueueMsgs now ms old q = fmap (fromRight newMessageStats) . runExceptT $ do
          (expired_, stored) <- idleDeleteExpiredMsgs now ms q old
          pure MessageStats {storedMsgsCount = stored, expiredMsgsCount = fromMaybe 0 expired_, storedQueues = 1}

    expireNtfsThread :: ServerConfig -> M ()
    expireNtfsThread ServerConfig {notificationExpiration = expCfg} = do
      ns <- asks ntfStore
      let interval = checkInterval expCfg * 1000000
      stats <- asks serverStats
      labelMyThread "expireNtfsThread"
      liftIO $ forever $ do
        threadDelay' interval
        old <- expireBeforeEpoch expCfg
        expired <- deleteExpiredNtfs ns old
        when (expired > 0) $ do
          atomicModifyIORef'_ (msgNtfExpired stats) (+ expired)
          atomicModifyIORef'_ (ntfCount stats) (subtract expired)

    serverStatsThread_ :: ServerConfig -> [M ()]
    serverStatsThread_ ServerConfig {logStatsInterval = Just interval, logStatsStartTime, serverStatsLogFile} =
      [logServerStats logStatsStartTime interval serverStatsLogFile]
    serverStatsThread_ _ = []

    logServerStats :: Int64 -> Int64 -> FilePath -> M ()
    logServerStats startAt logInterval statsFilePath = do
      labelMyThread "logServerStats"
      initialDelay <- (startAt -) . fromIntegral . (`div` 1000000_000000) . diffTimeToPicoseconds . utctDayTime <$> liftIO getCurrentTime
      liftIO $ putStrLn $ "server stats log enabled: " <> statsFilePath
      liftIO $ threadDelay' $ 1000000 * (initialDelay + if initialDelay < 0 then 86400 else 0)
      ss@ServerStats {fromTime, qCreated, qSecured, qDeletedAll, qDeletedAllB, qDeletedNew, qDeletedSecured, qSub, qSubAllB, qSubAuth, qSubDuplicate, qSubProhibited, qSubEnd, qSubEndB, ntfCreated, ntfDeleted, ntfDeletedB, ntfSub, ntfSubB, ntfSubAuth, ntfSubDuplicate, msgSent, msgSentAuth, msgSentQuota, msgSentLarge, msgRecv, msgRecvGet, msgGet, msgGetNoMsg, msgGetAuth, msgGetDuplicate, msgGetProhibited, msgExpired, activeQueues, msgSentNtf, msgRecvNtf, activeQueuesNtf, qCount, msgCount, ntfCount, pRelays, pRelaysOwn, pMsgFwds, pMsgFwdsOwn, pMsgFwdsRecv}
        <- asks serverStats
      AMS _ _ (st :: s) <- asks msgStore
      QueueCounts {queueCount, notifierCount} <- liftIO $ queueCounts @(StoreQueue s) $ queueStore st
      let interval = 1000000 * logInterval
      forever $ do
        withFile statsFilePath AppendMode $ \h -> liftIO $ do
          hSetBuffering h LineBuffering
          ts <- getCurrentTime
          fromTime' <- atomicSwapIORef fromTime ts
          qCreated' <- atomicSwapIORef qCreated 0
          qSecured' <- atomicSwapIORef qSecured 0
          qDeletedAll' <- atomicSwapIORef qDeletedAll 0
          qDeletedAllB' <- atomicSwapIORef qDeletedAllB 0
          qDeletedNew' <- atomicSwapIORef qDeletedNew 0
          qDeletedSecured' <- atomicSwapIORef qDeletedSecured 0
          qSub' <- atomicSwapIORef qSub 0
          qSubAllB' <- atomicSwapIORef qSubAllB 0
          qSubAuth' <- atomicSwapIORef qSubAuth 0
          qSubDuplicate' <- atomicSwapIORef qSubDuplicate 0
          qSubProhibited' <- atomicSwapIORef qSubProhibited 0
          qSubEnd' <- atomicSwapIORef qSubEnd 0
          qSubEndB' <- atomicSwapIORef qSubEndB 0
          ntfCreated' <- atomicSwapIORef ntfCreated 0
          ntfDeleted' <- atomicSwapIORef ntfDeleted 0
          ntfDeletedB' <- atomicSwapIORef ntfDeletedB 0
          ntfSub' <- atomicSwapIORef ntfSub 0
          ntfSubB' <- atomicSwapIORef ntfSubB 0
          ntfSubAuth' <- atomicSwapIORef ntfSubAuth 0
          ntfSubDuplicate' <- atomicSwapIORef ntfSubDuplicate 0
          msgSent' <- atomicSwapIORef msgSent 0
          msgSentAuth' <- atomicSwapIORef msgSentAuth 0
          msgSentQuota' <- atomicSwapIORef msgSentQuota 0
          msgSentLarge' <- atomicSwapIORef msgSentLarge 0
          msgRecv' <- atomicSwapIORef msgRecv 0
          msgRecvGet' <- atomicSwapIORef msgRecvGet 0
          msgGet' <- atomicSwapIORef msgGet 0
          msgGetNoMsg' <- atomicSwapIORef msgGetNoMsg 0
          msgGetAuth' <- atomicSwapIORef msgGetAuth 0
          msgGetDuplicate' <- atomicSwapIORef msgGetDuplicate 0
          msgGetProhibited' <- atomicSwapIORef msgGetProhibited 0
          msgExpired' <- atomicSwapIORef msgExpired 0
          ps <- liftIO $ periodStatCounts activeQueues ts
          msgSentNtf' <- atomicSwapIORef msgSentNtf 0
          msgRecvNtf' <- atomicSwapIORef msgRecvNtf 0
          psNtf <- liftIO $ periodStatCounts activeQueuesNtf ts
          msgNtfs' <- atomicSwapIORef (msgNtfs ss) 0
          msgNtfsB' <- atomicSwapIORef (msgNtfsB ss) 0
          msgNtfNoSub' <- atomicSwapIORef (msgNtfNoSub ss) 0
          msgNtfLost' <- atomicSwapIORef (msgNtfLost ss) 0
          msgNtfExpired' <- atomicSwapIORef (msgNtfExpired ss) 0
          pRelays' <- getResetProxyStatsData pRelays
          pRelaysOwn' <- getResetProxyStatsData pRelaysOwn
          pMsgFwds' <- getResetProxyStatsData pMsgFwds
          pMsgFwdsOwn' <- getResetProxyStatsData pMsgFwdsOwn
          pMsgFwdsRecv' <- atomicSwapIORef pMsgFwdsRecv 0
          qCount' <- readIORef qCount
          msgCount' <- readIORef msgCount
          ntfCount' <- readIORef ntfCount
          hPutStrLn h $
            intercalate
              ","
              ( [ iso8601Show $ utctDay fromTime',
                  show qCreated',
                  show qSecured',
                  show qDeletedAll',
                  show msgSent',
                  show msgRecv',
                  dayCount ps,
                  weekCount ps,
                  monthCount ps,
                  show msgSentNtf',
                  show msgRecvNtf',
                  dayCount psNtf,
                  weekCount psNtf,
                  monthCount psNtf,
                  show qCount',
                  show msgCount',
                  show msgExpired',
                  show qDeletedNew',
                  show qDeletedSecured'
                ]
                  <> showProxyStats pRelays'
                  <> showProxyStats pRelaysOwn'
                  <> showProxyStats pMsgFwds'
                  <> showProxyStats pMsgFwdsOwn'
                  <> [ show pMsgFwdsRecv',
                       show qSub',
                       show qSubAuth',
                       show qSubDuplicate',
                       show qSubProhibited',
                       show msgSentAuth',
                       show msgSentQuota',
                       show msgSentLarge',
                       show msgNtfs',
                       show msgNtfNoSub',
                       show msgNtfLost',
                       "0", -- qSubNoMsg' is removed for performance.
                       -- Use qSubAllB for the approximate number of all subscriptions.
                       -- Average observed batch size is 25-30 subscriptions.
                       show msgRecvGet',
                       show msgGet',
                       show msgGetNoMsg',
                       show msgGetAuth',
                       show msgGetDuplicate',
                       show msgGetProhibited',
                       "0", -- dayCount psSub; psSub is removed to reduce memory usage
                       "0", -- weekCount psSub
                       "0", -- monthCount psSub
                       show queueCount,
                       show ntfCreated',
                       show ntfDeleted',
                       show ntfSub',
                       show ntfSubAuth',
                       show ntfSubDuplicate',
                       show notifierCount,
                       show qDeletedAllB',
                       show qSubAllB',
                       show qSubEnd',
                       show qSubEndB',
                       show ntfDeletedB',
                       show ntfSubB',
                       show msgNtfsB',
                       show msgNtfExpired',
                       show ntfCount'
                     ]
              )
        liftIO $ threadDelay' interval
      where
        showProxyStats ProxyStatsData {_pRequests, _pSuccesses, _pErrorsConnect, _pErrorsCompat, _pErrorsOther} =
          [show _pRequests, show _pSuccesses, show _pErrorsConnect, show _pErrorsCompat, show _pErrorsOther]

    prometheusMetricsThread_ :: ServerConfig -> [M ()]
    prometheusMetricsThread_ ServerConfig {prometheusInterval = Just interval, prometheusMetricsFile} =
      [savePrometheusMetrics interval prometheusMetricsFile]
    prometheusMetricsThread_ _ = []

    savePrometheusMetrics :: Int -> FilePath -> M ()
    savePrometheusMetrics saveInterval metricsFile = do
      labelMyThread "savePrometheusMetrics"
      liftIO $ putStrLn $ "Prometheus metrics saved every " <> show saveInterval <> " seconds to " <> metricsFile
      AMS _ _ st <- asks msgStore
      ss <- asks serverStats
      env <- ask
      let interval = 1000000 * saveInterval
      liftIO $ forever $ do
        threadDelay interval
        ts <- getCurrentTime
        sm <- getServerMetrics st ss
        rtm <- getRealTimeMetrics env
        T.writeFile metricsFile $ prometheusMetrics sm rtm ts

    getServerMetrics :: forall s. MsgStoreClass s => s -> ServerStats -> IO ServerMetrics
    getServerMetrics st ss = do
      d <- getServerStatsData ss
      let ps = periodStatDataCounts $ _activeQueues d
          psNtf = periodStatDataCounts $ _activeQueuesNtf d
      QueueCounts {queueCount, notifierCount} <- queueCounts @(StoreQueue s) $ queueStore st
      pure ServerMetrics {statsData = d, activeQueueCounts = ps, activeNtfCounts = psNtf, queueCount, notifierCount}

    getRealTimeMetrics :: Env -> IO RealTimeMetrics
    getRealTimeMetrics Env {clients, sockets, server = Server {subscribers, notifiers, subClients, ntfSubClients}} = do
      socketStats <- mapM (traverse getSocketStats) =<< readTVarIO sockets
#if MIN_VERSION_base(4,18,0)
      threadsCount <- length <$> listThreads
#else
      let threadsCount = 0
#endif
      clientsCount <- IM.size <$> readTVarIO clients
      smpSubsCount <- M.size <$> readTVarIO subscribers
      smpSubClientsCount <- IM.size <$> readTVarIO subClients
      ntfSubsCount <- M.size <$> readTVarIO notifiers
      ntfSubClientsCount <- IM.size <$> readTVarIO ntfSubClients
      pure RealTimeMetrics {socketStats, threadsCount, clientsCount, smpSubsCount, smpSubClientsCount, ntfSubsCount, ntfSubClientsCount}

    runClient :: Transport c => C.APrivateSignKey -> TProxy c -> c -> M ()
    runClient signKey tp h = do
      kh <- asks serverIdentity
      ks <- atomically . C.generateKeyPair =<< asks random
      ServerConfig {smpServerVRange, smpHandshakeTimeout} <- asks config
      labelMyThread $ "smp handshake for " <> transportName tp
      liftIO (timeout smpHandshakeTimeout . runExceptT $ smpServerHandshake signKey h ks kh smpServerVRange) >>= \case
        Just (Right th) -> runClientTransport th
        _ -> pure ()

    controlPortThread_ :: ServerConfig -> [M ()]
    controlPortThread_ ServerConfig {controlPort = Just port} = [runCPServer port]
    controlPortThread_ _ = []

    runCPServer :: ServiceName -> M ()
    runCPServer port = do
      srv <- asks server
      cpStarted <- newEmptyTMVarIO
      u <- askUnliftIO
      liftIO $ do
        labelMyThread "control port server"
        runLocalTCPServer cpStarted port $ runCPClient u srv
      where
        runCPClient :: UnliftIO (ReaderT Env IO) -> Server -> Socket -> IO ()
        runCPClient u srv sock = do
          labelMyThread "control port client"
          h <- socketToHandle sock ReadWriteMode
          hSetBuffering h LineBuffering
          hSetNewlineMode h universalNewlineMode
          hPutStrLn h "SMP server control port\n'help' for supported commands"
          role <- newTVarIO CPRNone
          cpLoop h role
          where
            cpLoop h role = do
              s <- trimCR <$> B.hGetLine h
              case strDecode s of
                Right CPQuit -> hClose h
                Right cmd -> logCmd s cmd >> processCP h role cmd >> cpLoop h role
                Left err -> hPutStrLn h ("error: " <> err) >> cpLoop h role
            logCmd s cmd = when shouldLog $ logWarn $ "ControlPort: " <> tshow s
              where
                shouldLog = case cmd of
                  CPAuth _ -> False
                  CPHelp -> False
                  CPQuit -> False
                  CPSkip -> False
                  _ -> True
            processCP h role = \case
              CPAuth auth -> atomically $ writeTVar role $! newRole cfg
                where
                  newRole ServerConfig {controlPortUserAuth = user, controlPortAdminAuth = admin}
                    | Just auth == admin = CPRAdmin
                    | Just auth == user = CPRUser
                    | otherwise = CPRNone
              CPSuspend -> withAdminRole $ hPutStrLn h "suspend not implemented"
              CPResume -> withAdminRole $ hPutStrLn h "resume not implemented"
              CPClients -> withAdminRole $ do
                active <- unliftIO u (asks clients) >>= readTVarIO
                hPutStrLn h "clientId,sessionId,connected,createdAt,rcvActiveAt,sndActiveAt,age,subscriptions"
                forM_ (IM.toList active) $ \(cid, cl) -> forM_ cl $ \(AClient _ _ Client {sessionId, connected, createdAt, rcvActiveAt, sndActiveAt, subscriptions}) -> do
                  connected' <- bshow <$> readTVarIO connected
                  rcvActiveAt' <- strEncode <$> readTVarIO rcvActiveAt
                  sndActiveAt' <- strEncode <$> readTVarIO sndActiveAt
                  now <- liftIO getSystemTime
                  let age = systemSeconds now - systemSeconds createdAt
                  subscriptions' <- bshow . M.size <$> readTVarIO subscriptions
                  hPutStrLn h . B.unpack $ B.intercalate "," [bshow cid, encode sessionId, connected', strEncode createdAt, rcvActiveAt', sndActiveAt', bshow age, subscriptions']
              CPStats -> withUserRole $ do
                ss <- unliftIO u $ asks serverStats
                AMS _ _ (st :: s) <- unliftIO u $ asks msgStore
                QueueCounts {queueCount, notifierCount} <- queueCounts @(StoreQueue s) $ queueStore st
                let getStat :: (ServerStats -> IORef a) -> IO a
                    getStat var = readIORef (var ss)
                    putStat :: Show a => String -> (ServerStats -> IORef a) -> IO ()
                    putStat label var = getStat var >>= \v -> hPutStrLn h $ label <> ": " <> show v
                    putProxyStat :: String -> (ServerStats -> ProxyStats) -> IO ()
                    putProxyStat label var = do
                      ProxyStatsData {_pRequests, _pSuccesses, _pErrorsConnect, _pErrorsCompat, _pErrorsOther} <- getProxyStatsData $ var ss
                      hPutStrLn h $ label <> ": requests=" <> show _pRequests <> ", successes=" <> show _pSuccesses <> ", errorsConnect=" <> show _pErrorsConnect <> ", errorsCompat=" <> show _pErrorsCompat <> ", errorsOther=" <> show _pErrorsOther
                putStat "fromTime" fromTime
                putStat "qCreated" qCreated
                putStat "qSecured" qSecured
                putStat "qDeletedAll" qDeletedAll
                putStat "qDeletedAllB" qDeletedAllB
                putStat "qDeletedNew" qDeletedNew
                putStat "qDeletedSecured" qDeletedSecured
                getStat (day . activeQueues) >>= \v -> hPutStrLn h $ "daily active queues: " <> show (IS.size v)
                -- removed to reduce memory usage
                -- getStat (day . subscribedQueues) >>= \v -> hPutStrLn h $ "daily subscribed queues: " <> show (S.size v)
                putStat "qSub" qSub
                putStat "qSubAllB" qSubAllB
                putStat "qSubEnd" qSubEnd
                putStat "qSubEndB" qSubEndB
                subs <- (,,) <$> getStat qSubAuth <*> getStat qSubDuplicate <*> getStat qSubProhibited
                hPutStrLn h $ "other SUB events (auth, duplicate, prohibited): " <> show subs
                putStat "msgSent" msgSent
                putStat "msgRecv" msgRecv
                putStat "msgRecvGet" msgRecvGet
                putStat "msgGet" msgGet
                putStat "msgGetNoMsg" msgGetNoMsg
                gets <- (,,) <$> getStat msgGetAuth <*> getStat msgGetDuplicate <*> getStat msgGetProhibited
                hPutStrLn h $ "other GET events (auth, duplicate, prohibited): " <> show gets
                putStat "msgSentNtf" msgSentNtf
                putStat "msgRecvNtf" msgRecvNtf
                putStat "msgNtfs" msgNtfs
                putStat "msgNtfsB" msgNtfsB
                putStat "msgNtfExpired" msgNtfExpired
                putStat "qCount" qCount
                hPutStrLn h $ "qCount 2: " <> show queueCount
                hPutStrLn h $ "notifiers: " <> show notifierCount
                putStat "msgCount" msgCount
                putStat "ntfCount" ntfCount
                readTVarIO role >>= \case
                  CPRAdmin -> do
                    NtfStore ns <- unliftIO u $ asks ntfStore
                    ntfCount2 <- liftIO . foldM (\(!n) q -> (n +) . length <$> readTVarIO q) 0 =<< readTVarIO ns
                    hPutStrLn h $ "ntfCount 2: " <> show ntfCount2
                  _ -> pure ()
                putProxyStat "pRelays" pRelays
                putProxyStat "pRelaysOwn" pRelaysOwn
                putProxyStat "pMsgFwds" pMsgFwds
                putProxyStat "pMsgFwdsOwn" pMsgFwdsOwn
                putStat "pMsgFwdsRecv" pMsgFwdsRecv
              CPStatsRTS -> getRTSStats >>= hPrint h
              CPThreads -> withAdminRole $ do
#if MIN_VERSION_base(4,18,0)
                threads <- liftIO listThreads
                hPutStrLn h $ "Threads: " <> show (length threads)
                forM_ (sort threads) $ \tid -> do
                  label <- threadLabel tid
                  status <- threadStatus tid
                  hPutStrLn h $ show tid <> " (" <> show status <> ") " <> fromMaybe "" label
#else
                hPutStrLn h "Not available on GHC 8.10"
#endif
              CPSockets -> withUserRole $ unliftIO u (asks sockets) >>= readTVarIO >>= mapM_ putSockets
                where
                  putSockets (tcpPort, socketsState) = do
                    ss <- getSocketStats socketsState
                    hPutStrLn h $ "Sockets for port " <> tcpPort <> ":"
                    hPutStrLn h $ "accepted: " <> show (socketsAccepted ss)
                    hPutStrLn h $ "closed: " <> show (socketsClosed ss)
                    hPutStrLn h $ "active: " <> show (socketsActive ss)
                    hPutStrLn h $ "leaked: " <> show (socketsLeaked ss)
              CPSocketThreads -> withAdminRole $ do
#if MIN_VERSION_base(4,18,0)
                unliftIO u (asks sockets) >>= readTVarIO >>= mapM_ putSocketThreads
                where
                  putSocketThreads (tcpPort, (_, _, active')) = do
                    active <- readTVarIO active'
                    forM_ (IM.toList active) $ \(sid, tid') ->
                      deRefWeak tid' >>= \case
                        Nothing -> hPutStrLn h $ intercalate "," [tcpPort, show sid, "", "gone", ""]
                        Just tid -> do
                          label <- threadLabel tid
                          status <- threadStatus tid
                          hPutStrLn h $ intercalate "," [tcpPort, show sid, show tid, show status, fromMaybe "" label]
#else
                hPutStrLn h "Not available on GHC 8.10"
#endif
              CPServerInfo -> readTVarIO role >>= \case
                CPRNone -> do
                  logError "Unauthorized control port command"
                  hPutStrLn h "AUTH"
                r -> do
#if MIN_VERSION_base(4,18,0)
                  threads <- liftIO listThreads
                  hPutStrLn h $ "Threads: " <> show (length threads)
#else
                  hPutStrLn h "Threads: not available on GHC 8.10"
#endif
                  Env {clients, server = Server {subscribers, notifiers, subClients, ntfSubClients}} <- unliftIO u ask
                  activeClients <- readTVarIO clients
                  hPutStrLn h $ "Clients: " <> show (IM.size activeClients)
                  when (r == CPRAdmin) $ do
                    clQs <- clientTBQueueLengths' activeClients
                    hPutStrLn h $ "Client queues (rcvQ, sndQ, msgQ): " <> show clQs
                    (smpSubCnt, smpSubCntByGroup, smpClCnt, smpClQs) <- countClientSubs subscriptions (Just countSMPSubs) activeClients
                    hPutStrLn h $ "SMP subscriptions (via clients): " <> show smpSubCnt
                    hPutStrLn h $ "SMP subscriptions (by group: NoSub, SubPending, SubThread, ProhibitSub): " <> show smpSubCntByGroup
                    hPutStrLn h $ "SMP subscribed clients (via clients): " <> show smpClCnt
                    hPutStrLn h $ "SMP subscribed clients queues (via clients, rcvQ, sndQ, msgQ): " <> show smpClQs
                    (ntfSubCnt, _, ntfClCnt, ntfClQs) <- countClientSubs ntfSubscriptions Nothing activeClients
                    hPutStrLn h $ "Ntf subscriptions (via clients): " <> show ntfSubCnt
                    hPutStrLn h $ "Ntf subscribed clients (via clients): " <> show ntfClCnt
                    hPutStrLn h $ "Ntf subscribed clients queues (via clients, rcvQ, sndQ, msgQ): " <> show ntfClQs
                  putActiveClientsInfo "SMP" subscribers False
                  putActiveClientsInfo "Ntf" notifiers True
                  putSubscribedClients "SMP" subClients False
                  putSubscribedClients "Ntf" ntfSubClients True
                  where
                    putActiveClientsInfo :: String -> TMap QueueId (TVar AClient) -> Bool -> IO ()
                    putActiveClientsInfo protoName clients showIds = do
                      activeSubs <- readTVarIO clients
                      hPutStrLn h $ protoName <> " subscriptions: " <> show (M.size activeSubs)
                      clnts <- countSubClients activeSubs
                      hPutStrLn h $ protoName <> " subscribed clients: " <> show (IS.size clnts) <> (if showIds then " " <> show (IS.toList clnts) else "")
                      where
                        countSubClients :: M.Map QueueId (TVar AClient) -> IO IS.IntSet
                        countSubClients = foldM (\ !s c -> (`IS.insert` s) . clientId' <$> readTVarIO c) IS.empty
                    putSubscribedClients :: String -> TVar (IM.IntMap AClient) -> Bool -> IO ()
                    putSubscribedClients protoName subClnts showIds = do
                      clnts <- readTVarIO subClnts
                      hPutStrLn h $ protoName <> " subscribed clients count 2: " <> show (IM.size clnts) <> (if showIds then " " <> show (IM.keys clnts) else "")
                    countClientSubs :: (forall s. Client s -> TMap QueueId a) -> Maybe (M.Map QueueId a -> IO (Int, Int, Int, Int)) -> IM.IntMap (Maybe AClient) -> IO (Int, (Int, Int, Int, Int), Int, (Natural, Natural, Natural))
                    countClientSubs subSel countSubs_ = foldM addSubs (0, (0, 0, 0, 0), 0, (0, 0, 0))
                      where
                        addSubs :: (Int, (Int, Int, Int, Int), Int, (Natural, Natural, Natural)) -> Maybe AClient -> IO (Int, (Int, Int, Int, Int), Int, (Natural, Natural, Natural))
                        addSubs acc Nothing = pure acc
                        addSubs (!subCnt, cnts@(!c1, !c2, !c3, !c4), !clCnt, !qs) (Just acl@(AClient _ _ cl)) = do
                          subs <- readTVarIO $ subSel cl
                          cnts' <- case countSubs_ of
                            Nothing -> pure cnts
                            Just countSubs -> do
                              (c1', c2', c3', c4') <- countSubs subs
                              pure (c1 + c1', c2 + c2', c3 + c3', c4 + c4')
                          let cnt = M.size subs
                              clCnt' = if cnt == 0 then clCnt else clCnt + 1
                          qs' <- if cnt == 0 then pure qs else addQueueLengths qs acl
                          pure (subCnt + cnt, cnts', clCnt', qs')
                    clientTBQueueLengths' :: Foldable t => t (Maybe AClient) -> IO (Natural, Natural, Natural)
                    clientTBQueueLengths' = foldM (\acc -> maybe (pure acc) (addQueueLengths acc)) (0, 0, 0)
                    addQueueLengths (!rl, !sl, !ml) (AClient _ _ cl) = do
                      (rl', sl', ml') <- queueLengths cl
                      pure (rl + rl', sl + sl', ml + ml')
                    queueLengths Client {rcvQ, sndQ, msgQ} = do
                      rl <- atomically $ lengthTBQueue rcvQ
                      sl <- atomically $ lengthTBQueue sndQ
                      ml <- atomically $ lengthTBQueue msgQ
                      pure (rl, sl, ml)
                    countSMPSubs :: M.Map QueueId Sub -> IO (Int, Int, Int, Int)
                    countSMPSubs = foldM countSubs (0, 0, 0, 0)
                      where
                        countSubs (c1, c2, c3, c4) Sub {subThread} = case subThread of
                          ServerSub t -> do
                            st <- readTVarIO t
                            pure $ case st of
                              NoSub -> (c1 + 1, c2, c3, c4)
                              SubPending -> (c1, c2 + 1, c3, c4)
                              SubThread _ -> (c1, c2, c3 + 1, c4)
                          ProhibitSub -> pure (c1, c2, c3, c4 + 1)
              CPDelete sId -> withUserRole $ unliftIO u $ do
                AMS _ _ st <- asks msgStore
                r <- liftIO $ runExceptT $ do
                  q <- ExceptT $ getQueue st SSender sId
                  ExceptT $ deleteQueueSize st q
                case r of
                  Left e -> liftIO $ hPutStrLn h $ "error: " <> show e
                  Right (qr, numDeleted) -> do
                    updateDeletedStats qr
                    liftIO $ hPutStrLn h $ "ok, " <> show numDeleted <> " messages deleted"
              CPStatus sId -> withUserRole $ unliftIO u $ do
                AMS _ _ st <- asks msgStore
                q <- liftIO $ getQueueRec st SSender sId
                liftIO $ hPutStrLn h $ case q of
                  Left e -> "error: " <> show e
                  Right (_, QueueRec {sndSecure, status, updatedAt}) ->
                    "status: " <> show status <> ", updatedAt: " <> show updatedAt <> ", sndSecure: " <> show sndSecure
              CPBlock sId info -> withUserRole $ unliftIO u $ do
                AMS _ _ (st :: s) <- asks msgStore
                r <- liftIO $ runExceptT $ do
                  q <- ExceptT $ getQueue st SSender sId
                  ExceptT $ blockQueue (queueStore st) q info
                case r of
                  Left e -> liftIO $ hPutStrLn h $ "error: " <> show e
                  Right () -> do
                    incStat . qBlocked =<< asks serverStats
                    liftIO $ hPutStrLn h "ok"
              CPUnblock sId -> withUserRole $ unliftIO u $ do
                AMS _ _ (st :: s) <- asks msgStore
                r <- liftIO $ runExceptT $ do
                  q <- ExceptT $ getQueue st SSender sId
                  ExceptT $ unblockQueue (queueStore st) q
                liftIO $ hPutStrLn h $ case r of
                  Left e -> "error: " <> show e
                  Right () -> "ok"
              CPSave -> withAdminRole $ withLock' (savingLock srv) "control" $ do
                hPutStrLn h "saving server state..."
                unliftIO u $ saveServer False
                hPutStrLn h "server state saved!"
              CPHelp -> hPutStrLn h "commands: stats, stats-rts, clients, sockets, socket-threads, threads, server-info, delete, save, help, quit"
              CPQuit -> pure ()
              CPSkip -> pure ()
              where
                withUserRole action = readTVarIO role >>= \case
                  CPRAdmin -> action
                  CPRUser -> action
                  _ -> do
                    logError "Unauthorized control port command"
                    hPutStrLn h "AUTH"
                withAdminRole action = readTVarIO role >>= \case
                  CPRAdmin -> action
                  _ -> do
                    logError "Unauthorized control port command"
                    hPutStrLn h "AUTH"

runClientTransport :: Transport c => THandleSMP c 'TServer -> M ()
runClientTransport h@THandle {params = thParams@THandleParams {thVersion, sessionId}} = do
  q <- asks $ tbqSize . config
  ts <- liftIO getSystemTime
  active <- asks clients
  nextClientId <- asks clientSeq
  clientId <- atomically $ stateTVar nextClientId $ \next -> (next, next + 1)
  atomically $ modifyTVar' active $ IM.insert clientId Nothing
  AMS qt mt ms <- asks msgStore
  c <- liftIO $ newClient qt mt clientId q thVersion sessionId ts
  runClientThreads qt mt ms active c clientId `finally` clientDisconnected c
  where
    runClientThreads :: MsgStoreClass (MsgStore qs ms) => SQSType qs -> SMSType ms -> MsgStore qs ms -> TVar (IM.IntMap (Maybe AClient)) -> Client (MsgStore qs ms) -> IS.Key -> M ()
    runClientThreads qt mt ms active c clientId = do
      atomically $ modifyTVar' active $ IM.insert clientId $ Just (AClient qt mt c)
      s <- asks server
      expCfg <- asks $ inactiveClientExpiration . config
      th <- newMVar h -- put TH under a fair lock to interleave messages and command responses
      labelMyThread . B.unpack $ "client $" <> encode sessionId
      raceAny_ $ [liftIO $ send th c, liftIO $ sendMsg th c, client thParams s ms c, receive h ms c] <> disconnectThread_ c s expCfg
    disconnectThread_ :: Client s -> Server -> Maybe ExpirationConfig -> [M ()]
    disconnectThread_ c s (Just expCfg) = [liftIO $ disconnectTransport h (rcvActiveAt c) (sndActiveAt c) expCfg (noSubscriptions c s)]
    disconnectThread_ _ _ _ = []
    noSubscriptions Client {clientId} s = do
      hasSubs <- IM.member clientId <$> readTVarIO (subClients s)
      if hasSubs
        then pure False
        else not . IM.member clientId <$> readTVarIO (ntfSubClients s)

clientDisconnected :: Client s -> M ()
clientDisconnected c@Client {clientId, subscriptions, ntfSubscriptions, connected, sessionId, endThreads} = do
  labelMyThread . B.unpack $ "client $" <> encode sessionId <> " disc"
  -- these can be in separate transactions,
  -- because the client already disconnected and they won't change
  atomically $ writeTVar connected False
  subs <- atomically $ swapTVar subscriptions M.empty
  ntfSubs <- atomically $ swapTVar ntfSubscriptions M.empty
  liftIO $ mapM_ cancelSub subs
  whenM (asks serverActive >>= readTVarIO) $ do
    Server {subscribers, notifiers, subClients, ntfSubClients} <- asks server
    liftIO $ updateSubscribers subs subscribers
    liftIO $ updateSubscribers ntfSubs notifiers
    asks clients >>= atomically . (`modifyTVar'` IM.delete clientId)
    atomically $ modifyTVar' subClients $ IM.delete clientId
    atomically $ modifyTVar' ntfSubClients $ IM.delete clientId
  tIds <- atomically $ swapTVar endThreads IM.empty
  liftIO $ mapM_ (mapM_ killThread <=< deRefWeak) tIds
  where
    updateSubscribers :: M.Map QueueId a -> TMap QueueId (TVar AClient) -> IO ()
    updateSubscribers subs srvSubs =
      forM_ (M.keys subs) $ \qId ->
        -- lookup of the subscribed client TVar can be in separate transaction,
        -- as long as the client is read in the same transaction -
        -- it prevents removing the next subscribed client.
        TM.lookupIO qId srvSubs >>=
          mapM_ (\c' -> atomically $ whenM (sameClientId c <$> readTVar c') $ TM.delete qId srvSubs)

sameClientId :: Client s -> AClient -> Bool
sameClientId Client {clientId} ac = clientId == clientId' ac

cancelSub :: Sub -> IO ()
cancelSub s = case subThread s of
  ServerSub st ->
    readTVarIO st >>= \case
      SubThread t -> liftIO $ deRefWeak t >>= mapM_ killThread
      _ -> pure ()
  ProhibitSub -> pure ()

receive :: forall c s. (Transport c, MsgStoreClass s) => THandleSMP c 'TServer -> s -> Client s -> M ()
receive h@THandle {params = THandleParams {thAuth}} ms Client {rcvQ, sndQ, rcvActiveAt, sessionId} = do
  labelMyThread . B.unpack $ "client $" <> encode sessionId <> " receive"
  sa <- asks serverActive
  forever $ do
    ts <- L.toList <$> liftIO (tGet h)
    unlessM (readTVarIO sa) $ throwIO $ userError "server stopped"
    atomically . (writeTVar rcvActiveAt $!) =<< liftIO getSystemTime
    stats <- asks serverStats
    (errs, cmds) <- partitionEithers <$> mapM (cmdAction stats) ts
    updateBatchStats stats cmds
    write sndQ errs
    write rcvQ cmds
  where
    updateBatchStats :: ServerStats -> [(Maybe (StoreQueue s, QueueRec), Transmission Cmd)] -> M ()
    updateBatchStats stats = \case
      (_, (_, _, (Cmd _ cmd))) : _ -> do
        let sel_ = case cmd of
              SUB -> Just qSubAllB
              DEL -> Just qDeletedAllB
              NSUB -> Just ntfSubB
              NDEL -> Just ntfDeletedB
              _ -> Nothing
        mapM_ (\sel -> incStat $ sel stats) sel_
      [] -> pure ()
    cmdAction :: ServerStats -> SignedTransmission ErrorType Cmd -> M (Either (Transmission BrokerMsg) (Maybe (StoreQueue s, QueueRec), Transmission Cmd))
    cmdAction stats (tAuth, authorized, (corrId, entId, cmdOrError)) =
      case cmdOrError of
        Left e -> pure $ Left (corrId, entId, ERR e)
        Right cmd -> verified =<< verifyTransmission ms ((,C.cbNonce (bs corrId)) <$> thAuth) tAuth authorized entId cmd
          where
            verified = \case
              VRVerified q -> pure $ Right (q, (corrId, entId, cmd))
              VRFailed -> do
                case cmd of
                  Cmd _ SEND {} -> incStat $ msgSentAuth stats
                  Cmd _ SUB -> incStat $ qSubAuth stats
                  Cmd _ NSUB -> incStat $ ntfSubAuth stats
                  Cmd _ GET -> incStat $ msgGetAuth stats
                  _ -> pure ()
                pure $ Left (corrId, entId, ERR AUTH)
    write q = mapM_ (atomically . writeTBQueue q) . L.nonEmpty

send :: Transport c => MVar (THandleSMP c 'TServer) -> Client s -> IO ()
send th c@Client {sndQ, msgQ, sessionId} = do
  labelMyThread . B.unpack $ "client $" <> encode sessionId <> " send"
  forever $ atomically (readTBQueue sndQ) >>= sendTransmissions
  where
    sendTransmissions :: NonEmpty (Transmission BrokerMsg) -> IO ()
    sendTransmissions ts
      | L.length ts <= 2 = tSend th c ts
      | otherwise = do
          let (msgs_, ts') = mapAccumR splitMessages [] ts
          -- If the request had batched subscriptions and L.length ts > 2
          -- this will reply OK to all SUBs in the first batched transmission,
          -- to reduce client timeouts.
          tSend th c ts'
          -- After that all messages will be sent in separate transmissions,
          -- without any client response timeouts, and allowing them to interleave
          -- with other requests responses.
          mapM_ (atomically . writeTBQueue msgQ) $ L.nonEmpty msgs_
      where
        splitMessages :: [Transmission BrokerMsg] -> Transmission BrokerMsg -> ([Transmission BrokerMsg], Transmission BrokerMsg)
        splitMessages msgs t@(corrId, entId, cmd) = case cmd of
          -- replace MSG response with OK, accumulating MSG in a separate list.
          MSG {} -> ((CorrId "", entId, cmd) : msgs, (corrId, entId, OK))
          _ -> (msgs, t)

sendMsg :: Transport c => MVar (THandleSMP c 'TServer) -> Client s -> IO ()
sendMsg th c@Client {msgQ, sessionId} = do
  labelMyThread . B.unpack $ "client $" <> encode sessionId <> " sendMsg"
  forever $ atomically (readTBQueue msgQ) >>= mapM_ (\t -> tSend th c [t])

tSend :: Transport c => MVar (THandleSMP c 'TServer) -> Client s -> NonEmpty (Transmission BrokerMsg) -> IO ()
tSend th Client {sndActiveAt} ts = do
  withMVar th $ \h@THandle {params} ->
    void . tPut h $ L.map (\t -> Right (Nothing, encodeTransmission params t)) ts
  atomically . (writeTVar sndActiveAt $!) =<< liftIO getSystemTime

disconnectTransport :: Transport c => THandle v c 'TServer -> TVar SystemTime -> TVar SystemTime -> ExpirationConfig -> IO Bool -> IO ()
disconnectTransport THandle {connection, params = THandleParams {sessionId}} rcvActiveAt sndActiveAt expCfg noSubscriptions = do
  labelMyThread . B.unpack $ "client $" <> encode sessionId <> " disconnectTransport"
  loop
  where
    loop = do
      threadDelay' $ checkInterval expCfg * 1000000
      ifM noSubscriptions checkExpired loop
    checkExpired = do
      old <- expireBeforeEpoch expCfg
      ts <- max <$> readTVarIO rcvActiveAt <*> readTVarIO sndActiveAt
      if systemSeconds ts < old then closeConnection connection else loop

data VerificationResult s = VRVerified (Maybe (StoreQueue s, QueueRec)) | VRFailed

-- This function verifies queue command authorization, with the objective to have constant time between the three AUTH error scenarios:
-- - the queue and party key exist, and the provided authorization has type matching queue key, but it is made with the different key.
-- - the queue and party key exist, but the provided authorization has incorrect type.
-- - the queue or party key do not exist.
-- In all cases, the time of the verification should depend only on the provided authorization type,
-- a dummy key is used to run verification in the last two cases, and failure is returned irrespective of the result.
verifyTransmission :: forall s. MsgStoreClass s => s -> Maybe (THandleAuth 'TServer, C.CbNonce) -> Maybe TransmissionAuth -> ByteString -> QueueId -> Cmd -> M (VerificationResult s)
verifyTransmission ms auth_ tAuth authorized queueId cmd =
  case cmd of
    Cmd SRecipient (NEW k _ _ _ _) -> pure $ Nothing `verifiedWith` k
    Cmd SRecipient _ -> verifyQueue (\q -> Just q `verifiedWith` recipientKey (snd q)) <$> get SRecipient
    -- SEND will be accepted without authorization before the queue is secured with KEY or SKEY command
    Cmd SSender (SKEY k) -> verifyQueue (\q -> if maybe True (k ==) (senderKey $ snd q) then Just q `verifiedWith` k else dummyVerify) <$> get SSender
    Cmd SSender SEND {} -> verifyQueue (\q -> Just q `verified` maybe (isNothing tAuth) verify (senderKey $ snd q)) <$> get SSender
    Cmd SSender PING -> pure $ VRVerified Nothing
    Cmd SSender RFWD {} -> pure $ VRVerified Nothing
    -- NSUB will not be accepted without authorization
    Cmd SNotifier NSUB -> verifyQueue (\q -> maybe dummyVerify (\n -> Just q `verifiedWith` notifierKey n) (notifier $ snd q)) <$> get SNotifier
    Cmd SProxiedClient _ -> pure $ VRVerified Nothing
  where
    verify = verifyCmdAuthorization auth_ tAuth authorized
    dummyVerify = verify (dummyAuthKey tAuth) `seq` VRFailed
    verifyQueue :: ((StoreQueue s, QueueRec) -> VerificationResult s) -> Either ErrorType (StoreQueue s, QueueRec) -> VerificationResult s
    verifyQueue = either (const dummyVerify)
    verified q cond = if cond then VRVerified q else VRFailed
    verifiedWith :: Maybe (StoreQueue s, QueueRec) -> C.APublicAuthKey -> VerificationResult s
    verifiedWith q k = q `verified` verify k
    get :: DirectParty p => SParty p -> M (Either ErrorType (StoreQueue s, QueueRec))
    get party = liftIO $ getQueueRec ms party queueId

verifyCmdAuthorization :: Maybe (THandleAuth 'TServer, C.CbNonce) -> Maybe TransmissionAuth -> ByteString -> C.APublicAuthKey -> Bool
verifyCmdAuthorization auth_ tAuth authorized key = maybe False (verify key) tAuth
  where
    verify :: C.APublicAuthKey -> TransmissionAuth -> Bool
    verify (C.APublicAuthKey a k) = \case
      TASignature (C.ASignature a' s) -> case testEquality a a' of
        Just Refl -> C.verify' k s authorized
        _ -> C.verify' (dummySignKey a') s authorized `seq` False
      TAAuthenticator s -> case a of
        C.SX25519 -> verifyCmdAuth auth_ k s authorized
        _ -> verifyCmdAuth auth_ dummyKeyX25519 s authorized `seq` False

verifyCmdAuth :: Maybe (THandleAuth 'TServer, C.CbNonce) -> C.PublicKeyX25519 -> C.CbAuthenticator -> ByteString -> Bool
verifyCmdAuth auth_ k authenticator authorized = case auth_ of
  Just (THAuthServer {serverPrivKey = pk}, nonce) -> C.cbVerify k pk nonce authenticator authorized
  Nothing -> False

dummyVerifyCmd :: Maybe (THandleAuth 'TServer, C.CbNonce) -> ByteString -> TransmissionAuth -> Bool
dummyVerifyCmd auth_ authorized = \case
  TASignature (C.ASignature a s) -> C.verify' (dummySignKey a) s authorized
  TAAuthenticator s -> verifyCmdAuth auth_ dummyKeyX25519 s authorized

-- These dummy keys are used with `dummyVerify` function to mitigate timing attacks
-- by having the same time of the response whether a queue exists or nor, for all valid key/signature sizes
dummySignKey :: C.SignatureAlgorithm a => C.SAlgorithm a -> C.PublicKey a
dummySignKey = \case
  C.SEd25519 -> dummyKeyEd25519
  C.SEd448 -> dummyKeyEd448

dummyAuthKey :: Maybe TransmissionAuth -> C.APublicAuthKey
dummyAuthKey = \case
  Just (TASignature (C.ASignature a _)) -> case a of
    C.SEd25519 -> C.APublicAuthKey C.SEd25519 dummyKeyEd25519
    C.SEd448 -> C.APublicAuthKey C.SEd448 dummyKeyEd448
  _ -> C.APublicAuthKey C.SX25519 dummyKeyX25519

dummyKeyEd25519 :: C.PublicKey 'C.Ed25519
dummyKeyEd25519 = "MCowBQYDK2VwAyEA139Oqs4QgpqbAmB0o7rZf6T19ryl7E65k4AYe0kE3Qs="

dummyKeyEd448 :: C.PublicKey 'C.Ed448
dummyKeyEd448 = "MEMwBQYDK2VxAzoA6ibQc9XpkSLtwrf7PLvp81qW/etiumckVFImCMRdftcG/XopbOSaq9qyLhrgJWKOLyNrQPNVvpMA"

dummyKeyX25519 :: C.PublicKey 'C.X25519
dummyKeyX25519 = "MCowBQYDK2VuAyEA4JGSMYht18H4mas/jHeBwfcM7jLwNYJNOAhi2/g4RXg="

forkClient :: Client s -> String -> M () -> M ()
forkClient Client {endThreads, endThreadSeq} label action = do
  tId <- atomically $ stateTVar endThreadSeq $ \next -> (next, next + 1)
  t <- forkIO $ do
    labelMyThread label
    action `finally` atomically (modifyTVar' endThreads $ IM.delete tId)
  mkWeakThreadId t >>= atomically . modifyTVar' endThreads . IM.insert tId

client :: forall s. MsgStoreClass s => THandleParams SMPVersion 'TServer -> Server -> s -> Client s -> M ()
client
  thParams'
  Server {subscribedQ, ntfSubscribedQ, subscribers}
  ms
  clnt@Client {clientId, subscriptions, ntfSubscriptions, rcvQ, sndQ, sessionId, procThreads} = do
    labelMyThread . B.unpack $ "client $" <> encode sessionId <> " commands"
    let THandleParams {thVersion} = thParams'
    forever $
      atomically (readTBQueue rcvQ)
        >>= mapM (processCommand thVersion)
        >>= mapM_ reply . L.nonEmpty . catMaybes . L.toList
  where
    reply :: MonadIO m => NonEmpty (Transmission BrokerMsg) -> m ()
    reply = atomically . writeTBQueue sndQ
    processProxiedCmd :: Transmission (Command 'ProxiedClient) -> M (Maybe (Transmission BrokerMsg))
    processProxiedCmd (corrId, EntityId sessId, command) = (corrId,EntityId sessId,) <$$> case command of
      PRXY srv auth -> ifM allowProxy getRelay (pure $ Just $ ERR $ PROXY BASIC_AUTH)
        where
          allowProxy = do
            ServerConfig {allowSMPProxy, newQueueBasicAuth} <- asks config
            pure $ allowSMPProxy && maybe True ((== auth) . Just) newQueueBasicAuth
          getRelay = do
            ProxyAgent {smpAgent = a} <- asks proxyAgent
            liftIO (getConnectedSMPServerClient a srv) >>= \case
              Just r -> Just <$> proxyServerResponse a r
              Nothing ->
                forkProxiedCmd $
                  liftIO (runExceptT (getSMPServerClient'' a srv) `catch` (pure . Left . PCEIOError))
                    >>= proxyServerResponse a
          proxyServerResponse :: SMPClientAgent -> Either SMPClientError (OwnServer, SMPClient) -> M BrokerMsg
          proxyServerResponse a smp_ = do
            ServerStats {pRelays, pRelaysOwn} <- asks serverStats
            let inc = mkIncProxyStats pRelays pRelaysOwn
            case smp_ of
              Right (own, smp) -> do
                inc own pRequests
                case proxyResp smp of
                  r@PKEY {} -> r <$ inc own pSuccesses
                  r -> r <$ inc own pErrorsCompat
              Left e -> do
                let own = isOwnServer a srv
                inc own pRequests
                inc own $ if temporaryClientError e then pErrorsConnect else pErrorsOther
                logWarn $ "Error connecting: " <> decodeLatin1 (strEncode $ host srv) <> " " <> tshow e
                pure . ERR $ smpProxyError e
            where
              proxyResp smp =
                let THandleParams {sessionId = srvSessId, thVersion, thServerVRange, thAuth} = thParams smp
                  in case compatibleVRange thServerVRange proxiedSMPRelayVRange of
                      -- Cap the destination relay version range to prevent client version fingerprinting.
                      -- See comment for proxiedSMPRelayVersion.
                      Just (Compatible vr) | thVersion >= sendingProxySMPVersion -> case thAuth of
                        Just THAuthClient {serverCertKey} -> PKEY srvSessId vr serverCertKey
                        Nothing -> ERR $ transportErr TENoServerAuth
                      _ -> ERR $ transportErr TEVersion
      PFWD fwdV pubKey encBlock -> do
        ProxyAgent {smpAgent = a} <- asks proxyAgent
        ServerStats {pMsgFwds, pMsgFwdsOwn} <- asks serverStats
        let inc = mkIncProxyStats pMsgFwds pMsgFwdsOwn
        liftIO (lookupSMPServerClient a sessId) >>= \case
          Just (own, smp) -> do
            inc own pRequests
            if v >= sendingProxySMPVersion
              then forkProxiedCmd $ do
                liftIO (runExceptT (forwardSMPTransmission smp corrId fwdV pubKey encBlock) `catch` (pure . Left . PCEIOError))  >>= \case
                  Right r -> PRES r <$ inc own pSuccesses
                  Left e -> ERR (smpProxyError e) <$ case e of
                    PCEProtocolError {} -> inc own pSuccesses
                    _ -> inc own pErrorsOther
              else Just (ERR $ transportErr TEVersion) <$ inc own pErrorsCompat
            where
              THandleParams {thVersion = v} = thParams smp
          Nothing -> inc False pRequests >> inc False pErrorsConnect $> Just (ERR $ PROXY NO_SESSION)
      where
        forkProxiedCmd :: M BrokerMsg -> M (Maybe BrokerMsg)
        forkProxiedCmd cmdAction = do
          bracket_ wait signal . forkClient clnt (B.unpack $ "client $" <> encode sessionId <> " proxy") $ do
            -- commands MUST be processed under a reasonable timeout or the client would halt
            cmdAction >>= \t -> reply [(corrId, EntityId sessId, t)]
          pure Nothing
          where
            wait = do
              ServerConfig {serverClientConcurrency} <- asks config
              atomically $ do
                used <- readTVar procThreads
                when (used >= serverClientConcurrency) retry
                writeTVar procThreads $! used + 1
            signal = atomically $ modifyTVar' procThreads (\t -> t - 1)
    transportErr :: TransportError -> ErrorType
    transportErr = PROXY . BROKER . TRANSPORT
    mkIncProxyStats :: MonadIO m => ProxyStats -> ProxyStats -> OwnServer -> (ProxyStats -> IORef Int) -> m ()
    mkIncProxyStats ps psOwn own sel = do
      incStat $ sel ps
      when own $ incStat $ sel psOwn
    processCommand :: VersionSMP -> (Maybe (StoreQueue s, QueueRec), Transmission Cmd) -> M (Maybe (Transmission BrokerMsg))
    processCommand clntVersion (q_, (corrId, entId, cmd)) = case cmd of
      Cmd SProxiedClient command -> processProxiedCmd (corrId, entId, command)
      Cmd SSender command -> Just <$> case command of
        SKEY sKey ->
          withQueue $ \q QueueRec {sndSecure} ->
            (corrId,entId,) <$> if sndSecure then secureQueue_ q sKey else pure $ ERR AUTH
        SEND flags msgBody -> withQueue_ False $ sendMessage flags msgBody
        PING -> pure (corrId, NoEntity, PONG)
        RFWD encBlock -> (corrId, NoEntity,) <$> processForwardedCommand encBlock
      Cmd SNotifier NSUB -> Just <$> subscribeNotifications
      Cmd SRecipient command ->
        Just <$> case command of
          NEW rKey dhKey auth subMode sndSecure ->
            ifM
              allowNew
              (createQueue rKey dhKey subMode sndSecure)
              (pure (corrId, entId, ERR AUTH))
            where
              allowNew = do
                ServerConfig {allowNewQueues, newQueueBasicAuth} <- asks config
                pure $ allowNewQueues && maybe True ((== auth) . Just) newQueueBasicAuth
          SUB -> withQueue subscribeQueue
          GET -> withQueue getMessage
          ACK msgId -> withQueue $ acknowledgeMsg msgId
          KEY sKey -> withQueue $ \q _ -> (corrId,entId,) <$> secureQueue_ q sKey
          NKEY nKey dhKey -> withQueue $ \q _ -> addQueueNotifier_ q nKey dhKey
          NDEL -> withQueue $ \q _ -> deleteQueueNotifier_ q
          OFF -> maybe (pure $ err INTERNAL) suspendQueue_ q_
          DEL -> maybe (pure $ err INTERNAL) delQueueAndMsgs q_
          QUE -> withQueue $ \q qr -> (corrId,entId,) <$> getQueueInfo q qr
      where
        createQueue :: RcvPublicAuthKey -> RcvPublicDhKey -> SubscriptionMode -> SenderCanSecure -> M (Transmission BrokerMsg)
        createQueue recipientKey dhKey subMode sndSecure = time "NEW" $ do
          (rcvPublicDhKey, privDhKey) <- atomically . C.generateKeyPair =<< asks random
          updatedAt <- Just <$> liftIO getSystemDate
          let rcvDhSecret = C.dh' dhKey privDhKey
              qik (rcvId, sndId) = QIK {rcvId, sndId, rcvPublicDhKey, sndSecure}
              qRec senderId =
                QueueRec
                  { senderId,
                    recipientKey,
                    rcvDhSecret,
                    senderKey = Nothing,
                    notifier = Nothing,
                    status = EntityActive,
                    sndSecure,
                    updatedAt
                  }
          (corrId,entId,) <$> addQueueRetry 3 qik qRec
          where
            addQueueRetry ::
              Int -> ((RecipientId, SenderId) -> QueueIdsKeys) -> (SenderId -> QueueRec) -> M BrokerMsg
            addQueueRetry 0 _ _ = pure $ ERR INTERNAL
            addQueueRetry n qik qRec = do
              ids@(rId, sId) <- getIds
              let qr = qRec sId
              liftIO (addQueue ms rId qr) >>= \case
                Left DUPLICATE_ -> addQueueRetry (n - 1) qik qRec
                Left e -> pure $ ERR e
                Right q -> do
                  stats <- asks serverStats
                  incStat $ qCreated stats
                  incStat $ qCount stats
                  case subMode of
                    SMOnlyCreate -> pure ()
                    SMSubscribe -> void $ subscribeQueue q qr
                  pure $ IDS (qik ids)

            getIds :: M (RecipientId, SenderId)
            getIds = do
              n <- asks $ queueIdBytes . config
              liftM2 (,) (randomId n) (randomId n)

        secureQueue_ :: StoreQueue s -> SndPublicAuthKey -> M BrokerMsg
        secureQueue_ q sKey = do
          liftIO (secureQueue (queueStore ms) q sKey) >>= \case
            Left e -> pure $ ERR e
            Right () -> do
              stats <- asks serverStats
              incStat $ qSecured stats
              pure OK

        addQueueNotifier_ :: StoreQueue s -> NtfPublicAuthKey -> RcvNtfPublicDhKey -> M (Transmission BrokerMsg)
        addQueueNotifier_ q notifierKey dhKey = time "NKEY" $ do
          (rcvPublicDhKey, privDhKey) <- atomically . C.generateKeyPair =<< asks random
          let rcvNtfDhSecret = C.dh' dhKey privDhKey
          (corrId,entId,) <$> addNotifierRetry 3 rcvPublicDhKey rcvNtfDhSecret
          where
            addNotifierRetry :: Int -> RcvNtfPublicDhKey -> RcvNtfDhSecret -> M BrokerMsg
            addNotifierRetry 0 _ _ = pure $ ERR INTERNAL
            addNotifierRetry n rcvPublicDhKey rcvNtfDhSecret = do
              notifierId <- randomId =<< asks (queueIdBytes . config)
              let ntfCreds = NtfCreds {notifierId, notifierKey, rcvNtfDhSecret}
              liftIO (addQueueNotifier (queueStore ms) q ntfCreds) >>= \case
                Left DUPLICATE_ -> addNotifierRetry (n - 1) rcvPublicDhKey rcvNtfDhSecret
                Left e -> pure $ ERR e
                Right nId_ -> do
                  incStat . ntfCreated =<< asks serverStats
                  forM_ nId_ $ \nId -> atomically $ writeTQueue ntfSubscribedQ (nId, clientId, False)
                  pure $ NID notifierId rcvPublicDhKey

        deleteQueueNotifier_ :: StoreQueue s -> M (Transmission BrokerMsg)
        deleteQueueNotifier_ q =
          liftIO (deleteQueueNotifier (queueStore ms) q) >>= \case
            Right (Just nId) -> do
              -- Possibly, the same should be done if the queue is suspended, but currently we do not use it
              stats <- asks serverStats
              deleted <- asks ntfStore >>= liftIO . (`deleteNtfs` nId)
              when (deleted > 0) $ liftIO $ atomicModifyIORef'_ (ntfCount stats) (subtract deleted)
              atomically $ writeTQueue ntfSubscribedQ (nId, clientId, False)
              incStat $ ntfDeleted stats
              pure ok
            Right Nothing -> pure ok
            Left e -> pure $ err e

        suspendQueue_ :: (StoreQueue s, QueueRec) -> M (Transmission BrokerMsg)
        suspendQueue_ (q, _) = liftIO $ either err (const ok) <$> suspendQueue (queueStore ms) q

        subscribeQueue :: StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)
        subscribeQueue q qr =
          atomically (TM.lookup rId subscriptions) >>= \case
            Nothing -> newSub >>= deliver True
            Just s@Sub {subThread} -> do
              stats <- asks serverStats
              case subThread of
                ProhibitSub -> do
                  -- cannot use SUB in the same connection where GET was used
                  incStat $ qSubProhibited stats
                  pure (corrId, rId, ERR $ CMD PROHIBITED)
                _ -> do
                  incStat $ qSubDuplicate stats
                  atomically (tryTakeTMVar $ delivered s) >> deliver False s
          where
            rId = recipientId q
            newSub :: M Sub
            newSub = time "SUB newSub" . atomically $ do
              writeTQueue subscribedQ (rId, clientId, True)
              sub <- newSubscription NoSub
              TM.insert rId sub subscriptions
              pure sub
            deliver :: Bool -> Sub -> M (Transmission BrokerMsg)
            deliver inc sub = do
              stats <- asks serverStats
              fmap (either (\e -> (corrId, rId, ERR e)) id) $ liftIO $ runExceptT $ do
                msg_ <- tryPeekMsg ms q
                liftIO $ when (inc && isJust msg_) $ incStat (qSub stats)
                liftIO $ deliverMessage "SUB" qr rId sub msg_

        -- clients that use GET are not added to server subscribers
        getMessage :: StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)
        getMessage q qr = time "GET" $ do
          atomically (TM.lookup entId subscriptions) >>= \case
            Nothing ->
              atomically newSub >>= (`getMessage_` Nothing)
            Just s@Sub {subThread} ->
              case subThread of
                ProhibitSub ->
                  atomically (tryTakeTMVar $ delivered s)
                    >>= getMessage_ s
                -- cannot use GET in the same connection where there is an active subscription
                _ -> do
                  stats <- asks serverStats
                  incStat $ msgGetProhibited stats
                  pure $ err $ CMD PROHIBITED
          where
            newSub :: STM Sub
            newSub = do
              s <- newProhibitedSub
              TM.insert entId s subscriptions
              -- Here we don't account for this client as subscribed in the server
              -- and don't notify other subscribed clients.
              -- This is tracked as "subscription" in the client to prevent these
              -- clients from being able to subscribe.
              pure s
            getMessage_ :: Sub -> Maybe MsgId -> M (Transmission BrokerMsg)
            getMessage_ s delivered_ = do
              stats <- asks serverStats
              fmap (either err id) $ liftIO $ runExceptT $
                tryPeekMsg ms q >>= \case
                  Just msg -> do
                    let encMsg = encryptMsg qr msg
                    incStat $ (if isJust delivered_ then msgGetDuplicate else msgGet) stats
                    atomically $ setDelivered s msg $> (corrId, entId, MSG encMsg)
                  Nothing -> incStat (msgGetNoMsg stats) $> ok

        withQueue :: (StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)) -> M (Transmission BrokerMsg)
        withQueue = withQueue_ True

        withQueue_ :: Bool -> (StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)) -> M (Transmission BrokerMsg)
        withQueue_ queueNotBlocked action = case q_ of
          Nothing -> pure $ err INTERNAL
          Just (q, qr@QueueRec {status, updatedAt}) -> case status of
            EntityBlocked info | queueNotBlocked -> pure $ err $ BLOCKED info
            _ -> do
              t <- liftIO getSystemDate
              if updatedAt == Just t
                then action q qr
                else liftIO (updateQueueTime (queueStore ms) q t) >>= either (pure . err) (action q)

        subscribeNotifications :: M (Transmission BrokerMsg)
        subscribeNotifications = do
          statCount <-
            time "NSUB" . atomically $ do
              ifM
                (TM.member entId ntfSubscriptions)
                (pure ntfSubDuplicate)
                (newSub $> ntfSub)
          incStat . statCount =<< asks serverStats
          pure ok
          where
            newSub = do
              writeTQueue ntfSubscribedQ (entId, clientId, True)
              TM.insert entId () ntfSubscriptions

        acknowledgeMsg :: MsgId -> StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)
        acknowledgeMsg msgId q qr = time "ACK" $ do
          liftIO (TM.lookupIO entId subscriptions) >>= \case
            Nothing -> pure $ err NO_MSG
            Just sub ->
              atomically (getDelivered sub) >>= \case
                Just st -> do
                  stats <- asks serverStats
                  fmap (either err id) $ liftIO $ runExceptT $ do
                    case st of
                      ProhibitSub -> do
                        deletedMsg_ <- tryDelMsg ms q msgId
                        liftIO $ mapM_ (updateStats stats True) deletedMsg_
                        pure ok
                      _ -> do
                        (deletedMsg_, msg_) <- tryDelPeekMsg ms q msgId
                        liftIO $ mapM_ (updateStats stats False) deletedMsg_
                        liftIO $ deliverMessage "ACK" qr entId sub msg_
                _ -> pure $ err NO_MSG
          where
            getDelivered :: Sub -> STM (Maybe ServerSub)
            getDelivered Sub {delivered, subThread} = do
              tryTakeTMVar delivered $>>= \msgId' ->
                if msgId == msgId' || B.null msgId
                  then pure $ Just subThread
                  else putTMVar delivered msgId' $> Nothing
            updateStats :: ServerStats -> Bool -> Message -> IO ()
            updateStats stats isGet = \case
              MessageQuota {} -> pure ()
              Message {msgFlags} -> do
                incStat $ msgRecv stats
                if isGet
                  then incStat $ msgRecvGet stats
                  else pure () -- TODO skip notification delivery for delivered message  
                  -- skipping delivery fails tests, it should be counted in msgNtfSkipped
                  -- forM_ (notifierId <$> notifier qr) $ \nId -> do
                  --   ns <- asks ntfStore
                  --   atomically $ TM.lookup nId ns >>=
                  --     mapM_ (\MsgNtf {ntfMsgId} -> when (msgId == msgId') $ TM.delete nId ns)
                atomicModifyIORef'_ (msgCount stats) (subtract 1)
                updatePeriodStats (activeQueues stats) entId
                when (notification msgFlags) $ do
                  incStat $ msgRecvNtf stats
                  updatePeriodStats (activeQueuesNtf stats) entId

        sendMessage :: MsgFlags -> MsgBody -> StoreQueue s -> QueueRec -> M (Transmission BrokerMsg)
        sendMessage msgFlags msgBody q qr
          | B.length msgBody > maxMessageLength clntVersion = do
              stats <- asks serverStats
              incStat $ msgSentLarge stats
              pure $ err LARGE_MSG
          | otherwise = do
              stats <- asks serverStats
              case status qr of
                EntityOff -> do
                  incStat $ msgSentAuth stats
                  pure $ err AUTH
                EntityBlocked info -> do
                  incStat $ msgSentBlock stats
                  pure $ err $ BLOCKED info
                EntityActive ->
                  case C.maxLenBS msgBody of
                    Left _ -> pure $ err LARGE_MSG
                    Right body -> do
                      ServerConfig {messageExpiration, msgIdBytes} <- asks config
                      msgId <- randomId' msgIdBytes
                      msg_ <- liftIO $ time "SEND" $ runExceptT $ do
                        expireMessages messageExpiration stats
                        msg <- liftIO $ mkMessage msgId body
                        writeMsg ms q True msg
                      case msg_ of
                        Left e -> pure $ err e
                        Right Nothing -> do
                          incStat $ msgSentQuota stats
                          pure $ err QUOTA
                        Right (Just (msg, wasEmpty)) -> time "SEND ok" $ do
                          when wasEmpty $ liftIO $ tryDeliverMessage msg
                          when (notification msgFlags) $ do
                            mapM_ (`enqueueNotification` msg) (notifier qr)
                            incStat $ msgSentNtf stats
                            liftIO $ updatePeriodStats (activeQueuesNtf stats) (recipientId q)
                          incStat $ msgSent stats
                          incStat $ msgCount stats
                          liftIO $ updatePeriodStats (activeQueues stats) (recipientId q)
                          pure ok
          where
            mkMessage :: MsgId -> C.MaxLenBS MaxMessageLen -> IO Message
            mkMessage msgId body = do
              msgTs <- getSystemTime
              pure $! Message msgId msgTs msgFlags body

            expireMessages :: Maybe ExpirationConfig -> ServerStats -> ExceptT ErrorType IO ()
            expireMessages msgExp stats = do
              deleted <- maybe (pure 0) (deleteExpiredMsgs ms q <=< liftIO . expireBeforeEpoch) msgExp
              liftIO $ when (deleted > 0) $ atomicModifyIORef'_ (msgExpired stats) (+ deleted)

            -- The condition for delivery of the message is:
            -- - the queue was empty when the message was sent,
            -- - there is subscribed recipient,
            -- - no message was "delivered" that was not acknowledged.
            -- If the send queue of the subscribed client is not full the message is put there in the same transaction.
            -- If the queue is not full, then the thread is created where these checks are made:
            -- - it is the same subscribed client (in case it was reconnected it would receive message via SUB command)
            -- - nothing was delivered to this subscription (to avoid race conditions with the recipient).
            tryDeliverMessage :: Message -> IO ()
            tryDeliverMessage msg =
              -- the subscription is checked outside of STM to avoid transaction cost
              -- in case no client is subscribed.
              whenM (TM.memberIO rId subscribers) $
                atomically deliverToSub >>= mapM_ forkDeliver
              where
                rId = recipientId q
                deliverToSub =
                  -- lookup has ot be in the same transaction,
                  -- so that if subscription ends, it re-evalutates
                  -- and delivery is cancelled -
                  -- the new client will receive message in response to SUB.
                  (TM.lookup rId subscribers >>= mapM readTVar)
                    $>>= \rc@(AClient _ _ Client {subscriptions = subs, sndQ = sndQ'}) -> TM.lookup rId subs
                    $>>= \s@Sub {subThread, delivered} -> case subThread of
                      ProhibitSub -> pure Nothing
                      ServerSub st -> readTVar st >>= \case
                        NoSub ->
                          tryTakeTMVar delivered >>= \case
                            Just _ -> pure Nothing -- if a message was already delivered, should not deliver more
                            Nothing ->
                              ifM
                                (isFullTBQueue sndQ')
                                (writeTVar st SubPending $> Just (rc, s, st))
                                (deliver sndQ' s $> Nothing)
                        _ -> pure Nothing
                deliver sndQ' s = do
                  let encMsg = encryptMsg qr msg
                  writeTBQueue sndQ' [(CorrId "", rId, MSG encMsg)]
                  void $ setDelivered s msg
                forkDeliver ((AClient _ _ rc@Client {sndQ = sndQ'}), s@Sub {delivered}, st) = do
                  t <- mkWeakThreadId =<< forkIO deliverThread
                  atomically $ modifyTVar' st $ \case
                    -- this case is needed because deliverThread can exit before it
                    SubPending -> SubThread t
                    st' -> st'
                  where
                    deliverThread = do
                      labelMyThread $ B.unpack ("client $" <> encode sessionId) <> " deliver/SEND"
                      -- lookup can be outside of STM transaction,
                      -- as long as the check that it is the same client is inside.
                      TM.lookupIO rId subscribers >>= mapM_ deliverIfSame
                    deliverIfSame rc' = time "deliver" . atomically $
                      whenM (sameClientId rc <$> readTVar rc') $
                        tryTakeTMVar delivered >>= \case
                          Just _ -> pure () -- if a message was already delivered, should not deliver more
                          Nothing -> do
                            -- a separate thread is needed because it blocks when client sndQ is full.
                            deliver sndQ' s
                            writeTVar st NoSub

            enqueueNotification :: NtfCreds -> Message -> M ()
            enqueueNotification _ MessageQuota {} = pure ()
            enqueueNotification NtfCreds {notifierId = nId, rcvNtfDhSecret} Message {msgId, msgTs} = do
              -- stats <- asks serverStats
              ns <- asks ntfStore
              ntf <- mkMessageNotification msgId msgTs rcvNtfDhSecret
              liftIO $ storeNtf ns nId ntf
              incStat . ntfCount =<< asks serverStats

            mkMessageNotification :: ByteString -> SystemTime -> RcvNtfDhSecret -> M MsgNtf
            mkMessageNotification msgId msgTs rcvNtfDhSecret = do
              ntfNonce <- atomically . C.randomCbNonce =<< asks random
              let msgMeta = NMsgMeta {msgId, msgTs}
                  encNMsgMeta = C.cbEncrypt rcvNtfDhSecret ntfNonce (smpEncode msgMeta) 128
              pure $ MsgNtf {ntfMsgId = msgId, ntfTs = msgTs, ntfNonce, ntfEncMeta = fromRight "" encNMsgMeta}

        processForwardedCommand :: EncFwdTransmission -> M BrokerMsg
        processForwardedCommand (EncFwdTransmission s) = fmap (either ERR id) . runExceptT $ do
          THAuthServer {serverPrivKey, sessSecret'} <- maybe (throwE $ transportErr TENoServerAuth) pure (thAuth thParams')
          sessSecret <- maybe (throwE $ transportErr TENoServerAuth) pure sessSecret'
          let proxyNonce = C.cbNonce $ bs corrId
          s' <- liftEitherWith (const CRYPTO) $ C.cbDecryptNoPad sessSecret proxyNonce s
          FwdTransmission {fwdCorrId, fwdVersion, fwdKey, fwdTransmission = EncTransmission et} <- liftEitherWith (const $ CMD SYNTAX) $ smpDecode s'
          let clientSecret = C.dh' fwdKey serverPrivKey
              clientNonce = C.cbNonce $ bs fwdCorrId
          b <- liftEitherWith (const CRYPTO) $ C.cbDecrypt clientSecret clientNonce et
          let clntTHParams = smpTHParamsSetVersion fwdVersion thParams'
          -- only allowing single forwarded transactions
          t' <- case tParse clntTHParams b of
            t :| [] -> pure $ tDecodeParseValidate clntTHParams t
            _ -> throwE BLOCK
          let clntThAuth = Just $ THAuthServer {serverPrivKey, sessSecret' = Just clientSecret}
          -- process forwarded command
          r <-
            lift (rejectOrVerify clntThAuth t') >>= \case
              Left r -> pure r
              -- rejectOrVerify filters allowed commands, no need to repeat it here.
              -- INTERNAL is used because processCommand never returns Nothing for sender commands (could be extracted for better types).
              Right t''@(_, (corrId', entId', _)) -> fromMaybe (corrId', entId', ERR INTERNAL) <$> lift (processCommand fwdVersion t'')
          -- encode response
          r' <- case batchTransmissions (batch clntTHParams) (blockSize clntTHParams) [Right (Nothing, encodeTransmission clntTHParams r)] of
            [] -> throwE INTERNAL -- at least 1 item is guaranteed from NonEmpty/Right
            TBError _ _ : _ -> throwE BLOCK
            TBTransmission b' _ : _ -> pure b'
            TBTransmissions b' _ _ : _ -> pure b'
          -- encrypt to client
          r2 <- liftEitherWith (const BLOCK) $ EncResponse <$> C.cbEncrypt clientSecret (C.reverseNonce clientNonce) r' paddedProxiedTLength
          -- encrypt to proxy
          let fr = FwdResponse {fwdCorrId, fwdResponse = r2}
              r3 = EncFwdResponse $ C.cbEncryptNoPad sessSecret (C.reverseNonce proxyNonce) (smpEncode fr)
          stats <- asks serverStats
          incStat $ pMsgFwdsRecv stats
          pure $ RRES r3
          where
            rejectOrVerify :: Maybe (THandleAuth 'TServer) -> SignedTransmission ErrorType Cmd -> M (Either (Transmission BrokerMsg) (Maybe (StoreQueue s, QueueRec), Transmission Cmd))
            rejectOrVerify clntThAuth (tAuth, authorized, (corrId', entId', cmdOrError)) =
              case cmdOrError of
                Left e -> pure $ Left (corrId', entId', ERR e)
                Right cmd'
                  | allowed -> verified <$> verifyTransmission ms ((,C.cbNonce (bs corrId')) <$> clntThAuth) tAuth authorized entId' cmd'
                  | otherwise -> pure $ Left (corrId', entId', ERR $ CMD PROHIBITED)
                  where
                    allowed = case cmd' of
                      Cmd SSender SEND {} -> True
                      Cmd SSender (SKEY _) -> True
                      _ -> False
                    verified = \case
                      VRVerified q -> Right (q, (corrId', entId', cmd'))
                      VRFailed -> Left (corrId', entId', ERR AUTH)
        deliverMessage :: T.Text -> QueueRec -> RecipientId -> Sub -> Maybe Message -> IO (Transmission BrokerMsg)
        deliverMessage name qr rId s@Sub {subThread} msg_ = time (name <> " deliver") . atomically $
          case subThread of
            ProhibitSub -> pure resp
            _ -> case msg_ of
              Just msg ->
                let encMsg = encryptMsg qr msg
                 in setDelivered s msg $> (corrId, rId, MSG encMsg)
              _ -> pure resp
          where
            resp = (corrId, rId, OK)

        time :: MonadIO m => T.Text -> m a -> m a
        time name = timed name entId

        encryptMsg :: QueueRec -> Message -> RcvMessage
        encryptMsg qr msg = encrypt . encodeRcvMsgBody $ case msg of
          Message {msgFlags, msgBody} -> RcvMsgBody {msgTs = msgTs', msgFlags, msgBody}
          MessageQuota {} -> RcvMsgQuota msgTs'
          where
            encrypt :: KnownNat i => C.MaxLenBS i -> RcvMessage
            encrypt body = RcvMessage msgId' . EncRcvMsgBody $ C.cbEncryptMaxLenBS (rcvDhSecret qr) (C.cbNonce msgId') body
            msgId' = messageId msg
            msgTs' = messageTs msg

        setDelivered :: Sub -> Message -> STM Bool
        setDelivered s msg = tryPutTMVar (delivered s) $! messageId msg

        delQueueAndMsgs :: (StoreQueue s, QueueRec) -> M (Transmission BrokerMsg)
        delQueueAndMsgs (q, _) = do
          liftIO (deleteQueue ms q) >>= \case
            Right qr -> do
              -- Possibly, the same should be done if the queue is suspended, but currently we do not use it
              atomically $ do
                writeTQueue subscribedQ (entId, clientId, False)
                -- queue is usually deleted by the same client that is currently subscribed,
                -- we delete subscription here, so the client with no subscriptions can be disconnected.
                TM.delete entId subscriptions
              forM_ (notifierId <$> notifier qr) $ \nId -> do
                -- queue is deleted by a different client from the one subscribed to notifications,
                -- so we don't need to remove subscription from the current client.
                stats <- asks serverStats
                deleted <- asks ntfStore >>= liftIO . (`deleteNtfs` nId)
                when (deleted > 0) $ liftIO $ atomicModifyIORef'_ (ntfCount stats) (subtract deleted)
                atomically $ writeTQueue ntfSubscribedQ (nId, clientId, False)
              updateDeletedStats qr
              pure ok
            Left e -> pure $ err e

        getQueueInfo :: StoreQueue s -> QueueRec -> M BrokerMsg
        getQueueInfo q QueueRec {senderKey, notifier} = do
          fmap (either ERR id) $ liftIO $ runExceptT $ do
            qiSub <- liftIO $ TM.lookupIO entId subscriptions >>= mapM mkQSub
            qiSize <- getQueueSize ms q
            qiMsg <- toMsgInfo <$$> tryPeekMsg ms q
            let info = QueueInfo {qiSnd = isJust senderKey, qiNtf = isJust notifier, qiSub, qiSize, qiMsg}
            pure $ INFO info
          where
            mkQSub Sub {subThread, delivered} = do
              qSubThread <- case subThread of
                ServerSub t -> do
                  st <- readTVarIO t
                  pure $ case st of
                    NoSub -> QNoSub
                    SubPending -> QSubPending
                    SubThread _ -> QSubThread
                ProhibitSub -> pure QProhibitSub
              qDelivered <- atomically $ decodeLatin1 . encode <$$> tryReadTMVar delivered
              pure QSub {qSubThread, qDelivered}

        ok :: Transmission BrokerMsg
        ok = (corrId, entId, OK)

        err :: ErrorType -> Transmission BrokerMsg
        err e = (corrId, entId, ERR e)

updateDeletedStats :: QueueRec -> M ()
updateDeletedStats q = do
  stats <- asks serverStats
  let delSel = if isNothing (senderKey q) then qDeletedNew else qDeletedSecured
  incStat $ delSel stats
  incStat $ qDeletedAll stats
  liftIO $ atomicModifyIORef'_ (qCount stats) (subtract 1)

incStat :: MonadIO m => IORef Int -> m ()
incStat r = liftIO $ atomicModifyIORef'_ r (+ 1)
{-# INLINE incStat #-}

timed :: MonadIO m => T.Text -> RecipientId -> m a -> m a
timed name (EntityId qId) a = do
  t <- liftIO getSystemTime
  r <- a
  t' <- liftIO getSystemTime
  let int = diff t t'
  when (int > sec) . logDebug $ T.unwords [name, tshow $ encode qId, tshow int]
  pure r
  where
    diff t t' = (systemSeconds t' - systemSeconds t) * sec + fromIntegral (systemNanoseconds t' - systemNanoseconds t)
    sec = 1000_000000

randomId' :: Int -> M ByteString
randomId' n = atomically . C.randomBytes n =<< asks random

randomId :: Int -> M EntityId
randomId = fmap EntityId . randomId'
{-# INLINE randomId #-}

saveServerMessages :: Bool -> AMsgStore -> IO ()
saveServerMessages drainMsgs = \case
  AMS SQSMemory SMSMemory ms@STMMsgStore {storeConfig = STMStoreConfig {storePath}} -> case storePath of
    Just f -> exportMessages False ms f drainMsgs
    Nothing -> logInfo "undelivered messages are not saved"
  AMS _ SMSJournal _ -> logInfo "closed journal message storage"

exportMessages :: MsgStoreClass s => Bool -> s -> FilePath -> Bool -> IO ()
exportMessages tty ms f drainMsgs = do
  logInfo $ "saving messages to file " <> T.pack f
  liftIO $ withFile f WriteMode $ \h ->
    tryAny (withAllMsgQueues tty ms $ saveQueueMsgs h) >>= \case
      Right (Sum total) -> logInfo $ "messages saved: " <> tshow total
      Left e -> do
        logError $ "error exporting messages: " <> tshow e
        exitFailure
  where
    saveQueueMsgs h q = do
      let rId = recipientId q
      runExceptT (getQueueMessages drainMsgs ms q) >>= \case
        Right msgs -> Sum (length msgs) <$ BLD.hPutBuilder h (encodeMessages rId msgs)
        Left e -> do
          logError $ "STORE: saveQueueMsgs, error exporting messages from queue " <> decodeLatin1 (strEncode rId) <> ", " <> tshow e
          exitFailure
    encodeMessages rId = mconcat . map (\msg -> BLD.byteString (strEncode $ MLRv3 rId msg) <> BLD.char8 '\n')

processServerMessages :: M (Maybe MessageStats)
processServerMessages = do
  old_ <- asks (messageExpiration . config) $>>= (liftIO . fmap Just . expireBeforeEpoch)
  expire <- asks $ expireMessagesOnStart . config
  asks msgStore >>= liftIO . processMessages old_ expire
    where
      processMessages :: Maybe Int64 -> Bool -> AMsgStore -> IO (Maybe MessageStats)
      processMessages old_ expire = \case
        AMS SQSMemory SMSMemory ms@STMMsgStore {storeConfig = STMStoreConfig {storePath}} -> case storePath of
          Just f -> ifM (doesFileExist f) (Just <$> importMessages False ms f old_) (pure Nothing)
          Nothing -> pure Nothing
        AMS _ SMSJournal ms -> processJournalMessages old_ expire ms
        -- AMS (SType SQSPostgres SMSJournal) ms -> processJournalMessages old_ expire ms
      processJournalMessages :: forall s. Maybe Int64 -> Bool -> JournalMsgStore s -> IO (Maybe MessageStats)
      processJournalMessages old_ expire ms
        | expire = Just <$> case old_ of
            Just old -> do
              logInfo "expiring journal store messages..."
              withAllMsgQueues False ms $ processExpireQueue old
            Nothing -> do
              logInfo "validating journal store messages..."
              withAllMsgQueues False ms $ processValidateQueue
        | otherwise = logWarn "skipping message expiration" $> Nothing
        where
          processExpireQueue :: Int64 -> JournalQueue s -> IO MessageStats
          processExpireQueue old q =
            runExceptT expireQueue >>= \case
              Right (storedMsgsCount, expiredMsgsCount) ->
                pure MessageStats {storedMsgsCount, expiredMsgsCount, storedQueues = 1}
              Left e -> do
                logError $ "STORE: processExpireQueue, failed expiring messages in queue, " <> tshow e
                exitFailure
            where
              expireQueue = do
                expired'' <- deleteExpiredMsgs ms q old
                stored'' <- getQueueSize ms q
                liftIO $ closeMsgQueue q
                pure (stored'', expired'')
          processValidateQueue :: JournalQueue s -> IO MessageStats
          processValidateQueue q =
            runExceptT (getQueueSize ms q) >>= \case
              Right storedMsgsCount -> pure newMessageStats {storedMsgsCount, storedQueues = 1}
              Left e -> do
                logError $ "STORE: processValidateQueue, failed opening message queue, " <> tshow e
                exitFailure

-- TODO this function should be called after importing queues from store log
importMessages :: forall s. MsgStoreClass s => Bool -> s -> FilePath -> Maybe Int64 -> IO MessageStats
importMessages tty ms f old_ = do
  logInfo $ "restoring messages from file " <> T.pack f
  LB.readFile f >>= runExceptT . foldM restoreMsg (0, Nothing, (0, 0, M.empty)) . LB.lines >>= \case
    Left e -> do
      when tty $ putStrLn ""
      logError . T.pack $ "error restoring messages: " <> e
      liftIO exitFailure
    Right (lineCount, _, (storedMsgsCount, expiredMsgsCount, overQuota)) -> do
      putStrLn $ progress lineCount
      renameFile f $ f <> ".bak"
      mapM_ setOverQuota_ overQuota
      logQueueStates ms
      QueueCounts {queueCount} <- liftIO $ queueCounts @(StoreQueue s) $ queueStore ms
      pure MessageStats {storedMsgsCount, expiredMsgsCount, storedQueues = queueCount}
  where
    progress i = "Processed " <> show i <> " lines"
    restoreMsg :: (Int, Maybe (RecipientId, StoreQueue s), (Int, Int, M.Map RecipientId (StoreQueue s))) -> LB.ByteString -> ExceptT String IO (Int, Maybe (RecipientId, StoreQueue s), (Int, Int, M.Map RecipientId (StoreQueue s)))
    restoreMsg (!i, q_, (!stored, !expired, !overQuota)) s' = do
      when (tty && i `mod` 1000 == 0) $ liftIO $ putStr (progress i <> "\r") >> hFlush stdout
      MLRv3 rId msg <- liftEither . first (msgErr "parsing") $ strDecode s
      liftError show $ addToMsgQueue rId msg
      where
        s = LB.toStrict s'
        addToMsgQueue rId msg = do
          q <- case q_ of
            -- to avoid lookup when restoring the next message to the same queue
            Just (rId', q') | rId' == rId -> pure q'
            _ -> ExceptT $ getQueue ms SRecipient rId
          (i + 1,Just (rId, q),) <$> case msg of
            Message {msgTs}
              | maybe True (systemSeconds msgTs >=) old_ -> do
                  writeMsg ms q False msg >>= \case
                    Just _ -> pure (stored + 1, expired, overQuota)
                    Nothing -> do
                      logError $ decodeLatin1 $ "message queue " <> strEncode rId <> " is full, message not restored: " <> strEncode (messageId msg)
                      pure (stored, expired, overQuota)
              | otherwise -> pure (stored, expired + 1, overQuota)
            MessageQuota {} ->
              -- queue was over quota at some point,
              -- it will be set as over quota once fully imported
              mergeQuotaMsgs >> writeMsg ms q False msg $> (stored, expired, M.insert rId q overQuota)
              where
                -- if the first message in queue head is "quota", remove it.
                mergeQuotaMsgs =
                  withPeekMsgQueue ms q "mergeQuotaMsgs" $ maybe (pure ()) $ \case
                    (mq, MessageQuota {}) -> tryDeleteMsg_ q mq False
                    _ -> pure ()
        msgErr :: Show e => String -> e -> String
        msgErr op e = op <> " error (" <> show e <> "): " <> B.unpack (B.take 100 s)

printMessageStats :: T.Text -> MessageStats -> IO ()
printMessageStats name MessageStats {storedMsgsCount, expiredMsgsCount, storedQueues} =
  logInfo $ name <> " stored: " <> tshow storedMsgsCount <> ", expired: " <> tshow expiredMsgsCount <> ", queues: " <> tshow storedQueues

saveServerNtfs :: M ()
saveServerNtfs = asks (storeNtfsFile . config) >>= mapM_ saveNtfs
  where
    saveNtfs f = do
      logInfo $ "saving notifications to file " <> T.pack f
      NtfStore ns <- asks ntfStore
      liftIO . withFile f WriteMode $ \h ->
        readTVarIO ns >>= mapM_ (saveQueueNtfs h) . M.assocs
      logInfo "notifications saved"
      where
        -- reverse on save, to save notifications in order, will become reversed again when restoring.
        saveQueueNtfs h (nId, v) = BLD.hPutBuilder h . encodeNtfs nId . reverse =<< readTVarIO v
        encodeNtfs nId = mconcat . map (\ntf -> BLD.byteString (strEncode $ NLRv1 nId ntf) <> BLD.char8 '\n')

restoreServerNtfs :: M MessageStats
restoreServerNtfs =
  asks (storeNtfsFile . config) >>= \case
    Just f -> ifM (doesFileExist f) (restoreNtfs f) (pure newMessageStats)
    Nothing -> pure newMessageStats
  where
    restoreNtfs f = do
      logInfo $ "restoring notifications from file " <> T.pack f
      ns <- asks ntfStore
      old <- asks (notificationExpiration . config) >>= liftIO . expireBeforeEpoch
      liftIO $
        LB.readFile f >>= runExceptT . foldM (restoreNtf ns old) (0, 0, 0) . LB.lines >>= \case
          Left e -> do
            logError . T.pack $ "error restoring notifications: " <> e
            liftIO exitFailure
          Right (lineCount, storedMsgsCount, expiredMsgsCount) -> do
            renameFile f $ f <> ".bak"
            let NtfStore ns' = ns
            storedQueues <- M.size <$> readTVarIO ns'
            logInfo $ "notifications restored, " <> tshow lineCount <> " lines processed" 
            pure MessageStats {storedMsgsCount, expiredMsgsCount, storedQueues}
      where
        restoreNtf :: NtfStore -> Int64 -> (Int, Int, Int) -> LB.ByteString -> ExceptT String IO (Int, Int, Int)
        restoreNtf ns old (!lineCount, !stored, !expired) s' = do
          NLRv1 nId ntf <- liftEither . first (ntfErr "parsing") $ strDecode s
          liftIO $ addToNtfs nId ntf
          where
            s = LB.toStrict s'
            addToNtfs nId ntf@MsgNtf {ntfTs}
              | systemSeconds ntfTs < old = pure (lineCount + 1, stored, expired + 1)
              | otherwise = storeNtf ns nId ntf $> (lineCount + 1, stored + 1, expired)
            ntfErr :: Show e => String -> e -> String
            ntfErr op e = op <> " error (" <> show e <> "): " <> B.unpack (B.take 100 s)

saveServerStats :: M ()
saveServerStats =
  asks (serverStatsBackupFile . config)
    >>= mapM_ (\f -> asks serverStats >>= liftIO . getServerStatsData >>= liftIO . saveStats f)
  where
    saveStats f stats = do
      logInfo $ "saving server stats to file " <> T.pack f
      B.writeFile f $ strEncode stats
      logInfo "server stats saved"

restoreServerStats :: Maybe MessageStats -> MessageStats -> M ()
restoreServerStats msgStats_ ntfStats = asks (serverStatsBackupFile . config) >>= mapM_ restoreStats
  where
    restoreStats f = whenM (doesFileExist f) $ do
      logInfo $ "restoring server stats from file " <> T.pack f
      liftIO (strDecode <$> B.readFile f) >>= \case
        Right d@ServerStatsData {_qCount = statsQCount, _msgCount = statsMsgCount, _ntfCount = statsNtfCount} -> do
          s <- asks serverStats
          AMS _ _ (st :: s) <- asks msgStore
          QueueCounts {queueCount = _qCount} <- liftIO $ queueCounts @(StoreQueue s) $ queueStore st
          let _msgCount = maybe statsMsgCount storedMsgsCount msgStats_
              _ntfCount = storedMsgsCount ntfStats
              _msgExpired' = _msgExpired d + maybe 0 expiredMsgsCount msgStats_
              _msgNtfExpired' = _msgNtfExpired d + expiredMsgsCount ntfStats
          liftIO $ setServerStats s d {_qCount, _msgCount, _ntfCount, _msgExpired = _msgExpired', _msgNtfExpired = _msgNtfExpired'}
          renameFile f $ f <> ".bak"
          logInfo "server stats restored"
          compareCounts "Queue" statsQCount _qCount
          compareCounts "Message" statsMsgCount _msgCount
          compareCounts "Notification" statsNtfCount _ntfCount
        Left e -> do
          logInfo $ "error restoring server stats: " <> T.pack e
          liftIO exitFailure
    compareCounts name statsCnt storeCnt =
      when (statsCnt /= storeCnt) $ logWarn $ name <> " count differs: stats: " <> tshow statsCnt <> ", store: " <> tshow storeCnt