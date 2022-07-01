{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Agent.NtfSubSupervisor
  ( runNtfSupervisor,
    nsUpdateToken,
    nsRemoveNtfToken,
    sendNtfSubCommand,
    closeNtfSupervisor,
    getNtfServer,
  )
where

import Control.Concurrent.Async (Async, uninterruptibleCancel)
import Control.Concurrent.STM (stateTVar)
import Control.Logger.Simple (logInfo)
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Reader
import Data.Bifunctor (first)
import Data.Fixed (Fixed (MkFixed), Pico)
import qualified Data.Map.Strict as M
import Data.Time (UTCTime, addUTCTime, diffUTCTime, getCurrentTime, nominalDiffTimeToSeconds)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol (AgentErrorType (..), BrokerErrorType (..), ConnId, NotificationsMode (..))
import qualified Simplex.Messaging.Agent.Protocol as AP
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite
import Simplex.Messaging.Client.Agent ()
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Notifications.Protocol (NtfSubStatus (..), NtfTknStatus (..), SMPQueueNtf (..))
import Simplex.Messaging.Notifications.Types
import Simplex.Messaging.Protocol
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (tshow, unlessM)
import System.Random (randomR)
import UnliftIO (async)
import UnliftIO.Concurrent (forkIO, threadDelay)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

runNtfSupervisor :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
runNtfSupervisor c = forever $ do
  ns <- asks ntfSupervisor
  cmd <- atomically . readTBQueue $ ntfSubQ ns
  runExceptT (processNtfSub c cmd) >>= \case
    Left e -> liftIO $ print e
    Right _ -> return ()

processNtfSub :: forall m. AgentMonad m => AgentClient -> (ConnId, NtfSupervisorCommand) -> m ()
processNtfSub c@AgentClient {subQ} (connId, cmd) = do
  logInfo $ "processNtfSub - connId = " <> tshow connId <> " - cmd = " <> tshow cmd
  case cmd of
    NSCCreate -> do
      (a, RcvQueue {server = smpServer, clientNtfCreds}) <- withStore c $ \db -> runExceptT $ do
        a <- liftIO $ getNtfSubscription db connId
        q <- ExceptT $ getRcvQueue db connId
        pure (a, q)
      logInfo $ "processNtfSub, NSCCreate - a = " <> tshow a
      case a of
        Nothing -> do
          withNtfServer c $ \ntfServer -> do
            case clientNtfCreds of
              Just ClientNtfCreds {notifierId} -> do
                let newSub = newNtfSubscription connId smpServer (Just notifierId) ntfServer NASKey
                ts <- liftIO getCurrentTime
                withStore' c $ \db -> createNtfSubscription db newSub (NtfSubNTFAction NSACreate) ts
                addNtfNTFWorker ntfServer
              Nothing -> do
                let newSub = newNtfSubscription connId smpServer Nothing ntfServer NASNew
                ts <- liftIO getCurrentTime
                withStore' c $ \db -> createNtfSubscription db newSub (NtfSubSMPAction NSASmpKey) ts
                addNtfSMPWorker smpServer
        (Just (sub@NtfSubscription {ntfSubStatus, ntfServer = subNtfServer}, action_)) -> do
          case action_ of
            -- action was set to NULL after worker internal error
            Nothing -> resetSubscription
            Just (action, _)
              -- subscription was marked for deletion / is being deleted
              | isDeleteNtfSubAction action -> do
                if ntfSubStatus == NASNew || ntfSubStatus == NASOff || ntfSubStatus == NASDeleted
                  then resetSubscription
                  else withNtfServer c $ \ntfServer -> do
                    ts <- liftIO getCurrentTime
                    withStore' c $ \db ->
                      supervisorUpdateNtfSubscription db sub {ntfServer} (NtfSubNTFAction NSACreate) ts
                    addNtfNTFWorker ntfServer
              | otherwise -> case action of
                NtfSubNTFAction _ -> addNtfNTFWorker subNtfServer
                NtfSubSMPAction _ -> addNtfSMPWorker smpServer
          where
            resetSubscription :: m ()
            resetSubscription =
              withNtfServer c $ \ntfServer -> do
                ts <- liftIO getCurrentTime
                withStore' c $ \db ->
                  supervisorUpdateNtfSubscription db sub {ntfQueueId = Nothing, ntfServer, ntfSubId = Nothing, ntfSubStatus = NASNew} (NtfSubSMPAction NSASmpKey) ts
                addNtfSMPWorker smpServer
    NSCDelete -> do
      sub_ <- withStore' c $ \db -> do
        ts <- liftIO getCurrentTime
        supervisorUpdateNtfSubAction db connId (NtfSubNTFAction NSADelete) ts
        getNtfSubscription db connId
      logInfo $ "processNtfSub, NSCDelete - sub_ = " <> tshow sub_
      case sub_ of
        (Just (NtfSubscription {ntfServer}, _)) -> addNtfNTFWorker ntfServer
        _ -> pure () -- err "NSCDelete - no subscription"
    NSCSmpDelete -> do
      withStore' c (`getRcvQueue` connId) >>= \case
        Right rq@RcvQueue {server = smpServer} -> do
          logInfo $ "processNtfSub, NSCSmpDelete - rq = " <> tshow rq
          ts <- liftIO getCurrentTime
          withStore' c $ \db -> supervisorUpdateNtfSubAction db connId (NtfSubSMPAction NSASmpDelete) ts
          addNtfSMPWorker smpServer
        _ -> err "NSCSmpDelete - no rcv queue"
    NSCNtfWorker ntfServer ->
      addNtfNTFWorker ntfServer
    NSCNtfSMPWorker smpServer ->
      addNtfSMPWorker smpServer
  where
    addNtfNTFWorker = addWorker ntfWorkers runNtfWorker
    addNtfSMPWorker = addWorker ntfSMPWorkers runNtfSMPWorker
    addWorker ::
      (NtfSupervisor -> TMap ProtocolServer (TMVar (), Async ())) ->
      (AgentClient -> ProtocolServer -> TMVar () -> m ()) ->
      ProtocolServer ->
      m ()
    addWorker wsSel runWorker srv = do
      ws <- asks $ wsSel . ntfSupervisor
      atomically (TM.lookup srv ws) >>= \case
        Nothing -> do
          doWork <- newTMVarIO ()
          worker <- async $ runWorker c srv doWork `E.finally` atomically (TM.delete srv ws)
          atomically $ TM.insert srv (doWork, worker) ws
        Just (doWork, _) ->
          void . atomically $ tryPutTMVar doWork ()
    err :: String -> m ()
    err internalErrStr = atomically $ writeTBQueue subQ ("", connId, AP.ERR $ AP.INTERNAL internalErrStr)

withNtfServer :: AgentMonad m => AgentClient -> (NtfServer -> m ()) -> m ()
withNtfServer c action = getNtfServer c >>= mapM_ action

runNtfWorker :: forall m. AgentMonad m => AgentClient -> NtfServer -> TMVar () -> m ()
runNtfWorker c srv doWork = forever $ do
  void . atomically $ readTMVar doWork
  nextSub_ <- withStore' c (`getNextNtfSubNTFAction` srv)
  logInfo $ "runNtfWorker, nextSub_ " <> tshow nextSub_
  case nextSub_ of
    Nothing -> noWorkToDo
    Just a@(NtfSubscription {connId}, _, _) -> do
      ri <- asks $ reconnectInterval . config
      withRetryInterval ri $ \loop ->
        processAction a
          `catchError` ( \e -> do
                           logInfo $ "runNtfWorker, error " <> tshow e
                           case e of
                             BROKER NETWORK -> loop
                             BROKER TIMEOUT -> loop
                             _ -> ntfInternalError c connId (show e)
                       )
  throttle <- asks $ ntfWorkerThrottle . config
  liftIO $ threadDelay throttle
  where
    noWorkToDo = void . atomically $ tryTakeTMVar doWork
    processAction :: (NtfSubscription, NtfSubNTFAction, NtfActionTs) -> m ()
    processAction (sub@NtfSubscription {connId, smpServer, ntfSubId}, action, actionTs) = do
      ts <- liftIO getCurrentTime
      unlessM (rescheduleAction doWork ts actionTs) $
        case action of
          NSACreate ->
            getNtfToken >>= \case
              Just tkn@NtfToken {ntfTokenId = Just tknId, ntfTknStatus = NTActive, ntfMode = NMInstant} -> do
                RcvQueue {clientNtfCreds} <- withStore c (`getRcvQueue` connId)
                case clientNtfCreds of
                  Just ClientNtfCreds {ntfPrivateKey, notifierId} -> do
                    nSubId <- agentNtfCreateSubscription c tknId tkn (SMPQueueNtf smpServer notifierId) ntfPrivateKey
                    -- TODO smaller retry until Active, less frequently (daily?) once Active
                    let actionTs' = addUTCTime 30 ts
                    withStore' c $ \db ->
                      updateNtfSubscription db sub {ntfSubId = Just nSubId, ntfSubStatus = NASCreated NSNew} (NtfSubNTFAction NSACheck) actionTs'
                  _ -> ntfInternalError c connId "NSACreate - no notifier queue credentials"
              _ -> ntfInternalError c connId "NSACreate - no active token"
          NSACheck ->
            getNtfToken >>= \case
              Just tkn ->
                case ntfSubId of
                  Just nSubId ->
                    agentNtfCheckSubscription c nSubId tkn >>= \case
                      NSSMPAuth -> do
                        getNtfServer c >>= \case
                          Just ntfServer -> do
                            withStore' c $ \db ->
                              updateNtfSubscription db sub {ntfServer, ntfQueueId = Nothing, ntfSubId = Nothing, ntfSubStatus = NASNew} (NtfSubSMPAction NSASmpKey) ts
                            ns <- asks ntfSupervisor
                            atomically $ writeTBQueue (ntfSubQ ns) (connId, NSCNtfSMPWorker smpServer)
                          _ -> ntfInternalError c connId "NSACheck - failed to reset subscription, notification server not configured"
                      status -> updateSubNextCheck ts status
                  Nothing -> ntfInternalError c connId "NSACheck - no subscription ID"
              _ -> ntfInternalError c connId "NSACheck - no active token"
          NSADelete -> case ntfSubId of
            Just nSubId ->
              (getNtfToken >>= \tkn -> forM_ tkn $ agentNtfDeleteSubscription c nSubId)
                `E.finally` carryOnWithDeletion
            Nothing -> carryOnWithDeletion
            where
              carryOnWithDeletion :: m ()
              carryOnWithDeletion = do
                withStore' c $ \db ->
                  updateNtfSubscription db sub {ntfSubId = Nothing, ntfSubStatus = NASOff} (NtfSubSMPAction NSASmpDelete) ts
                ns <- asks ntfSupervisor
                atomically $ writeTBQueue (ntfSubQ ns) (connId, NSCNtfSMPWorker smpServer)
      where
        updateSubNextCheck ts toStatus = do
          checkInterval <- asks $ ntfSubCheckInterval . config
          let nextCheckTs = addUTCTime checkInterval ts
          updateSub (NASCreated toStatus) (NtfSubNTFAction NSACheck) nextCheckTs
        updateSub toStatus toAction actionTs' =
          withStore' c $ \db ->
            updateNtfSubscription db sub {ntfSubStatus = toStatus} toAction actionTs'

runNtfSMPWorker :: forall m. AgentMonad m => AgentClient -> SMPServer -> TMVar () -> m ()
runNtfSMPWorker c srv doWork = forever $ do
  void . atomically $ readTMVar doWork
  nextSub_ <- withStore' c (`getNextNtfSubSMPAction` srv)
  logInfo $ "runNtfSMPWorker, nextSub_ " <> tshow nextSub_
  case nextSub_ of
    Nothing -> noWorkToDo
    Just a@(NtfSubscription {connId}, _, _) -> do
      ri <- asks $ reconnectInterval . config
      withRetryInterval ri $ \loop ->
        processAction a
          `catchError` ( \e -> do
                           logInfo $ "runNtfSMPWorker, error " <> tshow e
                           case e of
                             BROKER NETWORK -> loop
                             BROKER TIMEOUT -> loop
                             _ -> ntfInternalError c connId (show e)
                       )
  throttle <- asks $ ntfWorkerThrottle . config
  liftIO $ threadDelay throttle
  where
    noWorkToDo = void . atomically $ tryTakeTMVar doWork
    processAction :: (NtfSubscription, NtfSubSMPAction, NtfActionTs) -> m ()
    processAction (sub@NtfSubscription {connId, ntfServer}, smpAction, actionTs) = do
      ts <- liftIO getCurrentTime
      unlessM (rescheduleAction doWork ts actionTs) $
        case smpAction of
          NSASmpKey ->
            getNtfToken >>= \case
              Just NtfToken {ntfTknStatus = NTActive, ntfMode = NMInstant} -> do
                rq <- withStore c (`getRcvQueue` connId)
                C.SignAlg a <- asks (cmdSignAlg . config)
                (ntfPublicKey, ntfPrivateKey) <- liftIO $ C.generateSignatureKeyPair a
                (rcvNtfPubDhKey, rcvNtfPrivDhKey) <- liftIO C.generateKeyPair'
                (notifierId, rcvNtfSrvPubDhKey) <- enableQueueNotifications c rq ntfPublicKey rcvNtfPubDhKey
                let rcvNtfDhSecret = C.dh' rcvNtfSrvPubDhKey rcvNtfPrivDhKey
                withStore' c $ \db -> do
                  setRcvQueueNtfCreds db connId $ Just ClientNtfCreds {ntfPublicKey, ntfPrivateKey, notifierId, rcvNtfDhSecret}
                  updateNtfSubscription db sub {ntfQueueId = Just notifierId, ntfSubStatus = NASKey} (NtfSubNTFAction NSACreate) ts
                ns <- asks ntfSupervisor
                atomically $ sendNtfSubCommand ns (connId, NSCNtfWorker ntfServer)
              _ -> ntfInternalError c connId "NSASmpKey - no active token"
          NSASmpDelete -> do
            rq_ <- withStore' c $ \db -> do
              setRcvQueueNtfCreds db connId Nothing
              getRcvQueue db connId
            forM_ rq_ $ \rq -> disableQueueNotifications c rq
            withStore' c $ \db -> deleteNtfSubscription db connId

rescheduleAction :: AgentMonad m => TMVar () -> UTCTime -> UTCTime -> m Bool
rescheduleAction doWork ts actionTs
  | actionTs <= ts = pure False
  | otherwise = do
    void . atomically $ tryTakeTMVar doWork
    void . forkIO $ do
      threadDelay $ diffInMicros actionTs ts
      void . atomically $ tryPutTMVar doWork ()
    pure True

fromPico :: Pico -> Integer
fromPico (MkFixed i) = i

diffInMicros :: UTCTime -> UTCTime -> Int
diffInMicros a b = (`div` 1000000) . fromInteger . fromPico . nominalDiffTimeToSeconds $ diffUTCTime a b

ntfInternalError :: AgentMonad m => AgentClient -> ConnId -> String -> m ()
ntfInternalError c@AgentClient {subQ} connId internalErrStr = do
  withStore' c $ \db -> setNullNtfSubscriptionAction db connId
  atomically $ writeTBQueue subQ ("", connId, AP.ERR $ AP.INTERNAL internalErrStr)

getNtfToken :: AgentMonad m => m (Maybe NtfToken)
getNtfToken = do
  tkn <- asks $ ntfTkn . ntfSupervisor
  readTVarIO tkn

nsUpdateToken :: NtfSupervisor -> NtfToken -> STM ()
nsUpdateToken ns tkn = writeTVar (ntfTkn ns) $ Just tkn

nsRemoveNtfToken :: NtfSupervisor -> STM ()
nsRemoveNtfToken ns = writeTVar (ntfTkn ns) Nothing

sendNtfSubCommand :: NtfSupervisor -> (ConnId, NtfSupervisorCommand) -> STM ()
sendNtfSubCommand ns cmd =
  readTVar (ntfTkn ns)
    >>= mapM_
      ( \NtfToken {ntfTknStatus, ntfMode} ->
          when (ntfTknStatus == NTActive && ntfMode == NMInstant) $
            writeTBQueue (ntfSubQ ns) cmd
      )

closeNtfSupervisor :: NtfSupervisor -> IO ()
closeNtfSupervisor ns = do
  cancelNtfWorkers_ $ ntfWorkers ns
  cancelNtfWorkers_ $ ntfSMPWorkers ns

cancelNtfWorkers_ :: TMap ProtocolServer (TMVar (), Async ()) -> IO ()
cancelNtfWorkers_ wsVar = do
  ws <- atomically $ stateTVar wsVar (,M.empty)
  forM_ ws $ uninterruptibleCancel . snd

getNtfServer :: AgentMonad m => AgentClient -> m (Maybe NtfServer)
getNtfServer c = do
  ntfServers <- readTVarIO $ ntfServers c
  case ntfServers of
    [] -> pure Nothing
    [srv] -> pure $ Just srv
    servers -> do
      gen <- asks randomServer
      atomically . stateTVar gen $
        first (Just . (servers !!)) . randomR (0, length servers - 1)
