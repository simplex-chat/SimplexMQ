{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}

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
  ( -- * SMP agent functional API
    AgentClient (..),
    AE,
    SubscriptionsInfo (..),
    MsgReq,
    getSMPAgentClient,
    getSMPAgentClient_,
    disconnectAgentClient,
    disposeAgentClient,
    resumeAgentClient,
    withConnLock,
    withInvLock,
    createUser,
    deleteUser,
    connRequestPQSupport,
    createConnectionAsync,
    joinConnectionAsync,
    allowConnectionAsync,
    acceptContactAsync,
    ackMessageAsync,
    switchConnectionAsync,
    deleteConnectionAsync,
    deleteConnectionsAsync,
    createConnection,
    changeConnectionUser,
    prepareConnectionToJoin,
    prepareConnectionToAccept,
    joinConnection,
    allowConnection,
    acceptContact,
    rejectContact,
    subscribeConnection,
    subscribeConnections,
    getConnectionMessages,
    getNotificationConns,
    resubscribeConnection,
    resubscribeConnections,
    sendMessage,
    sendMessages,
    sendMessagesB,
    ackMessage,
    getConnectionQueueInfo,
    switchConnection,
    abortConnectionSwitch,
    synchronizeRatchet,
    suspendConnection,
    deleteConnection,
    deleteConnections,
    getConnectionServers,
    getConnectionRatchetAdHash,
    setProtocolServers,
    checkUserServers,
    testProtocolServer,
    setNtfServers,
    setNetworkConfig,
    setUserNetworkInfo,
    reconnectAllServers,
    reconnectSMPServer,
    registerNtfToken,
    verifyNtfToken,
    checkNtfToken,
    deleteNtfToken,
    getNtfToken,
    getNtfTokenData,
    toggleConnectionNtfs,
    xftpStartWorkers,
    xftpStartSndWorkers,
    xftpReceiveFile,
    xftpDeleteRcvFile,
    xftpDeleteRcvFiles,
    xftpSendFile,
    xftpSendDescription,
    xftpDeleteSndFileInternal,
    xftpDeleteSndFilesInternal,
    xftpDeleteSndFileRemote,
    xftpDeleteSndFilesRemote,
    rcNewHostPairing,
    rcConnectHost,
    rcConnectCtrl,
    rcDiscoverCtrl,
    getAgentSubsTotal,
    getAgentServersSummary,
    resetAgentServersStats,
    foregroundAgent,
    suspendAgent,
    execAgentStoreSQL,
    getAgentMigrations,
    debugAgentLocks,
    getAgentSubscriptions,
    logConnection,
    -- for tests
    withAgentEnv,
  )
where

import Control.Logger.Simple
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Trans.Except
import Crypto.Random (ChaChaDRG)
import qualified Data.Aeson as J
import Data.Bifunctor (bimap, first, second)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Composition ((.:), (.:.), (.::), (.::.))
import Data.Either (isRight, partitionEithers, rights)
import Data.Foldable (foldl', toList)
import Data.Functor (($>))
import Data.Functor.Identity
import Data.List (find)
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing, mapMaybe)
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock.System (systemToUTCTime)
import Data.Traversable (mapAccumL)
import Data.Word (Word16)
import Simplex.FileTransfer.Agent (closeXFTPAgent, deleteSndFileInternal, deleteSndFileRemote, deleteSndFilesInternal, deleteSndFilesRemote, startXFTPSndWorkers, startXFTPWorkers, toFSFilePath, xftpDeleteRcvFile', xftpDeleteRcvFiles', xftpReceiveFile', xftpSendDescription', xftpSendFile')
import Simplex.FileTransfer.Description (ValidFileDescription)
import Simplex.FileTransfer.Protocol (FileParty (..))
import Simplex.FileTransfer.Types (RcvFileId, SndFileId)
import Simplex.FileTransfer.Util (removePath)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Lock (withLock, withLock')
import Simplex.Messaging.Agent.NtfSubSupervisor
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Stats
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.AgentStore
import Simplex.Messaging.Agent.Store.Common (DBStore)
import qualified Simplex.Messaging.Agent.Store.DB as DB
import Simplex.Messaging.Agent.Store.Interface (closeDBStore, execSQL)
import qualified Simplex.Messaging.Agent.Store.Migrations as Migrations
import Simplex.Messaging.Agent.Store.Shared (UpMigration (..), upMigration)
import Simplex.Messaging.Client (SMPClientError, ServerTransmission (..), ServerTransmissionBatch, temporaryClientError, unexpectedResponse)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.File (CryptoFile, CryptoFileArgs)
import Simplex.Messaging.Crypto.Ratchet (PQEncryption, PQSupport (..), pattern PQEncOff, pattern PQEncOn, pattern PQSupportOff, pattern PQSupportOn)
import qualified Simplex.Messaging.Crypto.Ratchet as CR
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Notifications.Protocol (DeviceToken, NtfRegCode (NtfRegCode), NtfTknStatus (..), NtfTokenId, PNMessageData (..), pnMessagesP)
import Simplex.Messaging.Notifications.Types
import Simplex.Messaging.Parsers (parse)
import Simplex.Messaging.Protocol (BrokerMsg, Cmd (..), ErrorType (AUTH), MsgBody, MsgFlags (..), NtfServer, ProtoServerWithAuth, ProtocolType (..), ProtocolTypeI (..), SMPMsgMeta, SParty (..), SProtocolType (..), SndPublicAuthKey, SubscriptionMode (..), UserProtocol, VersionSMPC, sndAuthKeySMPClientVersion)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.ServiceScheme (ServiceScheme (..))
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport (SMPVersion)
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import Simplex.RemoteControl.Client
import Simplex.RemoteControl.Invitation
import Simplex.RemoteControl.Types
import System.Mem.Weak (deRefWeak)
import UnliftIO.Concurrent (forkFinally, forkIO, killThread, mkWeakThreadId, threadDelay)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

-- import GHC.Conc (unsafeIOToSTM)

type AE a = ExceptT AgentErrorType IO a

-- | Creates an SMP agent client instance
getSMPAgentClient :: AgentConfig -> InitialAgentServers -> DBStore -> Bool -> IO AgentClient
getSMPAgentClient = getSMPAgentClient_ 1
{-# INLINE getSMPAgentClient #-}

getSMPAgentClient_ :: Int -> AgentConfig -> InitialAgentServers -> DBStore -> Bool -> IO AgentClient
getSMPAgentClient_ clientId cfg initServers@InitialAgentServers {smp, xftp} store backgroundMode =
  newSMPAgentEnv cfg store >>= runReaderT runAgent
  where
    runAgent = do
      liftIO $ checkServers "SMP" smp >> checkServers "XFTP" xftp
      currentTs <- liftIO getCurrentTime
      c@AgentClient {acThread} <- liftIO . newAgentClient clientId initServers currentTs =<< ask
      t <- runAgentThreads c `forkFinally` const (liftIO $ disconnectAgentClient c)
      atomically . writeTVar acThread . Just =<< mkWeakThreadId t
      pure c
    checkServers protocol srvs =
      forM_ (M.assocs srvs) $ \(userId, srvs') -> checkUserServers ("getSMPAgentClient " <> protocol <> " " <> tshow userId) srvs'
    runAgentThreads c
      | backgroundMode = run c "subscriber" $ subscriber c
      | otherwise = do
          restoreServersStats c
          raceAny_
            [ run c "subscriber" $ subscriber c,
              run c "runNtfSupervisor" $ runNtfSupervisor c,
              run c "cleanupManager" $ cleanupManager c,
              run c "logServersStats" $ logServersStats c
            ]
            `E.finally` saveServersStats c
    run AgentClient {subQ, acThread} name a =
      a `E.catchAny` \e -> whenM (isJust <$> readTVarIO acThread) $ do
        logError $ "Agent thread " <> name <> " crashed: " <> tshow e
        atomically $ writeTBQueue subQ ("", "", AEvt SAEConn $ ERR $ CRITICAL True $ show e)

logServersStats :: AgentClient -> AM' ()
logServersStats c = do
  delay <- asks (initialLogStatsDelay . config)
  liftIO $ threadDelay' delay
  int <- asks (logStatsInterval . config)
  forever $ do
    liftIO $ waitUntilActive c
    saveServersStats c
    liftIO $ threadDelay' int

saveServersStats :: AgentClient -> AM' ()
saveServersStats c@AgentClient {subQ, smpServersStats, xftpServersStats, ntfServersStats} = do
  sss <- mapM (liftIO . getAgentSMPServerStats) =<< readTVarIO smpServersStats
  xss <- mapM (liftIO . getAgentXFTPServerStats) =<< readTVarIO xftpServersStats
  nss <- mapM (liftIO . getAgentNtfServerStats) =<< readTVarIO ntfServersStats
  let stats = AgentPersistedServerStats {smpServersStats = sss, xftpServersStats = xss, ntfServersStats = OptionalMap nss}
  tryAgentError' (withStore' c (`updateServersStats` stats)) >>= \case
    Left e -> atomically $ writeTBQueue subQ ("", "", AEvt SAEConn $ ERR $ INTERNAL $ show e)
    Right () -> pure ()

restoreServersStats :: AgentClient -> AM' ()
restoreServersStats c@AgentClient {smpServersStats, xftpServersStats, ntfServersStats, srvStatsStartedAt} = do
  tryAgentError' (withStore c getServersStats) >>= \case
    Left e -> atomically $ writeTBQueue (subQ c) ("", "", AEvt SAEConn $ ERR $ INTERNAL $ show e)
    Right (startedAt, Nothing) -> atomically $ writeTVar srvStatsStartedAt startedAt
    Right (startedAt, Just AgentPersistedServerStats {smpServersStats = sss, xftpServersStats = xss, ntfServersStats = OptionalMap nss}) -> do
      atomically $ writeTVar srvStatsStartedAt startedAt
      atomically . writeTVar smpServersStats =<< mapM (atomically . newAgentSMPServerStats') sss
      atomically . writeTVar xftpServersStats =<< mapM (atomically . newAgentXFTPServerStats') xss
      atomically . writeTVar ntfServersStats =<< mapM (atomically . newAgentNtfServerStats') nss

disconnectAgentClient :: AgentClient -> IO ()
disconnectAgentClient c@AgentClient {agentEnv = Env {ntfSupervisor = ns, xftpAgent = xa}} = do
  closeAgentClient c
  closeNtfSupervisor ns
  closeXFTPAgent xa
  logConnection c False

-- only used in the tests
disposeAgentClient :: AgentClient -> IO ()
disposeAgentClient c@AgentClient {acThread, agentEnv = Env {store}} = do
  t_ <- atomically (swapTVar acThread Nothing) $>>= (liftIO . deRefWeak)
  disconnectAgentClient c
  mapM_ killThread t_
  liftIO $ closeDBStore store

resumeAgentClient :: AgentClient -> IO ()
resumeAgentClient c = atomically $ writeTVar (active c) True
{-# INLINE resumeAgentClient #-}

createUser :: AgentClient -> NonEmpty (ServerCfg 'PSMP) -> NonEmpty (ServerCfg 'PXFTP) -> AE UserId
createUser c = withAgentEnv c .: createUser' c
{-# INLINE createUser #-}

-- | Delete user record optionally deleting all user's connections on SMP servers
deleteUser :: AgentClient -> UserId -> Bool -> AE ()
deleteUser c = withAgentEnv c .: deleteUser' c
{-# INLINE deleteUser #-}

-- | Create SMP agent connection (NEW command) asynchronously, synchronous response is new connection id
createConnectionAsync :: ConnectionModeI c => AgentClient -> UserId -> ACorrId -> Bool -> SConnectionMode c -> CR.InitialKeys -> SubscriptionMode -> AE ConnId
createConnectionAsync c userId aCorrId enableNtfs = withAgentEnv c .:. newConnAsync c userId aCorrId enableNtfs
{-# INLINE createConnectionAsync #-}

-- | Join SMP agent connection (JOIN command) asynchronously, synchronous response is new connection id
joinConnectionAsync :: AgentClient -> UserId -> ACorrId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> AE ConnId
joinConnectionAsync c userId aCorrId enableNtfs = withAgentEnv c .:: joinConnAsync c userId aCorrId enableNtfs
{-# INLINE joinConnectionAsync #-}

-- | Allow connection to continue after CONF notification (LET command), no synchronous response
allowConnectionAsync :: AgentClient -> ACorrId -> ConnId -> ConfirmationId -> ConnInfo -> AE ()
allowConnectionAsync c = withAgentEnv c .:: allowConnectionAsync' c
{-# INLINE allowConnectionAsync #-}

-- | Accept contact after REQ notification (ACPT command) asynchronously, synchronous response is new connection id
acceptContactAsync :: AgentClient -> ACorrId -> Bool -> ConfirmationId -> ConnInfo -> PQSupport -> SubscriptionMode -> AE ConnId
acceptContactAsync c aCorrId enableNtfs = withAgentEnv c .:: acceptContactAsync' c aCorrId enableNtfs
{-# INLINE acceptContactAsync #-}

-- | Acknowledge message (ACK command) asynchronously, no synchronous response
ackMessageAsync :: AgentClient -> ACorrId -> ConnId -> AgentMsgId -> Maybe MsgReceiptInfo -> AE ()
ackMessageAsync c = withAgentEnv c .:: ackMessageAsync' c
{-# INLINE ackMessageAsync #-}

-- | Switch connection to the new receive queue
switchConnectionAsync :: AgentClient -> ACorrId -> ConnId -> AE ConnectionStats
switchConnectionAsync c = withAgentEnv c .: switchConnectionAsync' c
{-# INLINE switchConnectionAsync #-}

-- | Delete SMP agent connection (DEL command) asynchronously, no synchronous response
deleteConnectionAsync :: AgentClient -> Bool -> ConnId -> AE ()
deleteConnectionAsync c waitDelivery = withAgentEnv c . deleteConnectionAsync' c waitDelivery
{-# INLINE deleteConnectionAsync #-}

-- | Delete SMP agent connections using batch commands asynchronously, no synchronous response
deleteConnectionsAsync :: AgentClient -> Bool -> [ConnId] -> AE ()
deleteConnectionsAsync c waitDelivery = withAgentEnv c . deleteConnectionsAsync' c waitDelivery
{-# INLINE deleteConnectionsAsync #-}

-- | Create SMP agent connection (NEW command)
createConnection :: AgentClient -> UserId -> Bool -> SConnectionMode c -> Maybe CRClientData -> CR.InitialKeys -> SubscriptionMode -> AE (ConnId, ConnectionRequestUri c)
createConnection c userId enableNtfs = withAgentEnv c .:: newConn c userId enableNtfs
{-# INLINE createConnection #-}

-- | Changes the user id associated with a connection
changeConnectionUser :: AgentClient -> UserId -> ConnId -> UserId -> AE ()
changeConnectionUser c oldUserId connId newUserId = withAgentEnv c $ changeConnectionUser' c oldUserId connId newUserId
{-# INLINE changeConnectionUser #-}

-- | Create SMP agent connection without queue (to be joined with joinConnection passing connection ID).
-- This method is required to prevent race condition when confirmation from peer is received before
-- the caller of joinConnection saves connection ID to the database.
-- Instead of it we could send confirmation asynchronously, but then it would be harder to report
-- "link deleted" (SMP AUTH) interactively, so this approach is simpler overall.
prepareConnectionToJoin :: AgentClient -> UserId -> Bool -> ConnectionRequestUri c -> PQSupport -> AE ConnId
prepareConnectionToJoin c userId enableNtfs = withAgentEnv c .: newConnToJoin c userId "" enableNtfs

-- | Create SMP agent connection without queue (to be joined with acceptContact passing invitation ID).
prepareConnectionToAccept :: AgentClient -> Bool -> ConfirmationId -> PQSupport -> AE ConnId
prepareConnectionToAccept c enableNtfs = withAgentEnv c .: newConnToAccept c "" enableNtfs

-- | Join SMP agent connection (JOIN command).
joinConnection :: AgentClient -> UserId -> ConnId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> AE SndQueueSecured
joinConnection c userId connId enableNtfs = withAgentEnv c .:: joinConn c userId connId enableNtfs
{-# INLINE joinConnection #-}

-- | Allow connection to continue after CONF notification (LET command)
allowConnection :: AgentClient -> ConnId -> ConfirmationId -> ConnInfo -> AE ()
allowConnection c = withAgentEnv c .:. allowConnection' c
{-# INLINE allowConnection #-}

-- | Accept contact after REQ notification (ACPT command)
acceptContact :: AgentClient -> ConnId -> Bool -> ConfirmationId -> ConnInfo -> PQSupport -> SubscriptionMode -> AE SndQueueSecured
acceptContact c connId enableNtfs = withAgentEnv c .:: acceptContact' c connId enableNtfs
{-# INLINE acceptContact #-}

-- | Reject contact (RJCT command)
rejectContact :: AgentClient -> ConnId -> ConfirmationId -> AE ()
rejectContact c = withAgentEnv c .: rejectContact' c
{-# INLINE rejectContact #-}

-- | Subscribe to receive connection messages (SUB command)
subscribeConnection :: AgentClient -> ConnId -> AE ()
subscribeConnection c = withAgentEnv c . subscribeConnection' c
{-# INLINE subscribeConnection #-}

-- | Subscribe to receive connection messages from multiple connections, batching commands when possible
subscribeConnections :: AgentClient -> [ConnId] -> AE (Map ConnId (Either AgentErrorType ()))
subscribeConnections c = withAgentEnv c . subscribeConnections' c
{-# INLINE subscribeConnections #-}

-- | Get messages for connections (GET commands)
getConnectionMessages :: AgentClient -> NonEmpty ConnId -> IO (NonEmpty (Maybe SMPMsgMeta))
getConnectionMessages c = withAgentEnv' c . getConnectionMessages' c
{-# INLINE getConnectionMessages #-}

-- | Get connections for received notification
getNotificationConns :: AgentClient -> C.CbNonce -> ByteString -> AE (NonEmpty NotificationInfo)
getNotificationConns c = withAgentEnv c .: getNotificationConns' c
{-# INLINE getNotificationConns #-}

resubscribeConnection :: AgentClient -> ConnId -> AE ()
resubscribeConnection c = withAgentEnv c . resubscribeConnection' c
{-# INLINE resubscribeConnection #-}

resubscribeConnections :: AgentClient -> [ConnId] -> AE (Map ConnId (Either AgentErrorType ()))
resubscribeConnections c = withAgentEnv c . resubscribeConnections' c
{-# INLINE resubscribeConnections #-}

-- | Send message to the connection (SEND command)
sendMessage :: AgentClient -> ConnId -> PQEncryption -> MsgFlags -> MsgBody -> AE (AgentMsgId, PQEncryption)
sendMessage c = withAgentEnv c .:: sendMessage' c
{-# INLINE sendMessage #-}

-- When sending multiple messages to the same connection,
-- only the first MsgReq for this connection should have non-empty ConnId.
-- All subsequent MsgReq in traversable for this connection must be empty.
-- This is done to optimize processing by grouping all messages to one connection together.
type MsgReq = (ConnId, PQEncryption, MsgFlags, MsgBody)

-- | Send multiple messages to different connections (SEND command)
sendMessages :: AgentClient -> [MsgReq] -> AE [Either AgentErrorType (AgentMsgId, PQEncryption)]
sendMessages c = withAgentEnv c . sendMessages' c
{-# INLINE sendMessages #-}

sendMessagesB :: Traversable t => AgentClient -> t (Either AgentErrorType MsgReq) -> AE (t (Either AgentErrorType (AgentMsgId, PQEncryption)))
sendMessagesB c = withAgentEnv c . sendMessagesB' c
{-# INLINE sendMessagesB #-}

ackMessage :: AgentClient -> ConnId -> AgentMsgId -> Maybe MsgReceiptInfo -> AE ()
ackMessage c = withAgentEnv c .:. ackMessage' c
{-# INLINE ackMessage #-}

getConnectionQueueInfo :: AgentClient -> ConnId -> AE ServerQueueInfo
getConnectionQueueInfo c = withAgentEnv c . getConnectionQueueInfo' c
{-# INLINE getConnectionQueueInfo #-}

-- | Switch connection to the new receive queue
switchConnection :: AgentClient -> ConnId -> AE ConnectionStats
switchConnection c = withAgentEnv c . switchConnection' c
{-# INLINE switchConnection #-}

-- | Abort switching connection to the new receive queue
abortConnectionSwitch :: AgentClient -> ConnId -> AE ConnectionStats
abortConnectionSwitch c = withAgentEnv c . abortConnectionSwitch' c
{-# INLINE abortConnectionSwitch #-}

-- | Re-synchronize connection ratchet keys
synchronizeRatchet :: AgentClient -> ConnId -> PQSupport -> Bool -> AE ConnectionStats
synchronizeRatchet c = withAgentEnv c .:. synchronizeRatchet' c
{-# INLINE synchronizeRatchet #-}

-- | Suspend SMP agent connection (OFF command)
suspendConnection :: AgentClient -> ConnId -> AE ()
suspendConnection c = withAgentEnv c . suspendConnection' c
{-# INLINE suspendConnection #-}

-- | Delete SMP agent connection (DEL command)
deleteConnection :: AgentClient -> ConnId -> AE ()
deleteConnection c = withAgentEnv c . deleteConnection' c
{-# INLINE deleteConnection #-}

-- | Delete multiple connections, batching commands when possible
deleteConnections :: AgentClient -> [ConnId] -> AE (Map ConnId (Either AgentErrorType ()))
deleteConnections c = withAgentEnv c . deleteConnections' c
{-# INLINE deleteConnections #-}

-- | get servers used for connection
getConnectionServers :: AgentClient -> ConnId -> AE ConnectionStats
getConnectionServers c = withAgentEnv c . getConnectionServers' c
{-# INLINE getConnectionServers #-}

-- | get connection ratchet associated data hash for verification (should match peer AD hash)
getConnectionRatchetAdHash :: AgentClient -> ConnId -> AE ByteString
getConnectionRatchetAdHash c = withAgentEnv c . getConnectionRatchetAdHash' c
{-# INLINE getConnectionRatchetAdHash #-}

-- | Test protocol server
testProtocolServer :: forall p. ProtocolTypeI p => AgentClient -> UserId -> ProtoServerWithAuth p -> IO (Maybe ProtocolTestFailure)
testProtocolServer c userId srv = withAgentEnv' c $ case protocolTypeI @p of
  SPSMP -> runSMPServerTest c userId srv
  SPXFTP -> runXFTPServerTest c userId srv
  SPNTF -> runNTFServerTest c userId srv

-- | set SOCKS5 proxy on/off and optionally set TCP timeouts for fast network
setNetworkConfig :: AgentClient -> NetworkConfig -> IO ()
setNetworkConfig c@AgentClient {useNetworkConfig, proxySessTs} cfg' = do
  (spChanged, changed) <- atomically $ do
    (_, cfg) <- readTVar useNetworkConfig
    let changed = cfg /= cfg'
        !cfgSlow = slowNetworkConfig cfg'
    when changed $ writeTVar useNetworkConfig (cfgSlow, cfg')
    pure (socksProxy cfg /= socksProxy cfg', changed)
  when spChanged $ getCurrentTime >>= atomically . writeTVar proxySessTs
  when changed $ reconnectAllServers c

setUserNetworkInfo :: AgentClient -> UserNetworkInfo -> IO ()
setUserNetworkInfo c@AgentClient {userNetworkInfo, userNetworkUpdated} ni = withAgentEnv' c $ do
  ts' <- liftIO getCurrentTime
  i <- asks $ userOfflineDelay . config
  -- if network offline event happens in less than `userOfflineDelay` after the previous event, it is ignored
  atomically . whenM ((isOnline ni ||) <$> notRecentlyChanged ts' i) $ do
    writeTVar userNetworkInfo ni
    writeTVar userNetworkUpdated $ Just ts'
  where
    notRecentlyChanged ts' i =
      maybe True (\ts -> diffUTCTime ts' ts > i) <$> readTVar userNetworkUpdated

reconnectAllServers :: AgentClient -> IO ()
reconnectAllServers c = do
  reconnectServerClients c smpClients
  reconnectServerClients c xftpClients
  reconnectServerClients c ntfClients

-- | Register device notifications token
registerNtfToken :: AgentClient -> DeviceToken -> NotificationsMode -> AE NtfTknStatus
registerNtfToken c = withAgentEnv c .: registerNtfToken' c
{-# INLINE registerNtfToken #-}

-- | Verify device notifications token
verifyNtfToken :: AgentClient -> DeviceToken -> C.CbNonce -> ByteString -> AE ()
verifyNtfToken c = withAgentEnv c .:. verifyNtfToken' c
{-# INLINE verifyNtfToken #-}

checkNtfToken :: AgentClient -> DeviceToken -> AE NtfTknStatus
checkNtfToken c = withAgentEnv c . checkNtfToken' c
{-# INLINE checkNtfToken #-}

deleteNtfToken :: AgentClient -> DeviceToken -> AE ()
deleteNtfToken c = withAgentEnv c . deleteNtfToken' c
{-# INLINE deleteNtfToken #-}

getNtfToken :: AgentClient -> AE (DeviceToken, NtfTknStatus, NotificationsMode, NtfServer)
getNtfToken c = withAgentEnv c $ getNtfToken' c
{-# INLINE getNtfToken #-}

getNtfTokenData :: AgentClient -> AE NtfToken
getNtfTokenData c = withAgentEnv c $ getNtfTokenData' c
{-# INLINE getNtfTokenData #-}

-- | Set connection notifications on/off
toggleConnectionNtfs :: AgentClient -> ConnId -> Bool -> AE ()
toggleConnectionNtfs c = withAgentEnv c .: toggleConnectionNtfs' c
{-# INLINE toggleConnectionNtfs #-}

xftpStartWorkers :: AgentClient -> Maybe FilePath -> AE ()
xftpStartWorkers c = withAgentEnv c . startXFTPWorkers c
{-# INLINE xftpStartWorkers #-}

xftpStartSndWorkers :: AgentClient -> Maybe FilePath -> AE ()
xftpStartSndWorkers c = withAgentEnv c . startXFTPSndWorkers c
{-# INLINE xftpStartSndWorkers #-}

-- | Receive XFTP file
xftpReceiveFile :: AgentClient -> UserId -> ValidFileDescription 'FRecipient -> Maybe CryptoFileArgs -> Bool -> AE RcvFileId
xftpReceiveFile c = withAgentEnv c .:: xftpReceiveFile' c
{-# INLINE xftpReceiveFile #-}

-- | Delete XFTP rcv file (deletes work files from file system and db records)
xftpDeleteRcvFile :: AgentClient -> RcvFileId -> IO ()
xftpDeleteRcvFile c = withAgentEnv' c . xftpDeleteRcvFile' c
{-# INLINE xftpDeleteRcvFile #-}

-- | Delete multiple rcv files, batching operations when possible (deletes work files from file system and db records)
xftpDeleteRcvFiles :: AgentClient -> [RcvFileId] -> IO ()
xftpDeleteRcvFiles c = withAgentEnv' c . xftpDeleteRcvFiles' c
{-# INLINE xftpDeleteRcvFiles #-}

-- | Send XFTP file
xftpSendFile :: AgentClient -> UserId -> CryptoFile -> Int -> AE SndFileId
xftpSendFile c = withAgentEnv c .:. xftpSendFile' c
{-# INLINE xftpSendFile #-}

-- | Send XFTP file
xftpSendDescription :: AgentClient -> UserId -> ValidFileDescription 'FRecipient -> Int -> AE SndFileId
xftpSendDescription c = withAgentEnv c .:. xftpSendDescription' c
{-# INLINE xftpSendDescription #-}

-- | Delete XFTP snd file internally (deletes work files from file system and db records)
xftpDeleteSndFileInternal :: AgentClient -> SndFileId -> IO ()
xftpDeleteSndFileInternal c = withAgentEnv' c . deleteSndFileInternal c
{-# INLINE xftpDeleteSndFileInternal #-}

-- | Delete multiple snd files internally, batching operations when possible (deletes work files from file system and db records)
xftpDeleteSndFilesInternal :: AgentClient -> [SndFileId] -> IO ()
xftpDeleteSndFilesInternal c = withAgentEnv' c . deleteSndFilesInternal c
{-# INLINE xftpDeleteSndFilesInternal #-}

-- | Delete XFTP snd file chunks on servers
xftpDeleteSndFileRemote :: AgentClient -> UserId -> SndFileId -> ValidFileDescription 'FSender -> IO ()
xftpDeleteSndFileRemote c = withAgentEnv' c .:. deleteSndFileRemote c
{-# INLINE xftpDeleteSndFileRemote #-}

-- | Delete XFTP snd file chunks on servers for multiple snd files, batching operations when possible
xftpDeleteSndFilesRemote :: AgentClient -> UserId -> [(SndFileId, ValidFileDescription 'FSender)] -> IO ()
xftpDeleteSndFilesRemote c = withAgentEnv' c .: deleteSndFilesRemote c
{-# INLINE xftpDeleteSndFilesRemote #-}

-- | Create new remote host pairing
rcNewHostPairing :: AgentClient -> IO RCHostPairing
rcNewHostPairing AgentClient {agentEnv = Env {random}} = newRCHostPairing random
{-# INLINE rcNewHostPairing #-}

-- | start TLS server for remote host with optional multicast
rcConnectHost :: AgentClient -> RCHostPairing -> J.Value -> Bool -> Maybe RCCtrlAddress -> Maybe Word16 -> AE RCHostConnection
rcConnectHost AgentClient {agentEnv = Env {random}} = withExceptT RCP .::. connectRCHost random
{-# INLINE rcConnectHost #-}

-- | connect to remote controller via URI
rcConnectCtrl :: AgentClient -> RCVerifiedInvitation -> Maybe RCCtrlPairing -> J.Value -> AE RCCtrlConnection
rcConnectCtrl AgentClient {agentEnv = Env {random}} = withExceptT RCP .:. connectRCCtrl random
{-# INLINE rcConnectCtrl #-}

-- | connect to known remote controller via multicast
rcDiscoverCtrl :: AgentClient -> NonEmpty RCCtrlPairing -> AE (RCCtrlPairing, RCVerifiedInvitation)
rcDiscoverCtrl AgentClient {agentEnv = Env {multicastSubscribers = subs}} = withExceptT RCP . discoverRCCtrl subs
{-# INLINE rcDiscoverCtrl #-}

resetAgentServersStats :: AgentClient -> AE ()
resetAgentServersStats c = withAgentEnv c $ resetAgentServersStats' c
{-# INLINE resetAgentServersStats #-}

withAgentEnv' :: AgentClient -> AM' a -> IO a
withAgentEnv' c = (`runReaderT` agentEnv c)
{-# INLINE withAgentEnv' #-}

withAgentEnv :: AgentClient -> AM a -> AE a
withAgentEnv c a = ExceptT $ runExceptT a `runReaderT` agentEnv c
{-# INLINE withAgentEnv #-}

logConnection :: AgentClient -> Bool -> IO ()
logConnection c connected =
  let event = if connected then "connected to" else "disconnected from"
   in logInfo $ T.unwords ["client", tshow (clientId c), event, "Agent"]

createUser' :: AgentClient -> NonEmpty (ServerCfg 'PSMP) -> NonEmpty (ServerCfg 'PXFTP) -> AM UserId
createUser' c smp xftp = do
  liftIO $ checkUserServers "createUser SMP" smp
  liftIO $ checkUserServers "createUser XFTP" xftp
  userId <- withStore' c createUserRecord
  atomically $ TM.insert userId (mkUserServers smp) $ smpServers c
  atomically $ TM.insert userId (mkUserServers xftp) $ xftpServers c
  pure userId

deleteUser' :: AgentClient -> UserId -> Bool -> AM ()
deleteUser' c@AgentClient {smpServersStats, xftpServersStats} userId delSMPQueues = do
  if delSMPQueues
    then withStore c (`setUserDeleted` userId) >>= deleteConnectionsAsync_ delUser c False
    else withStore c (`deleteUserRecord` userId)
  atomically $ TM.delete userId $ smpServers c
  atomically $ TM.delete userId $ xftpServers c
  atomically $ modifyTVar' smpServersStats $ M.filterWithKey (\(userId', _) _ -> userId' /= userId)
  atomically $ modifyTVar' xftpServersStats $ M.filterWithKey (\(userId', _) _ -> userId' /= userId)
  lift $ saveServersStats c
  where
    delUser =
      whenM (withStore' c (`deleteUserWithoutConns` userId)) . atomically $
        writeTBQueue (subQ c) ("", "", AEvt SAENone $ DEL_USER userId)

newConnAsync :: ConnectionModeI c => AgentClient -> UserId -> ACorrId -> Bool -> SConnectionMode c -> CR.InitialKeys -> SubscriptionMode -> AM ConnId
newConnAsync c userId corrId enableNtfs cMode pqInitKeys subMode = do
  connId <- newConnNoQueues c userId enableNtfs cMode (CR.connPQEncryption pqInitKeys)
  enqueueCommand c corrId connId Nothing $ AClientCommand $ NEW enableNtfs (ACM cMode) pqInitKeys subMode
  pure connId

newConnNoQueues :: AgentClient -> UserId -> Bool -> SConnectionMode c -> PQSupport -> AM ConnId
newConnNoQueues c userId enableNtfs cMode pqSupport = do
  g <- asks random
  connAgentVersion <- asks $ maxVersion . smpAgentVRange . config
  let cData = ConnData {userId, connId = "", connAgentVersion, enableNtfs, lastExternalSndId = 0, deleted = False, ratchetSyncState = RSOk, pqSupport}
  withStore c $ \db -> createNewConn db g cData cMode

joinConnAsync :: AgentClient -> UserId -> ACorrId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> AM ConnId
joinConnAsync c userId corrId enableNtfs cReqUri@CRInvitationUri {} cInfo pqSup subMode = do
  withInvLock c (strEncode cReqUri) "joinConnAsync" $ do
    lift (compatibleInvitationUri cReqUri) >>= \case
      Just (_, Compatible (CR.E2ERatchetParams v _ _ _), Compatible connAgentVersion) -> do
        g <- asks random
        let pqSupport = pqSup `CR.pqSupportAnd` versionPQSupport_ connAgentVersion (Just v)
            cData = ConnData {userId, connId = "", connAgentVersion, enableNtfs, lastExternalSndId = 0, deleted = False, ratchetSyncState = RSOk, pqSupport}
        connId <- withStore c $ \db -> createNewConn db g cData SCMInvitation
        enqueueCommand c corrId connId Nothing $ AClientCommand $ JOIN enableNtfs (ACR sConnectionMode cReqUri) pqSupport subMode cInfo
        pure connId
      Nothing -> throwE $ AGENT A_VERSION
joinConnAsync _c _userId _corrId _enableNtfs (CRContactUri _) _subMode _cInfo _pqEncryption =
  throwE $ CMD PROHIBITED "joinConnAsync"

allowConnectionAsync' :: AgentClient -> ACorrId -> ConnId -> ConfirmationId -> ConnInfo -> AM ()
allowConnectionAsync' c corrId connId confId ownConnInfo =
  withStore c (`getConn` connId) >>= \case
    SomeConn _ (RcvConnection _ RcvQueue {server}) ->
      enqueueCommand c corrId connId (Just server) $ AClientCommand $ LET confId ownConnInfo
    _ -> throwE $ CMD PROHIBITED "allowConnectionAsync"

-- TODO
-- Unlike `acceptContact` (synchronous version), `acceptContactAsync` uses `unacceptInvitation` in case of error,
-- because we're not taking lock here. In practice it is less likely to fail because it doesn't involve network IO,
-- and also it can't be triggered by user concurrently several times in a row. It could be improved similarly to
-- `acceptContact` by creating a new map for invitation locks and taking lock here, and removing `unacceptInvitation`
-- while marking invitation as accepted inside "lock level transaction" after successful `joinConnAsync`.
acceptContactAsync' :: AgentClient -> ACorrId -> Bool -> InvitationId -> ConnInfo -> PQSupport -> SubscriptionMode -> AM ConnId
acceptContactAsync' c corrId enableNtfs invId ownConnInfo pqSupport subMode = do
  Invitation {contactConnId, connReq} <- withStore c $ \db -> getInvitation db "acceptContactAsync'" invId
  withStore c (`getConn` contactConnId) >>= \case
    SomeConn _ (ContactConnection ConnData {userId} _) -> do
      withStore' c $ \db -> acceptInvitation db invId ownConnInfo
      joinConnAsync c userId corrId enableNtfs connReq ownConnInfo pqSupport subMode `catchAgentError` \err -> do
        withStore' c (`unacceptInvitation` invId)
        throwE err
    _ -> throwE $ CMD PROHIBITED "acceptContactAsync"

ackMessageAsync' :: AgentClient -> ACorrId -> ConnId -> AgentMsgId -> Maybe MsgReceiptInfo -> AM ()
ackMessageAsync' c corrId connId msgId rcptInfo_ = do
  SomeConn cType _ <- withStore c (`getConn` connId)
  case cType of
    SCDuplex -> enqueueAck
    SCRcv -> enqueueAck
    SCSnd -> throwE $ CONN SIMPLEX
    SCContact -> throwE $ CMD PROHIBITED "ackMessageAsync: SCContact"
    SCNew -> throwE $ CMD PROHIBITED "ackMessageAsync: SCNew"
  where
    enqueueAck :: AM ()
    enqueueAck = do
      let mId = InternalId msgId
      RcvMsg {msgType} <- withStore c $ \db -> getRcvMsg db connId mId
      when (isJust rcptInfo_ && msgType /= AM_A_MSG_) $ throwE $ CMD PROHIBITED "ackMessageAsync: receipt not allowed"
      (RcvQueue {server}, _) <- withStore c $ \db -> setMsgUserAck db connId mId
      enqueueCommand c corrId connId (Just server) . AClientCommand $ ACK msgId rcptInfo_

deleteConnectionAsync' :: AgentClient -> Bool -> ConnId -> AM ()
deleteConnectionAsync' c waitDelivery connId = deleteConnectionsAsync' c waitDelivery [connId]
{-# INLINE deleteConnectionAsync' #-}

deleteConnectionsAsync' :: AgentClient -> Bool -> [ConnId] -> AM ()
deleteConnectionsAsync' = deleteConnectionsAsync_ $ pure ()
{-# INLINE deleteConnectionsAsync' #-}

timeItM :: String -> AM a -> AM a
timeItM s action = do
  t1 <- liftIO getCurrentTime
  a <- action
  t2 <- liftIO getCurrentTime
  let diff = diffToMilliseconds $ diffUTCTime t2 t1
  liftIO . print $ show diff <> " ms - " <> s
  pure a

timeItM' :: String -> AM' a -> AM' a
timeItM' s action = do
  t1 <- liftIO getCurrentTime
  a <- action
  t2 <- liftIO getCurrentTime
  let diff = diffToMilliseconds $ diffUTCTime t2 t1
  liftIO . print $ show diff <> " ms - " <> s
  pure a

deleteConnectionsAsync_ :: AM () -> AgentClient -> Bool -> [ConnId] -> AM ()
deleteConnectionsAsync_ onSuccess c waitDelivery connIds = case connIds of
  [] -> onSuccess
  _ -> do
    (_, rqs, connIds') <- timeItM "prepareDeleteConnections_" $ prepareDeleteConnections_ getConns c waitDelivery connIds
    withStore' c $ \db -> forM_ connIds' $ setConnDeleted db waitDelivery
    void . lift . forkIO $
      withLock' (deleteLock c) "deleteConnectionsAsync" $
        timeItM' "deleteConnQueues" (deleteConnQueues c waitDelivery True rqs) >> void (runExceptT onSuccess)

-- | Add connection to the new receive queue
switchConnectionAsync' :: AgentClient -> ACorrId -> ConnId -> AM ConnectionStats
switchConnectionAsync' c corrId connId =
  withConnLock c connId "switchConnectionAsync" $
    withStore c (`getConn` connId) >>= \case
      SomeConn _ (DuplexConnection cData rqs@(rq :| _rqs) sqs)
        | isJust (switchingRQ rqs) -> throwE $ CMD PROHIBITED "switchConnectionAsync: already switching"
        | otherwise -> do
            when (ratchetSyncSendProhibited cData) $ throwE $ CMD PROHIBITED "switchConnectionAsync: send prohibited"
            rq1 <- withStore' c $ \db -> setRcvSwitchStatus db rq $ Just RSSwitchStarted
            enqueueCommand c corrId connId Nothing $ AClientCommand SWCH
            let rqs' = updatedQs rq1 rqs
            pure . connectionStats $ DuplexConnection cData rqs' sqs
      _ -> throwE $ CMD PROHIBITED "switchConnectionAsync: not duplex"

newConn :: AgentClient -> UserId -> Bool -> SConnectionMode c -> Maybe CRClientData -> CR.InitialKeys -> SubscriptionMode -> AM (ConnId, ConnectionRequestUri c)
newConn c userId enableNtfs cMode clientData pqInitKeys subMode = do
  srv <- getSMPServer c userId
  connId <- newConnNoQueues c userId enableNtfs cMode (CR.connPQEncryption pqInitKeys)
  cReq <- newRcvConnSrv c userId connId enableNtfs cMode clientData pqInitKeys subMode srv
  pure (connId, cReq)

changeConnectionUser' :: AgentClient -> UserId -> ConnId -> UserId -> AM ()
changeConnectionUser' c oldUserId connId newUserId = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    NewConnection {} -> updateConn
    RcvConnection {} -> updateConn
    _ -> throwE $ CMD PROHIBITED "changeConnectionUser: established connection"
  where
    updateConn = withStore' c $ \db -> setConnUserId db oldUserId connId newUserId

newRcvConnSrv :: AgentClient -> UserId -> ConnId -> Bool -> SConnectionMode c -> Maybe CRClientData -> CR.InitialKeys -> SubscriptionMode -> SMPServerWithAuth -> AM (ConnectionRequestUri c)
newRcvConnSrv c userId connId enableNtfs cMode clientData pqInitKeys subMode srvWithAuth@(ProtoServerWithAuth srv _) = do
  case (cMode, pqInitKeys) of
    (SCMContact, CR.IKUsePQ) -> throwE $ CMD PROHIBITED "newRcvConnSrv"
    _ -> pure ()
  AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
  let sndSecure = case cMode of SCMInvitation -> True; SCMContact -> False
  (rq, qUri, tSess, sessId) <- newRcvQueue c userId connId srvWithAuth smpClientVRange subMode sndSecure `catchAgentError` \e -> liftIO (print e) >> throwE e
  atomically $ incSMPServerStat c userId srv connCreated
  rq' <- withStore c $ \db -> updateNewConnRcv db connId rq
  lift . when (subMode == SMSubscribe) $ addNewQueueSubscription c rq' tSess sessId
  when enableNtfs $ do
    ns <- asks ntfSupervisor
    atomically $ sendNtfSubCommand ns (NSCCreate, [connId])
  let crData = ConnReqUriData SSSimplex smpAgentVRange [qUri] clientData
  case cMode of
    SCMContact -> pure $ CRContactUri crData
    SCMInvitation -> do
      g <- asks random
      (pk1, pk2, pKem, e2eRcvParams) <- liftIO $ CR.generateRcvE2EParams g (maxVersion e2eEncryptVRange) (CR.initialPQEncryption pqInitKeys)
      withStore' c $ \db -> createRatchetX3dhKeys db connId pk1 pk2 pKem
      pure $ CRInvitationUri crData $ toVersionRangeT e2eRcvParams e2eEncryptVRange

newConnToJoin :: forall c. AgentClient -> UserId -> ConnId -> Bool -> ConnectionRequestUri c -> PQSupport -> AM ConnId
newConnToJoin c userId connId enableNtfs cReq pqSup = case cReq of
  CRInvitationUri {} ->
    lift (compatibleInvitationUri cReq) >>= \case
      Just (_, Compatible (CR.E2ERatchetParams v _ _ _), aVersion) -> create aVersion (Just v)
      Nothing -> throwE $ AGENT A_VERSION
  CRContactUri {} ->
    lift (compatibleContactUri cReq) >>= \case
      Just (_, aVersion) -> create aVersion Nothing
      Nothing -> throwE $ AGENT A_VERSION
  where
    create :: Compatible VersionSMPA -> Maybe CR.VersionE2E -> AM ConnId
    create (Compatible connAgentVersion) e2eV_ = do
      g <- asks random
      let pqSupport = pqSup `CR.pqSupportAnd` versionPQSupport_ connAgentVersion e2eV_
          cData = ConnData {userId, connId, connAgentVersion, enableNtfs, lastExternalSndId = 0, deleted = False, ratchetSyncState = RSOk, pqSupport}
      withStore c $ \db -> createNewConn db g cData SCMInvitation

newConnToAccept :: AgentClient -> ConnId -> Bool -> ConfirmationId -> PQSupport -> AM ConnId
newConnToAccept c connId enableNtfs invId pqSup = do
  Invitation {connReq, contactConnId} <- withStore c $ \db -> getInvitation db "newConnToAccept" invId
  withStore c (`getConn` contactConnId) >>= \case
    SomeConn _ (ContactConnection ConnData {userId} _) ->
      newConnToJoin c userId connId enableNtfs connReq pqSup
    _ -> throwE $ CMD PROHIBITED "newConnToAccept"

joinConn :: AgentClient -> UserId -> ConnId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> AM SndQueueSecured
joinConn c userId connId enableNtfs cReq cInfo pqSupport subMode = do
  srv <- getNextSMPServer c userId [qServer cReqQueue]
  joinConnSrv c userId connId enableNtfs cReq cInfo pqSupport subMode srv
  where
    cReqQueue :: SMPQueueUri
    cReqQueue = case cReq of
      CRInvitationUri ConnReqUriData {crSmpQueues = q :| _} _ -> q
      CRContactUri ConnReqUriData {crSmpQueues = q :| _} -> q

startJoinInvitation :: AgentClient -> UserId -> ConnId -> Maybe SndQueue -> Bool -> ConnectionRequestUri 'CMInvitation -> PQSupport -> AM (ConnData, SndQueue, CR.SndE2ERatchetParams 'C.X448)
startJoinInvitation c userId connId sq_ enableNtfs cReqUri pqSup =
  lift (compatibleInvitationUri cReqUri) >>= \case
    Just (qInfo, Compatible e2eRcvParams@(CR.E2ERatchetParams v _ _ _), Compatible connAgentVersion) -> do
      -- this case avoids re-generating queue keys and subsequent failure of SKEY that timed out
      -- e2ePubKey is always present, it's Maybe historically
      let pqSupport = pqSup `CR.pqSupportAnd` versionPQSupport_ connAgentVersion (Just v)
      (sq', e2eSndParams) <- case sq_ of
        Just sq@SndQueue {e2ePubKey = Just _k} -> do
          e2eSndParams <-
            withStore' c (\db -> getSndRatchet db connId v) >>= \case
              Right r -> pure $ snd r
              Left e -> do
                atomically $ writeTBQueue (subQ c) ("", connId, AEvt SAEConn (ERR $ INTERNAL $ "no snd ratchet " <> show e))
                createRatchet_ pqSupport e2eRcvParams
          pure (sq, e2eSndParams)
        _ -> do
          q <- lift $ fst <$> newSndQueue userId "" qInfo
          e2eSndParams <- createRatchet_ pqSupport e2eRcvParams
          withStore c $ \db -> runExceptT $ do
            sq' <- maybe (ExceptT $ updateNewConnSnd db connId q) pure sq_
            pure (sq', e2eSndParams)
      let cData = ConnData {userId, connId, connAgentVersion, enableNtfs, lastExternalSndId = 0, deleted = False, ratchetSyncState = RSOk, pqSupport}
      pure (cData, sq', e2eSndParams)
    Nothing -> throwE $ AGENT A_VERSION
  where
    createRatchet_ pqSupport e2eRcvParams@(CR.E2ERatchetParams v _ rcDHRr kem_) = do
      g <- asks random
      (pk1, pk2, pKem, e2eSndParams) <- liftIO $ CR.generateSndE2EParams g v (CR.replyKEM_ v kem_ pqSupport)
      (_, rcDHRs) <- atomically $ C.generateKeyPair g
      rcParams <- liftEitherWith cryptoError $ CR.pqX3dhSnd pk1 pk2 pKem e2eRcvParams
      maxSupported <- asks $ maxVersion . e2eEncryptVRange . config
      let rcVs = CR.RatchetVersions {current = v, maxSupported}
          rc = CR.initSndRatchet rcVs rcDHRr rcDHRs rcParams
      withStore' c $ \db -> createSndRatchet db connId rc e2eSndParams
      pure e2eSndParams

connRequestPQSupport :: AgentClient -> PQSupport -> ConnectionRequestUri c -> IO (Maybe (VersionSMPA, PQSupport))
connRequestPQSupport c pqSup cReq = withAgentEnv' c $ case cReq of
  CRInvitationUri {} -> invPQSupported <$$> compatibleInvitationUri cReq
    where
      invPQSupported (_, Compatible (CR.E2ERatchetParams e2eV _ _ _), Compatible agentV) = (agentV, pqSup `CR.pqSupportAnd` versionPQSupport_ agentV (Just e2eV))
  CRContactUri {} -> ctPQSupported <$$> compatibleContactUri cReq
    where
      ctPQSupported (_, Compatible agentV) = (agentV, pqSup `CR.pqSupportAnd` versionPQSupport_ agentV Nothing)

compatibleInvitationUri :: ConnectionRequestUri 'CMInvitation -> AM' (Maybe (Compatible SMPQueueInfo, Compatible (CR.RcvE2ERatchetParams 'C.X448), Compatible VersionSMPA))
compatibleInvitationUri (CRInvitationUri ConnReqUriData {crAgentVRange, crSmpQueues = (qUri :| _)} e2eRcvParamsUri) = do
  AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
  pure $
    (,,)
      <$> (qUri `compatibleVersion` smpClientVRange)
      <*> (e2eRcvParamsUri `compatibleVersion` e2eEncryptVRange)
      <*> (crAgentVRange `compatibleVersion` smpAgentVRange)

compatibleContactUri :: ConnectionRequestUri 'CMContact -> AM' (Maybe (Compatible SMPQueueInfo, Compatible VersionSMPA))
compatibleContactUri (CRContactUri ConnReqUriData {crAgentVRange, crSmpQueues = (qUri :| _)}) = do
  AgentConfig {smpClientVRange, smpAgentVRange} <- asks config
  pure $
    (,)
      <$> (qUri `compatibleVersion` smpClientVRange)
      <*> (crAgentVRange `compatibleVersion` smpAgentVRange)

versionPQSupport_ :: VersionSMPA -> Maybe CR.VersionE2E -> PQSupport
versionPQSupport_ agentV e2eV_ = PQSupport $ agentV >= pqdrSMPAgentVersion && maybe True (>= CR.pqRatchetE2EEncryptVersion) e2eV_
{-# INLINE versionPQSupport_ #-}

joinConnSrv :: AgentClient -> UserId -> ConnId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> SMPServerWithAuth -> AM SndQueueSecured
joinConnSrv c userId connId enableNtfs inv@CRInvitationUri {} cInfo pqSup subMode srv =
  withInvLock c (strEncode inv) "joinConnSrv" $ do
    SomeConn cType conn <- withStore c (`getConn` connId)
    case conn of
      NewConnection _ -> doJoin Nothing
      SndConnection _ sq -> doJoin $ Just sq
      DuplexConnection _ (RcvQueue {status = New} :| _) (sq@SndQueue {status = New} :| _) -> doJoin $ Just sq
      _ -> throwE $ CMD PROHIBITED $ "joinConnSrv: bad connection " <> show cType
  where
    doJoin :: Maybe SndQueue -> AM SndQueueSecured
    doJoin sq_ = do
      (cData, sq, e2eSndParams) <- startJoinInvitation c userId connId sq_ enableNtfs inv pqSup
      secureConfirmQueue c cData sq srv cInfo (Just e2eSndParams) subMode
joinConnSrv c userId connId enableNtfs cReqUri@CRContactUri {} cInfo pqSup subMode srv =
  lift (compatibleContactUri cReqUri) >>= \case
    Just (qInfo, vrsn) -> do
      cReq <- newRcvConnSrv c userId connId enableNtfs SCMInvitation Nothing (CR.IKNoPQ pqSup) subMode srv
      void $ sendInvitation c userId connId qInfo vrsn cReq cInfo
      pure False
    Nothing -> throwE $ AGENT A_VERSION

joinConnSrvAsync :: AgentClient -> UserId -> ConnId -> Bool -> ConnectionRequestUri c -> ConnInfo -> PQSupport -> SubscriptionMode -> SMPServerWithAuth -> AM SndQueueSecured
joinConnSrvAsync c userId connId enableNtfs inv@CRInvitationUri {} cInfo pqSupport subMode srv = do
  SomeConn cType conn <- withStore c (`getConn` connId)
  case conn of
    NewConnection _ -> doJoin Nothing
    SndConnection _ sq -> doJoin $ Just sq
    _ -> throwE $ CMD PROHIBITED $ "joinConnSrvAsync: bad connection " <> show cType
  where
    doJoin :: Maybe SndQueue -> AM SndQueueSecured
    doJoin sq_ = do
      (cData, sq, e2eSndParams) <- startJoinInvitation c userId connId sq_ enableNtfs inv pqSupport
      secureConfirmQueueAsync c cData sq srv cInfo (Just e2eSndParams) subMode
joinConnSrvAsync _c _userId _connId _enableNtfs (CRContactUri _) _cInfo _subMode _pqSupport _srv = do
  throwE $ CMD PROHIBITED "joinConnSrvAsync"

createReplyQueue :: AgentClient -> ConnData -> SndQueue -> SubscriptionMode -> SMPServerWithAuth -> AM SMPQueueInfo
createReplyQueue c ConnData {userId, connId, enableNtfs} SndQueue {smpClientVersion} subMode srv = do
  let sndSecure = smpClientVersion >= sndAuthKeySMPClientVersion
  (rq, qUri, tSess, sessId) <- newRcvQueue c userId connId srv (versionToRange smpClientVersion) subMode sndSecure
  atomically $ incSMPServerStat c userId (qServer rq) connCreated
  let qInfo = toVersionT qUri smpClientVersion
  rq' <- withStore c $ \db -> upgradeSndConnToDuplex db connId rq
  lift . when (subMode == SMSubscribe) $ addNewQueueSubscription c rq' tSess sessId
  when enableNtfs $ do
    ns <- asks ntfSupervisor
    atomically $ sendNtfSubCommand ns (NSCCreate, [connId])
  pure qInfo

-- | Approve confirmation (LET command) in Reader monad
allowConnection' :: AgentClient -> ConnId -> ConfirmationId -> ConnInfo -> AM ()
allowConnection' c connId confId ownConnInfo = withConnLock c connId "allowConnection" $ do
  withStore c (`getConn` connId) >>= \case
    SomeConn _ (RcvConnection _ RcvQueue {server, rcvId}) -> do
      AcceptedConfirmation {senderConf = SMPConfirmation {senderKey}} <-
        withStore c $ \db -> acceptConfirmation db confId ownConnInfo
      enqueueCommand c "" connId (Just server) . AInternalCommand $ ICAllowSecure rcvId senderKey
    _ -> throwE $ CMD PROHIBITED "allowConnection"

-- | Accept contact (ACPT command) in Reader monad
acceptContact' :: AgentClient -> ConnId -> Bool -> InvitationId -> ConnInfo -> PQSupport -> SubscriptionMode -> AM SndQueueSecured
acceptContact' c connId enableNtfs invId ownConnInfo pqSupport subMode = withConnLock c connId "acceptContact" $ do
  Invitation {contactConnId, connReq} <- withStore c $ \db -> getInvitation db "acceptContact'" invId
  withStore c (`getConn` contactConnId) >>= \case
    SomeConn _ (ContactConnection ConnData {userId} _) -> do
      sqSecured <- joinConn c userId connId enableNtfs connReq ownConnInfo pqSupport subMode
      withStore' c $ \db -> acceptInvitation db invId ownConnInfo
      pure sqSecured
    _ -> throwE $ CMD PROHIBITED "acceptContact"

-- | Reject contact (RJCT command) in Reader monad
rejectContact' :: AgentClient -> ConnId -> InvitationId -> AM ()
rejectContact' c contactConnId invId =
  withStore c $ \db -> deleteInvitation db contactConnId invId
{-# INLINE rejectContact' #-}

-- | Subscribe to receive connection messages (SUB command) in Reader monad
subscribeConnection' :: AgentClient -> ConnId -> AM ()
subscribeConnection' c connId = toConnResult connId =<< subscribeConnections' c [connId]
{-# INLINE subscribeConnection' #-}

toConnResult :: ConnId -> Map ConnId (Either AgentErrorType ()) -> AM ()
toConnResult connId rs = case M.lookup connId rs of
  Just (Right ()) -> when (M.size rs > 1) $ logError $ T.pack $ "too many results " <> show (M.size rs)
  Just (Left e) -> throwE e
  _ -> throwE $ INTERNAL $ "no result for connection " <> B.unpack connId

type QCmdResult = (QueueStatus, Either AgentErrorType ())

subscribeConnections' :: AgentClient -> [ConnId] -> AM (Map ConnId (Either AgentErrorType ()))
subscribeConnections' _ [] = pure M.empty
subscribeConnections' c connIds = do
  conns :: Map ConnId (Either StoreError SomeConn) <- M.fromList . zip connIds <$> withStore' c (`getConns` connIds)
  let (errs, cs) = M.mapEither id conns
      errs' = M.map (Left . storeError) errs
      (subRs, rcvQs) = M.mapEither rcvQueueOrResult cs
  resumeDelivery cs
  lift $ resumeConnCmds c $ M.keys cs
  rcvRs <- lift $ connResults . fst <$> subscribeQueues c (concat $ M.elems rcvQs)
  ns <- asks ntfSupervisor
  tkn <- readTVarIO (ntfTkn ns)
  lift $ when (instantNotifications tkn) . void . forkIO . void $ sendNtfCreate ns rcvRs cs
  let rs = M.unions ([errs', subRs, rcvRs] :: [Map ConnId (Either AgentErrorType ())])
  notifyResultError rs
  pure rs
  where
    rcvQueueOrResult :: SomeConn -> Either (Either AgentErrorType ()) [RcvQueue]
    rcvQueueOrResult (SomeConn _ conn) = case conn of
      DuplexConnection _ rqs _ -> Right $ L.toList rqs
      SndConnection _ sq -> Left $ sndSubResult sq
      RcvConnection _ rq -> Right [rq]
      ContactConnection _ rq -> Right [rq]
      NewConnection _ -> Left (Right ())
    sndSubResult :: SndQueue -> Either AgentErrorType ()
    sndSubResult SndQueue {status} = case status of
      Confirmed -> Right ()
      Active -> Left $ CONN SIMPLEX
      _ -> Left $ INTERNAL "unexpected queue status"
    connResults :: [(RcvQueue, Either AgentErrorType ())] -> Map ConnId (Either AgentErrorType ())
    connResults = M.map snd . foldl' addResult M.empty
      where
        -- collects results by connection ID
        addResult :: Map ConnId QCmdResult -> (RcvQueue, Either AgentErrorType ()) -> Map ConnId QCmdResult
        addResult rs (RcvQueue {connId, status}, r) = M.alter (combineRes (status, r)) connId rs
        -- combines two results for one connection, by using only Active queues (if there is at least one Active queue)
        combineRes :: QCmdResult -> Maybe QCmdResult -> Maybe QCmdResult
        combineRes r' (Just r) = Just $ if order r <= order r' then r else r'
        combineRes r' _ = Just r'
        order :: QCmdResult -> Int
        order (Active, Right _) = 1
        order (Active, _) = 2
        order (_, Right _) = 3
        order _ = 4
    sendNtfCreate :: NtfSupervisor -> Map ConnId (Either AgentErrorType ()) -> Map ConnId SomeConn -> AM' ()
    sendNtfCreate ns rcvRs cs = do
      let oks = M.keysSet $ M.filter (either temporaryAgentError $ const True) rcvRs
          cs' = M.restrictKeys cs oks
          (csCreate, csDelete) = M.partition (\(SomeConn _ conn) -> enableNtfs $ toConnData conn) cs'
      sendNtfCmd NSCCreate csCreate
      sendNtfCmd NSCSmpDelete csDelete
      where
        sendNtfCmd cmd cs' = forM_ (L.nonEmpty $ M.keys cs') $ \cids -> atomically $ writeTBQueue (ntfSubQ ns) (cmd, cids)
    resumeDelivery :: Map ConnId SomeConn -> AM ()
    resumeDelivery conns = do
      conns' <- M.restrictKeys conns . S.fromList <$> withStore' c getConnectionsForDelivery
      lift $ mapM_ (mapM_ (\(cData, sqs) -> mapM_ (resumeMsgDelivery c cData) sqs) . sndQueue) conns'
    sndQueue :: SomeConn -> Maybe (ConnData, NonEmpty SndQueue)
    sndQueue (SomeConn _ conn) = case conn of
      DuplexConnection cData _ sqs -> Just (cData, sqs)
      SndConnection cData sq -> Just (cData, [sq])
      _ -> Nothing
    notifyResultError :: Map ConnId (Either AgentErrorType ()) -> AM ()
    notifyResultError rs = do
      let actual = M.size rs
          expected = length connIds
      when (actual /= expected) . atomically $
        writeTBQueue (subQ c) ("", "", AEvt SAEConn $ ERR $ INTERNAL $ "subscribeConnections result size: " <> show actual <> ", expected " <> show expected)

resubscribeConnection' :: AgentClient -> ConnId -> AM ()
resubscribeConnection' c connId = toConnResult connId =<< resubscribeConnections' c [connId]
{-# INLINE resubscribeConnection' #-}

resubscribeConnections' :: AgentClient -> [ConnId] -> AM (Map ConnId (Either AgentErrorType ()))
resubscribeConnections' _ [] = pure M.empty
resubscribeConnections' c connIds = do
  let r = M.fromList . zip connIds . repeat $ Right ()
  connIds' <- filterM (fmap not . atomically . hasActiveSubscription c) connIds
  -- union is left-biased, so results returned by subscribeConnections' take precedence
  (`M.union` r) <$> subscribeConnections' c connIds'

getConnectionMessages' :: AgentClient -> NonEmpty ConnId -> AM' (NonEmpty (Maybe SMPMsgMeta))
getConnectionMessages' c = mapM getMsg
  where
    getMsg :: ConnId -> AM' (Maybe SMPMsgMeta)
    getMsg connId =
      getConnectionMessage connId `catchAgentError'` \e -> do
        logError $ "Error loading message: " <> tshow e
        pure Nothing
    getConnectionMessage :: ConnId -> AM (Maybe SMPMsgMeta)
    getConnectionMessage connId = do
      whenM (atomically $ hasActiveSubscription c connId) . throwE $ CMD PROHIBITED "getConnectionMessage: subscribed"
      SomeConn _ conn <- withStore c (`getConn` connId)
      case conn of
        DuplexConnection _ (rq :| _) _ -> getQueueMessage c rq
        RcvConnection _ rq -> getQueueMessage c rq
        ContactConnection _ rq -> getQueueMessage c rq
        SndConnection _ _ -> throwE $ CONN SIMPLEX
        NewConnection _ -> throwE $ CMD PROHIBITED "getConnectionMessage: NewConnection"

getNotificationConns' :: AgentClient -> C.CbNonce -> ByteString -> AM (NonEmpty NotificationInfo)
getNotificationConns' c nonce encNtfInfo =
  withStore' c getActiveNtfToken >>= \case
    Just NtfToken {ntfDhSecret = Just dhSecret} -> do
      ntfData <- liftEither $ agentCbDecrypt dhSecret nonce encNtfInfo
      pnMsgs <- liftEither (parse pnMessagesP (INTERNAL "error parsing PNMessageData") ntfData)
      let (initNtfs, lastNtf) = (L.init pnMsgs, L.last pnMsgs)
      rs <-
        lift $ withStoreBatch c $ \db ->
          let initNtfInfos = map (getInitNtfInfo db) initNtfs
              lastNtfInfo = Just . fst <$$> getNtfInfo db lastNtf
           in initNtfInfos <> [lastNtfInfo]
      let (errs, ntfInfos_) = partitionEithers rs
      logError $ "Error(s) loading notifications: " <> tshow errs
      case L.nonEmpty $ catMaybes ntfInfos_ of
        Just r -> pure r
        Nothing -> throwE $ INTERNAL "getNotificationConns: couldn't get conn info"
    _ -> throwE $ CMD PROHIBITED "getNotificationConns"
  where
    getNtfInfo :: DB.Connection -> PNMessageData -> IO (Either AgentErrorType (NotificationInfo, Maybe UTCTime))
    getNtfInfo db PNMessageData {smpQueue, ntfTs, nmsgNonce, encNMsgMeta} = runExceptT $ do
      (ntfConnId, rcvNtfDhSecret, lastBrokerTs_) <- liftError' storeError $ getNtfRcvQueue db smpQueue
      let ntfMsgMeta = eitherToMaybe $ smpDecode =<< first show (C.cbDecrypt rcvNtfDhSecret nmsgNonce encNMsgMeta)
          ntfInfo = NotificationInfo {ntfConnId, ntfTs, ntfMsgMeta}
      pure (ntfInfo, lastBrokerTs_)
    getInitNtfInfo :: DB.Connection -> PNMessageData -> IO (Either AgentErrorType (Maybe NotificationInfo))
    getInitNtfInfo db msgData = runExceptT $ do
      (nftInfo, lastBrokerTs_) <- ExceptT $ getNtfInfo db msgData
      pure $ case (ntfMsgMeta nftInfo, lastBrokerTs_) of
        (Just SMP.NMsgMeta {msgTs}, Just lastBrokerTs)
          | systemToUTCTime msgTs > lastBrokerTs -> Just nftInfo
        _ -> Nothing

-- | Send message to the connection (SEND command) in Reader monad
sendMessage' :: AgentClient -> ConnId -> PQEncryption -> MsgFlags -> MsgBody -> AM (AgentMsgId, PQEncryption)
sendMessage' c connId pqEnc msgFlags msg = ExceptT $ runIdentity <$> sendMessagesB_ c (Identity (Right (connId, pqEnc, msgFlags, msg))) (S.singleton connId)
{-# INLINE sendMessage' #-}

-- | Send multiple messages to different connections (SEND command) in Reader monad
sendMessages' :: AgentClient -> [MsgReq] -> AM [Either AgentErrorType (AgentMsgId, PQEncryption)]
sendMessages' c = sendMessagesB' c . map Right
{-# INLINE sendMessages' #-}

sendMessagesB' :: forall t. Traversable t => AgentClient -> t (Either AgentErrorType MsgReq) -> AM (t (Either AgentErrorType (AgentMsgId, PQEncryption)))
sendMessagesB' c reqs = do
  (_, connIds) <- liftEither $ foldl' addConnId (Right ("", S.empty)) reqs
  lift $ sendMessagesB_ c reqs connIds
  where
    addConnId acc@(Right (prevId, s)) (Right (connId, _, _, _))
      | B.null connId = if B.null prevId then Left $ INTERNAL "sendMessages: empty first connId" else acc
      | connId `S.member` s = Left $ INTERNAL "sendMessages: duplicate connId"
      | otherwise = Right (connId, S.insert connId s)
    addConnId acc _ = acc

sendMessagesB_ :: forall t. Traversable t => AgentClient -> t (Either AgentErrorType MsgReq) -> Set ConnId -> AM' (t (Either AgentErrorType (AgentMsgId, PQEncryption)))
sendMessagesB_ c reqs connIds = withConnLocks c connIds "sendMessages" $ do
  prev <- newTVarIO Nothing
  reqs' <- withStoreBatch c $ \db -> fmap (bindRight $ getConn_ db prev) reqs
  let (toEnable, reqs'') = mapAccumL prepareConn [] reqs'
  void $ withStoreBatch' c $ \db -> map (\connId -> setConnPQSupport db connId PQSupportOn) $ S.toList toEnable
  enqueueMessagesB c reqs''
  where
    getConn_ :: DB.Connection -> TVar (Maybe (Either AgentErrorType SomeConn)) -> MsgReq -> IO (Either AgentErrorType (MsgReq, SomeConn))
    getConn_ db prev req@(connId, _, _, _) =
      (req,)
        <$$> if B.null connId
          then fromMaybe (Left $ INTERNAL "sendMessagesB_: empty prev connId") <$> readTVarIO prev
          else do
            conn <- first storeError <$> getConn db connId
            conn <$ atomically (writeTVar prev $ Just conn)
    prepareConn :: Set ConnId -> Either AgentErrorType (MsgReq, SomeConn) -> (Set ConnId, Either AgentErrorType (ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage))
    prepareConn s (Left e) = (s, Left e)
    prepareConn s (Right ((_, pqEnc, msgFlags, msg), SomeConn _ conn)) = case conn of
      DuplexConnection cData _ sqs -> prepareMsg cData sqs
      SndConnection cData sq -> prepareMsg cData [sq]
      _ -> (s, Left $ CONN SIMPLEX)
      where
        prepareMsg :: ConnData -> NonEmpty SndQueue -> (Set ConnId, Either AgentErrorType (ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage))
        prepareMsg cData@ConnData {connId, pqSupport} sqs
          | ratchetSyncSendProhibited cData = (s, Left $ CMD PROHIBITED "sendMessagesB: send prohibited")
          -- connection is only updated if PQ encryption was disabled, and now it has to be enabled.
          -- support for PQ encryption (small message envelopes) will not be disabled when message is sent.
          | pqEnc == PQEncOn && pqSupport == PQSupportOff =
              let cData' = cData {pqSupport = PQSupportOn} :: ConnData
               in (S.insert connId s, mkReq cData')
          | otherwise = (s, mkReq cData)
          where
            mkReq cData' = Right (cData', sqs, Just pqEnc, msgFlags, A_MSG msg)

-- / async command processing v v v

enqueueCommand :: AgentClient -> ACorrId -> ConnId -> Maybe SMPServer -> AgentCommand -> AM ()
enqueueCommand c corrId connId server aCommand = do
  withStore c $ \db -> createCommand db corrId connId server aCommand
  lift . void $ getAsyncCmdWorker True c connId server

resumeSrvCmds :: AgentClient -> ConnId -> Maybe SMPServer -> AM' ()
resumeSrvCmds = void .:. getAsyncCmdWorker False
{-# INLINE resumeSrvCmds #-}

resumeConnCmds :: AgentClient -> [ConnId] -> AM' ()
resumeConnCmds c connIds = do
  connSrvs <- rights . zipWith (second . (,)) connIds <$> withStoreBatch' c (\db -> fmap (getPendingCommandServers db) connIds)
  mapM_ (\(connId, srvs) -> mapM_ (resumeSrvCmds c connId) srvs) connSrvs

getAsyncCmdWorker :: Bool -> AgentClient -> ConnId -> Maybe SMPServer -> AM' Worker
getAsyncCmdWorker hasWork c connId server =
  getAgentWorker "async_cmd" hasWork c (connId, server) (asyncCmdWorkers c) (runCommandProcessing c connId server)

data CommandCompletion = CCMoved | CCCompleted

runCommandProcessing :: AgentClient -> ConnId -> Maybe SMPServer -> Worker -> AM ()
runCommandProcessing c@AgentClient {subQ} connId server_ Worker {doWork} = do
  ri <- asks $ messageRetryInterval . config -- different retry interval?
  forever $ do
    atomically $ endAgentOperation c AOSndNetwork
    lift $ waitForWork doWork
    liftIO $ throwWhenInactive c
    atomically $ beginAgentOperation c AOSndNetwork
    withWork c doWork (\db -> getPendingServerCommand db connId server_) $ runProcessCmd (riFast ri)
  where
    runProcessCmd ri cmd = do
      pending <- newTVarIO []
      processCmd ri cmd pending
      mapM_ (atomically . writeTBQueue subQ) . reverse =<< readTVarIO pending
    processCmd :: RetryInterval -> PendingCommand -> TVar [ATransmission] -> AM ()
    processCmd ri PendingCommand {cmdId, corrId, userId, command} pendingCmds = case command of
      AClientCommand cmd -> case cmd of
        NEW enableNtfs (ACM cMode) pqEnc subMode -> noServer $ do
          triedHosts <- newTVarIO S.empty
          tryCommand . withNextSrv c userId storageSrvs triedHosts [] $ \srv -> do
            cReq <- newRcvConnSrv c userId connId enableNtfs cMode Nothing pqEnc subMode srv
            notify $ INV (ACR cMode cReq)
        JOIN enableNtfs (ACR _ cReq@(CRInvitationUri ConnReqUriData {crSmpQueues = q :| _} _)) pqEnc subMode connInfo -> noServer $ do
          triedHosts <- newTVarIO S.empty
          tryCommand . withNextSrv c userId storageSrvs triedHosts [qServer q] $ \srv -> do
            sqSecured <- joinConnSrvAsync c userId connId enableNtfs cReq connInfo pqEnc subMode srv
            notify $ JOINED sqSecured
        LET confId ownCInfo -> withServer' . tryCommand $ allowConnection' c connId confId ownCInfo >> notify OK
        ACK msgId rcptInfo_ -> withServer' . tryCommand $ ackMessage' c connId msgId rcptInfo_ >> notify OK
        SWCH ->
          noServer . tryWithLock "switchConnection" $
            withStore c (`getConn` connId) >>= \case
              SomeConn _ conn@(DuplexConnection _ (replaced :| _rqs) _) ->
                switchDuplexConnection c conn replaced >>= notify . SWITCH QDRcv SPStarted
              _ -> throwE $ CMD PROHIBITED "SWCH: not duplex"
        DEL -> withServer' . tryCommand $ deleteConnection' c connId >> notify OK
        _ -> notify $ ERR $ INTERNAL $ "unsupported async command " <> show (aCommandTag cmd)
      AInternalCommand cmd -> case cmd of
        ICAckDel rId srvMsgId msgId -> withServer $ \srv -> tryWithLock "ICAckDel" $ ack srv rId srvMsgId >> withStore' c (\db -> deleteMsg db connId msgId)
        ICAck rId srvMsgId -> withServer $ \srv -> tryWithLock "ICAck" $ ack srv rId srvMsgId
        ICAllowSecure _rId senderKey -> withServer' . tryMoveableWithLock "ICAllowSecure" $ do
          (SomeConn _ conn, AcceptedConfirmation {senderConf, ownConnInfo}) <-
            withStore c $ \db -> runExceptT $ (,) <$> ExceptT (getConn db connId) <*> ExceptT (getAcceptedConfirmation db connId)
          case conn of
            RcvConnection cData rq -> do
              mapM_ (secure rq) senderKey
              mapM_ (connectReplyQueues c cData ownConnInfo Nothing) (L.nonEmpty $ smpReplyQueues senderConf)
              pure CCCompleted
            -- duplex connection is matched to handle SKEY retries
            DuplexConnection cData _ (sq :| _) -> do
              tryAgentError (mapM_ (connectReplyQueues c cData ownConnInfo (Just sq)) (L.nonEmpty $ smpReplyQueues senderConf)) >>= \case
                Right () -> pure CCCompleted
                Left e
                  | temporaryOrHostError e && Just server /= server_ -> do
                      -- In case the server is different we update server to remove command from this (connId, srv) queue
                      withStore c $ \db -> updateCommandServer db cmdId server
                      lift . void $ getAsyncCmdWorker True c connId (Just server)
                      pure CCMoved
                  | otherwise -> throwE e
              where
                server = qServer sq
            _ -> throwE $ INTERNAL $ "incorrect connection type " <> show (internalCmdTag cmd)
        ICDuplexSecure _rId senderKey -> withServer' . tryWithLock "ICDuplexSecure" . withDuplexConn $ \(DuplexConnection cData (rq :| _) (sq :| _)) -> do
          secure rq senderKey
          void $ enqueueMessage c cData sq SMP.MsgFlags {notification = True} HELLO
        -- ICDeleteConn is no longer used, but it can be present in old client databases
        ICDeleteConn -> withStore' c (`deleteCommand` cmdId)
        ICDeleteRcvQueue rId -> withServer $ \srv -> tryWithLock "ICDeleteRcvQueue" $ do
          rq <- withStore c (\db -> getDeletedRcvQueue db connId srv rId)
          deleteQueue c rq
          withStore' c (`deleteConnRcvQueue` rq)
        ICQSecure rId senderKey ->
          withServer $ \srv -> tryWithLock "ICQSecure" . withDuplexConn $ \(DuplexConnection cData rqs sqs) ->
            case find (sameQueue (srv, rId)) rqs of
              Just rq'@RcvQueue {server, sndId, status, dbReplaceQueueId = Just replaceQId} ->
                case find ((replaceQId ==) . dbQId) rqs of
                  Just rq1 -> when (status == Confirmed) $ do
                    secureQueue c rq' senderKey
                    -- we may add more statistics special to queue rotation later on,
                    -- not accounting secure during rotation for now:
                    -- atomically $ incSMPServerStat c userId server connSecured
                    withStore' c $ \db -> setRcvQueueStatus db rq' Secured
                    void . enqueueMessages c cData sqs SMP.noMsgFlags $ QUSE [((server, sndId), True)]
                    rq1' <- withStore' c $ \db -> setRcvSwitchStatus db rq1 $ Just RSSendingQUSE
                    let rqs' = updatedQs rq1' rqs
                        conn' = DuplexConnection cData rqs' sqs
                    notify . SWITCH QDRcv SPSecured $ connectionStats conn'
                  _ -> internalErr "ICQSecure: no switching queue found"
              _ -> internalErr "ICQSecure: queue address not found in connection"
        ICQDelete rId -> do
          withServer $ \srv -> tryWithLock "ICQDelete" . withDuplexConn $ \(DuplexConnection cData rqs sqs) -> do
            case removeQ (srv, rId) rqs of
              Nothing -> internalErr "ICQDelete: queue address not found in connection"
              Just (rq'@RcvQueue {primary}, rq'' : rqs')
                | primary -> internalErr "ICQDelete: cannot delete primary rcv queue"
                | otherwise -> do
                    checkRQSwchStatus rq' RSReceivedMessage
                    tryError (deleteQueue c rq') >>= \case
                      Right () -> finalizeSwitch
                      Left e
                        | temporaryOrHostError e -> throwE e
                        | otherwise -> finalizeSwitch >> throwE e
                where
                  finalizeSwitch = do
                    withStore' c $ \db -> deleteConnRcvQueue db rq'
                    when (enableNtfs cData) $ do
                      ns <- asks ntfSupervisor
                      atomically $ sendNtfSubCommand ns (NSCCreate, [connId])
                    let conn' = DuplexConnection cData (rq'' :| rqs') sqs
                    notify $ SWITCH QDRcv SPCompleted $ connectionStats conn'
              _ -> internalErr "ICQDelete: cannot delete the only queue in connection"
        where
          ack srv rId srvMsgId = do
            rq <- withStore c $ \db -> getRcvQueue db connId srv rId
            ackQueueMessage c rq srvMsgId
          secure :: RcvQueue -> SMP.SndPublicAuthKey -> AM ()
          secure rq@RcvQueue {server} senderKey = do
            secureQueue c rq senderKey
            atomically $ incSMPServerStat c userId server connSecured
            withStore' c $ \db -> setRcvQueueStatus db rq Secured
      where
        withServer a = case server_ of
          Just srv -> a srv
          _ -> internalErr "command requires server"
        withServer' = withServer . const
        noServer a = case server_ of
          Nothing -> a
          _ -> internalErr "command requires no server"
        withDuplexConn :: (Connection 'CDuplex -> AM ()) -> AM ()
        withDuplexConn a =
          withStore c (`getConn` connId) >>= \case
            SomeConn _ conn@DuplexConnection {} -> a conn
            _ -> internalErr "command requires duplex connection"
        tryCommand action = tryMoveableCommand (action $> CCCompleted)
        tryMoveableCommand action = withRetryInterval ri $ \_ loop -> do
          liftIO $ waitWhileSuspended c
          liftIO $ waitForUserNetwork c
          tryAgentError action >>= \case
            Left e
              | temporaryOrHostError e -> retrySndOp c loop
              | otherwise -> cmdError e
            Right CCCompleted -> withStore' c (`deleteCommand` cmdId)
            Right CCMoved -> pure () -- command processing moved to another command queue
        tryWithLock name = tryCommand . withConnLock c connId name
        tryMoveableWithLock name = tryMoveableCommand . withConnLock c connId name
        internalErr s = cmdError $ INTERNAL $ s <> ": " <> show (agentCommandTag command)
        cmdError e = notify (ERR e) >> withStore' c (`deleteCommand` cmdId)
        notify :: forall e. AEntityI e => AEvent e -> AM ()
        notify cmd =
          let t = (corrId, connId, AEvt (sAEntity @e) cmd)
           in atomically $ ifM (isFullTBQueue subQ) (modifyTVar' pendingCmds (t :)) (writeTBQueue subQ t)
-- ^ ^ ^ async command processing /

enqueueMessages :: AgentClient -> ConnData -> NonEmpty SndQueue -> MsgFlags -> AMessage -> AM (AgentMsgId, PQEncryption)
enqueueMessages c cData sqs msgFlags aMessage = do
  when (ratchetSyncSendProhibited cData) $ throwE $ INTERNAL "enqueueMessages: ratchet is not synchronized"
  enqueueMessages' c cData sqs msgFlags aMessage

enqueueMessages' :: AgentClient -> ConnData -> NonEmpty SndQueue -> MsgFlags -> AMessage -> AM (AgentMsgId, CR.PQEncryption)
enqueueMessages' c cData sqs msgFlags aMessage =
  ExceptT $ runIdentity <$> enqueueMessagesB c (Identity (Right (cData, sqs, Nothing, msgFlags, aMessage)))
{-# INLINE enqueueMessages' #-}

enqueueMessagesB :: Traversable t => AgentClient -> t (Either AgentErrorType (ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage)) -> AM' (t (Either AgentErrorType (AgentMsgId, PQEncryption)))
enqueueMessagesB c reqs = do
  reqs' <- enqueueMessageB c reqs
  enqueueSavedMessageB c $ mapMaybe snd $ rights $ toList reqs'
  pure $ fst <$$> reqs'

isActiveSndQ :: SndQueue -> Bool
isActiveSndQ SndQueue {status} = status == Secured || status == Active
{-# INLINE isActiveSndQ #-}

enqueueMessage :: AgentClient -> ConnData -> SndQueue -> MsgFlags -> AMessage -> AM (AgentMsgId, PQEncryption)
enqueueMessage c cData sq msgFlags aMessage =
  ExceptT $ fmap fst . runIdentity <$> enqueueMessageB c (Identity (Right (cData, [sq], Nothing, msgFlags, aMessage)))
{-# INLINE enqueueMessage #-}

-- this function is used only for sending messages in batch, it returns the list of successes to enqueue additional deliveries
enqueueMessageB :: forall t. Traversable t => AgentClient -> t (Either AgentErrorType (ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage)) -> AM' (t (Either AgentErrorType ((AgentMsgId, PQEncryption), Maybe (ConnData, [SndQueue], AgentMsgId))))
enqueueMessageB c reqs = do
  cfg <- asks config
  reqMids <- withStoreBatch c $ \db -> fmap (bindRight $ storeSentMsg db cfg) reqs
  forME reqMids $ \((cData, sq :| sqs, _, _, _), InternalId msgId, pqSecr) -> do
    submitPendingMsg c cData sq
    let sqs' = filter isActiveSndQ sqs
    pure $ Right ((msgId, pqSecr), if null sqs' then Nothing else Just (cData, sqs', msgId))
  where
    storeSentMsg :: DB.Connection -> AgentConfig -> (ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage) -> IO (Either AgentErrorType ((ConnData, NonEmpty SndQueue, Maybe PQEncryption, MsgFlags, AMessage), InternalId, PQEncryption))
    storeSentMsg db cfg req@(cData@ConnData {connId}, sq :| _, pqEnc_, msgFlags, aMessage) = fmap (first storeError) $ runExceptT $ do
      let AgentConfig {smpAgentVRange, e2eEncryptVRange} = cfg
      internalTs <- liftIO getCurrentTime
      (internalId, internalSndId, prevMsgHash) <- ExceptT $ updateSndIds db connId
      let privHeader = APrivHeader (unSndId internalSndId) prevMsgHash
          agentMsg = AgentMessage privHeader aMessage
          agentMsgStr = smpEncode agentMsg
          internalHash = C.sha256Hash agentMsgStr
          currentE2EVersion = maxVersion e2eEncryptVRange
      (encAgentMessage, pqEnc) <- agentRatchetEncrypt db cData agentMsgStr e2eEncAgentMsgLength pqEnc_ currentE2EVersion
      let agentVersion = maxVersion smpAgentVRange
          msgBody = smpEncode $ AgentMsgEnvelope {agentVersion, encAgentMessage}
          msgType = agentMessageType agentMsg
          msgData = SndMsgData {internalId, internalSndId, internalTs, msgType, msgFlags, msgBody, pqEncryption = pqEnc, internalHash, prevMsgHash}
      liftIO $ createSndMsg db connId msgData
      liftIO $ createSndMsgDelivery db connId sq internalId
      pure (req, internalId, pqEnc)

enqueueSavedMessage :: AgentClient -> ConnData -> AgentMsgId -> SndQueue -> AM' ()
enqueueSavedMessage c cData msgId sq = enqueueSavedMessageB c $ Identity (cData, [sq], msgId)
{-# INLINE enqueueSavedMessage #-}

enqueueSavedMessageB :: Foldable t => AgentClient -> t (ConnData, [SndQueue], AgentMsgId) -> AM' ()
enqueueSavedMessageB c reqs = do
  -- saving to the database is in the start to avoid race conditions when delivery is read from queue before it is saved
  void $ withStoreBatch' c $ \db -> concatMap (storeDeliveries db) reqs
  forM_ reqs $ \(cData, sqs, _) ->
    forM sqs $ submitPendingMsg c cData
  where
    storeDeliveries :: DB.Connection -> (ConnData, [SndQueue], AgentMsgId) -> [IO ()]
    storeDeliveries db (ConnData {connId}, sqs, msgId) = do
      let mId = InternalId msgId
       in map (\sq -> createSndMsgDelivery db connId sq mId) sqs

resumeMsgDelivery :: AgentClient -> ConnData -> SndQueue -> AM' ()
resumeMsgDelivery = void .:. getDeliveryWorker False
{-# INLINE resumeMsgDelivery #-}

getDeliveryWorker :: Bool -> AgentClient -> ConnData -> SndQueue -> AM' (Worker, TMVar ())
getDeliveryWorker hasWork c cData sq =
  getAgentWorker' fst mkLock "msg_delivery" hasWork c (qAddress sq) (smpDeliveryWorkers c) (runSmpQueueMsgDelivery c cData sq)
  where
    mkLock w = do
      retryLock <- newEmptyTMVar
      pure (w, retryLock)

submitPendingMsg :: AgentClient -> ConnData -> SndQueue -> AM' ()
submitPendingMsg c cData sq = do
  atomically $ modifyTVar' (msgDeliveryOp c) $ \s -> s {opsInProgress = opsInProgress s + 1}
  void $ getDeliveryWorker True c cData sq

runSmpQueueMsgDelivery :: AgentClient -> ConnData -> SndQueue -> (Worker, TMVar ()) -> AM ()
runSmpQueueMsgDelivery c@AgentClient {subQ} ConnData {connId} sq@SndQueue {userId, server, sndSecure} (Worker {doWork}, qLock) = do
  AgentConfig {messageRetryInterval = ri, messageTimeout, helloTimeout, quotaExceededTimeout} <- asks config
  forever $ do
    atomically $ endAgentOperation c AOSndNetwork
    lift $ waitForWork doWork
    liftIO $ throwWhenInactive c
    liftIO $ throwWhenNoDelivery c sq
    atomically $ beginAgentOperation c AOSndNetwork
    withWork c doWork (\db -> getPendingQueueMsg db connId sq) $
      \(rq_, PendingMsgData {msgId, msgType, msgBody, pqEncryption, msgFlags, msgRetryState, internalTs}) -> do
        atomically $ endAgentOperation c AOMsgDelivery -- this operation begins in submitPendingMsg
        let mId = unId msgId
            ri' = maybe id updateRetryInterval2 msgRetryState ri
        withRetryLock2 ri' qLock $ \riState loop -> do
          liftIO $ waitWhileSuspended c
          liftIO $ waitForUserNetwork c
          resp <- tryError $ case msgType of
            AM_CONN_INFO -> sendConfirmation c sq msgBody
            AM_CONN_INFO_REPLY -> sendConfirmation c sq msgBody
            _ -> sendAgentMessage c sq msgFlags msgBody
          case resp of
            Left e -> do
              let err = if msgType == AM_A_MSG_ then MERR mId e else ERR e
              case e of
                SMP _ SMP.QUOTA -> do
                  atomically $ incSMPServerStat c userId server sentQuotaErrs
                  case msgType of
                    AM_CONN_INFO -> connError msgId NOT_AVAILABLE
                    AM_CONN_INFO_REPLY -> connError msgId NOT_AVAILABLE
                    _ -> do
                      expireTs <- addUTCTime (-quotaExceededTimeout) <$> liftIO getCurrentTime
                      if internalTs < expireTs
                        then notifyDelMsgs msgId e expireTs
                        else do
                          notify $ MWARN (unId msgId) e
                          retrySndMsg RISlow
                SMP _ SMP.AUTH -> do
                  atomically $ incSMPServerStat c userId server sentAuthErrs
                  case msgType of
                    AM_CONN_INFO -> connError msgId NOT_AVAILABLE
                    AM_CONN_INFO_REPLY -> connError msgId NOT_AVAILABLE
                    AM_RATCHET_INFO -> connError msgId NOT_AVAILABLE
                    -- in duplexHandshake mode (v2) HELLO is only sent once, without retrying,
                    -- because the queue must be secured by the time the confirmation or the first HELLO is received
                    AM_HELLO_ -> case rq_ of
                      -- party initiating connection
                      Just _ -> connError msgId NOT_AVAILABLE
                      -- party joining connection
                      _ -> connError msgId NOT_ACCEPTED
                    AM_A_MSG_ -> notifyDel msgId err
                    AM_A_RCVD_ -> notifyDel msgId err
                    AM_QCONT_ -> notifyDel msgId err
                    AM_QADD_ -> qError msgId "QADD: AUTH"
                    AM_QKEY_ -> qError msgId "QKEY: AUTH"
                    AM_QUSE_ -> qError msgId "QUSE: AUTH"
                    AM_QTEST_ -> qError msgId "QTEST: AUTH"
                    AM_EREADY_ -> notifyDel msgId err
                _
                  -- for other operations BROKER HOST is treated as a permanent error (e.g., when connecting to the server),
                  -- the message sending would be retried
                  | temporaryOrHostError e -> do
                      let msgTimeout = if msgType == AM_HELLO_ then helloTimeout else messageTimeout
                      expireTs <- addUTCTime (-msgTimeout) <$> liftIO getCurrentTime
                      if internalTs < expireTs
                        then notifyDelMsgs msgId e expireTs
                        else do
                          when (serverHostError e) $ notify $ MWARN (unId msgId) e
                          retrySndMsg RIFast
                  | otherwise -> do
                      atomically $ incSMPServerStat c userId server sentOtherErrs
                      notifyDel msgId err
              where
                retrySndMsg riMode = do
                  withStore' c $ \db -> updatePendingMsgRIState db connId msgId riState
                  retrySndOp c $ loop riMode
            Right proxySrv_ -> do
              case msgType of
                AM_CONN_INFO
                  | sndSecure -> notify (CON pqEncryption) >> setStatus Active
                  | otherwise -> setStatus Confirmed
                AM_CONN_INFO_REPLY -> setStatus Confirmed
                AM_RATCHET_INFO -> pure ()
                AM_HELLO_ -> do
                  withStore' c $ \db -> setSndQueueStatus db sq Active
                  case rq_ of
                    -- party initiating connection (in v1)
                    Just rq@RcvQueue {status} ->
                      -- it is unclear why subscribeQueue was needed here,
                      -- message delivery can only be enabled for queues that were created in the current session or subscribed
                      -- subscribeQueue c rq connId
                      --
                      -- If initiating party were to send CON to the user without waiting for reply HELLO (to reduce handshake time),
                      -- it would lead to the non-deterministic internal ID of the first sent message, at to some other race conditions,
                      -- because it can be sent before HELLO is received
                      -- With `status == Active` condition, CON is sent here only by the accepting party, that previously received HELLO
                      when (status == Active) $ do
                        atomically $ incSMPServerStat c userId (qServer rq) connCompleted
                        notify $ CON pqEncryption
                    -- this branch should never be reached as receive queue is created before the confirmation,
                    _ -> logError "HELLO sent without receive queue"
                AM_A_MSG_ -> notify $ SENT mId proxySrv_
                AM_A_RCVD_ -> pure ()
                AM_QCONT_ -> pure ()
                AM_QADD_ -> pure ()
                AM_QKEY_ -> do
                  SomeConn _ conn <- withStore c (`getConn` connId)
                  notify . SWITCH QDSnd SPConfirmed $ connectionStats conn
                AM_QUSE_ -> pure ()
                AM_QTEST_ -> withConnLock c connId "runSmpQueueMsgDelivery AM_QTEST_" $ do
                  withStore' c $ \db -> setSndQueueStatus db sq Active
                  SomeConn _ conn <- withStore c (`getConn` connId)
                  case conn of
                    DuplexConnection cData' rqs sqs -> do
                      -- remove old snd queue from connection once QTEST is sent to the new queue
                      let addr = qAddress sq
                      case findQ addr sqs of
                        -- this is the same queue where this loop delivers messages to but with updated state
                        Just SndQueue {dbReplaceQueueId = Just replacedId, primary} ->
                          -- second part of this condition is a sanity check because dbReplaceQueueId cannot point to the same queue, see switchConnection'
                          case removeQP (\sq' -> dbQId sq' == replacedId && not (sameQueue addr sq')) sqs of
                            Nothing -> internalErr msgId "sent QTEST: queue not found in connection"
                            Just (sq', sq'' : sqs') -> do
                              checkSQSwchStatus sq' SSSendingQTEST
                              -- remove the delivery from the map to stop the thread when the delivery loop is complete
                              atomically $ TM.delete (qAddress sq') $ smpDeliveryWorkers c
                              withStore' c $ \db -> do
                                when primary $ setSndQueuePrimary db connId sq
                                deletePendingMsgs db connId sq'
                                deleteConnSndQueue db connId sq'
                              let sqs'' = sq'' :| sqs'
                                  conn' = DuplexConnection cData' rqs sqs''
                              notify . SWITCH QDSnd SPCompleted $ connectionStats conn'
                            _ -> internalErr msgId "sent QTEST: there is only one queue in connection"
                        _ -> internalErr msgId "sent QTEST: queue not in connection or not replacing another queue"
                    _ -> internalErr msgId "QTEST sent not in duplex connection"
                AM_EREADY_ -> pure ()
              delMsgKeep (msgType == AM_A_MSG_) msgId
              where
                setStatus status = do
                  withStore' c $ \db -> do
                    setSndQueueStatus db sq status
                    when (isJust rq_) $ removeConfirmations db connId
  where
    notifyDelMsgs :: InternalId -> AgentErrorType -> UTCTime -> AM ()
    notifyDelMsgs msgId err expireTs = do
      notifyDel msgId $ MERR (unId msgId) err
      msgIds_ <- withStore' c $ \db -> getExpiredSndMessages db connId sq expireTs
      forM_ (L.nonEmpty msgIds_) $ \msgIds -> do
        notify $ MERRS (L.map unId msgIds) err
        withStore' c $ \db -> forM_ msgIds $ \msgId' -> deleteSndMsgDelivery db connId sq msgId' False `catchAll_` pure ()
      atomically $ incSMPServerStat' c userId server sentExpiredErrs (length msgIds_ + 1)
    delMsg :: InternalId -> AM ()
    delMsg = delMsgKeep False
    delMsgKeep :: Bool -> InternalId -> AM ()
    delMsgKeep keepForReceipt msgId = withStore' c $ \db -> deleteSndMsgDelivery db connId sq msgId keepForReceipt
    notify :: forall e. AEntityI e => AEvent e -> AM ()
    notify cmd = atomically $ writeTBQueue subQ ("", connId, AEvt (sAEntity @e) cmd)
    notifyDel :: AEntityI e => InternalId -> AEvent e -> AM ()
    notifyDel msgId cmd = notify cmd >> delMsg msgId
    connError msgId = notifyDel msgId . ERR . CONN
    qError msgId = notifyDel msgId . ERR . AGENT . A_QUEUE
    internalErr msgId = notifyDel msgId . ERR . INTERNAL

retrySndOp :: AgentClient -> AM () -> AM ()
retrySndOp c loop = do
  -- end... is in a separate atomically because if begin... blocks, SUSPENDED won't be sent
  atomically $ endAgentOperation c AOSndNetwork
  liftIO $ throwWhenInactive c
  atomically $ beginAgentOperation c AOSndNetwork
  loop

ackMessage' :: AgentClient -> ConnId -> AgentMsgId -> Maybe MsgReceiptInfo -> AM ()
ackMessage' c connId msgId rcptInfo_ = withConnLock c connId "ackMessage" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection {} -> ack >> sendRcpt conn >> del
    RcvConnection {} -> ack >> del
    SndConnection {} -> throwE $ CONN SIMPLEX
    ContactConnection {} -> throwE $ CMD PROHIBITED "ackMessage: ContactConnection"
    NewConnection _ -> throwE $ CMD PROHIBITED "ackMessage: NewConnection"
  where
    ack :: AM ()
    ack = do
      -- the stored message was delivered via a specific queue, the rest failed to decrypt and were already acknowledged
      (rq, srvMsgId) <- withStore c $ \db -> setMsgUserAck db connId $ InternalId msgId
      ackQueueMessage c rq srvMsgId
    del :: AM ()
    del = withStore' c $ \db -> deleteMsg db connId $ InternalId msgId
    sendRcpt :: Connection 'CDuplex -> AM ()
    sendRcpt (DuplexConnection cData@ConnData {connAgentVersion} _ sqs) = do
      msg@RcvMsg {msgType, msgReceipt} <- withStore c $ \db -> getRcvMsg db connId $ InternalId msgId
      case rcptInfo_ of
        Just rcptInfo -> do
          unless (msgType == AM_A_MSG_) . throwE $ CMD PROHIBITED "ackMessage: receipt not allowed"
          when (connAgentVersion >= deliveryRcptsSMPAgentVersion) $ do
            let RcvMsg {msgMeta = MsgMeta {sndMsgId}, internalHash} = msg
                rcpt = A_RCVD [AMessageReceipt {agentMsgId = sndMsgId, msgHash = internalHash, rcptInfo}]
            void $ enqueueMessages c cData sqs SMP.MsgFlags {notification = False} rcpt
        Nothing -> case (msgType, msgReceipt) of
          -- only remove sent message if receipt hash was Ok, both to debug and for future redundancy
          (AM_A_RCVD_, Just MsgReceipt {agentMsgId = sndMsgId, msgRcptStatus = MROk}) ->
            withStore' c $ \db -> deleteDeliveredSndMsg db connId $ InternalId sndMsgId
          _ -> pure ()

getConnectionQueueInfo' :: AgentClient -> ConnId -> AM ServerQueueInfo
getConnectionQueueInfo' c connId = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ (rq :| _) _ -> getQueueInfo c rq
    RcvConnection _ rq -> getQueueInfo c rq
    ContactConnection _ rq -> getQueueInfo c rq
    SndConnection {} -> throwE $ CONN SIMPLEX
    NewConnection _ -> throwE $ CMD PROHIBITED "getConnectionQueueInfo': NewConnection"

switchConnection' :: AgentClient -> ConnId -> AM ConnectionStats
switchConnection' c connId =
  withConnLock c connId "switchConnection" $
    withStore c (`getConn` connId) >>= \case
      SomeConn _ conn@(DuplexConnection cData rqs@(rq :| _rqs) _)
        | isJust (switchingRQ rqs) -> throwE $ CMD PROHIBITED "switchConnection: already switching"
        | otherwise -> do
            when (ratchetSyncSendProhibited cData) $ throwE $ CMD PROHIBITED "switchConnection: send prohibited"
            rq' <- withStore' c $ \db -> setRcvSwitchStatus db rq $ Just RSSwitchStarted
            switchDuplexConnection c conn rq'
      _ -> throwE $ CMD PROHIBITED "switchConnection: not duplex"

switchDuplexConnection :: AgentClient -> Connection 'CDuplex -> RcvQueue -> AM ConnectionStats
switchDuplexConnection c (DuplexConnection cData@ConnData {connId, userId} rqs sqs) rq@RcvQueue {server, dbQueueId = DBQueueId dbQueueId, sndId} = do
  checkRQSwchStatus rq RSSwitchStarted
  clientVRange <- asks $ smpClientVRange . config
  -- try to get the server that is different from all queues, or at least from the primary rcv queue
  srvAuth@(ProtoServerWithAuth srv _) <- getNextSMPServer c userId $ map qServer (L.toList rqs) <> map qServer (L.toList sqs)
  srv' <- if srv == server then getNextSMPServer c userId [server] else pure srvAuth
  (q, qUri, tSess, sessId) <- newRcvQueue c userId connId srv' clientVRange SMSubscribe False
  let rq' = (q :: NewRcvQueue) {primary = True, dbReplaceQueueId = Just dbQueueId}
  rq'' <- withStore c $ \db -> addConnRcvQueue db connId rq'
  lift $ addNewQueueSubscription c rq'' tSess sessId
  void . enqueueMessages c cData sqs SMP.noMsgFlags $ QADD [(qUri, Just (server, sndId))]
  rq1 <- withStore' c $ \db -> setRcvSwitchStatus db rq $ Just RSSendingQADD
  let rqs' = updatedQs rq1 rqs <> [rq'']
  pure . connectionStats $ DuplexConnection cData rqs' sqs

abortConnectionSwitch' :: AgentClient -> ConnId -> AM ConnectionStats
abortConnectionSwitch' c connId =
  withConnLock c connId "abortConnectionSwitch" $
    withStore c (`getConn` connId) >>= \case
      SomeConn _ (DuplexConnection cData rqs sqs) -> case switchingRQ rqs of
        Just rq
          | canAbortRcvSwitch rq -> do
              when (ratchetSyncSendProhibited cData) $ throwE $ CMD PROHIBITED "abortConnectionSwitch: send prohibited"
              -- multiple queues to which the connections switches were possible when repeating switch was allowed
              let (delRqs, keepRqs) = L.partition ((Just (dbQId rq) ==) . dbReplaceQId) rqs
              case L.nonEmpty keepRqs of
                Just rqs' -> do
                  rq' <- withStore' c $ \db -> do
                    mapM_ (setRcvQueueDeleted db) delRqs
                    setRcvSwitchStatus db rq Nothing
                  forM_ delRqs $ \RcvQueue {server, rcvId} -> enqueueCommand c "" connId (Just server) $ AInternalCommand $ ICDeleteRcvQueue rcvId
                  let rqs'' = updatedQs rq' rqs'
                      conn' = DuplexConnection cData rqs'' sqs
                  pure $ connectionStats conn'
                _ -> throwE $ INTERNAL "won't delete all rcv queues in connection"
          | otherwise -> throwE $ CMD PROHIBITED "abortConnectionSwitch: no rcv queues left"
        _ -> throwE $ CMD PROHIBITED "abortConnectionSwitch: not allowed"
      _ -> throwE $ CMD PROHIBITED "abortConnectionSwitch: not duplex"

synchronizeRatchet' :: AgentClient -> ConnId -> PQSupport -> Bool -> AM ConnectionStats
synchronizeRatchet' c connId pqSupport' force = withConnLock c connId "synchronizeRatchet" $ do
  withStore c (`getConn` connId) >>= \case
    SomeConn _ (DuplexConnection cData@ConnData {pqSupport} rqs sqs)
      | ratchetSyncAllowed cData || force -> do
          -- check queues are not switching?
          when (pqSupport' /= pqSupport) $ withStore' c $ \db -> setConnPQSupport db connId pqSupport'
          let cData' = cData {pqSupport = pqSupport'} :: ConnData
          AgentConfig {e2eEncryptVRange} <- asks config
          g <- asks random
          (pk1, pk2, pKem, e2eParams) <- liftIO $ CR.generateRcvE2EParams g (maxVersion e2eEncryptVRange) pqSupport'
          enqueueRatchetKeyMsgs c cData' sqs e2eParams
          withStore' c $ \db -> do
            setConnRatchetSync db connId RSStarted
            setRatchetX3dhKeys db connId pk1 pk2 pKem
          let cData'' = cData' {ratchetSyncState = RSStarted} :: ConnData
              conn' = DuplexConnection cData'' rqs sqs
          pure $ connectionStats conn'
      | otherwise -> throwE $ CMD PROHIBITED "synchronizeRatchet: not allowed"
    _ -> throwE $ CMD PROHIBITED "synchronizeRatchet: not duplex"

ackQueueMessage :: AgentClient -> RcvQueue -> SMP.MsgId -> AM ()
ackQueueMessage c rq@RcvQueue {userId, connId, server} srvMsgId = do
  atomically $ incSMPServerStat c userId server ackAttempts
  tryAgentError (sendAck c rq srvMsgId) >>= \case
    Right _ -> sendMsgNtf ackMsgs
    Left (SMP _ SMP.NO_MSG) -> sendMsgNtf ackNoMsgErrs
    Left e -> do
      unless (temporaryOrHostError e) $ atomically $ incSMPServerStat c userId server ackOtherErrs
      throwE e
  where
    sendMsgNtf stat = do
      atomically $ incSMPServerStat c userId server stat
      whenM (liftIO $ hasGetLock c rq) $ do
        atomically $ releaseGetLock c rq
        brokerTs_ <- eitherToMaybe <$> tryAgentError (withStore c $ \db -> getRcvMsgBrokerTs db connId srvMsgId)
        atomically $ writeTBQueue (subQ c) ("", connId, AEvt SAEConn $ MSGNTF srvMsgId brokerTs_)

-- | Suspend SMP agent connection (OFF command) in Reader monad
suspendConnection' :: AgentClient -> ConnId -> AM ()
suspendConnection' c connId = withConnLock c connId "suspendConnection" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ rqs _ -> mapM_ (suspendQueue c) rqs
    RcvConnection _ rq -> suspendQueue c rq
    ContactConnection _ rq -> suspendQueue c rq
    SndConnection _ _ -> throwE $ CONN SIMPLEX
    NewConnection _ -> throwE $ CMD PROHIBITED "suspendConnection"

-- | Delete SMP agent connection (DEL command) in Reader monad
-- unlike deleteConnectionAsync, this function does not mark connection as deleted in case of deletion failure
-- currently it is used only in tests
deleteConnection' :: AgentClient -> ConnId -> AM ()
deleteConnection' c connId = toConnResult connId =<< deleteConnections' c [connId]
{-# INLINE deleteConnection' #-}

connRcvQueues :: Connection d -> [RcvQueue]
connRcvQueues = \case
  DuplexConnection _ rqs _ -> L.toList rqs
  RcvConnection _ rq -> [rq]
  ContactConnection _ rq -> [rq]
  SndConnection _ _ -> []
  NewConnection _ -> []

-- Unlike deleteConnectionsAsync, this function does not mark connections as deleted in case of deletion failure.
deleteConnections' :: AgentClient -> [ConnId] -> AM (Map ConnId (Either AgentErrorType ()))
deleteConnections' = deleteConnections_ getConns False False
{-# INLINE deleteConnections' #-}

deleteDeletedConns :: AgentClient -> [ConnId] -> AM (Map ConnId (Either AgentErrorType ()))
deleteDeletedConns = deleteConnections_ getDeletedConns True False
{-# INLINE deleteDeletedConns #-}

deleteDeletedWaitingDeliveryConns :: AgentClient -> [ConnId] -> AM (Map ConnId (Either AgentErrorType ()))
deleteDeletedWaitingDeliveryConns = deleteConnections_ getConns True True
{-# INLINE deleteDeletedWaitingDeliveryConns #-}

prepareDeleteConnections_ ::
  (DB.Connection -> [ConnId] -> IO [Either StoreError SomeConn]) ->
  AgentClient ->
  Bool ->
  [ConnId] ->
  AM (Map ConnId (Either AgentErrorType ()), [RcvQueue], [ConnId])
prepareDeleteConnections_ getConnections c waitDelivery connIds = do
  conns :: Map ConnId (Either StoreError SomeConn) <- timeItM "[prepareDeleteConnections_] getConnections" $ M.fromList . zip connIds <$> withStore' c (`getConnections` connIds)
  let (errs, cs) = M.mapEither id conns
      errs' = M.map (Left . storeError) errs
      (delRs, rcvQs) = M.mapEither rcvQueues cs
      rqs = concat $ M.elems rcvQs
      connIds' = M.keys rcvQs
  timeItM "[prepareDeleteConnections_] unsubConnIds" $ lift $ forM_ (L.nonEmpty connIds') unsubConnIds
  -- ! delRs is not used to notify about the result in any of the calling functions,
  -- ! it is only used to check results count in deleteConnections_;
  -- ! if it was used to notify about the result, it might be necessary to differentiate
  -- ! between completed deletions of connections, and deletions delayed due to wait for delivery (see deleteConn)
  deliveryTimeout <- if waitDelivery then asks (Just . connDeleteDeliveryTimeout . config) else pure Nothing
  cIds_ <- timeItM "[prepareDeleteConnections_] batch deleteConn" $ lift $ L.nonEmpty . catMaybes . rights <$> withStoreBatch' c (\db -> map (deleteConn db deliveryTimeout) (M.keys delRs))
  timeItM "[prepareDeleteConnections_] notify DEL_CONNS" $ forM_ cIds_ $ \cIds -> notify ("", "", AEvt SAEConn $ DEL_CONNS cIds)
  pure (errs' <> delRs, rqs, connIds')
  where
    rcvQueues :: SomeConn -> Either (Either AgentErrorType ()) [RcvQueue]
    rcvQueues (SomeConn _ conn) = case connRcvQueues conn of
      [] -> Left $ Right ()
      rqs -> Right rqs
    unsubConnIds :: NonEmpty ConnId -> AM' ()
    unsubConnIds connIds' = do
      forM_ connIds' $ \connId ->
        atomically $ removeSubscription c connId
      ns <- asks ntfSupervisor
      atomically $ writeTBQueue (ntfSubQ ns) (NSCDeleteSub, connIds')
    notify = atomically . writeTBQueue (subQ c)

deleteConnQueues :: AgentClient -> Bool -> Bool -> [RcvQueue] -> AM' (Map ConnId (Either AgentErrorType ()))
deleteConnQueues c waitDelivery ntf rqs = do
  rs <-
    connResults
      <$> ( do
              dqs <- timeItM' "[deleteConnQueues] deleteQueues" $ deleteQueues c rqs
              timeItM' "[deleteConnQueues] deleteQueueRecs" $ deleteQueueRecs dqs
          )
  let connIds = M.keys $ M.filter isRight rs
  deliveryTimeout <- if waitDelivery then asks (Just . connDeleteDeliveryTimeout . config) else pure Nothing
  cIds_ <- timeItM' "[deleteConnQueues] batch deleteConn" $ L.nonEmpty . catMaybes . rights <$> withStoreBatch' c (\db -> map (deleteConn db deliveryTimeout) connIds)
  timeItM' "[deleteConnQueues] notify DEL_CONNS" $ forM_ cIds_ $ \cIds -> notify ("", "", AEvt SAEConn $ DEL_CONNS cIds)
  pure rs
  where
    deleteQueueRecs :: [(RcvQueue, Either AgentErrorType ())] -> AM' [(RcvQueue, Either AgentErrorType ())]
    deleteQueueRecs rs = do
      maxErrs <- asks $ deleteErrorCount . config
      rs' <- timeItM' "[deleteConnQueues, deleteQueueRecs] batch deleteQueueRec" $ rights <$> withStoreBatch' c (\db -> map (deleteQueueRec db maxErrs) rs)
      let delQ ((rq, _), err_) =  (qConnId rq,qServer rq,queueId rq,) <$> err_
          delQs_ = L.nonEmpty $ mapMaybe delQ rs'
      timeItM' "[deleteConnQueues, deleteQueueRecs] notify DEL_RCVQS" $ forM_ delQs_ $ \delQs -> notify ("", "", AEvt SAEConn $ DEL_RCVQS delQs)
      pure $ map fst rs'
      where
        deleteQueueRec ::
          DB.Connection ->
          Int ->
          (RcvQueue, Either AgentErrorType ()) ->
          IO ((RcvQueue, Either AgentErrorType ()), Maybe (Maybe AgentErrorType)) -- Nothing - no event, Just Nothing - no error
        deleteQueueRec db maxErrs (rq@RcvQueue {userId, server}, r) = case r of
          Right _ -> deleteConnRcvQueue db rq $> ((rq, r), Just Nothing)
          Left e
            | temporaryOrHostError e && deleteErrors rq + 1 < maxErrs -> incRcvDeleteErrors db rq $> ((rq, r), Nothing)
            | otherwise -> do
                deleteConnRcvQueue db rq
                -- attempts and successes are counted in deleteQueues function
                atomically $ incSMPServerStat c userId server connDeleted
                pure ((rq, Right ()), Just (Just e))
    notify = when ntf . atomically . writeTBQueue (subQ c)
    connResults :: [(RcvQueue, Either AgentErrorType ())] -> Map ConnId (Either AgentErrorType ())
    connResults = M.map snd . foldl' addResult M.empty
      where
        -- collects results by connection ID
        addResult :: Map ConnId QCmdResult -> (RcvQueue, Either AgentErrorType ()) -> Map ConnId QCmdResult
        addResult rs (RcvQueue {connId, status}, r) = M.alter (combineRes (status, r)) connId rs
        -- combines two results for one connection, by prioritizing errors in Active queues
        combineRes :: QCmdResult -> Maybe QCmdResult -> Maybe QCmdResult
        combineRes r' (Just r) = Just $ if order r <= order r' then r else r'
        combineRes r' _ = Just r'
        order :: QCmdResult -> Int
        order (Active, Left _) = 1
        order (_, Left _) = 2
        order _ = 3

deleteConnections_ ::
  (DB.Connection -> [ConnId] -> IO [Either StoreError SomeConn]) ->
  Bool ->
  Bool ->
  AgentClient ->
  [ConnId] ->
  AM (Map ConnId (Either AgentErrorType ()))
deleteConnections_ _ _ _ _ [] = pure M.empty
deleteConnections_ getConnections ntf waitDelivery c connIds = do
  (rs, rqs, _) <- prepareDeleteConnections_ getConnections c waitDelivery connIds
  rcvRs <- lift $ deleteConnQueues c waitDelivery ntf rqs
  let rs' = M.union rs rcvRs
  notifyResultError rs'
  pure rs'
  where
    notifyResultError :: Map ConnId (Either AgentErrorType ()) -> AM ()
    notifyResultError rs = do
      let actual = M.size rs
          expected = length connIds
      when (actual /= expected) . atomically $
        writeTBQueue (subQ c) ("", "", AEvt SAEConn $ ERR $ INTERNAL $ "deleteConnections result size: " <> show actual <> ", expected " <> show expected)

getConnectionServers' :: AgentClient -> ConnId -> AM ConnectionStats
getConnectionServers' c connId = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  pure $ connectionStats conn

getConnectionRatchetAdHash' :: AgentClient -> ConnId -> AM ByteString
getConnectionRatchetAdHash' c connId = do
  CR.Ratchet {rcAD = Str rcAD} <- withStore c (`getRatchet` connId)
  pure $ C.sha256Hash rcAD

connectionStats :: Connection c -> ConnectionStats
connectionStats = \case
  RcvConnection cData rq ->
    (stats cData) {rcvQueuesInfo = [rcvQueueInfo rq]}
  SndConnection cData sq ->
    (stats cData) {sndQueuesInfo = [sndQueueInfo sq]}
  DuplexConnection cData rqs sqs ->
    (stats cData) {rcvQueuesInfo = map rcvQueueInfo $ L.toList rqs, sndQueuesInfo = map sndQueueInfo $ L.toList sqs}
  ContactConnection cData rq ->
    (stats cData) {rcvQueuesInfo = [rcvQueueInfo rq]}
  NewConnection cData ->
    stats cData
  where
    stats ConnData {connAgentVersion, ratchetSyncState} =
      ConnectionStats
        { connAgentVersion,
          rcvQueuesInfo = [],
          sndQueuesInfo = [],
          ratchetSyncState,
          ratchetSyncSupported = connAgentVersion >= ratchetSyncSMPAgentVersion
        }

-- | Change servers to be used for creating new queues.
-- This function will set all servers as enabled in case all passed servers are disabled.
setProtocolServers :: forall p. (ProtocolTypeI p, UserProtocol p) => AgentClient -> UserId -> NonEmpty (ServerCfg p) -> IO ()
setProtocolServers c userId srvs = do
  checkUserServers "setProtocolServers" srvs
  atomically $ TM.insert userId (mkUserServers srvs) (userServers c)

checkUserServers :: Text -> NonEmpty (ServerCfg p) -> IO ()
checkUserServers name srvs =
  unless (any (\ServerCfg {enabled} -> enabled) srvs) $
    logWarn (name <> ": all passed servers are disabled, using all servers.")

registerNtfToken' :: AgentClient -> DeviceToken -> NotificationsMode -> AM NtfTknStatus
registerNtfToken' c suppliedDeviceToken suppliedNtfMode =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId, ntfTknStatus, ntfTknAction, ntfMode = savedNtfMode} -> do
      status <- case (ntfTokenId, ntfTknAction) of
        (Nothing, Just NTARegister) -> do
          when (savedDeviceToken /= suppliedDeviceToken) $ withStore' c $ \db -> updateDeviceToken db tkn suppliedDeviceToken
          registerToken tkn $> NTRegistered
        -- possible improvement: add minimal time before repeat registration
        (Just tknId, Nothing)
          | savedDeviceToken == suppliedDeviceToken ->
              when (ntfTknStatus == NTRegistered) (registerToken tkn) $> NTRegistered
          | otherwise -> replaceToken tknId
        (Just tknId, Just (NTAVerify code))
          | savedDeviceToken == suppliedDeviceToken ->
              t tkn (NTActive, Just NTACheck) $ agentNtfVerifyToken c tknId tkn code
          | otherwise -> replaceToken tknId
        (Just tknId, Just NTACheck)
          | savedDeviceToken == suppliedDeviceToken -> do
              ns <- asks ntfSupervisor
              atomically $ nsUpdateToken ns tkn {ntfMode = suppliedNtfMode}
              when (ntfTknStatus == NTActive) $ do
                cron <- asks $ ntfCron . config
                agentNtfEnableCron c tknId tkn cron
                when (suppliedNtfMode == NMInstant) $ initializeNtfSubs c
                when (suppliedNtfMode == NMPeriodic && savedNtfMode == NMInstant) $ deleteNtfSubs c NSCSmpDelete
              -- possible improvement: get updated token status from the server, or maybe TCRON could return the current status
              pure ntfTknStatus
          | otherwise -> replaceToken tknId
        -- deprecated
        (Just _tknId, Just NTADelete) -> deleteToken c tkn $> NTExpired
        _ -> pure ntfTknStatus
      withStore' c $ \db -> updateNtfMode db tkn suppliedNtfMode
      pure status
      where
        replaceToken :: NtfTokenId -> AM NtfTknStatus
        replaceToken tknId = do
          ns <- asks ntfSupervisor
          tryReplace ns `catchAgentError` \e ->
            if temporaryOrHostError e
              then throwE e
              else do
                withStore' c $ \db -> removeNtfToken db tkn
                atomically $ nsRemoveNtfToken ns
                createToken
          where
            tryReplace ns = do
              agentNtfReplaceToken c tknId tkn suppliedDeviceToken
              withStore' c $ \db -> updateDeviceToken db tkn suppliedDeviceToken
              atomically $ nsUpdateToken ns tkn {deviceToken = suppliedDeviceToken, ntfTknStatus = NTRegistered, ntfMode = suppliedNtfMode}
              pure NTRegistered
    _ -> createToken
  where
    t tkn = withToken c tkn Nothing
    createToken :: AM NtfTknStatus
    createToken =
      lift (getNtfServer c) >>= \case
        Just ntfServer ->
          asks (rcvAuthAlg . config) >>= \case
            C.AuthAlg a -> do
              g <- asks random
              tknKeys <- atomically $ C.generateAuthKeyPair a g
              dhKeys <- atomically $ C.generateKeyPair g
              let tkn = newNtfToken suppliedDeviceToken ntfServer tknKeys dhKeys suppliedNtfMode
              withStore' c (`createNtfToken` tkn)
              registerToken tkn
              pure NTRegistered
        _ -> throwE $ CMD PROHIBITED "createToken"
    registerToken :: NtfToken -> AM ()
    registerToken tkn@NtfToken {ntfPubKey, ntfDhKeys = (pubDhKey, privDhKey)} = do
      (tknId, srvPubDhKey) <- agentNtfRegisterToken c tkn ntfPubKey pubDhKey
      let dhSecret = C.dh' srvPubDhKey privDhKey
      withStore' c $ \db -> updateNtfTokenRegistration db tkn tknId dhSecret
      ns <- asks ntfSupervisor
      atomically $ nsUpdateToken ns tkn {deviceToken = suppliedDeviceToken, ntfTknStatus = NTRegistered, ntfMode = suppliedNtfMode}

verifyNtfToken' :: AgentClient -> DeviceToken -> C.CbNonce -> ByteString -> AM ()
verifyNtfToken' c deviceToken nonce code =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId = Just tknId, ntfDhSecret = Just dhSecret, ntfMode} -> do
      when (deviceToken /= savedDeviceToken) . throwE $ CMD PROHIBITED "verifyNtfToken: different token"
      code' <- liftEither . bimap cryptoError NtfRegCode $ C.cbDecrypt dhSecret nonce code
      toStatus <-
        withToken c tkn (Just (NTConfirmed, NTAVerify code')) (NTActive, Just NTACheck) $
          agentNtfVerifyToken c tknId tkn code'
      when (toStatus == NTActive) $ do
        cron <- asks $ ntfCron . config
        agentNtfEnableCron c tknId tkn cron
        when (ntfMode == NMInstant) $ initializeNtfSubs c
    _ -> throwE $ CMD PROHIBITED "verifyNtfToken: no token"

checkNtfToken' :: AgentClient -> DeviceToken -> AM NtfTknStatus
checkNtfToken' c deviceToken =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId = Just tknId} -> do
      when (deviceToken /= savedDeviceToken) . throwE $ CMD PROHIBITED "checkNtfToken: different token"
      agentNtfCheckToken c tknId tkn
    _ -> throwE $ CMD PROHIBITED "checkNtfToken: no token"

deleteNtfToken' :: AgentClient -> DeviceToken -> AM ()
deleteNtfToken' c deviceToken =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken} -> do
      when (deviceToken /= savedDeviceToken) $ logWarn "deleteNtfToken: different token"
      deleteToken c tkn
      deleteNtfSubs c NSCSmpDelete
    _ -> throwE $ CMD PROHIBITED "deleteNtfToken: no token"

getNtfToken' :: AgentClient -> AM (DeviceToken, NtfTknStatus, NotificationsMode, NtfServer)
getNtfToken' c =
  withStore' c getSavedNtfToken >>= \case
    Just NtfToken {deviceToken, ntfTknStatus, ntfMode, ntfServer} -> pure (deviceToken, ntfTknStatus, ntfMode, ntfServer)
    _ -> throwE $ CMD PROHIBITED "getNtfToken"

getNtfTokenData' :: AgentClient -> AM NtfToken
getNtfTokenData' c =
  withStore' c getSavedNtfToken >>= \case
    Just tkn -> pure tkn
    _ -> throwE $ CMD PROHIBITED "getNtfTokenData"

-- | Set connection notifications, in Reader monad
toggleConnectionNtfs' :: AgentClient -> ConnId -> Bool -> AM ()
toggleConnectionNtfs' c connId enable = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection cData _ _ -> toggle cData
    RcvConnection cData _ -> toggle cData
    ContactConnection cData _ -> toggle cData
    _ -> throwE $ CONN SIMPLEX
  where
    toggle :: ConnData -> AM ()
    toggle cData
      | enableNtfs cData == enable = pure ()
      | otherwise = do
          withStore' c $ \db -> setConnectionNtfs db connId enable
          ns <- asks ntfSupervisor
          let cmd = if enable then NSCCreate else NSCSmpDelete
          atomically $ sendNtfSubCommand ns (cmd, [connId])

withToken :: AgentClient -> NtfToken -> Maybe (NtfTknStatus, NtfTknAction) -> (NtfTknStatus, Maybe NtfTknAction) -> AM a -> AM NtfTknStatus
withToken c tkn@NtfToken {deviceToken, ntfMode} from_ (toStatus, toAction_) f = do
  ns <- asks ntfSupervisor
  forM_ from_ $ \(status, action) -> do
    withStore' c $ \db -> updateNtfToken db tkn status (Just action)
    atomically $ nsUpdateToken ns tkn {ntfTknStatus = status, ntfTknAction = Just action}
  tryError f >>= \case
    Right _ -> do
      withStore' c $ \db -> updateNtfToken db tkn toStatus toAction_
      let updatedToken = tkn {ntfTknStatus = toStatus, ntfTknAction = toAction_}
      atomically $ nsUpdateToken ns updatedToken
      pure toStatus
    Left e@(NTF _ AUTH) -> do
      withStore' c $ \db -> removeNtfToken db tkn
      atomically $ nsRemoveNtfToken ns
      void $ registerNtfToken' c deviceToken ntfMode
      throwE e
    Left e -> throwE e

initializeNtfSubs :: AgentClient -> AM ()
initializeNtfSubs c = sendNtfConnCommands c NSCCreate
{-# INLINE initializeNtfSubs #-}

deleteNtfSubs :: AgentClient -> NtfSupervisorCommand -> AM ()
deleteNtfSubs c deleteCmd = do
  ns <- asks ntfSupervisor
  void . atomically . flushTBQueue $ ntfSubQ ns
  sendNtfConnCommands c deleteCmd

sendNtfConnCommands :: AgentClient -> NtfSupervisorCommand -> AM ()
sendNtfConnCommands c cmd = do
  ns <- asks ntfSupervisor
  connIds <- liftIO $ S.toList <$> getSubscriptions c
  rs <- lift $ withStoreBatch' c (\db -> map (getConnData db) connIds)
  let (connIds', cErrs) = enabledNtfConns (zip connIds rs)
  forM_ (L.nonEmpty connIds') $ \connIds'' ->
    atomically $ writeTBQueue (ntfSubQ ns) (cmd, connIds'')
  unless (null cErrs) $ atomically $ writeTBQueue (subQ c) ("", "", AEvt SAENone $ ERRS cErrs)
  where
    enabledNtfConns :: [(ConnId, Either AgentErrorType (Maybe (ConnData, ConnectionMode)))] -> ([ConnId], [(ConnId, AgentErrorType)])
    enabledNtfConns = foldr addEnabledConn ([], [])
      where
        addEnabledConn ::
          (ConnId, Either AgentErrorType (Maybe (ConnData, ConnectionMode))) ->
          ([ConnId], [(ConnId, AgentErrorType)]) ->
          ([ConnId], [(ConnId, AgentErrorType)])
        addEnabledConn cData_ (cIds, errs) = case cData_ of
          (_, Right (Just (ConnData {connId, enableNtfs}, _))) -> if enableNtfs then (connId : cIds, errs) else (cIds, errs)
          (connId, Right Nothing) -> (cIds, (connId, INTERNAL "no connection data") : errs)
          (connId, Left e) -> (cIds, (connId, e) : errs)

setNtfServers :: AgentClient -> [NtfServer] -> IO ()
setNtfServers c = atomically . writeTVar (ntfServers c)
{-# INLINE setNtfServers #-}

resetAgentServersStats' :: AgentClient -> AM ()
resetAgentServersStats' c@AgentClient {smpServersStats, xftpServersStats, srvStatsStartedAt} = do
  startedAt <- liftIO getCurrentTime
  atomically $ writeTVar srvStatsStartedAt startedAt
  atomically $ TM.clear smpServersStats
  atomically $ TM.clear xftpServersStats
  withStore' c (`resetServersStats` startedAt)

-- | Activate operations
foregroundAgent :: AgentClient -> IO ()
foregroundAgent c = do
  atomically $ writeTVar (agentState c) ASForeground
  mapM_ activate $ reverse agentOperations
  where
    activate opSel = atomically $ modifyTVar' (opSel c) $ \s -> s {opSuspended = False}

-- | Suspend operations with max delay to deliver pending messages
suspendAgent :: AgentClient -> Int -> IO ()
suspendAgent c 0 = do
  atomically $ writeTVar (agentState c) ASSuspended
  mapM_ suspend agentOperations
  where
    suspend opSel = atomically $ modifyTVar' (opSel c) $ \s -> s {opSuspended = True}
suspendAgent c@AgentClient {agentState = as} maxDelay = do
  state <-
    atomically $ do
      writeTVar as ASSuspending
      suspendOperation c AONtfNetwork $ pure ()
      suspendOperation c AORcvNetwork $
        suspendOperation c AOMsgDelivery $
          suspendSendingAndDatabase c
      readTVar as
  when (state == ASSuspending) . void . forkIO $ do
    threadDelay maxDelay
    -- liftIO $ putStrLn "suspendAgent after timeout"
    atomically . whenSuspending c $ do
      -- unsafeIOToSTM $ putStrLn $ "in timeout: suspendSendingAndDatabase"
      suspendSendingAndDatabase c

execAgentStoreSQL :: AgentClient -> Text -> AE [Text]
execAgentStoreSQL c sql = withAgentEnv c $ withStore' c (`execSQL` sql)

getAgentMigrations :: AgentClient -> AE [UpMigration]
getAgentMigrations c = withAgentEnv c $ map upMigration <$> withStore' c Migrations.getCurrent

debugAgentLocks :: AgentClient -> IO AgentLocks
debugAgentLocks AgentClient {connLocks = cs, invLocks = is, deleteLock = d} = do
  connLocks <- getLocks cs
  invLocks <- getLocks is
  delLock <- atomically $ tryReadTMVar d
  pure AgentLocks {connLocks, invLocks, delLock}
  where
    getLocks ls = atomically $ M.mapKeys (B.unpack . strEncode) . M.mapMaybe id <$> (mapM tryReadTMVar =<< readTVar ls)

getSMPServer :: AgentClient -> UserId -> AM SMPServerWithAuth
getSMPServer c userId = getNextSMPServer c userId []
{-# INLINE getSMPServer #-}

getNextSMPServer :: AgentClient -> UserId -> [SMPServer] -> AM SMPServerWithAuth
getNextSMPServer c userId = getNextServer c userId storageSrvs
{-# INLINE getNextSMPServer #-}

subscriber :: AgentClient -> AM' ()
subscriber c@AgentClient {msgQ} = forever $ do
  t <- atomically $ readTBQueue msgQ
  agentOperationBracket c AORcvNetwork waitUntilActive $
    processSMPTransmissions c t

cleanupManager :: AgentClient -> AM' ()
cleanupManager c@AgentClient {subQ} = do
  delay <- asks (initialCleanupDelay . config)
  liftIO $ threadDelay' delay
  int <- asks (cleanupInterval . config)
  ttl <- asks $ storedMsgDataTTL . config
  forever $ waitActive $ do
    run ERR deleteConns
    run ERR $ withStore' c (`deleteRcvMsgHashesExpired` ttl)
    run ERR $ withStore' c (`deleteSndMsgsExpired` ttl)
    run ERR $ withStore' c (`deleteRatchetKeyHashesExpired` ttl)
    run ERR $ withStore' c (`deleteExpiredNtfTokensToDelete` ttl)
    run RFERR deleteRcvFilesExpired
    run RFERR deleteRcvFilesDeleted
    run RFERR deleteRcvFilesTmpPaths
    run SFERR deleteSndFilesExpired
    run SFERR deleteSndFilesDeleted
    run SFERR deleteSndFilesPrefixPaths
    run SFERR deleteExpiredReplicasForDeletion
    liftIO $ threadDelay' int
  where
    run :: forall e. AEntityI e => (AgentErrorType -> AEvent e) -> AM () -> AM' ()
    run err a = do
      waitActive . runExceptT $ a `catchAgentError` (notify "" . err)
      step <- asks $ cleanupStepInterval . config
      liftIO $ threadDelay step
    -- we are catching it to avoid CRITICAL errors in tests when this is the only remaining handle to active
    waitActive :: ReaderT Env IO a -> AM' ()
    waitActive a = liftIO (E.tryAny $ waitUntilActive c) >>= either (\_ -> pure ()) (\_ -> void a)
    deleteConns =
      withLock (deleteLock c) "cleanupManager" $ do
        void $ withStore' c getDeletedConnIds >>= deleteDeletedConns c
        void $ withStore' c getDeletedWaitingDeliveryConnIds >>= deleteDeletedWaitingDeliveryConns c
        withStore' c deleteUsersWithoutConns >>= mapM_ (notify "" . DEL_USER)
    deleteRcvFilesExpired = do
      rcvFilesTTL <- asks $ rcvFilesTTL . config
      rcvExpired <- withStore' c (`getRcvFilesExpired` rcvFilesTTL)
      forM_ rcvExpired $ \(dbId, entId, p) -> flip catchAgentError (notify entId . RFERR) $ do
        lift $ removePath =<< toFSFilePath p
        withStore' c (`deleteRcvFile'` dbId)
    deleteRcvFilesDeleted = do
      rcvDeleted <- withStore' c getCleanupRcvFilesDeleted
      forM_ rcvDeleted $ \(dbId, entId, p) -> flip catchAgentError (notify entId . RFERR) $ do
        lift $ removePath =<< toFSFilePath p
        withStore' c (`deleteRcvFile'` dbId)
    deleteRcvFilesTmpPaths = do
      rcvTmpPaths <- withStore' c getCleanupRcvFilesTmpPaths
      forM_ rcvTmpPaths $ \(dbId, entId, p) -> flip catchAgentError (notify entId . RFERR) $ do
        lift $ removePath =<< toFSFilePath p
        withStore' c (`updateRcvFileNoTmpPath` dbId)
    deleteSndFilesExpired = do
      sndFilesTTL <- asks $ sndFilesTTL . config
      sndExpired <- withStore' c (`getSndFilesExpired` sndFilesTTL)
      forM_ sndExpired $ \(dbId, entId, p) -> flip catchAgentError (notify entId . SFERR) $ do
        lift . forM_ p $ removePath <=< toFSFilePath
        withStore' c (`deleteSndFile'` dbId)
    deleteSndFilesDeleted = do
      sndDeleted <- withStore' c getCleanupSndFilesDeleted
      forM_ sndDeleted $ \(dbId, entId, p) -> flip catchAgentError (notify entId . SFERR) $ do
        lift . forM_ p $ removePath <=< toFSFilePath
        withStore' c (`deleteSndFile'` dbId)
    deleteSndFilesPrefixPaths = do
      sndPrefixPaths <- withStore' c getCleanupSndFilesPrefixPaths
      forM_ sndPrefixPaths $ \(dbId, entId, p) -> flip catchAgentError (notify entId . SFERR) $ do
        lift $ removePath =<< toFSFilePath p
        withStore' c (`updateSndFileNoPrefixPath` dbId)
    deleteExpiredReplicasForDeletion = do
      rcvFilesTTL <- asks $ rcvFilesTTL . config
      withStore' c (`deleteDeletedSndChunkReplicasExpired` rcvFilesTTL)
    notify :: forall e. AEntityI e => AEntityId -> AEvent e -> AM ()
    notify entId cmd = atomically $ writeTBQueue subQ ("", entId, AEvt (sAEntity @e) cmd)

data ACKd = ACKd | ACKPending

-- | Make sure to ACK or throw in each message processing branch
-- It cannot be finally, as sometimes it needs to be ACK+DEL,
-- and sometimes ACK has to be sent from the consumer.
processSMPTransmissions :: AgentClient -> ServerTransmissionBatch SMPVersion ErrorType BrokerMsg -> AM' ()
processSMPTransmissions c@AgentClient {subQ} (tSess@(userId, srv, _), _v, sessId, ts) = do
  upConnIds <- newTVarIO []
  forM_ ts $ \(entId, t) -> case t of
    STEvent msgOrErr ->
      withRcvConn entId $ \rq@RcvQueue {connId} conn -> case msgOrErr of
        Right msg -> runProcessSMP rq conn (toConnData conn) msg
        Left e -> lift $ notifyErr connId e
    STResponse (Cmd SRecipient cmd) respOrErr ->
      withRcvConn entId $ \rq conn -> case cmd of
        SMP.SUB -> case respOrErr of
          Right SMP.OK -> processSubOk rq upConnIds
          Right msg@SMP.MSG {} -> do
            processSubOk rq upConnIds -- the connection is UP even when processing this particular message fails
            runProcessSMP rq conn (toConnData conn) msg
          Right r -> processSubErr rq $ unexpectedResponse r
          Left e -> unless (temporaryClientError e) $ processSubErr rq e -- timeout/network was already reported
        SMP.ACK _ -> case respOrErr of
          Right msg@SMP.MSG {} -> runProcessSMP rq conn (toConnData conn) msg
          _ -> pure () -- TODO process OK response to ACK
        _ -> pure () -- TODO process expired response to DEL
    STResponse {} -> pure () -- TODO process expired responses to sent messages
    STUnexpectedError e -> do
      logServer "<--" c srv entId $ "error: " <> bshow e
      notifyErr "" e
  connIds <- readTVarIO upConnIds
  unless (null connIds) $ do
    notify' "" $ UP srv connIds
    atomically $ incSMPServerStat' c userId srv connSubscribed $ length connIds
  where
    withRcvConn :: SMP.RecipientId -> (forall c. RcvQueue -> Connection c -> AM ()) -> AM' ()
    withRcvConn rId a = do
      tryAgentError' (withStore c $ \db -> getRcvConn db srv rId) >>= \case
        Left e -> notify' "" (ERR e)
        Right (rq@RcvQueue {connId}, SomeConn _ conn) ->
          tryAgentError' (a rq conn) >>= \case
            Left e -> notify' connId (ERR e)
            Right () -> pure ()
    processSubOk :: RcvQueue -> TVar [ConnId] -> AM ()
    processSubOk rq@RcvQueue {connId} upConnIds =
      atomically . whenM (isPendingSub connId) $ do
        addSubscription c sessId rq
        modifyTVar' upConnIds (connId :)
    processSubErr :: RcvQueue -> SMPClientError -> AM ()
    processSubErr rq@RcvQueue {connId} e = do
      atomically . whenM (isPendingSub connId) $
        failSubscription c rq e >> incSMPServerStat c userId srv connSubErrs
      lift $ notifyErr connId e
    isPendingSub connId = do
      pending <- (&&) <$> hasPendingSubscription c connId <*> activeClientSession c tSess sessId
      unless pending $ incSMPServerStat c userId srv connSubIgnored
      pure pending
    notify' :: forall e m. (AEntityI e, MonadIO m) => ConnId -> AEvent e -> m ()
    notify' connId msg = atomically $ writeTBQueue subQ ("", connId, AEvt (sAEntity @e) msg)
    notifyErr :: ConnId -> SMPClientError -> AM' ()
    notifyErr connId = notify' connId . ERR . protocolClientError SMP (B.unpack $ strEncode srv)
    runProcessSMP :: RcvQueue -> Connection c -> ConnData -> BrokerMsg -> AM ()
    runProcessSMP rq conn cData msg = do
      pending <- newTVarIO []
      processSMP rq conn cData msg pending
      mapM_ (atomically . writeTBQueue subQ) . reverse =<< readTVarIO pending
    processSMP :: forall c. RcvQueue -> Connection c -> ConnData -> BrokerMsg -> TVar [ATransmission] -> AM ()
    processSMP
      rq@RcvQueue {rcvId = rId, sndSecure, e2ePrivKey, e2eDhSecret, status}
      conn
      cData@ConnData {connId, connAgentVersion, ratchetSyncState = rss}
      smpMsg
      pendingMsgs =
        withConnLock c connId "processSMP" $ case smpMsg of
          SMP.MSG msg@SMP.RcvMessage {msgId = srvMsgId} -> do
            atomically $ incSMPServerStat c userId srv recvMsgs
            void . handleNotifyAck $ do
              msg' <- decryptSMPMessage rq msg
              handleNotifyAck $ case msg' of
                SMP.ClientRcvMsgBody {msgTs = srvTs, msgFlags, msgBody} -> processClientMsg srvTs msgFlags msgBody
                SMP.ClientRcvMsgQuota {} -> queueDrained >> ack
            where
              queueDrained = case conn of
                DuplexConnection _ _ sqs -> void $ enqueueMessages c cData sqs SMP.noMsgFlags $ A_QCONT (sndAddress rq)
                _ -> pure ()
              processClientMsg srvTs msgFlags msgBody = do
                clientMsg@SMP.ClientMsgEnvelope {cmHeader = SMP.PubHeader phVer e2ePubKey_} <-
                  parseMessage msgBody
                clientVRange <- asks $ smpClientVRange . config
                unless (phVer `isCompatible` clientVRange) . throwE $ AGENT A_VERSION
                case (e2eDhSecret, e2ePubKey_) of
                  (Nothing, Just e2ePubKey) -> do
                    let e2eDh = C.dh' e2ePubKey e2ePrivKey
                    decryptClientMessage e2eDh clientMsg >>= \case
                      (SMP.PHConfirmation senderKey, AgentConfirmation {e2eEncryption_, encConnInfo, agentVersion}) ->
                        smpConfirmation srvMsgId conn (Just senderKey) e2ePubKey e2eEncryption_ encConnInfo phVer agentVersion >> ack
                      (SMP.PHEmpty, AgentConfirmation {e2eEncryption_, encConnInfo, agentVersion})
                        | sndSecure -> smpConfirmation srvMsgId conn Nothing e2ePubKey e2eEncryption_ encConnInfo phVer agentVersion >> ack
                        | otherwise -> prohibited "handshake: missing sender key" >> ack
                      (SMP.PHEmpty, AgentInvitation {connReq, connInfo}) ->
                        smpInvitation srvMsgId conn connReq connInfo >> ack
                      _ -> prohibited "handshake: incorrect state" >> ack
                  (Just e2eDh, Nothing) -> do
                    decryptClientMessage e2eDh clientMsg >>= \case
                      (SMP.PHEmpty, AgentRatchetKey {agentVersion, e2eEncryption}) -> do
                        conn' <- updateConnVersion conn cData agentVersion
                        qDuplex conn' "AgentRatchetKey" $ \a -> newRatchetKey e2eEncryption a >> ack
                      (SMP.PHEmpty, AgentMsgEnvelope {agentVersion, encAgentMessage}) -> do
                        conn' <- updateConnVersion conn cData agentVersion
                        -- primary queue is set as Active in helloMsg, below is to set additional queues Active
                        let RcvQueue {primary, dbReplaceQueueId} = rq
                        unless (status == Active) . withStore' c $ \db -> setRcvQueueStatus db rq Active
                        case (conn', dbReplaceQueueId) of
                          (DuplexConnection _ rqs _, Just replacedId) -> do
                            when primary . withStore' c $ \db -> setRcvQueuePrimary db connId rq
                            case find ((replacedId ==) . dbQId) rqs of
                              Just rq'@RcvQueue {server, rcvId} -> do
                                checkRQSwchStatus rq' RSSendingQUSE
                                void $ withStore' c $ \db -> setRcvSwitchStatus db rq' $ Just RSReceivedMessage
                                enqueueCommand c "" connId (Just server) $ AInternalCommand $ ICQDelete rcvId
                              _ -> notify . ERR . AGENT $ A_QUEUE "replaced RcvQueue not found in connection"
                          _ -> pure ()
                        let encryptedMsgHash = C.sha256Hash encAgentMessage
                        g <- asks random
                        tryAgentError (agentClientMsg g encryptedMsgHash) >>= \case
                          Right (Just (msgId, msgMeta, aMessage, rcPrev)) -> do
                            conn'' <- resetRatchetSync
                            case aMessage of
                              HELLO -> helloMsg srvMsgId msgMeta conn'' >> ackDel msgId
                              -- note that there is no ACK sent for A_MSG, it is sent with agent's user ACK command
                              A_MSG body -> do
                                logServer "<--" c srv rId $ "MSG <MSG>:" <> logSecret' srvMsgId
                                notify $ MSG msgMeta msgFlags body
                                pure ACKPending
                              A_RCVD rcpts -> qDuplex conn'' "RCVD" $ messagesRcvd rcpts msgMeta
                              A_QCONT addr -> qDuplexAckDel conn'' "QCONT" $ continueSending srvMsgId addr
                              QADD qs -> qDuplexAckDel conn'' "QADD" $ qAddMsg srvMsgId qs
                              QKEY qs -> qDuplexAckDel conn'' "QKEY" $ qKeyMsg srvMsgId qs
                              QUSE qs -> qDuplexAckDel conn'' "QUSE" $ qUseMsg srvMsgId qs
                              -- no action needed for QTEST
                              -- any message in the new queue will mark it active and trigger deletion of the old queue
                              QTEST _ -> logServer "<--" c srv rId ("MSG <QTEST>:" <> logSecret' srvMsgId) >> ackDel msgId
                              EREADY _ -> qDuplexAckDel conn'' "EREADY" $ ereadyMsg rcPrev
                            where
                              qDuplexAckDel :: Connection c -> String -> (Connection 'CDuplex -> AM ()) -> AM ACKd
                              qDuplexAckDel conn'' name a = qDuplex conn'' name a >> ackDel msgId
                              resetRatchetSync :: AM (Connection c)
                              resetRatchetSync
                                | rss `notElem` ([RSOk, RSStarted] :: [RatchetSyncState]) = do
                                    let cData'' = (toConnData conn') {ratchetSyncState = RSOk} :: ConnData
                                        conn'' = updateConnection cData'' conn'
                                    notify . RSYNC RSOk Nothing $ connectionStats conn''
                                    withStore' c $ \db -> setConnRatchetSync db connId RSOk
                                    pure conn''
                                | otherwise = pure conn'
                          Right Nothing -> prohibited "msg: bad agent msg" >> ack
                          Left e@(AGENT A_DUPLICATE) -> do
                            atomically $ incSMPServerStat c userId srv recvDuplicates
                            withStore' c (\db -> getLastMsg db connId srvMsgId) >>= \case
                              Just RcvMsg {internalId, msgMeta, msgBody = agentMsgBody, userAck}
                                | userAck -> ackDel internalId
                                | otherwise ->
                                    liftEither (parse smpP (AGENT A_MESSAGE) agentMsgBody) >>= \case
                                      AgentMessage _ (A_MSG body) -> do
                                        logServer "<--" c srv rId $ "MSG <MSG>:" <> logSecret' srvMsgId
                                        notify $ MSG msgMeta msgFlags body
                                        pure ACKPending
                                      _ -> ack
                              _ -> checkDuplicateHash e encryptedMsgHash >> ack
                          Left (AGENT (A_CRYPTO e)) -> do
                            atomically $ incSMPServerStat c userId srv recvCryptoErrs
                            exists <- withStore' c $ \db -> checkRcvMsgHashExists db connId encryptedMsgHash
                            unless exists notifySync
                            ack
                            where
                              notifySync :: AM ()
                              notifySync = qDuplex conn' "AGENT A_CRYPTO error" $ \connDuplex -> do
                                let rss' = cryptoErrToSyncState e
                                when (rss `elem` ([RSOk, RSAllowed, RSRequired] :: [RatchetSyncState])) $ do
                                  let cData'' = (toConnData conn') {ratchetSyncState = rss'} :: ConnData
                                      conn'' = updateConnection cData'' connDuplex
                                  notify . RSYNC rss' (Just e) $ connectionStats conn''
                                  withStore' c $ \db -> setConnRatchetSync db connId rss'
                          Left e -> do
                            atomically $ incSMPServerStat c userId srv recvErrs
                            checkDuplicateHash e encryptedMsgHash >> ack
                        where
                          checkDuplicateHash :: AgentErrorType -> ByteString -> AM ()
                          checkDuplicateHash e encryptedMsgHash =
                            unlessM (withStore' c $ \db -> checkRcvMsgHashExists db connId encryptedMsgHash) $
                              throwE e
                          agentClientMsg :: TVar ChaChaDRG -> ByteString -> AM (Maybe (InternalId, MsgMeta, AMessage, CR.RatchetX448))
                          agentClientMsg g encryptedMsgHash = withStore c $ \db -> runExceptT $ do
                            rc <- ExceptT $ getRatchet db connId -- ratchet state pre-decryption - required for processing EREADY
                            (agentMsgBody, pqEncryption) <- agentRatchetDecrypt' g db connId rc encAgentMessage
                            liftEither (parse smpP (SEAgentError $ AGENT A_MESSAGE) agentMsgBody) >>= \case
                              agentMsg@(AgentMessage APrivHeader {sndMsgId, prevMsgHash} aMessage) -> do
                                let msgType = agentMessageType agentMsg
                                    internalHash = C.sha256Hash agentMsgBody
                                internalTs <- liftIO getCurrentTime
                                (internalId, internalRcvId, prevExtSndId, prevRcvMsgHash) <- liftIO $ updateRcvIds db connId
                                let integrity = checkMsgIntegrity prevExtSndId sndMsgId prevRcvMsgHash prevMsgHash
                                    recipient = (unId internalId, internalTs)
                                    broker = (srvMsgId, systemToUTCTime srvTs)
                                    msgMeta = MsgMeta {integrity, recipient, broker, sndMsgId, pqEncryption}
                                    rcvMsg = RcvMsgData {msgMeta, msgType, msgFlags, msgBody = agentMsgBody, internalRcvId, internalHash, externalPrevSndHash = prevMsgHash, encryptedMsgHash}
                                liftIO $ createRcvMsg db connId rq rcvMsg
                                pure $ Just (internalId, msgMeta, aMessage, rc)
                              _ -> pure Nothing
                      _ -> prohibited "msg: bad client msg" >> ack
                  (Just e2eDh, Just _) ->
                    decryptClientMessage e2eDh clientMsg >>= \case
                      -- this is a repeated confirmation delivery because ack failed to be sent
                      (_, AgentConfirmation {}) -> ack
                      _ -> prohibited "msg: public header" >> ack
                  (Nothing, Nothing) -> prohibited "msg: no keys" >> ack
              updateConnVersion :: Connection c -> ConnData -> VersionSMPA -> AM (Connection c)
              updateConnVersion conn' cData' msgAgentVersion = do
                aVRange <- asks $ smpAgentVRange . config
                let msgAVRange = fromMaybe (versionToRange msgAgentVersion) $ safeVersionRange (minVersion aVRange) msgAgentVersion
                case msgAVRange `compatibleVersion` aVRange of
                  Just (Compatible av)
                    | av > connAgentVersion -> do
                        withStore' c $ \db -> setConnAgentVersion db connId av
                        let cData'' = cData' {connAgentVersion = av} :: ConnData
                        pure $ updateConnection cData'' conn'
                    | otherwise -> pure conn'
                  Nothing -> pure conn'
              ack :: AM ACKd
              ack = enqueueCmd (ICAck rId srvMsgId) $> ACKd
              ackDel :: InternalId -> AM ACKd
              ackDel aId = enqueueCmd (ICAckDel rId srvMsgId aId) $> ACKd
              handleNotifyAck :: AM ACKd -> AM ACKd
              handleNotifyAck m = m `catchAgentError` \e -> notify (ERR e) >> ack
          SMP.END ->
            atomically (ifM (activeClientSession c tSess sessId) (removeSubscription c connId $> True) (pure False))
              >>= notifyEnd
            where
              notifyEnd removed
                | removed = notify END >> logServer "<--" c srv rId "END"
                | otherwise = logServer "<--" c srv rId "END from disconnected client - ignored"
          -- Possibly, we need to add some flag to connection that it was deleted
          SMP.DELD -> atomically (removeSubscription c connId) >> notify DELD
          SMP.ERR e -> notify $ ERR $ SMP (B.unpack $ strEncode srv) e
          r -> unexpected r
        where
          notify :: forall e m. (AEntityI e, MonadIO m) => AEvent e -> m ()
          notify msg =
            let t = ("", connId, AEvt (sAEntity @e) msg)
             in atomically $ ifM (isFullTBQueue subQ) (modifyTVar' pendingMsgs (t :)) (writeTBQueue subQ t)

          prohibited :: Text -> AM ()
          prohibited s = do
            logError $ "prohibited: " <> s
            notify . ERR . AGENT $ A_PROHIBITED $ T.unpack s

          enqueueCmd :: InternalCommand -> AM ()
          enqueueCmd = enqueueCommand c "" connId (Just srv) . AInternalCommand

          unexpected :: BrokerMsg -> AM ()
          unexpected r = do
            logServer "<--" c srv rId $ "unexpected: " <> bshow r
            -- TODO add extended information about transmission type once UNEXPECTED has string
            notify . ERR $ BROKER (B.unpack $ strEncode srv) $ UNEXPECTED (take 32 $ show r)

          decryptClientMessage :: C.DhSecretX25519 -> SMP.ClientMsgEnvelope -> AM (SMP.PrivHeader, AgentMsgEnvelope)
          decryptClientMessage e2eDh SMP.ClientMsgEnvelope {cmNonce, cmEncBody} = do
            clientMsg <- liftEither $ agentCbDecrypt e2eDh cmNonce cmEncBody
            SMP.ClientMessage privHeader clientBody <- parseMessage clientMsg
            agentEnvelope <- parseMessage clientBody
            -- Version check is removed here, because when connecting via v1 contact address the agent still sends v2 message,
            -- to allow duplexHandshake mode, in case the receiving agent was updated to v2 after the address was created.
            -- aVRange <- asks $ smpAgentVRange . config
            -- if agentVersion agentEnvelope `isCompatible` aVRange
            --   then pure (privHeader, agentEnvelope)
            --   else throwE $ AGENT A_VERSION
            pure (privHeader, agentEnvelope)

          parseMessage :: Encoding a => ByteString -> AM a
          parseMessage = liftEither . parse smpP (AGENT A_MESSAGE)

          smpConfirmation :: SMP.MsgId -> Connection c -> Maybe C.APublicAuthKey -> C.PublicKeyX25519 -> Maybe (CR.SndE2ERatchetParams 'C.X448) -> ByteString -> VersionSMPC -> VersionSMPA -> AM ()
          smpConfirmation srvMsgId conn' senderKey e2ePubKey e2eEncryption encConnInfo smpClientVersion agentVersion = do
            logServer "<--" c srv rId $ "MSG <CONF>:" <> logSecret' srvMsgId
            AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
            let ConnData {pqSupport} = toConnData conn'
            unless
              (agentVersion `isCompatible` smpAgentVRange && smpClientVersion `isCompatible` smpClientVRange)
              (throwE $ AGENT A_VERSION)
            case status of
              New -> case (conn', e2eEncryption) of
                -- party initiating connection
                (RcvConnection _ _, Just (CR.AE2ERatchetParams _ e2eSndParams@(CR.E2ERatchetParams e2eVersion _ _ _))) -> do
                  unless (e2eVersion `isCompatible` e2eEncryptVRange) (throwE $ AGENT A_VERSION)
                  (pk1, rcDHRs, pKem) <- withStore c (`getRatchetX3dhKeys` connId)
                  rcParams <- liftError cryptoError $ CR.pqX3dhRcv pk1 rcDHRs pKem e2eSndParams
                  let rcVs = CR.RatchetVersions {current = e2eVersion, maxSupported = maxVersion e2eEncryptVRange}
                      pqSupport' = pqSupport `CR.pqSupportAnd` versionPQSupport_ agentVersion (Just e2eVersion)
                      rc = CR.initRcvRatchet rcVs rcDHRs rcParams pqSupport'
                  g <- asks random
                  (agentMsgBody_, rc', skipped) <- liftError cryptoError $ CR.rcDecrypt g rc M.empty encConnInfo
                  case (agentMsgBody_, skipped) of
                    (Right agentMsgBody, CR.SMDNoChange) ->
                      parseMessage agentMsgBody >>= \case
                        AgentConnInfoReply smpQueues connInfo -> do
                          processConf connInfo SMPConfirmation {senderKey, e2ePubKey, connInfo, smpReplyQueues = L.toList smpQueues, smpClientVersion}
                          withStore' c $ \db -> updateRcvMsgHash db connId 1 (InternalRcvId 0) (C.sha256Hash agentMsgBody)
                        _ -> prohibited "conf: not AgentConnInfoReply" -- including AgentConnInfo, that is prohibited here in v2
                      where
                        processConf connInfo senderConf = do
                          let newConfirmation = NewConfirmation {connId, senderConf, ratchetState = rc'}
                          confId <- withStore c $ \db -> do
                            setConnAgentVersion db connId agentVersion
                            when (pqSupport /= pqSupport') $ setConnPQSupport db connId pqSupport'
                            -- /
                            -- Starting with agent version 7 (ratchetOnConfSMPAgentVersion),
                            -- initiating party initializes ratchet on processing confirmation;
                            -- previously, it initialized ratchet on allowConnection;
                            -- this is to support decryption of messages that may be received before allowConnection
                            liftIO $ do
                              createRatchet db connId rc'
                              let RcvQueue {smpClientVersion = v, e2ePrivKey = e2ePrivKey'} = rq
                                  SMPConfirmation {smpClientVersion = v', e2ePubKey = e2ePubKey'} = senderConf
                                  dhSecret = C.dh' e2ePubKey' e2ePrivKey'
                              setRcvQueueConfirmedE2E db rq dhSecret $ min v v'
                            -- /
                            createConfirmation db g newConfirmation
                          let srvs = map qServer $ smpReplyQueues senderConf
                          notify $ CONF confId pqSupport' srvs connInfo
                    _ -> prohibited "conf: decrypt error or skipped"
                -- party accepting connection
                (DuplexConnection _ (rq'@RcvQueue {smpClientVersion = v'} :| _) _, Nothing) -> do
                  g <- asks random
                  (agentMsgBody, pqEncryption) <- withStore c $ \db -> runExceptT $ agentRatchetDecrypt g db connId encConnInfo
                  parseMessage agentMsgBody >>= \case
                    AgentConnInfo connInfo -> do
                      notify $ INFO pqSupport connInfo
                      let dhSecret = C.dh' e2ePubKey e2ePrivKey
                      withStore' c $ \db -> do
                        setRcvQueueConfirmedE2E db rq dhSecret $ min v' smpClientVersion
                        updateRcvMsgHash db connId 1 (InternalRcvId 0) (C.sha256Hash agentMsgBody)
                      case senderKey of
                        Just k -> enqueueCmd $ ICDuplexSecure rId k
                        Nothing -> do
                          notify $ CON pqEncryption
                          withStore' c $ \db -> setRcvQueueStatus db rq' Active
                    _ -> prohibited "conf: not AgentConnInfo"
                _ -> prohibited "conf: incorrect state"
              _ -> prohibited "conf: status /= new"

          helloMsg :: SMP.MsgId -> MsgMeta -> Connection c -> AM ()
          helloMsg srvMsgId MsgMeta {pqEncryption} conn' = do
            logServer "<--" c srv rId $ "MSG <HELLO>:" <> logSecret' srvMsgId
            case status of
              Active -> prohibited "hello: active"
              _ ->
                case conn' of
                  DuplexConnection _ _ (sq@SndQueue {status = sndStatus} :| _)
                    -- `sndStatus == Active` when HELLO was previously sent, and this is the reply HELLO
                    -- this branch is executed by the accepting party in duplexHandshake mode (v2)
                    -- (was executed by initiating party in v1 that is no longer supported)
                    | sndStatus == Active -> do
                        atomically $ incSMPServerStat c userId srv connCompleted
                        notify $ CON pqEncryption
                    | otherwise -> enqueueDuplexHello sq
                  _ -> pure ()
            where
              enqueueDuplexHello :: SndQueue -> AM ()
              enqueueDuplexHello sq = do
                let cData' = toConnData conn'
                void $ enqueueMessage c cData' sq SMP.MsgFlags {notification = True} HELLO

          continueSending :: SMP.MsgId -> (SMPServer, SMP.SenderId) -> Connection 'CDuplex -> AM ()
          continueSending srvMsgId addr (DuplexConnection _ _ sqs) =
            case findQ addr sqs of
              Just sq -> do
                logServer "<--" c srv rId $ "MSG <QCONT>:" <> logSecret' srvMsgId
                atomically $
                  TM.lookup (qAddress sq) (smpDeliveryWorkers c)
                    >>= mapM_ (\(_, retryLock) -> tryPutTMVar retryLock ())
                notify QCONT
              Nothing -> qError "QCONT: queue address not found"

          messagesRcvd :: NonEmpty AMessageReceipt -> MsgMeta -> Connection 'CDuplex -> AM ACKd
          messagesRcvd rcpts msgMeta@MsgMeta {broker = (srvMsgId, _)} _ = do
            logServer "<--" c srv rId $ "MSG <RCPT>:" <> logSecret' srvMsgId
            rs <- forM rcpts $ \rcpt -> clientReceipt rcpt `catchAgentError` \e -> notify (ERR e) $> Nothing
            case L.nonEmpty . catMaybes $ L.toList rs of
              Just rs' -> notify (RCVD msgMeta rs') $> ACKPending
              Nothing -> ack
            where
              ack :: AM ACKd
              ack = enqueueCmd (ICAck rId srvMsgId) $> ACKd
              clientReceipt :: AMessageReceipt -> AM (Maybe MsgReceipt)
              clientReceipt AMessageReceipt {agentMsgId, msgHash} = do
                let sndMsgId = InternalSndId agentMsgId
                SndMsg {internalId = InternalId msgId, msgType, internalHash, msgReceipt} <- withStore c $ \db -> getSndMsgViaRcpt db connId sndMsgId
                if msgType /= AM_A_MSG_
                  then prohibited "receipt: not a msg" $> Nothing
                  else case msgReceipt of
                    Just MsgReceipt {msgRcptStatus = MROk} -> pure Nothing -- already notified with MROk status
                    _ -> do
                      let msgRcptStatus = if msgHash == internalHash then MROk else MRBadMsgHash
                          rcpt = MsgReceipt {agentMsgId = msgId, msgRcptStatus}
                      withStore' c $ \db -> updateSndMsgRcpt db connId sndMsgId rcpt
                      pure $ Just rcpt

          -- processed by queue sender
          qAddMsg :: SMP.MsgId -> NonEmpty (SMPQueueUri, Maybe SndQAddr) -> Connection 'CDuplex -> AM ()
          qAddMsg _ ((_, Nothing) :| _) _ = qError "adding queue without switching is not supported"
          qAddMsg srvMsgId ((qUri, Just addr) :| _) (DuplexConnection cData' rqs sqs) = do
            when (ratchetSyncSendProhibited cData') $ throwE $ AGENT (A_QUEUE "ratchet is not synchronized")
            clientVRange <- asks $ smpClientVRange . config
            case qUri `compatibleVersion` clientVRange of
              Just qInfo@(Compatible sqInfo@SMPQueueInfo {queueAddress}) ->
                case (findQ (qAddress sqInfo) sqs, findQ addr sqs) of
                  (Just _, _) -> qError "QADD: queue address is already used in connection"
                  (_, Just sq@SndQueue {dbQueueId = DBQueueId dbQueueId}) -> do
                    let (delSqs, keepSqs) = L.partition ((Just dbQueueId ==) . dbReplaceQId) sqs
                    case L.nonEmpty keepSqs of
                      Just sqs' -> do
                        (sq_@SndQueue {sndPublicKey}, dhPublicKey) <- lift $ newSndQueue userId connId qInfo
                        sq2 <- withStore c $ \db -> do
                          liftIO $ mapM_ (deleteConnSndQueue db connId) delSqs
                          addConnSndQueue db connId (sq_ :: NewSndQueue) {primary = True, dbReplaceQueueId = Just dbQueueId}
                        logServer "<--" c srv rId $ "MSG <QADD>:" <> logSecret' srvMsgId <> " " <> logSecret (senderId queueAddress)
                        let sqInfo' = (sqInfo :: SMPQueueInfo) {queueAddress = queueAddress {dhPublicKey}}
                        void . enqueueMessages c cData' sqs SMP.noMsgFlags $ QKEY [(sqInfo', sndPublicKey)]
                        sq1 <- withStore' c $ \db -> setSndSwitchStatus db sq $ Just SSSendingQKEY
                        let sqs'' = updatedQs sq1 sqs' <> [sq2]
                            conn' = DuplexConnection cData' rqs sqs''
                        notify . SWITCH QDSnd SPStarted $ connectionStats conn'
                      _ -> qError "QADD: won't delete all snd queues in connection"
                  _ -> qError "QADD: replaced queue address is not found in connection"
              _ -> throwE $ AGENT A_VERSION

          -- processed by queue recipient
          qKeyMsg :: SMP.MsgId -> NonEmpty (SMPQueueInfo, SndPublicAuthKey) -> Connection 'CDuplex -> AM ()
          qKeyMsg srvMsgId ((qInfo, senderKey) :| _) conn'@(DuplexConnection cData' rqs _) = do
            when (ratchetSyncSendProhibited cData') $ throwE $ AGENT (A_QUEUE "ratchet is not synchronized")
            clientVRange <- asks $ smpClientVRange . config
            unless (qInfo `isCompatible` clientVRange) . throwE $ AGENT A_VERSION
            case findRQ (smpServer, senderId) rqs of
              Just rq'@RcvQueue {rcvId, e2ePrivKey = dhPrivKey, smpClientVersion = cVer, status = status'}
                | status' == New || status' == Confirmed -> do
                    checkRQSwchStatus rq RSSendingQADD
                    logServer "<--" c srv rId $ "MSG <QKEY>:" <> logSecret' srvMsgId <> " " <> logSecret senderId
                    let dhSecret = C.dh' dhPublicKey dhPrivKey
                    withStore' c $ \db -> setRcvQueueConfirmedE2E db rq' dhSecret $ min cVer cVer'
                    enqueueCommand c "" connId (Just smpServer) $ AInternalCommand $ ICQSecure rcvId senderKey
                    notify . SWITCH QDRcv SPConfirmed $ connectionStats conn'
                | otherwise -> qError "QKEY: queue already secured"
              _ -> qError "QKEY: queue address not found in connection"
            where
              SMPQueueInfo cVer' SMPQueueAddress {smpServer, senderId, dhPublicKey} = qInfo

          -- processed by queue sender
          -- mark queue as Secured and to start sending messages to it
          qUseMsg :: SMP.MsgId -> NonEmpty ((SMPServer, SMP.SenderId), Bool) -> Connection 'CDuplex -> AM ()
          -- NOTE: does not yet support the change of the primary status during the rotation
          qUseMsg srvMsgId ((addr, _primary) :| _) (DuplexConnection cData' rqs sqs) = do
            when (ratchetSyncSendProhibited cData') $ throwE $ AGENT (A_QUEUE "ratchet is not synchronized")
            case findQ addr sqs of
              Just sq'@SndQueue {dbReplaceQueueId = Just replaceQId} -> do
                case find ((replaceQId ==) . dbQId) sqs of
                  Just sq1 -> do
                    checkSQSwchStatus sq1 SSSendingQKEY
                    logServer "<--" c srv rId $ "MSG <QUSE>:" <> logSecret' srvMsgId <> " " <> logSecret (snd addr)
                    withStore' c $ \db -> setSndQueueStatus db sq' Secured
                    let sq'' = (sq' :: SndQueue) {status = Secured}
                    -- sending QTEST to the new queue only, the old one will be removed if sent successfully
                    void $ enqueueMessages c cData' [sq''] SMP.noMsgFlags $ QTEST [addr]
                    sq1' <- withStore' c $ \db -> setSndSwitchStatus db sq1 $ Just SSSendingQTEST
                    let sqs' = updatedQs sq1' sqs
                        conn' = DuplexConnection cData' rqs sqs'
                    notify . SWITCH QDSnd SPSecured $ connectionStats conn'
                  _ -> qError "QUSE: switching SndQueue not found in connection"
              _ -> qError "QUSE: switched queue address not found in connection"

          qError :: String -> AM a
          qError = throwE . AGENT . A_QUEUE

          ereadyMsg :: CR.RatchetX448 -> Connection 'CDuplex -> AM ()
          ereadyMsg rcPrev (DuplexConnection cData'@ConnData {lastExternalSndId} _ sqs) = do
            let CR.Ratchet {rcSnd} = rcPrev
            -- if ratchet was initialized as receiving, it means EREADY wasn't sent on key negotiation
            when (isNothing rcSnd) . void $
              enqueueMessages' c cData' sqs SMP.MsgFlags {notification = True} (EREADY lastExternalSndId)

          smpInvitation :: SMP.MsgId -> Connection c -> ConnectionRequestUri 'CMInvitation -> ConnInfo -> AM ()
          smpInvitation srvMsgId conn' connReq@(CRInvitationUri crData _) cInfo = do
            logServer "<--" c srv rId $ "MSG <KEY>:" <> logSecret' srvMsgId
            case conn' of
              ContactConnection {} -> do
                -- show connection request even if invitaion via contact address is not compatible.
                -- in case invitation not compatible, assume there is no PQ encryption support.
                pqSupport <- lift $ maybe PQSupportOff pqSupported <$> compatibleInvitationUri connReq
                g <- asks random
                let newInv = NewInvitation {contactConnId = connId, connReq, recipientConnInfo = cInfo}
                invId <- withStore c $ \db -> createInvitation db g newInv
                let srvs = L.map qServer $ crSmpQueues crData
                notify $ REQ invId pqSupport srvs cInfo
              _ -> prohibited "inv: sent to message conn"
            where
              pqSupported (_, Compatible (CR.E2ERatchetParams v _ _ _), Compatible agentVersion) =
                PQSupportOn `CR.pqSupportAnd` versionPQSupport_ agentVersion (Just v)

          qDuplex :: Connection c -> String -> (Connection 'CDuplex -> AM a) -> AM a
          qDuplex conn' name action = case conn' of
            DuplexConnection {} -> action conn'
            _ -> qError $ name <> ": message must be sent to duplex connection"

          newRatchetKey :: CR.RcvE2ERatchetParams 'C.X448 -> Connection 'CDuplex -> AM ()
          newRatchetKey e2eOtherPartyParams@(CR.E2ERatchetParams e2eVersion k1Rcv k2Rcv _) conn'@(DuplexConnection cData'@ConnData {lastExternalSndId, pqSupport} _ sqs) =
            unlessM ratchetExists $ do
              AgentConfig {e2eEncryptVRange} <- asks config
              unless (e2eVersion `isCompatible` e2eEncryptVRange) (throwE $ AGENT A_VERSION)
              keys <- getSendRatchetKeys
              let rcVs = CR.RatchetVersions {current = e2eVersion, maxSupported = maxVersion e2eEncryptVRange}
              initRatchet rcVs keys
              notifyAgreed
            where
              rkHashRcv = rkHash k1Rcv k2Rcv
              rkHash k1 k2 = C.sha256Hash $ C.pubKeyBytes k1 <> C.pubKeyBytes k2
              ratchetExists :: AM Bool
              ratchetExists = withStore' c $ \db -> do
                exists <- checkRatchetKeyHashExists db connId rkHashRcv
                unless exists $ addProcessedRatchetKeyHash db connId rkHashRcv
                pure exists
              getSendRatchetKeys :: AM (C.PrivateKeyX448, C.PrivateKeyX448, Maybe CR.RcvPrivRKEMParams)
              getSendRatchetKeys = case rss of
                RSOk -> sendReplyKey -- receiving client
                RSAllowed -> sendReplyKey
                RSRequired -> sendReplyKey
                RSStarted -> withStore c (`getRatchetX3dhKeys` connId) -- initiating client
                RSAgreed -> do
                  withStore' c $ \db -> setConnRatchetSync db connId RSRequired
                  notifyRatchetSyncError
                  -- can communicate for other client to reset to RSRequired
                  -- - need to add new AgentMsgEnvelope, AgentMessage, AgentMessageType
                  -- - need to deduplicate on receiving side
                  throwE $ AGENT (A_CRYPTO RATCHET_SYNC)
                where
                  sendReplyKey = do
                    g <- asks random
                    (pk1, pk2, pKem, e2eParams) <- liftIO $ CR.generateRcvE2EParams g e2eVersion pqSupport
                    enqueueRatchetKeyMsgs c cData' sqs e2eParams
                    pure (pk1, pk2, pKem)
                  notifyRatchetSyncError = do
                    let cData'' = cData' {ratchetSyncState = RSRequired} :: ConnData
                        conn'' = updateConnection cData'' conn'
                    notify $ RSYNC RSRequired (Just RATCHET_SYNC) (connectionStats conn'')
              notifyAgreed :: AM ()
              notifyAgreed = do
                let cData'' = cData' {ratchetSyncState = RSAgreed} :: ConnData
                    conn'' = updateConnection cData'' conn'
                notify . RSYNC RSAgreed Nothing $ connectionStats conn''
              recreateRatchet :: CR.Ratchet 'C.X448 -> AM ()
              recreateRatchet rc = withStore' c $ \db -> do
                setConnRatchetSync db connId RSAgreed
                deleteRatchet db connId
                createRatchet db connId rc
              -- compare public keys `k1` in AgentRatchetKey messages sent by self and other party
              -- to determine ratchet initilization ordering
              initRatchet :: CR.RatchetVersions -> (C.PrivateKeyX448, C.PrivateKeyX448, Maybe CR.RcvPrivRKEMParams) -> AM ()
              initRatchet rcVs (pk1, pk2, pKem)
                | rkHash (C.publicKey pk1) (C.publicKey pk2) <= rkHashRcv = do
                    rcParams <- liftError cryptoError $ CR.pqX3dhRcv pk1 pk2 pKem e2eOtherPartyParams
                    recreateRatchet $ CR.initRcvRatchet rcVs pk2 rcParams pqSupport
                | otherwise = do
                    (_, rcDHRs) <- atomically . C.generateKeyPair =<< asks random
                    rcParams <- liftEitherWith cryptoError $ CR.pqX3dhSnd pk1 pk2 (CR.APRKP CR.SRKSProposed <$> pKem) e2eOtherPartyParams
                    recreateRatchet $ CR.initSndRatchet rcVs k2Rcv rcDHRs rcParams
                    void . enqueueMessages' c cData' sqs SMP.MsgFlags {notification = True} $ EREADY lastExternalSndId

          checkMsgIntegrity :: PrevExternalSndId -> ExternalSndId -> PrevRcvMsgHash -> ByteString -> MsgIntegrity
          checkMsgIntegrity prevExtSndId extSndId internalPrevMsgHash receivedPrevMsgHash
            | extSndId == prevExtSndId + 1 && internalPrevMsgHash == receivedPrevMsgHash = MsgOk
            | extSndId < prevExtSndId = MsgError $ MsgBadId extSndId
            | extSndId == prevExtSndId = MsgError MsgDuplicate -- ? deduplicate
            | extSndId > prevExtSndId + 1 = MsgError $ MsgSkipped (prevExtSndId + 1) (extSndId - 1)
            | internalPrevMsgHash /= receivedPrevMsgHash = MsgError MsgBadHash
            | otherwise = MsgError MsgDuplicate -- this case is not possible

checkRQSwchStatus :: RcvQueue -> RcvSwitchStatus -> AM ()
checkRQSwchStatus rq@RcvQueue {rcvSwchStatus} expected =
  unless (rcvSwchStatus == Just expected) $ switchStatusError rq expected rcvSwchStatus
{-# INLINE checkRQSwchStatus #-}

checkSQSwchStatus :: SndQueue -> SndSwitchStatus -> AM ()
checkSQSwchStatus sq@SndQueue {sndSwchStatus} expected =
  unless (sndSwchStatus == Just expected) $ switchStatusError sq expected sndSwchStatus
{-# INLINE checkSQSwchStatus #-}

switchStatusError :: (SMPQueueRec q, Show a) => q -> a -> Maybe a -> AM ()
switchStatusError q expected actual =
  throwE . INTERNAL $
    ("unexpected switch status, queueId=" <> show (queueId q))
      <> (", expected=" <> show expected)
      <> (", actual=" <> show actual)

connectReplyQueues :: AgentClient -> ConnData -> ConnInfo -> Maybe SndQueue -> NonEmpty SMPQueueInfo -> AM ()
connectReplyQueues c cData@ConnData {userId, connId} ownConnInfo sq_ (qInfo :| _) = do
  clientVRange <- asks $ smpClientVRange . config
  case qInfo `proveCompatible` clientVRange of
    Nothing -> throwE $ AGENT A_VERSION
    Just qInfo' -> do
      -- in case of SKEY retry the connection is already duplex
      sq' <- maybe upgradeConn pure sq_
      void $ agentSecureSndQueue c cData sq'
      enqueueConfirmation c cData sq' ownConnInfo Nothing
      where
        upgradeConn = do
          (sq, _) <- lift $ newSndQueue userId connId qInfo'
          withStore c $ \db -> upgradeRcvConnToDuplex db connId sq

secureConfirmQueueAsync :: AgentClient -> ConnData -> SndQueue -> SMPServerWithAuth -> ConnInfo -> Maybe (CR.SndE2ERatchetParams 'C.X448) -> SubscriptionMode -> AM SndQueueSecured
secureConfirmQueueAsync c cData sq srv connInfo e2eEncryption_ subMode = do
  sqSecured <- agentSecureSndQueue c cData sq
  storeConfirmation c cData sq e2eEncryption_ =<< mkAgentConfirmation c cData sq srv connInfo subMode
  lift $ submitPendingMsg c cData sq
  pure sqSecured

secureConfirmQueue :: AgentClient -> ConnData -> SndQueue -> SMPServerWithAuth -> ConnInfo -> Maybe (CR.SndE2ERatchetParams 'C.X448) -> SubscriptionMode -> AM SndQueueSecured
secureConfirmQueue c cData@ConnData {connId, connAgentVersion, pqSupport} sq srv connInfo e2eEncryption_ subMode = do
  sqSecured <- agentSecureSndQueue c cData sq
  msg <- mkConfirmation =<< mkAgentConfirmation c cData sq srv connInfo subMode
  void $ sendConfirmation c sq msg
  withStore' c $ \db -> setSndQueueStatus db sq Confirmed
  pure sqSecured
  where
    mkConfirmation :: AgentMessage -> AM MsgBody
    mkConfirmation aMessage = do
      currentE2EVersion <- asks $ maxVersion . e2eEncryptVRange . config
      withStore c $ \db -> runExceptT $ do
        let agentMsgBody = smpEncode aMessage
        (_, internalSndId, _) <- ExceptT $ updateSndIds db connId
        liftIO $ updateSndMsgHash db connId internalSndId (C.sha256Hash agentMsgBody)
        let pqEnc = CR.pqSupportToEnc pqSupport
        (encConnInfo, _) <- agentRatchetEncrypt db cData agentMsgBody e2eEncConnInfoLength (Just pqEnc) currentE2EVersion
        pure . smpEncode $ AgentConfirmation {agentVersion = connAgentVersion, e2eEncryption_, encConnInfo}

agentSecureSndQueue :: AgentClient -> ConnData -> SndQueue -> AM SndQueueSecured
agentSecureSndQueue c ConnData {connAgentVersion} sq@SndQueue {sndSecure, status}
  | sndSecure && status == New = do
      secureSndQueue c sq
      withStore' c $ \db -> setSndQueueStatus db sq Secured
      pure initiatorRatchetOnConf
  -- on repeat JOIN processing (e.g. previous attempt to create reply queue failed)
  | sndSecure && status == Secured = pure initiatorRatchetOnConf
  | otherwise = pure False
  where
    initiatorRatchetOnConf = connAgentVersion >= ratchetOnConfSMPAgentVersion

mkAgentConfirmation :: AgentClient -> ConnData -> SndQueue -> SMPServerWithAuth -> ConnInfo -> SubscriptionMode -> AM AgentMessage
mkAgentConfirmation c cData sq srv connInfo subMode = do
  qInfo <- createReplyQueue c cData sq subMode srv
  pure $ AgentConnInfoReply (qInfo :| []) connInfo

enqueueConfirmation :: AgentClient -> ConnData -> SndQueue -> ConnInfo -> Maybe (CR.SndE2ERatchetParams 'C.X448) -> AM ()
enqueueConfirmation c cData sq connInfo e2eEncryption_ = do
  storeConfirmation c cData sq e2eEncryption_ $ AgentConnInfo connInfo
  lift $ submitPendingMsg c cData sq

storeConfirmation :: AgentClient -> ConnData -> SndQueue -> Maybe (CR.SndE2ERatchetParams 'C.X448) -> AgentMessage -> AM ()
storeConfirmation c cData@ConnData {connId, pqSupport, connAgentVersion = v} sq e2eEncryption_ agentMsg = do
  currentE2EVersion <- asks $ maxVersion . e2eEncryptVRange . config
  withStore c $ \db -> runExceptT $ do
    internalTs <- liftIO getCurrentTime
    (internalId, internalSndId, prevMsgHash) <- ExceptT $ updateSndIds db connId
    let agentMsgStr = smpEncode agentMsg
        internalHash = C.sha256Hash agentMsgStr
        pqEnc = CR.pqSupportToEnc pqSupport
    (encConnInfo, pqEncryption) <- agentRatchetEncrypt db cData agentMsgStr e2eEncConnInfoLength (Just pqEnc) currentE2EVersion
    let msgBody = smpEncode $ AgentConfirmation {agentVersion = v, e2eEncryption_, encConnInfo}
        msgType = agentMessageType agentMsg
        msgData = SndMsgData {internalId, internalSndId, internalTs, msgType, msgBody, pqEncryption, msgFlags = SMP.MsgFlags {notification = True}, internalHash, prevMsgHash}
    liftIO $ createSndMsg db connId msgData
    liftIO $ createSndMsgDelivery db connId sq internalId

enqueueRatchetKeyMsgs :: AgentClient -> ConnData -> NonEmpty SndQueue -> CR.RcvE2ERatchetParams 'C.X448 -> AM ()
enqueueRatchetKeyMsgs c cData (sq :| sqs) e2eEncryption = do
  msgId <- enqueueRatchetKey c cData sq e2eEncryption
  mapM_ (lift . enqueueSavedMessage c cData msgId) $ filter isActiveSndQ sqs

enqueueRatchetKey :: AgentClient -> ConnData -> SndQueue -> CR.RcvE2ERatchetParams 'C.X448 -> AM AgentMsgId
enqueueRatchetKey c cData@ConnData {connId} sq e2eEncryption = do
  aVRange <- asks $ smpAgentVRange . config
  msgId <- storeRatchetKey $ maxVersion aVRange
  lift $ submitPendingMsg c cData sq
  pure $ unId msgId
  where
    storeRatchetKey :: VersionSMPA -> AM InternalId
    storeRatchetKey agentVersion = withStore c $ \db -> runExceptT $ do
      internalTs <- liftIO getCurrentTime
      (internalId, internalSndId, prevMsgHash) <- ExceptT $ updateSndIds db connId
      let agentMsg = AgentRatchetInfo ""
          agentMsgStr = smpEncode agentMsg
          internalHash = C.sha256Hash agentMsgStr
      let msgBody = smpEncode $ AgentRatchetKey {agentVersion, e2eEncryption, info = agentMsgStr}
          msgType = agentMessageType agentMsg
          -- this message is e2e encrypted with queue key, not with double ratchet
          msgData = SndMsgData {internalId, internalSndId, internalTs, msgType, msgBody, pqEncryption = PQEncOff, msgFlags = SMP.MsgFlags {notification = True}, internalHash, prevMsgHash}
      liftIO $ createSndMsg db connId msgData
      liftIO $ createSndMsgDelivery db connId sq internalId
      pure internalId

-- encoded AgentMessage -> encoded EncAgentMessage
agentRatchetEncrypt :: DB.Connection -> ConnData -> ByteString -> (VersionSMPA -> PQSupport -> Int) -> Maybe PQEncryption -> CR.VersionE2E -> ExceptT StoreError IO (ByteString, PQEncryption)
agentRatchetEncrypt db ConnData {connId, connAgentVersion = v, pqSupport} msg getPaddedLen pqEnc_ currentE2EVersion = do
  rc <- ExceptT $ getRatchet db connId
  let paddedLen = getPaddedLen v pqSupport
  (encMsg, rc') <- withExceptT (SEAgentError . cryptoError) $ CR.rcEncrypt rc paddedLen msg pqEnc_ currentE2EVersion
  liftIO $ updateRatchet db connId rc' CR.SMDNoChange
  pure (encMsg, CR.rcSndKEM rc')

-- encoded EncAgentMessage -> encoded AgentMessage
agentRatchetDecrypt :: TVar ChaChaDRG -> DB.Connection -> ConnId -> ByteString -> ExceptT StoreError IO (ByteString, PQEncryption)
agentRatchetDecrypt g db connId encAgentMsg = do
  rc <- ExceptT $ getRatchet db connId
  agentRatchetDecrypt' g db connId rc encAgentMsg

agentRatchetDecrypt' :: TVar ChaChaDRG -> DB.Connection -> ConnId -> CR.RatchetX448 -> ByteString -> ExceptT StoreError IO (ByteString, PQEncryption)
agentRatchetDecrypt' g db connId rc encAgentMsg = do
  skipped <- liftIO $ getSkippedMsgKeys db connId
  (agentMsgBody_, rc', skippedDiff) <- withExceptT (SEAgentError . cryptoError) $ CR.rcDecrypt g rc skipped encAgentMsg
  liftIO $ updateRatchet db connId rc' skippedDiff
  liftEither $ bimap (SEAgentError . cryptoError) (,CR.rcRcvKEM rc') agentMsgBody_

newSndQueue :: UserId -> ConnId -> Compatible SMPQueueInfo -> AM' (NewSndQueue, C.PublicKeyX25519)
newSndQueue userId connId (Compatible (SMPQueueInfo smpClientVersion SMPQueueAddress {smpServer, senderId, sndSecure, dhPublicKey = rcvE2ePubDhKey})) = do
  C.AuthAlg a <- asks $ sndAuthAlg . config
  g <- asks random
  (sndPublicKey, sndPrivateKey) <- atomically $ C.generateAuthKeyPair a g
  (e2ePubKey, e2ePrivKey) <- atomically $ C.generateKeyPair g
  let sq =
        SndQueue
          { userId,
            connId,
            server = smpServer,
            sndId = senderId,
            sndSecure,
            sndPublicKey,
            sndPrivateKey,
            e2eDhSecret = C.dh' rcvE2ePubDhKey e2ePrivKey,
            e2ePubKey = Just e2ePubKey,
            status = New,
            dbQueueId = DBNewQueue,
            primary = True,
            dbReplaceQueueId = Nothing,
            sndSwchStatus = Nothing,
            smpClientVersion
          }
  pure (sq, e2ePubKey)
