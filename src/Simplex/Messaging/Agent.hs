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
  ( -- * queue-based SMP agent
    getAgentClient,
    runAgentClient,

    -- * SMP agent functional API
    AgentClient (..),
    AgentMonad,
    AgentErrorMonad,
    getSMPAgentClient,
    disconnectAgentClient,
    resumeAgentClient,
    withConnLock,
    createConnectionAsync,
    joinConnectionAsync,
    allowConnectionAsync,
    acceptContactAsync,
    ackMessageAsync,
    switchConnectionAsync,
    deleteConnectionAsync,
    createConnection,
    joinConnection,
    allowConnection,
    acceptContact,
    rejectContact,
    subscribeConnection,
    subscribeConnections,
    getConnectionMessage,
    getNotificationMessage,
    resubscribeConnection,
    resubscribeConnections,
    sendMessage,
    ackMessage,
    switchConnection,
    suspendConnection,
    deleteConnection,
    getConnectionServers,
    setSMPServers,
    setNtfServers,
    setNetworkConfig,
    getNetworkConfig,
    registerNtfToken,
    verifyNtfToken,
    checkNtfToken,
    deleteNtfToken,
    getNtfToken,
    getNtfTokenData,
    toggleConnectionNtfs,
    activateAgent,
    suspendAgent,
    execAgentStoreSQL,
    debugAgentLocks,
    logConnection,
  )
where

import Control.Concurrent.STM (stateTVar)
import Control.Logger.Simple (logInfo, showText)
import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Reader
import Crypto.Random (MonadRandom)
import Data.Bifunctor (bimap, first, second)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Composition ((.:), (.:.), (.::))
import Data.Foldable (foldl')
import Data.Functor (($>))
import Data.List (deleteFirstsBy, find)
import Data.List.NonEmpty (NonEmpty (..), (<|))
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (isJust)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock.System (systemToUTCTime)
import qualified Database.SQLite.Simple as DB
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.NtfSubSupervisor
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite
import Simplex.Messaging.Client (ProtocolClient (..), ServerTransmission)
import qualified Simplex.Messaging.Crypto as C
import qualified Simplex.Messaging.Crypto.Ratchet as CR
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String (StrEncoding (..))
import Simplex.Messaging.Notifications.Protocol (DeviceToken, NtfRegCode (NtfRegCode), NtfTknStatus (..), NtfTokenId)
import Simplex.Messaging.Notifications.Server.Push.APNS (PNMessageData (..))
import Simplex.Messaging.Notifications.Types
import Simplex.Messaging.Parsers (parse)
import Simplex.Messaging.Protocol (BrokerMsg, ErrorType (AUTH), MsgBody, MsgFlags, NtfServer, SMPMsgMeta, SndPublicVerifyKey, sameSrvAddr, sameSrvAddr')
import qualified Simplex.Messaging.Protocol as SMP
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import System.Random (randomR)
import UnliftIO.Async (async, mapConcurrently, race_)
import UnliftIO.Concurrent (forkFinally, forkIO, threadDelay)
import qualified UnliftIO.Exception as E
import UnliftIO.STM

-- import GHC.Conc (unsafeIOToSTM)

-- | Creates an SMP agent client instance
getSMPAgentClient :: (MonadRandom m, MonadUnliftIO m) => AgentConfig -> InitialAgentServers -> m AgentClient
getSMPAgentClient cfg initServers = newSMPAgentEnv cfg >>= runReaderT runAgent
  where
    runAgent = do
      c <- getAgentClient initServers
      void $ race_ (subscriber c) (runNtfSupervisor c) `forkFinally` const (disconnectAgentClient c)
      pure c

disconnectAgentClient :: MonadUnliftIO m => AgentClient -> m ()
disconnectAgentClient c@AgentClient {agentEnv = Env {ntfSupervisor = ns}} = do
  closeAgentClient c
  liftIO $ closeNtfSupervisor ns
  logConnection c False

resumeAgentClient :: MonadIO m => AgentClient -> m ()
resumeAgentClient c = atomically $ writeTVar (active c) True

-- |
type AgentErrorMonad m = (MonadUnliftIO m, MonadError AgentErrorType m)

-- | Create SMP agent connection (NEW command) asynchronously, synchronous response is new connection id
createConnectionAsync :: forall m c. (AgentErrorMonad m, ConnectionModeI c) => AgentClient -> ACorrId -> Bool -> SConnectionMode c -> m ConnId
createConnectionAsync c corrId enableNtfs cMode = withAgentEnv c $ newConnAsync c corrId enableNtfs cMode

-- | Join SMP agent connection (JOIN command) asynchronously, synchronous response is new connection id
joinConnectionAsync :: AgentErrorMonad m => AgentClient -> ACorrId -> Bool -> ConnectionRequestUri c -> ConnInfo -> m ConnId
joinConnectionAsync c corrId enableNtfs = withAgentEnv c .: joinConnAsync c corrId enableNtfs

-- | Allow connection to continue after CONF notification (LET command), no synchronous response
allowConnectionAsync :: AgentErrorMonad m => AgentClient -> ACorrId -> ConnId -> ConfirmationId -> ConnInfo -> m ()
allowConnectionAsync c = withAgentEnv c .:: allowConnectionAsync' c

-- | Accept contact after REQ notification (ACPT command) asynchronously, synchronous response is new connection id
acceptContactAsync :: AgentErrorMonad m => AgentClient -> ACorrId -> Bool -> ConfirmationId -> ConnInfo -> m ConnId
acceptContactAsync c corrId enableNtfs = withAgentEnv c .: acceptContactAsync' c corrId enableNtfs

-- | Acknowledge message (ACK command) asynchronously, no synchronous response
ackMessageAsync :: forall m. AgentErrorMonad m => AgentClient -> ACorrId -> ConnId -> AgentMsgId -> m ()
ackMessageAsync c = withAgentEnv c .:. ackMessageAsync' c

-- | Switch connection to the new receive queue
switchConnectionAsync :: AgentErrorMonad m => AgentClient -> ACorrId -> ConnId -> m ()
switchConnectionAsync c = withAgentEnv c .: switchConnectionAsync' c

-- | Delete SMP agent connection (DEL command) asynchronously, no synchronous response
deleteConnectionAsync :: AgentErrorMonad m => AgentClient -> ACorrId -> ConnId -> m ()
deleteConnectionAsync c = withAgentEnv c .: deleteConnectionAsync' c

-- | Create SMP agent connection (NEW command)
createConnection :: AgentErrorMonad m => AgentClient -> Bool -> SConnectionMode c -> Maybe CRClientData -> m (ConnId, ConnectionRequestUri c)
createConnection c enableNtfs cMode clientData = withAgentEnv c $ newConn c "" False enableNtfs cMode clientData

-- | Join SMP agent connection (JOIN command)
joinConnection :: AgentErrorMonad m => AgentClient -> Bool -> ConnectionRequestUri c -> ConnInfo -> m ConnId
joinConnection c enableNtfs = withAgentEnv c .: joinConn c "" False enableNtfs

-- | Allow connection to continue after CONF notification (LET command)
allowConnection :: AgentErrorMonad m => AgentClient -> ConnId -> ConfirmationId -> ConnInfo -> m ()
allowConnection c = withAgentEnv c .:. allowConnection' c

-- | Accept contact after REQ notification (ACPT command)
acceptContact :: AgentErrorMonad m => AgentClient -> Bool -> ConfirmationId -> ConnInfo -> m ConnId
acceptContact c enableNtfs = withAgentEnv c .: acceptContact' c "" enableNtfs

-- | Reject contact (RJCT command)
rejectContact :: AgentErrorMonad m => AgentClient -> ConnId -> ConfirmationId -> m ()
rejectContact c = withAgentEnv c .: rejectContact' c

-- | Subscribe to receive connection messages (SUB command)
subscribeConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
subscribeConnection c = withAgentEnv c . subscribeConnection' c

-- | Subscribe to receive connection messages from multiple connections, batching commands when possible
subscribeConnections :: AgentErrorMonad m => AgentClient -> [ConnId] -> m (Map ConnId (Either AgentErrorType ()))
subscribeConnections c = withAgentEnv c . subscribeConnections' c

-- | Get connection message (GET command)
getConnectionMessage :: AgentErrorMonad m => AgentClient -> ConnId -> m (Maybe SMPMsgMeta)
getConnectionMessage c = withAgentEnv c . getConnectionMessage' c

-- | Get connection message for received notification
getNotificationMessage :: AgentErrorMonad m => AgentClient -> C.CbNonce -> ByteString -> m (NotificationInfo, [SMPMsgMeta])
getNotificationMessage c = withAgentEnv c .: getNotificationMessage' c

resubscribeConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
resubscribeConnection c = withAgentEnv c . resubscribeConnection' c

resubscribeConnections :: AgentErrorMonad m => AgentClient -> [ConnId] -> m (Map ConnId (Either AgentErrorType ()))
resubscribeConnections c = withAgentEnv c . resubscribeConnections' c

-- | Send message to the connection (SEND command)
sendMessage :: AgentErrorMonad m => AgentClient -> ConnId -> MsgFlags -> MsgBody -> m AgentMsgId
sendMessage c = withAgentEnv c .:. sendMessage' c

ackMessage :: AgentErrorMonad m => AgentClient -> ConnId -> AgentMsgId -> m ()
ackMessage c = withAgentEnv c .: ackMessage' c

-- | Switch connection to the new receive queue
switchConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ConnectionStats
switchConnection c = withAgentEnv c . switchConnection' c

-- | Suspend SMP agent connection (OFF command)
suspendConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
suspendConnection c = withAgentEnv c . suspendConnection' c

-- | Delete SMP agent connection (DEL command)
deleteConnection :: AgentErrorMonad m => AgentClient -> ConnId -> m ()
deleteConnection c = withAgentEnv c . deleteConnection' c

-- | get servers used for connection
getConnectionServers :: AgentErrorMonad m => AgentClient -> ConnId -> m ConnectionStats
getConnectionServers c = withAgentEnv c . getConnectionServers' c

-- | Change servers to be used for creating new queues
setSMPServers :: AgentErrorMonad m => AgentClient -> NonEmpty SMPServerWithAuth -> m ()
setSMPServers c = withAgentEnv c . setSMPServers' c

setNtfServers :: AgentErrorMonad m => AgentClient -> [NtfServer] -> m ()
setNtfServers c = withAgentEnv c . setNtfServers' c

-- | set SOCKS5 proxy on/off and optionally set TCP timeout
setNetworkConfig :: AgentErrorMonad m => AgentClient -> NetworkConfig -> m ()
setNetworkConfig c cfg' = do
  cfg <- atomically $ do
    swapTVar (useNetworkConfig c) cfg'
  liftIO . when (cfg /= cfg') $ do
    closeProtocolServerClients c smpClients
    closeProtocolServerClients c ntfClients

getNetworkConfig :: AgentErrorMonad m => AgentClient -> m NetworkConfig
getNetworkConfig = readTVarIO . useNetworkConfig

-- | Register device notifications token
registerNtfToken :: AgentErrorMonad m => AgentClient -> DeviceToken -> NotificationsMode -> m NtfTknStatus
registerNtfToken c = withAgentEnv c .: registerNtfToken' c

-- | Verify device notifications token
verifyNtfToken :: AgentErrorMonad m => AgentClient -> DeviceToken -> C.CbNonce -> ByteString -> m ()
verifyNtfToken c = withAgentEnv c .:. verifyNtfToken' c

checkNtfToken :: AgentErrorMonad m => AgentClient -> DeviceToken -> m NtfTknStatus
checkNtfToken c = withAgentEnv c . checkNtfToken' c

deleteNtfToken :: AgentErrorMonad m => AgentClient -> DeviceToken -> m ()
deleteNtfToken c = withAgentEnv c . deleteNtfToken' c

getNtfToken :: AgentErrorMonad m => AgentClient -> m (DeviceToken, NtfTknStatus, NotificationsMode)
getNtfToken c = withAgentEnv c $ getNtfToken' c

getNtfTokenData :: AgentErrorMonad m => AgentClient -> m NtfToken
getNtfTokenData c = withAgentEnv c $ getNtfTokenData' c

-- | Set connection notifications on/off
toggleConnectionNtfs :: AgentErrorMonad m => AgentClient -> ConnId -> Bool -> m ()
toggleConnectionNtfs c = withAgentEnv c .: toggleConnectionNtfs' c

-- | Activate operations
activateAgent :: AgentErrorMonad m => AgentClient -> m ()
activateAgent c = withAgentEnv c $ activateAgent' c

-- | Suspend operations with max delay to deliver pending messages
suspendAgent :: AgentErrorMonad m => AgentClient -> Int -> m ()
suspendAgent c = withAgentEnv c . suspendAgent' c

execAgentStoreSQL :: AgentErrorMonad m => AgentClient -> Text -> m [Text]
execAgentStoreSQL c = withAgentEnv c . execAgentStoreSQL' c

debugAgentLocks :: AgentErrorMonad m => AgentClient -> m AgentLocks
debugAgentLocks c = withAgentEnv c $ debugAgentLocks' c

withAgentEnv :: AgentClient -> ReaderT Env m a -> m a
withAgentEnv c = (`runReaderT` agentEnv c)

-- | Creates an SMP agent client instance that receives commands and sends responses via 'TBQueue's.
getAgentClient :: (MonadUnliftIO m, MonadReader Env m) => InitialAgentServers -> m AgentClient
getAgentClient initServers = ask >>= atomically . newAgentClient initServers

logConnection :: MonadUnliftIO m => AgentClient -> Bool -> m ()
logConnection c connected =
  let event = if connected then "connected to" else "disconnected from"
   in logInfo $ T.unwords ["client", showText (clientId c), event, "Agent"]

-- | Runs an SMP agent instance that receives commands and sends responses via 'TBQueue's.
runAgentClient :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
runAgentClient c = race_ (subscriber c) (client c)

client :: forall m. (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
client c@AgentClient {rcvQ, subQ} = forever $ do
  (corrId, connId, cmd) <- atomically $ readTBQueue rcvQ
  runExceptT (processCommand c (connId, cmd))
    >>= atomically . writeTBQueue subQ . \case
      Left e -> (corrId, connId, ERR e)
      Right (connId', resp) -> (corrId, connId', resp)

-- | execute any SMP agent command
processCommand :: forall m. AgentMonad m => AgentClient -> (ConnId, ACommand 'Client) -> m (ConnId, ACommand 'Agent)
processCommand c (connId, cmd) = case cmd of
  NEW enableNtfs (ACM cMode) -> second (INV . ACR cMode) <$> newConn c connId False enableNtfs cMode Nothing
  JOIN enableNtfs (ACR _ cReq) connInfo -> (,OK) <$> joinConn c connId False enableNtfs cReq connInfo
  LET confId ownCInfo -> allowConnection' c connId confId ownCInfo $> (connId, OK)
  ACPT invId ownCInfo -> (,OK) <$> acceptContact' c connId True invId ownCInfo
  RJCT invId -> rejectContact' c connId invId $> (connId, OK)
  SUB -> subscribeConnection' c connId $> (connId, OK)
  SEND msgFlags msgBody -> (connId,) . MID <$> sendMessage' c connId msgFlags msgBody
  ACK msgId -> ackMessage' c connId msgId $> (connId, OK)
  SWCH -> switchConnection' c connId $> (connId, OK)
  OFF -> suspendConnection' c connId $> (connId, OK)
  DEL -> deleteConnection' c connId $> (connId, OK)
  CHK -> (connId,) . STAT <$> getConnectionServers' c connId

newConnAsync :: forall m c. (AgentMonad m, ConnectionModeI c) => AgentClient -> ACorrId -> Bool -> SConnectionMode c -> m ConnId
newConnAsync c corrId enableNtfs cMode = do
  g <- asks idsDrg
  connAgentVersion <- asks $ maxVersion . smpAgentVRange . config
  let cData = ConnData {connId = "", connAgentVersion, enableNtfs, duplexHandshake = Nothing, deleted = False} -- connection mode is determined by the accepting agent
  connId <- withStore c $ \db -> createNewConn db g cData cMode
  enqueueCommand c corrId connId Nothing $ AClientCommand $ NEW enableNtfs (ACM cMode)
  pure connId

joinConnAsync :: AgentMonad m => AgentClient -> ACorrId -> Bool -> ConnectionRequestUri c -> ConnInfo -> m ConnId
joinConnAsync c corrId enableNtfs cReqUri@(CRInvitationUri ConnReqUriData {crAgentVRange} _) cInfo = do
  aVRange <- asks $ smpAgentVRange . config
  case crAgentVRange `compatibleVersion` aVRange of
    Just (Compatible connAgentVersion) -> do
      g <- asks idsDrg
      let duplexHS = connAgentVersion /= 1
          cData = ConnData {connId = "", connAgentVersion, enableNtfs, duplexHandshake = Just duplexHS, deleted = False}
      connId <- withStore c $ \db -> createNewConn db g cData SCMInvitation
      enqueueCommand c corrId connId Nothing $ AClientCommand $ JOIN enableNtfs (ACR sConnectionMode cReqUri) cInfo
      pure connId
    _ -> throwError $ AGENT A_VERSION
joinConnAsync _c _corrId _enableNtfs (CRContactUri _) _cInfo =
  throwError $ CMD PROHIBITED

allowConnectionAsync' :: AgentMonad m => AgentClient -> ACorrId -> ConnId -> ConfirmationId -> ConnInfo -> m ()
allowConnectionAsync' c corrId connId confId ownConnInfo =
  withStore c (`getConn` connId) >>= \case
    SomeConn _ (RcvConnection _ RcvQueue {server}) ->
      enqueueCommand c corrId connId (Just server) $ AClientCommand $ LET confId ownConnInfo
    _ -> throwError $ CMD PROHIBITED

acceptContactAsync' :: AgentMonad m => AgentClient -> ACorrId -> Bool -> InvitationId -> ConnInfo -> m ConnId
acceptContactAsync' c corrId enableNtfs invId ownConnInfo = do
  Invitation {contactConnId, connReq} <- withStore c (`getInvitation` invId)
  withStore c (`getConn` contactConnId) >>= \case
    SomeConn _ ContactConnection {} -> do
      withStore' c $ \db -> acceptInvitation db invId ownConnInfo
      joinConnAsync c corrId enableNtfs connReq ownConnInfo `catchError` \err -> do
        withStore' c (`unacceptInvitation` invId)
        throwError err
    _ -> throwError $ CMD PROHIBITED

ackMessageAsync' :: forall m. AgentMonad m => AgentClient -> ACorrId -> ConnId -> AgentMsgId -> m ()
ackMessageAsync' c corrId connId msgId = do
  SomeConn cType _ <- withStore c (`getConn` connId)
  case cType of
    SCDuplex -> enqueueAck
    SCRcv -> enqueueAck
    SCSnd -> throwError $ CONN SIMPLEX
    SCContact -> throwError $ CMD PROHIBITED
    SCNew -> throwError $ CMD PROHIBITED
  where
    enqueueAck :: m ()
    enqueueAck = do
      (RcvQueue {server}, _) <- withStore c $ \db -> setMsgUserAck db connId $ InternalId msgId
      enqueueCommand c corrId connId (Just server) . AClientCommand $ ACK msgId

deleteConnectionAsync' :: forall m. AgentMonad m => AgentClient -> ACorrId -> ConnId -> m ()
deleteConnectionAsync' c@AgentClient {subQ} corrId connId = withConnLock c connId "deleteConnectionAsync" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ (rq :| _) _ -> enqueueDelete rq
    RcvConnection _ rq -> enqueueDelete rq
    ContactConnection _ rq -> enqueueDelete rq
    SndConnection _ _ -> delete
    NewConnection _ -> delete
  where
    enqueueDelete :: RcvQueue -> m ()
    enqueueDelete RcvQueue {server} = do
      withStore' c $ \db -> setConnDeleted db connId
      disableConn c connId
      enqueueCommand c corrId connId (Just server) $ AInternalCommand ICDeleteConn
    delete :: m ()
    delete = withStore' c (`deleteConn` connId) >> atomically (writeTBQueue subQ (corrId, connId, OK))

-- | Add connection to the new receive queue
switchConnectionAsync' :: AgentMonad m => AgentClient -> ACorrId -> ConnId -> m ()
switchConnectionAsync' c corrId connId =
  withStore c (`getConn` connId) >>= \case
    SomeConn _ DuplexConnection {} -> enqueueCommand c corrId connId Nothing $ AClientCommand SWCH
    _ -> throwError $ CMD PROHIBITED

newConn :: AgentMonad m => AgentClient -> ConnId -> Bool -> Bool -> SConnectionMode c -> Maybe CRClientData -> m (ConnId, ConnectionRequestUri c)
newConn c connId asyncMode enableNtfs cMode clientData =
  getSMPServer c >>= newConnSrv c connId asyncMode enableNtfs cMode clientData

newConnSrv :: AgentMonad m => AgentClient -> ConnId -> Bool -> Bool -> SConnectionMode c -> Maybe CRClientData -> SMPServerWithAuth -> m (ConnId, ConnectionRequestUri c)
newConnSrv c connId asyncMode enableNtfs cMode clientData srv = do
  AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
  (q, qUri) <- newRcvQueue c "" srv smpClientVRange
  connId' <- setUpConn asyncMode q $ maxVersion smpAgentVRange
  let rq = (q :: RcvQueue) {connId = connId'}
  addSubscription c rq
  when enableNtfs $ do
    ns <- asks ntfSupervisor
    atomically $ sendNtfSubCommand ns (connId', NSCCreate)
  let crData = ConnReqUriData simplexChat smpAgentVRange [qUri] clientData
  case cMode of
    SCMContact -> pure (connId', CRContactUri crData)
    SCMInvitation -> do
      (pk1, pk2, e2eRcvParams) <- liftIO . CR.generateE2EParams $ maxVersion e2eEncryptVRange
      withStore' c $ \db -> createRatchetX3dhKeys db connId' pk1 pk2
      pure (connId', CRInvitationUri crData $ toVersionRangeT e2eRcvParams e2eEncryptVRange)
  where
    setUpConn True rq _ = do
      void . withStore c $ \db -> updateNewConnRcv db connId rq
      pure connId
    setUpConn False rq connAgentVersion = do
      g <- asks idsDrg
      let cData = ConnData {connId, connAgentVersion, enableNtfs, duplexHandshake = Nothing, deleted = False} -- connection mode is determined by the accepting agent
      withStore c $ \db -> createRcvConn db g cData rq cMode

joinConn :: AgentMonad m => AgentClient -> ConnId -> Bool -> Bool -> ConnectionRequestUri c -> ConnInfo -> m ConnId
joinConn c connId asyncMode enableNtfs cReq cInfo = do
  srv <- case cReq of
    CRInvitationUri ConnReqUriData {crSmpQueues = q :| _} _ ->
      getNextSMPServer c [qServer q]
    _ -> getSMPServer c
  joinConnSrv c connId asyncMode enableNtfs cReq cInfo srv

joinConnSrv :: AgentMonad m => AgentClient -> ConnId -> Bool -> Bool -> ConnectionRequestUri c -> ConnInfo -> SMPServerWithAuth -> m ConnId
joinConnSrv c connId asyncMode enableNtfs (CRInvitationUri ConnReqUriData {crAgentVRange, crSmpQueues = (qUri :| _)} e2eRcvParamsUri) cInfo srv = do
  AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
  case ( qUri `compatibleVersion` smpClientVRange,
         e2eRcvParamsUri `compatibleVersion` e2eEncryptVRange,
         crAgentVRange `compatibleVersion` smpAgentVRange
       ) of
    (Just qInfo, Just (Compatible e2eRcvParams@(CR.E2ERatchetParams _ _ rcDHRr)), Just aVersion@(Compatible connAgentVersion)) -> do
      (pk1, pk2, e2eSndParams) <- liftIO . CR.generateE2EParams $ version e2eRcvParams
      (_, rcDHRs) <- liftIO C.generateKeyPair'
      let rc = CR.initSndRatchet e2eEncryptVRange rcDHRr rcDHRs $ CR.x3dhSnd pk1 pk2 e2eRcvParams
      q <- newSndQueue "" qInfo
      let duplexHS = connAgentVersion /= 1
          cData = ConnData {connId, connAgentVersion, enableNtfs, duplexHandshake = Just duplexHS, deleted = False}
      connId' <- setUpConn asyncMode cData q rc
      let sq = (q :: SndQueue) {connId = connId'}
          cData' = (cData :: ConnData) {connId = connId'}
      tryError (confirmQueue aVersion c cData' sq srv cInfo $ Just e2eSndParams) >>= \case
        Right _ -> do
          unless duplexHS . void $ enqueueMessage c cData' sq SMP.noMsgFlags HELLO
          pure connId'
        Left e -> do
          -- TODO recovery for failure on network timeout, see rfcs/2022-04-20-smp-conf-timeout-recovery.md
          unless asyncMode $ withStore' c (`deleteConn` connId')
          throwError e
      where
        setUpConn True _ sq rc =
          withStore c $ \db -> runExceptT $ do
            void . ExceptT $ updateNewConnSnd db connId sq
            liftIO $ createRatchet db connId rc
            pure connId
        setUpConn False cData sq rc = do
          g <- asks idsDrg
          withStore c $ \db -> runExceptT $ do
            connId' <- ExceptT $ createSndConn db g cData sq
            liftIO $ createRatchet db connId' rc
            pure connId'
    _ -> throwError $ AGENT A_VERSION
joinConnSrv c connId False enableNtfs (CRContactUri ConnReqUriData {crAgentVRange, crSmpQueues = (qUri :| _)}) cInfo srv = do
  aVRange <- asks $ smpAgentVRange . config
  clientVRange <- asks $ smpClientVRange . config
  case ( qUri `compatibleVersion` clientVRange,
         crAgentVRange `compatibleVersion` aVRange
       ) of
    (Just qInfo, Just vrsn) -> do
      (connId', cReq) <- newConnSrv c connId False enableNtfs SCMInvitation Nothing srv
      sendInvitation c qInfo vrsn cReq cInfo
      pure connId'
    _ -> throwError $ AGENT A_VERSION
joinConnSrv _c _connId True _enableNtfs (CRContactUri _) _cInfo _srv = do
  throwError $ CMD PROHIBITED

createReplyQueue :: AgentMonad m => AgentClient -> ConnData -> SndQueue -> SMPServerWithAuth -> m SMPQueueInfo
createReplyQueue c ConnData {connId, enableNtfs} SndQueue {smpClientVersion} srv = do
  (rq, qUri) <- newRcvQueue c connId srv $ versionToRange smpClientVersion
  let qInfo = toVersionT qUri smpClientVersion
  addSubscription c rq
  void . withStore c $ \db -> upgradeSndConnToDuplex db connId rq
  when enableNtfs $ do
    ns <- asks ntfSupervisor
    atomically $ sendNtfSubCommand ns (connId, NSCCreate)
  pure qInfo

-- | Approve confirmation (LET command) in Reader monad
allowConnection' :: AgentMonad m => AgentClient -> ConnId -> ConfirmationId -> ConnInfo -> m ()
allowConnection' c connId confId ownConnInfo = withConnLock c connId "allowConnection" $ do
  withStore c (`getConn` connId) >>= \case
    SomeConn _ (RcvConnection _ rq@RcvQueue {server, rcvId, e2ePrivKey, smpClientVersion = v}) -> do
      senderKey <- withStore c $ \db -> runExceptT $ do
        AcceptedConfirmation {ratchetState, senderConf = SMPConfirmation {senderKey, e2ePubKey, smpClientVersion = v'}} <- ExceptT $ acceptConfirmation db confId ownConnInfo
        liftIO $ createRatchet db connId ratchetState
        let dhSecret = C.dh' e2ePubKey e2ePrivKey
        liftIO $ setRcvQueueConfirmedE2E db rq dhSecret $ min v v'
        pure senderKey
      enqueueCommand c "" connId (Just server) . AInternalCommand $ ICAllowSecure rcvId senderKey
    _ -> throwError $ CMD PROHIBITED

-- | Accept contact (ACPT command) in Reader monad
acceptContact' :: AgentMonad m => AgentClient -> ConnId -> Bool -> InvitationId -> ConnInfo -> m ConnId
acceptContact' c connId enableNtfs invId ownConnInfo = withConnLock c connId "acceptContact" $ do
  Invitation {contactConnId, connReq} <- withStore c (`getInvitation` invId)
  withStore c (`getConn` contactConnId) >>= \case
    SomeConn _ ContactConnection {} -> do
      withStore' c $ \db -> acceptInvitation db invId ownConnInfo
      joinConn c connId False enableNtfs connReq ownConnInfo `catchError` \err -> do
        withStore' c (`unacceptInvitation` invId)
        throwError err
    _ -> throwError $ CMD PROHIBITED

-- | Reject contact (RJCT command) in Reader monad
rejectContact' :: AgentMonad m => AgentClient -> ConnId -> InvitationId -> m ()
rejectContact' c contactConnId invId =
  withStore c $ \db -> deleteInvitation db contactConnId invId

-- | Subscribe to receive connection messages (SUB command) in Reader monad
subscribeConnection' :: forall m. AgentMonad m => AgentClient -> ConnId -> m ()
subscribeConnection' c connId = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  resumeConnCmds c connId
  case conn of
    DuplexConnection cData (rq :| rqs) sqs -> do
      mapM_ (resumeMsgDelivery c cData) sqs
      subscribe cData rq
      mapM_ (\q -> subscribeQueue c q `catchError` \_ -> pure ()) rqs
    SndConnection cData sq -> do
      resumeMsgDelivery c cData sq
      case status (sq :: SndQueue) of
        Confirmed -> pure ()
        Active -> throwError $ CONN SIMPLEX
        _ -> throwError $ INTERNAL "unexpected queue status"
    RcvConnection cData rq -> subscribe cData rq
    ContactConnection cData rq -> subscribe cData rq
    NewConnection _ -> pure ()
  where
    subscribe :: ConnData -> RcvQueue -> m ()
    subscribe ConnData {enableNtfs} rq = do
      subscribeQueue c rq
      ns <- asks ntfSupervisor
      atomically $ sendNtfSubCommand ns (connId, if enableNtfs then NSCCreate else NSCDelete)

type QSubResult = (QueueStatus, Either AgentErrorType ())

subscribeConnections' :: forall m. AgentMonad m => AgentClient -> [ConnId] -> m (Map ConnId (Either AgentErrorType ()))
subscribeConnections' _ [] = pure M.empty
subscribeConnections' c connIds = do
  conns :: Map ConnId (Either StoreError SomeConn) <- M.fromList . zip connIds <$> withStore' c (forM connIds . getConn)
  let (errs, cs) = M.mapEither id conns
      errs' = M.map (Left . storeError) errs
      (subRs, rcvQs) = M.mapEither rcvQueueOrResult cs
      srvRcvQs :: Map SMPServer [RcvQueue] = M.foldl' (foldl' addRcvQueue) M.empty rcvQs
  mapM_ (mapM_ (\(cData, sqs) -> mapM_ (resumeMsgDelivery c cData) sqs) . sndQueue) cs
  mapM_ (resumeConnCmds c) $ M.keys cs
  rcvRs <- connResults . concat <$> mapConcurrently subscribe (M.assocs srvRcvQs)
  ns <- asks ntfSupervisor
  tkn <- readTVarIO (ntfTkn ns)
  when (instantNotifications tkn) . void . forkIO $ sendNtfCreate ns rcvRs conns
  let rs = M.unions ([errs', subRs, rcvRs] :: [Map ConnId (Either AgentErrorType ())])
  notifyResultError rs
  pure rs
  where
    rcvQueueOrResult :: SomeConn -> Either (Either AgentErrorType ()) (NonEmpty RcvQueue)
    rcvQueueOrResult (SomeConn _ conn) = case conn of
      DuplexConnection _ rqs _ -> Right rqs
      SndConnection _ sq -> Left $ sndSubResult sq
      RcvConnection _ rq -> Right [rq]
      ContactConnection _ rq -> Right [rq]
      NewConnection _ -> Left (Right ())
    sndSubResult :: SndQueue -> Either AgentErrorType ()
    sndSubResult sq = case status (sq :: SndQueue) of
      Confirmed -> Right ()
      Active -> Left $ CONN SIMPLEX
      _ -> Left $ INTERNAL "unexpected queue status"
    addRcvQueue :: Map SMPServer [RcvQueue] -> RcvQueue -> Map SMPServer [RcvQueue]
    addRcvQueue m rq@RcvQueue {server} = M.alter (Just . maybe [rq] (rq :)) server m
    subscribe :: (SMPServer, [RcvQueue]) -> m [(RcvQueue, Either AgentErrorType ())]
    subscribe (srv, qs) = snd <$> subscribeQueues c srv qs
    connResults :: [(RcvQueue, Either AgentErrorType ())] -> Map ConnId (Either AgentErrorType ())
    connResults = M.map snd . foldl' addResult M.empty
      where
        -- collects results by connection ID
        addResult :: Map ConnId QSubResult -> (RcvQueue, Either AgentErrorType ()) -> Map ConnId QSubResult
        addResult rs (RcvQueue {connId, status}, r) = M.alter (combineRes (status, r)) connId rs
        -- combines two results for one connection, by using only Active queues (if there is at least one Active queue)
        combineRes :: QSubResult -> Maybe QSubResult -> Maybe QSubResult
        combineRes r' (Just r) = Just $ if order r <= order r' then r else r'
        combineRes r' _ = Just r'
        order :: QSubResult -> Int
        order (Active, Right _) = 1
        order (Active, _) = 2
        order (_, Right _) = 3
        order _ = 4
    sendNtfCreate :: NtfSupervisor -> Map ConnId (Either AgentErrorType ()) -> Map ConnId (Either StoreError SomeConn) -> m ()
    sendNtfCreate ns rcvRs conns =
      forM_ (M.assocs rcvRs) $ \case
        (connId, Right _) -> forM_ (M.lookup connId conns) $ \case
          Right (SomeConn _ conn) -> do
            let cmd = if enableNtfs $ connData conn then NSCCreate else NSCDelete
            atomically $ writeTBQueue (ntfSubQ ns) (connId, cmd)
          _ -> pure ()
        _ -> pure ()
    sndQueue :: SomeConn -> Maybe (ConnData, NonEmpty SndQueue)
    sndQueue (SomeConn _ conn) = case conn of
      DuplexConnection cData _ sqs -> Just (cData, sqs)
      SndConnection cData sq -> Just (cData, [sq])
      _ -> Nothing
    notifyResultError :: Map ConnId (Either AgentErrorType ()) -> m ()
    notifyResultError rs = do
      let actual = M.size rs
          expected = length connIds
      when (actual /= expected) . atomically $
        writeTBQueue (subQ c) ("", "", ERR . INTERNAL $ "subscribeConnections result size: " <> show actual <> ", expected " <> show expected)

resubscribeConnection' :: AgentMonad m => AgentClient -> ConnId -> m ()
resubscribeConnection' c connId =
  unlessM
    (atomically $ hasActiveSubscription c connId)
    (subscribeConnection' c connId)

resubscribeConnections' :: forall m. AgentMonad m => AgentClient -> [ConnId] -> m (Map ConnId (Either AgentErrorType ()))
resubscribeConnections' _ [] = pure M.empty
resubscribeConnections' c connIds = do
  let r = M.fromList . zip connIds . repeat $ Right ()
  connIds' <- filterM (fmap not . atomically . hasActiveSubscription c) connIds
  -- union is left-biased, so results returned by subscribeConnections' take precedence
  (`M.union` r) <$> subscribeConnections' c connIds'

getConnectionMessage' :: AgentMonad m => AgentClient -> ConnId -> m (Maybe SMPMsgMeta)
getConnectionMessage' c connId = do
  whenM (atomically $ hasActiveSubscription c connId) . throwError $ CMD PROHIBITED
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ (rq :| _) _ -> getQueueMessage c rq
    RcvConnection _ rq -> getQueueMessage c rq
    ContactConnection _ rq -> getQueueMessage c rq
    SndConnection _ _ -> throwError $ CONN SIMPLEX
    NewConnection _ -> throwError $ CMD PROHIBITED

getNotificationMessage' :: forall m. AgentMonad m => AgentClient -> C.CbNonce -> ByteString -> m (NotificationInfo, [SMPMsgMeta])
getNotificationMessage' c nonce encNtfInfo = do
  withStore' c getActiveNtfToken >>= \case
    Just NtfToken {ntfDhSecret = Just dhSecret} -> do
      ntfData <- agentCbDecrypt dhSecret nonce encNtfInfo
      PNMessageData {smpQueue, ntfTs, nmsgNonce, encNMsgMeta} <- liftEither (parse strP (INTERNAL "error parsing PNMessageData") ntfData)
      (ntfConnId, rcvNtfDhSecret) <- withStore c (`getNtfRcvQueue` smpQueue)
      ntfMsgMeta <- (eitherToMaybe . smpDecode <$> agentCbDecrypt rcvNtfDhSecret nmsgNonce encNMsgMeta) `catchError` \_ -> pure Nothing
      maxMsgs <- asks $ ntfMaxMessages . config
      (NotificationInfo {ntfConnId, ntfTs, ntfMsgMeta},) <$> getNtfMessages ntfConnId maxMsgs ntfMsgMeta []
    _ -> throwError $ CMD PROHIBITED
  where
    getNtfMessages ntfConnId maxMs nMeta ms
      | length ms < maxMs =
        getConnectionMessage' c ntfConnId >>= \case
          Just m@SMP.SMPMsgMeta {msgId, msgTs, msgFlags} -> case nMeta of
            Just SMP.NMsgMeta {msgId = msgId', msgTs = msgTs'}
              | msgId == msgId' || msgTs > msgTs' -> pure $ reverse (m : ms)
              | otherwise -> getMsg (m : ms)
            _
              | SMP.notification msgFlags -> pure $ reverse (m : ms)
              | otherwise -> getMsg (m : ms)
          _ -> pure $ reverse ms
      | otherwise = pure $ reverse ms
      where
        getMsg = getNtfMessages ntfConnId maxMs nMeta

-- | Send message to the connection (SEND command) in Reader monad
sendMessage' :: forall m. AgentMonad m => AgentClient -> ConnId -> MsgFlags -> MsgBody -> m AgentMsgId
sendMessage' c connId msgFlags msg = withConnLock c connId "sendMessage" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection cData _ sqs -> enqueueMsgs cData sqs
    SndConnection cData sq -> enqueueMsgs cData [sq]
    _ -> throwError $ CONN SIMPLEX
  where
    enqueueMsgs :: ConnData -> NonEmpty SndQueue -> m AgentMsgId
    enqueueMsgs cData sqs = enqueueMessages c cData sqs msgFlags $ A_MSG msg

-- / async command processing v v v

enqueueCommand :: AgentMonad m => AgentClient -> ACorrId -> ConnId -> Maybe SMPServer -> AgentCommand -> m ()
enqueueCommand c corrId connId server aCommand = do
  resumeSrvCmds c server
  commandId <- withStore' c $ \db -> createCommand db corrId connId server aCommand
  queuePendingCommands c server [commandId]

resumeSrvCmds :: forall m. AgentMonad m => AgentClient -> Maybe SMPServer -> m ()
resumeSrvCmds c server =
  unlessM (cmdProcessExists c server) $
    async (runCommandProcessing c server)
      >>= \a -> atomically (TM.insert server a $ asyncCmdProcesses c)

resumeConnCmds :: forall m. AgentMonad m => AgentClient -> ConnId -> m ()
resumeConnCmds c connId =
  unlessM connQueued $
    withStore' c (`getPendingCommands` connId)
      >>= mapM_ (uncurry enqueueConnCmds)
  where
    enqueueConnCmds srv cmdIds = do
      resumeSrvCmds c srv
      queuePendingCommands c srv cmdIds
    connQueued = atomically $ isJust <$> TM.lookupInsert connId True (connCmdsQueued c)

cmdProcessExists :: AgentMonad m => AgentClient -> Maybe SMPServer -> m Bool
cmdProcessExists c srv = atomically $ TM.member srv (asyncCmdProcesses c)

queuePendingCommands :: AgentMonad m => AgentClient -> Maybe SMPServer -> [AsyncCmdId] -> m ()
queuePendingCommands c server cmdIds = atomically $ do
  q <- getPendingCommandQ c server
  mapM_ (writeTQueue q) cmdIds

getPendingCommandQ :: AgentClient -> Maybe SMPServer -> STM (TQueue AsyncCmdId)
getPendingCommandQ c server = do
  maybe newMsgQueue pure =<< TM.lookup server (asyncCmdQueues c)
  where
    newMsgQueue = do
      cq <- newTQueue
      TM.insert server cq $ asyncCmdQueues c
      pure cq

runCommandProcessing :: forall m. AgentMonad m => AgentClient -> Maybe SMPServer -> m ()
runCommandProcessing c@AgentClient {subQ} server_ = do
  cq <- atomically $ getPendingCommandQ c server_
  ri <- asks $ messageRetryInterval . config -- different retry interval?
  forever $ do
    atomically $ endAgentOperation c AOSndNetwork
    atomically $ throwWhenInactive c
    cmdId <- atomically $ readTQueue cq
    atomically $ beginAgentOperation c AOSndNetwork
    E.try (withStore c $ \db -> getPendingCommand db cmdId) >>= \case
      Left (e :: E.SomeException) -> atomically $ writeTBQueue subQ ("", "", ERR . INTERNAL $ show e)
      Right (corrId, connId, cmd) -> processCmd ri corrId connId cmdId cmd
  where
    processCmd :: RetryInterval -> ACorrId -> ConnId -> AsyncCmdId -> AgentCommand -> m ()
    processCmd ri corrId connId cmdId command = case command of
      AClientCommand cmd -> case cmd of
        NEW enableNtfs (ACM cMode) -> noServer $ do
          usedSrvs <- newTVarIO ([] :: [SMPServer])
          tryCommand . withNextSrv usedSrvs [] $ \srv -> do
            (_, cReq) <- newConnSrv c connId True enableNtfs cMode Nothing srv
            notify $ INV (ACR cMode cReq)
        JOIN enableNtfs (ACR _ cReq@(CRInvitationUri ConnReqUriData {crSmpQueues = q :| _} _)) connInfo -> noServer $ do
          let initUsed = [qServer q]
          usedSrvs <- newTVarIO initUsed
          tryCommand . withNextSrv usedSrvs initUsed $ \srv -> do
            void $ joinConnSrv c connId True enableNtfs cReq connInfo srv
            notify OK
        LET confId ownCInfo -> withServer' . tryCommand $ allowConnection' c connId confId ownCInfo >> notify OK
        ACK msgId -> withServer' . tryCommand $ ackMessage' c connId msgId >> notify OK
        SWCH -> noServer $ tryCommand $ switchConnection' c connId >>= notify . SWITCH QDRcv SPStarted
        DEL -> withServer' . tryCommand $ deleteConnection' c connId >> notify OK
        _ -> notify $ ERR $ INTERNAL $ "unsupported async command " <> show (aCommandTag cmd)
      AInternalCommand cmd -> case cmd of
        ICAckDel rId srvMsgId msgId -> withServer $ \srv -> tryWithLock "ICAckDel" $ ack srv rId srvMsgId >> withStore' c (\db -> deleteMsg db connId msgId)
        ICAck rId srvMsgId -> withServer $ \srv -> tryWithLock "ICAck" $ ack srv rId srvMsgId
        ICAllowSecure _rId senderKey -> withServer' . tryWithLock "ICAllowSecure" $ do
          (SomeConn _ conn, AcceptedConfirmation {senderConf, ownConnInfo}) <-
            withStore c $ \db -> runExceptT $ (,) <$> ExceptT (getConn db connId) <*> ExceptT (getAcceptedConfirmation db connId)
          case conn of
            RcvConnection cData rq -> do
              secure rq senderKey
              mapM_ (connectReplyQueues c cData ownConnInfo) (L.nonEmpty $ smpReplyQueues senderConf)
            _ -> throwError $ INTERNAL $ "incorrect connection type " <> show (internalCmdTag cmd)
        ICDuplexSecure _rId senderKey -> withServer' . tryWithLock "ICDuplexSecure" . withDuplexConn $ \(DuplexConnection cData (rq :| _) (sq :| _)) -> do
          secure rq senderKey
          when (duplexHandshake cData == Just True) . void $
            enqueueMessage c cData sq SMP.MsgFlags {notification = True} HELLO
        ICDeleteConn ->
          withServer $ \srv -> tryWithLock "ICDeleteConn" $ do
            SomeConn _ conn <- withStore c $ \db -> getAnyConn db connId True
            case conn of
              DuplexConnection _ (rq :| rqs) _ -> delete srv rq $ case rqs of
                [] -> notify OK
                RcvQueue {server = srv'} : _ -> enqueue srv'
              RcvConnection _ rq -> delete srv rq $ notify OK
              ContactConnection _ rq -> delete srv rq $ notify OK
              _ -> internalErr "command requires connection with rcv queue"
          where
            delete :: SMPServer -> RcvQueue -> m () -> m ()
            delete srv rq@RcvQueue {server} next
              | sameSrvAddr srv server = deleteConnQueue c rq >> next
              | otherwise = enqueue server
            enqueue :: SMPServer -> m ()
            enqueue srv = enqueueCommand c corrId connId (Just srv) $ AInternalCommand ICDeleteConn
        ICQSecure rId senderKey ->
          withServer $ \srv -> tryWithLock "ICQSecure" . withDuplexConn $ \(DuplexConnection cData rqs sqs) ->
            case find (sameQueue (srv, rId)) rqs of
              Just rq'@RcvQueue {server, sndId, status} -> when (status == Confirmed) $ do
                secureQueue c rq' senderKey
                withStore' c $ \db -> setRcvQueueStatus db rq' Secured
                void . enqueueMessages c cData sqs SMP.noMsgFlags $ QUSE [((server, sndId), True)]
              _ -> internalErr "ICQSecure: queue address not found in connection"
        ICQDelete rId -> do
          withServer $ \srv -> tryWithLock "ICQDelete" . withDuplexConn $ \(DuplexConnection cData rqs sqs) -> do
            case removeQ (srv, rId) rqs of
              Nothing -> internalErr "ICQDelete: queue address not found in connection"
              Just (rq'@RcvQueue {primary}, rq'' : rqs')
                | primary -> internalErr "ICQDelete: cannot delete primary rcv queue"
                | otherwise -> do
                  deleteQueue c rq'
                  withStore' c $ \db -> deleteConnRcvQueue db connId rq'
                  when (enableNtfs cData) $ do
                    ns <- asks ntfSupervisor
                    atomically $ sendNtfSubCommand ns (connId, NSCCreate)
                  let conn' = DuplexConnection cData (rq'' :| rqs') sqs
                  notify $ SWITCH QDRcv SPCompleted $ connectionStats conn'
              _ -> internalErr "ICQDelete: cannot delete the only queue in connection"
        where
          ack srv rId srvMsgId = do
            rq <- withStore c $ \db -> getRcvQueue db connId srv rId
            ackQueueMessage c rq srvMsgId
          secure :: RcvQueue -> SMP.SndPublicVerifyKey -> m ()
          secure rq senderKey = do
            secureQueue c rq senderKey
            withStore' c $ \db -> setRcvQueueStatus db rq Secured
      where
        withServer a = case server_ of
          Just srv -> a srv
          _ -> internalErr "command requires server"
        withServer' = withServer . const
        noServer a = case server_ of
          Nothing -> a
          _ -> internalErr "command requires no server"
        withDuplexConn :: (Connection 'CDuplex -> m ()) -> m ()
        withDuplexConn a =
          withStore c (`getConn` connId) >>= \case
            SomeConn _ conn@DuplexConnection {} -> a conn
            _ -> internalErr "command requires duplex connection"
        tryCommand action = withRetryInterval ri $ \loop ->
          tryError action >>= \case
            Left e
              | temporaryAgentError e || e == BROKER HOST -> retrySndOp c loop
              | otherwise -> cmdError e
            Right () -> withStore' c (`deleteCommand` cmdId)
        tryWithLock name = tryCommand . withConnLock c connId name
        internalErr s = cmdError $ INTERNAL $ s <> ": " <> show (agentCommandTag command)
        cmdError e = notify (ERR e) >> withStore' c (`deleteCommand` cmdId)
        notify cmd = atomically $ writeTBQueue subQ (corrId, connId, cmd)
        withNextSrv :: TVar [SMPServer] -> [SMPServer] -> (SMPServerWithAuth -> m ()) -> m ()
        withNextSrv usedSrvs initUsed action = do
          used <- readTVarIO usedSrvs
          srvAuth@(ProtoServerWithAuth srv _) <- getNextSMPServer c used
          atomically $ do
            srvs <- readTVar $ smpServers c
            let used' = if length used + 1 >= L.length srvs then initUsed else srv : used
            writeTVar usedSrvs used'
          action srvAuth
-- ^ ^ ^ async command processing /

enqueueMessages :: AgentMonad m => AgentClient -> ConnData -> NonEmpty SndQueue -> MsgFlags -> AMessage -> m AgentMsgId
enqueueMessages c cData (sq :| sqs) msgFlags aMessage = do
  msgId <- enqueueMessage c cData sq msgFlags aMessage
  mapM_ (enqueueSavedMessage c cData msgId) $
    filter (\SndQueue {status} -> status == Secured || status == Active) sqs
  pure msgId

enqueueMessage :: forall m. AgentMonad m => AgentClient -> ConnData -> SndQueue -> MsgFlags -> AMessage -> m AgentMsgId
enqueueMessage c cData@ConnData {connId, connAgentVersion} sq msgFlags aMessage = do
  resumeMsgDelivery c cData sq
  msgId <- storeSentMsg
  queuePendingMsgs c sq [msgId]
  pure $ unId msgId
  where
    storeSentMsg :: m InternalId
    storeSentMsg = withStore c $ \db -> runExceptT $ do
      internalTs <- liftIO getCurrentTime
      (internalId, internalSndId, prevMsgHash) <- liftIO $ updateSndIds db connId
      let privHeader = APrivHeader (unSndId internalSndId) prevMsgHash
          agentMsg = AgentMessage privHeader aMessage
          agentMsgStr = smpEncode agentMsg
          internalHash = C.sha256Hash agentMsgStr
      encAgentMessage <- agentRatchetEncrypt db connId agentMsgStr e2eEncUserMsgLength
      let msgBody = smpEncode $ AgentMsgEnvelope {agentVersion = connAgentVersion, encAgentMessage}
          msgType = agentMessageType agentMsg
          msgData = SndMsgData {internalId, internalSndId, internalTs, msgType, msgFlags, msgBody, internalHash, prevMsgHash}
      liftIO $ createSndMsg db connId msgData
      liftIO $ createSndMsgDelivery db connId sq internalId
      pure internalId

enqueueSavedMessage :: AgentMonad m => AgentClient -> ConnData -> AgentMsgId -> SndQueue -> m ()
enqueueSavedMessage c cData@ConnData {connId} msgId sq = do
  resumeMsgDelivery c cData sq
  let mId = InternalId msgId
  queuePendingMsgs c sq [mId]
  withStore' c $ \db -> createSndMsgDelivery db connId sq mId

resumeMsgDelivery :: forall m. AgentMonad m => AgentClient -> ConnData -> SndQueue -> m ()
resumeMsgDelivery c cData@ConnData {connId} sq@SndQueue {server, sndId} = do
  let qKey = (server, sndId)
  unlessM (queueDelivering qKey) $
    async (runSmpQueueMsgDelivery c cData sq)
      >>= \a -> atomically (TM.insert qKey a $ smpQueueMsgDeliveries c)
  unlessM msgsQueued $
    withStore' c (\db -> getPendingMsgs db connId sq)
      >>= queuePendingMsgs c sq
  where
    queueDelivering qKey = atomically $ TM.member qKey (smpQueueMsgDeliveries c)
    msgsQueued = atomically $ isJust <$> TM.lookupInsert (server, sndId) True (pendingMsgsQueued c)

queuePendingMsgs :: AgentMonad m => AgentClient -> SndQueue -> [InternalId] -> m ()
queuePendingMsgs c sq msgIds = atomically $ do
  modifyTVar' (msgDeliveryOp c) $ \s -> s {opsInProgress = opsInProgress s + length msgIds}
  -- s <- readTVar (msgDeliveryOp c)
  -- unsafeIOToSTM $ putStrLn $ "msgDeliveryOp: " <> show (opsInProgress s)
  q <- getPendingMsgQ c sq
  mapM_ (writeTQueue q) msgIds

getPendingMsgQ :: AgentClient -> SndQueue -> STM (TQueue InternalId)
getPendingMsgQ c SndQueue {server, sndId} = do
  let qKey = (server, sndId)
  maybe (newMsgQueue qKey) pure =<< TM.lookup qKey (smpQueueMsgQueues c)
  where
    newMsgQueue qKey = do
      mq <- newTQueue
      TM.insert qKey mq $ smpQueueMsgQueues c
      pure mq

runSmpQueueMsgDelivery :: forall m. AgentMonad m => AgentClient -> ConnData -> SndQueue -> m ()
runSmpQueueMsgDelivery c@AgentClient {subQ} cData@ConnData {connId, duplexHandshake} sq = do
  mq <- atomically $ getPendingMsgQ c sq
  ri <- asks $ messageRetryInterval . config
  forever $ do
    atomically $ endAgentOperation c AOSndNetwork
    atomically $ throwWhenInactive c
    atomically $ throwWhenNoDelivery c sq
    msgId <- atomically $ readTQueue mq
    atomically $ beginAgentOperation c AOSndNetwork
    atomically $ endAgentOperation c AOMsgDelivery
    let mId = unId msgId
    E.try (withStore c $ \db -> getPendingMsgData db connId msgId) >>= \case
      Left (e :: E.SomeException) ->
        notify $ MERR mId (INTERNAL $ show e)
      Right (rq_, PendingMsgData {msgType, msgBody, msgFlags, internalTs}) ->
        withRetryInterval ri $ \loop -> do
          resp <- tryError $ case msgType of
            AM_CONN_INFO -> sendConfirmation c sq msgBody
            _ -> sendAgentMessage c sq msgFlags msgBody
          case resp of
            Left e -> do
              let err = if msgType == AM_A_MSG_ then MERR mId e else ERR e
              case e of
                SMP SMP.QUOTA -> case msgType of
                  AM_CONN_INFO -> connError msgId NOT_AVAILABLE
                  AM_CONN_INFO_REPLY -> connError msgId NOT_AVAILABLE
                  _ -> retrySndOp c loop
                SMP SMP.AUTH -> case msgType of
                  AM_CONN_INFO -> connError msgId NOT_AVAILABLE
                  AM_CONN_INFO_REPLY -> connError msgId NOT_AVAILABLE
                  AM_HELLO_
                    -- in duplexHandshake mode (v2) HELLO is only sent once, without retrying,
                    -- because the queue must be secured by the time the confirmation or the first HELLO is received
                    | duplexHandshake == Just True -> connErr
                    | otherwise ->
                      ifM (msgExpired helloTimeout) connErr (retrySndOp c loop)
                    where
                      connErr = case rq_ of
                        -- party initiating connection
                        Just _ -> connError msgId NOT_AVAILABLE
                        -- party joining connection
                        _ -> connError msgId NOT_ACCEPTED
                  AM_REPLY_ -> notifyDel msgId err
                  AM_A_MSG_ -> notifyDel msgId err
                  AM_QADD_ -> qError msgId "QADD: AUTH"
                  AM_QKEY_ -> qError msgId "QKEY: AUTH"
                  AM_QUSE_ -> qError msgId "QUSE: AUTH"
                  AM_QTEST_ -> qError msgId "QTEST: AUTH"
                _
                  -- for other operations BROKER HOST is treated as a permanent error (e.g., when connecting to the server),
                  -- the message sending would be retried
                  | temporaryAgentError e || e == BROKER HOST -> do
                    let timeoutSel = if msgType == AM_HELLO_ then helloTimeout else messageTimeout
                    ifM (msgExpired timeoutSel) (notifyDel msgId err) (retrySndOp c loop)
                  | otherwise -> notifyDel msgId err
              where
                msgExpired timeoutSel = do
                  msgTimeout <- asks $ timeoutSel . config
                  currentTime <- liftIO getCurrentTime
                  pure $ diffUTCTime currentTime internalTs > msgTimeout
            Right () -> do
              case msgType of
                AM_CONN_INFO -> do
                  withStore' c $ \db -> do
                    setSndQueueStatus db sq Confirmed
                    when (isJust rq_) $ removeConfirmations db connId
                  -- TODO possibly notification flag should be ON for one of the parties, to result in contact connected notification
                  unless (duplexHandshake == Just True) . void $ enqueueMessage c cData sq SMP.noMsgFlags HELLO
                AM_CONN_INFO_REPLY -> pure ()
                AM_REPLY_ -> pure ()
                AM_HELLO_ -> do
                  withStore' c $ \db -> setSndQueueStatus db sq Active
                  case rq_ of
                    -- party initiating connection (in v1)
                    Just RcvQueue {status} ->
                      -- it is unclear why subscribeQueue was needed here,
                      -- message delivery can only be enabled for queues that were created in the current session or subscribed
                      -- subscribeQueue c rq connId
                      --
                      -- If initiating party were to send CON to the user without waiting for reply HELLO (to reduce handshake time),
                      -- it would lead to the non-deterministic internal ID of the first sent message, at to some other race conditions,
                      -- because it can be sent before HELLO is received
                      -- With `status == Active` condition, CON is sent here only by the accepting party, that previously received HELLO
                      when (status == Active) $ notify CON
                    -- Party joining connection sends REPLY after HELLO in v1,
                    -- it is an error to send REPLY in duplexHandshake mode (v2),
                    -- and this branch should never be reached as receive is created before the confirmation,
                    -- so the condition is not necessary here, strictly speaking.
                    _ -> unless (duplexHandshake == Just True) $ do
                      srv <- getSMPServer c
                      qInfo <- createReplyQueue c cData sq srv
                      void . enqueueMessage c cData sq SMP.noMsgFlags $ REPLY [qInfo]
                AM_A_MSG_ -> notify $ SENT mId
                AM_QADD_ -> pure ()
                AM_QKEY_ -> pure ()
                AM_QUSE_ -> pure ()
                AM_QTEST_ -> do
                  withStore' c $ \db -> setSndQueueStatus db sq Active
                  SomeConn _ conn <- withStore c (`getConn` connId)
                  case conn of
                    DuplexConnection cData' rqs sqs -> do
                      -- remove old snd queue from connection once QTEST is sent to the new queue
                      case findQ (qAddress sq) sqs of
                        -- this is the same queue where this loop delivers messages to but with updated state
                        Just SndQueue {dbReplaceQueueId = Just replacedId, primary} ->
                          case removeQP (\SndQueue {dbQueueId} -> dbQueueId == replacedId) sqs of
                            Nothing -> internalErr msgId "sent QTEST: queue not found in connection"
                            Just (sq', sq'' : sqs') -> do
                              -- remove the delivery from the map to stop the thread when the delivery loop is complete
                              atomically $ TM.delete (qAddress sq') $ smpQueueMsgQueues c
                              withStore' c $ \db -> do
                                when primary $ setSndQueuePrimary db connId sq'
                                deletePendingMsgs db connId sq'
                                deleteConnSndQueue db connId sq'
                              let sqs'' = sq'' :| sqs'
                                  conn' = DuplexConnection cData' rqs sqs''
                              notify . SWITCH QDSnd SPCompleted $ connectionStats conn'
                            _ -> internalErr msgId "sent QTEST: there is only one queue in connection"
                        _ -> internalErr msgId "sent QTEST: queue not in connection or not replacing another queue"
                    _ -> internalErr msgId "QTEST sent not in duplex connection"
              delMsg msgId
  where
    delMsg :: InternalId -> m ()
    delMsg msgId = withStore' c $ \db -> deleteSndMsgDelivery db connId sq msgId
    notify :: ACommand 'Agent -> m ()
    notify cmd = atomically $ writeTBQueue subQ ("", connId, cmd)
    notifyDel :: InternalId -> ACommand 'Agent -> m ()
    notifyDel msgId cmd = notify cmd >> delMsg msgId
    connError msgId = notifyDel msgId . ERR . CONN
    qError msgId = notifyDel msgId . ERR . AGENT . A_QUEUE
    internalErr msgId = notifyDel msgId . ERR . INTERNAL

retrySndOp :: AgentMonad m => AgentClient -> m () -> m ()
retrySndOp c loop = do
  -- end... is in a separate atomically because if begin... blocks, SUSPENDED won't be sent
  atomically $ endAgentOperation c AOSndNetwork
  atomically $ throwWhenInactive c
  atomically $ beginAgentOperation c AOSndNetwork
  loop

ackMessage' :: forall m. AgentMonad m => AgentClient -> ConnId -> AgentMsgId -> m ()
ackMessage' c connId msgId = withConnLock c connId "ackMessage" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection {} -> ack
    RcvConnection {} -> ack
    SndConnection {} -> throwError $ CONN SIMPLEX
    ContactConnection {} -> throwError $ CMD PROHIBITED
    NewConnection _ -> throwError $ CMD PROHIBITED
  where
    ack :: m ()
    ack = do
      let mId = InternalId msgId
      (rq, srvMsgId) <- withStore c $ \db -> setMsgUserAck db connId mId
      ackQueueMessage c rq srvMsgId
      withStore' c $ \db -> deleteMsg db connId mId

switchConnection' :: AgentMonad m => AgentClient -> ConnId -> m ConnectionStats
switchConnection' c connId = withConnLock c connId "switchConnection" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection cData rqs@(rq@RcvQueue {server, dbQueueId, sndId} :| rqs_) sqs -> do
      clientVRange <- asks $ smpClientVRange . config
      -- try to get the server that is different from all queues, or at least from the primary rcv queue
      srvAuth@(ProtoServerWithAuth srv _) <- getNextSMPServer c $ map qServer (L.toList rqs) <> map qServer (L.toList sqs)
      srv' <- if srv == server then getNextSMPServer c [server] else pure srvAuth
      (q, qUri) <- newRcvQueue c connId srv' clientVRange
      let rq' = (q :: RcvQueue) {primary = True, dbReplaceQueueId = Just dbQueueId}
      void . withStore c $ \db -> addConnRcvQueue db connId rq'
      addSubscription c rq'
      void . enqueueMessages c cData sqs SMP.noMsgFlags $ QADD [(qUri, Just (server, sndId))]
      pure . connectionStats $ DuplexConnection cData (rq <| rq' :| rqs_) sqs
    _ -> throwError $ CMD PROHIBITED

ackQueueMessage :: AgentMonad m => AgentClient -> RcvQueue -> SMP.MsgId -> m ()
ackQueueMessage c rq srvMsgId =
  sendAck c rq srvMsgId `catchError` \case
    SMP SMP.NO_MSG -> pure ()
    e -> throwError e

-- | Suspend SMP agent connection (OFF command) in Reader monad
suspendConnection' :: AgentMonad m => AgentClient -> ConnId -> m ()
suspendConnection' c connId = withConnLock c connId "suspendConnection" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ rqs _ -> mapM_ (suspendQueue c) rqs
    RcvConnection _ rq -> suspendQueue c rq
    ContactConnection _ rq -> suspendQueue c rq
    SndConnection _ _ -> throwError $ CONN SIMPLEX
    NewConnection _ -> throwError $ CMD PROHIBITED

-- | Delete SMP agent connection (DEL command) in Reader monad
deleteConnection' :: forall m. AgentMonad m => AgentClient -> ConnId -> m ()
deleteConnection' c connId = withConnLock c connId "deleteConnection" $ do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection _ rqs _ -> mapM_ (deleteConnQueue c) rqs >> disableConn c connId >> deleteConn'
    RcvConnection _ rq -> delete rq
    ContactConnection _ rq -> delete rq
    SndConnection _ _ -> deleteConn'
    NewConnection _ -> deleteConn'
  where
    delete :: RcvQueue -> m ()
    delete rq = deleteConnQueue c rq >> disableConn c connId >> deleteConn'
    deleteConn' = withStore' c (`deleteConn` connId)

deleteConnQueue :: AgentMonad m => AgentClient -> RcvQueue -> m ()
deleteConnQueue c rq@RcvQueue {connId} = do
  deleteQueue c rq
  withStore' c $ \db -> deleteConnRcvQueue db connId rq

disableConn :: AgentMonad m => AgentClient -> ConnId -> m ()
disableConn c connId = do
  atomically $ removeSubscription c connId
  ns <- asks ntfSupervisor
  atomically $ writeTBQueue (ntfSubQ ns) (connId, NSCDelete)

getConnectionServers' :: AgentMonad m => AgentClient -> ConnId -> m ConnectionStats
getConnectionServers' c connId = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  pure $ connectionStats conn

connectionStats :: Connection c -> ConnectionStats
connectionStats = \case
  RcvConnection _ rq -> ConnectionStats {rcvServers = [qServer rq], sndServers = []}
  SndConnection _ sq -> ConnectionStats {rcvServers = [], sndServers = [qServer sq]}
  DuplexConnection _ rqs sqs -> ConnectionStats {rcvServers = map qServer $ L.toList rqs, sndServers = map qServer $ L.toList sqs}
  ContactConnection _ rq -> ConnectionStats {rcvServers = [qServer rq], sndServers = []}
  NewConnection _ -> ConnectionStats {rcvServers = [], sndServers = []}

-- | Change servers to be used for creating new queues, in Reader monad
setSMPServers' :: AgentMonad m => AgentClient -> NonEmpty SMPServerWithAuth -> m ()
setSMPServers' c = atomically . writeTVar (smpServers c)

registerNtfToken' :: forall m. AgentMonad m => AgentClient -> DeviceToken -> NotificationsMode -> m NtfTknStatus
registerNtfToken' c suppliedDeviceToken suppliedNtfMode =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId, ntfTknStatus, ntfTknAction, ntfMode = savedNtfMode} -> do
      status <- case (ntfTokenId, ntfTknAction) of
        (Nothing, Just NTARegister) -> do
          when (savedDeviceToken /= suppliedDeviceToken) $ withStore' c $ \db -> updateDeviceToken db tkn suppliedDeviceToken
          registerToken tkn $> NTRegistered
        -- TODO minimal time before repeat registration
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
              when (suppliedNtfMode == NMPeriodic && savedNtfMode == NMInstant) $ deleteNtfSubs c NSCDelete
            pure ntfTknStatus -- TODO
            -- agentNtfCheckToken c tknId tkn >>= \case
          | otherwise -> replaceToken tknId
        (Just tknId, Just NTADelete) -> do
          agentNtfDeleteToken c tknId tkn
          withStore' c (`removeNtfToken` tkn)
          ns <- asks ntfSupervisor
          atomically $ nsRemoveNtfToken ns
          pure NTExpired
        _ -> pure ntfTknStatus
      withStore' c $ \db -> updateNtfMode db tkn suppliedNtfMode
      pure status
      where
        replaceToken :: NtfTokenId -> m NtfTknStatus
        replaceToken tknId = do
          ns <- asks ntfSupervisor
          tryReplace ns `catchError` \e ->
            if temporaryAgentError e || e == BROKER HOST
              then throwError e
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
    createToken :: m NtfTknStatus
    createToken =
      getNtfServer c >>= \case
        Just ntfServer ->
          asks (cmdSignAlg . config) >>= \case
            C.SignAlg a -> do
              tknKeys <- liftIO $ C.generateSignatureKeyPair a
              dhKeys <- liftIO C.generateKeyPair'
              let tkn = newNtfToken suppliedDeviceToken ntfServer tknKeys dhKeys suppliedNtfMode
              withStore' c (`createNtfToken` tkn)
              registerToken tkn
              pure NTRegistered
        _ -> throwError $ CMD PROHIBITED
    registerToken :: NtfToken -> m ()
    registerToken tkn@NtfToken {ntfPubKey, ntfDhKeys = (pubDhKey, privDhKey)} = do
      (tknId, srvPubDhKey) <- agentNtfRegisterToken c tkn ntfPubKey pubDhKey
      let dhSecret = C.dh' srvPubDhKey privDhKey
      withStore' c $ \db -> updateNtfTokenRegistration db tkn tknId dhSecret
      ns <- asks ntfSupervisor
      atomically $ nsUpdateToken ns tkn {deviceToken = suppliedDeviceToken, ntfTknStatus = NTRegistered, ntfMode = suppliedNtfMode}

verifyNtfToken' :: AgentMonad m => AgentClient -> DeviceToken -> C.CbNonce -> ByteString -> m ()
verifyNtfToken' c deviceToken nonce code =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId = Just tknId, ntfDhSecret = Just dhSecret, ntfMode} -> do
      when (deviceToken /= savedDeviceToken) . throwError $ CMD PROHIBITED
      code' <- liftEither . bimap cryptoError NtfRegCode $ C.cbDecrypt dhSecret nonce code
      toStatus <-
        withToken c tkn (Just (NTConfirmed, NTAVerify code')) (NTActive, Just NTACheck) $
          agentNtfVerifyToken c tknId tkn code'
      when (toStatus == NTActive) $ do
        cron <- asks $ ntfCron . config
        agentNtfEnableCron c tknId tkn cron
        when (ntfMode == NMInstant) $ initializeNtfSubs c
    _ -> throwError $ CMD PROHIBITED

checkNtfToken' :: AgentMonad m => AgentClient -> DeviceToken -> m NtfTknStatus
checkNtfToken' c deviceToken =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken, ntfTokenId = Just tknId} -> do
      when (deviceToken /= savedDeviceToken) . throwError $ CMD PROHIBITED
      agentNtfCheckToken c tknId tkn
    _ -> throwError $ CMD PROHIBITED

deleteNtfToken' :: AgentMonad m => AgentClient -> DeviceToken -> m ()
deleteNtfToken' c deviceToken =
  withStore' c getSavedNtfToken >>= \case
    Just tkn@NtfToken {deviceToken = savedDeviceToken} -> do
      when (deviceToken /= savedDeviceToken) . throwError $ CMD PROHIBITED
      deleteToken_ c tkn
      deleteNtfSubs c NSCSmpDelete
    _ -> throwError $ CMD PROHIBITED

getNtfToken' :: AgentMonad m => AgentClient -> m (DeviceToken, NtfTknStatus, NotificationsMode)
getNtfToken' c =
  withStore' c getSavedNtfToken >>= \case
    Just NtfToken {deviceToken, ntfTknStatus, ntfMode} -> pure (deviceToken, ntfTknStatus, ntfMode)
    _ -> throwError $ CMD PROHIBITED

getNtfTokenData' :: AgentMonad m => AgentClient -> m NtfToken
getNtfTokenData' c =
  withStore' c getSavedNtfToken >>= \case
    Just tkn -> pure tkn
    _ -> throwError $ CMD PROHIBITED

-- | Set connection notifications, in Reader monad
toggleConnectionNtfs' :: forall m. AgentMonad m => AgentClient -> ConnId -> Bool -> m ()
toggleConnectionNtfs' c connId enable = do
  SomeConn _ conn <- withStore c (`getConn` connId)
  case conn of
    DuplexConnection cData _ _ -> toggle cData
    RcvConnection cData _ -> toggle cData
    ContactConnection cData _ -> toggle cData
    _ -> throwError $ CONN SIMPLEX
  where
    toggle :: ConnData -> m ()
    toggle cData
      | enableNtfs cData == enable = pure ()
      | otherwise = do
        withStore' c $ \db -> setConnectionNtfs db connId enable
        ns <- asks ntfSupervisor
        let cmd = if enable then NSCCreate else NSCDelete
        atomically $ sendNtfSubCommand ns (connId, cmd)

deleteToken_ :: AgentMonad m => AgentClient -> NtfToken -> m ()
deleteToken_ c tkn@NtfToken {ntfTokenId, ntfTknStatus} = do
  ns <- asks ntfSupervisor
  forM_ ntfTokenId $ \tknId -> do
    let ntfTknAction = Just NTADelete
    withStore' c $ \db -> updateNtfToken db tkn ntfTknStatus ntfTknAction
    atomically $ nsUpdateToken ns tkn {ntfTknStatus, ntfTknAction}
    agentNtfDeleteToken c tknId tkn `catchError` \case
      NTF AUTH -> pure ()
      e -> throwError e
  withStore' c $ \db -> removeNtfToken db tkn
  atomically $ nsRemoveNtfToken ns

withToken :: AgentMonad m => AgentClient -> NtfToken -> Maybe (NtfTknStatus, NtfTknAction) -> (NtfTknStatus, Maybe NtfTknAction) -> m a -> m NtfTknStatus
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
    Left e@(NTF AUTH) -> do
      withStore' c $ \db -> removeNtfToken db tkn
      atomically $ nsRemoveNtfToken ns
      void $ registerNtfToken' c deviceToken ntfMode
      throwError e
    Left e -> throwError e

initializeNtfSubs :: AgentMonad m => AgentClient -> m ()
initializeNtfSubs c = sendNtfConnCommands c NSCCreate

deleteNtfSubs :: AgentMonad m => AgentClient -> NtfSupervisorCommand -> m ()
deleteNtfSubs c deleteCmd = do
  ns <- asks ntfSupervisor
  void . atomically . flushTBQueue $ ntfSubQ ns
  sendNtfConnCommands c deleteCmd

sendNtfConnCommands :: AgentMonad m => AgentClient -> NtfSupervisorCommand -> m ()
sendNtfConnCommands c cmd = do
  ns <- asks ntfSupervisor
  connIds <- atomically $ getSubscriptions c
  forM_ connIds $ \connId -> do
    withStore' c (`getConnData` connId) >>= \case
      Just (ConnData {enableNtfs}, _) ->
        when enableNtfs . atomically $ writeTBQueue (ntfSubQ ns) (connId, cmd)
      _ ->
        atomically $ writeTBQueue (subQ c) ("", connId, ERR $ INTERNAL "no connection data")

-- TODO
-- There should probably be another function to cancel all subscriptions that would flush the queue first,
-- so that supervisor stops processing pending commands?
-- It is an optimization, but I am thinking how it would behave if a user were to flip on/off quickly several times.

setNtfServers' :: AgentMonad m => AgentClient -> [NtfServer] -> m ()
setNtfServers' c = atomically . writeTVar (ntfServers c)

activateAgent' :: AgentMonad m => AgentClient -> m ()
activateAgent' c = do
  atomically $ writeTVar (agentState c) ASActive
  mapM_ activate $ reverse agentOperations
  where
    activate opSel = atomically $ modifyTVar' (opSel c) $ \s -> s {opSuspended = False}

suspendAgent' :: AgentMonad m => AgentClient -> Int -> m ()
suspendAgent' c 0 = do
  atomically $ writeTVar (agentState c) ASSuspended
  mapM_ suspend agentOperations
  where
    suspend opSel = atomically $ modifyTVar' (opSel c) $ \s -> s {opSuspended = True}
suspendAgent' c@AgentClient {agentState = as} maxDelay = do
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

execAgentStoreSQL' :: AgentMonad m => AgentClient -> Text -> m [Text]
execAgentStoreSQL' c sql = withStore' c (`execSQL` sql)

debugAgentLocks' :: AgentMonad m => AgentClient -> m AgentLocks
debugAgentLocks' AgentClient {connLocks = cs, reconnectLocks = rs} = do
  connLocks <- getLocks cs
  srvLocks <- getLocks rs
  pure AgentLocks {connLocks, srvLocks}
  where
    getLocks ls = atomically $ M.mapKeys (B.unpack . strEncode) . M.mapMaybe id <$> (mapM tryReadTMVar =<< readTVar ls)

getSMPServer :: AgentMonad m => AgentClient -> m SMPServerWithAuth
getSMPServer c = readTVarIO (smpServers c) >>= pickServer

pickServer :: AgentMonad m => NonEmpty SMPServerWithAuth -> m SMPServerWithAuth
pickServer = \case
  srv :| [] -> pure srv
  servers -> do
    gen <- asks randomServer
    atomically $ (servers L.!!) <$> stateTVar gen (randomR (0, L.length servers - 1))

getNextSMPServer :: AgentMonad m => AgentClient -> [SMPServer] -> m SMPServerWithAuth
getNextSMPServer c usedSrvs = do
  srvs <- readTVarIO $ smpServers c
  case L.nonEmpty $ deleteFirstsBy sameSrvAddr' (L.toList srvs) (map noAuthSrv usedSrvs) of
    Just srvs' -> pickServer srvs'
    _ -> pickServer srvs

subscriber :: (MonadUnliftIO m, MonadReader Env m) => AgentClient -> m ()
subscriber c@AgentClient {msgQ} = forever $ do
  t <- atomically $ readTBQueue msgQ
  agentOperationBracket c AORcvNetwork waitUntilActive $
    runExceptT (processSMPTransmission c t) >>= \case
      Left e -> liftIO $ print e
      Right _ -> return ()

processSMPTransmission :: forall m. AgentMonad m => AgentClient -> ServerTransmission BrokerMsg -> m ()
processSMPTransmission c@AgentClient {smpClients, subQ} (srv, v, sessId, rId, cmd) = do
  (rq, SomeConn _ conn) <- withStore c (\db -> getRcvConn db srv rId)
  processSMP rq conn $ connData conn
  where
    processSMP :: RcvQueue -> Connection c -> ConnData -> m ()
    processSMP rq@RcvQueue {e2ePrivKey, e2eDhSecret, status} conn cData@ConnData {connId, duplexHandshake} = withConnLock c connId "processSMP" $
      case cmd of
        SMP.MSG msg@SMP.RcvMessage {msgId = srvMsgId} -> handleNotifyAck $ do
          SMP.ClientRcvMsgBody {msgTs = srvTs, msgFlags, msgBody} <- decryptSMPMessage v rq msg
          clientMsg@SMP.ClientMsgEnvelope {cmHeader = SMP.PubHeader phVer e2ePubKey_} <-
            parseMessage msgBody
          clientVRange <- asks $ smpClientVRange . config
          unless (phVer `isCompatible` clientVRange) . throwError $ AGENT A_VERSION
          case (e2eDhSecret, e2ePubKey_) of
            (Nothing, Just e2ePubKey) -> do
              let e2eDh = C.dh' e2ePubKey e2ePrivKey
              decryptClientMessage e2eDh clientMsg >>= \case
                (SMP.PHConfirmation senderKey, AgentConfirmation {e2eEncryption, encConnInfo, agentVersion}) ->
                  smpConfirmation senderKey e2ePubKey e2eEncryption encConnInfo phVer agentVersion >> ack
                (SMP.PHEmpty, AgentInvitation {connReq, connInfo}) ->
                  smpInvitation connReq connInfo >> ack
                _ -> prohibited >> ack
            (Just e2eDh, Nothing) -> do
              decryptClientMessage e2eDh clientMsg >>= \case
                (SMP.PHEmpty, AgentMsgEnvelope _ encAgentMsg) -> do
                  -- primary queue is set as Active in helloMsg, below is to set additional queues Active
                  let RcvQueue {primary, dbReplaceQueueId} = rq
                  unless (status == Active) . withStore' c $ \db -> setRcvQueueStatus db rq Active
                  case (conn, dbReplaceQueueId) of
                    (DuplexConnection _ rqs _, Just replacedId) -> do
                      when primary . withStore' c $ \db -> setRcvQueuePrimary db connId rq
                      case find (\RcvQueue {dbQueueId} -> dbQueueId == replacedId) rqs of
                        Just RcvQueue {server, rcvId} -> do
                          enqueueCommand c "" connId (Just server) $ AInternalCommand $ ICQDelete rcvId
                        _ -> notify . ERR . AGENT $ A_QUEUE "replaced RcvQueue not found in connection"
                    _ -> pure ()
                  tryError agentClientMsg >>= \case
                    Right (Just (msgId, msgMeta, aMessage)) -> case aMessage of
                      HELLO -> helloMsg >> ackDel msgId
                      REPLY cReq -> replyMsg cReq >> ackDel msgId
                      -- note that there is no ACK sent for A_MSG, it is sent with agent's user ACK command
                      A_MSG body -> do
                        logServer "<--" c srv rId "MSG <MSG>"
                        notify $ MSG msgMeta msgFlags body
                      QADD qs -> qDuplex "QADD" $ qAddMsg qs
                      QKEY qs -> qDuplex "QKEY" $ qKeyMsg qs
                      QUSE qs -> qDuplex "QUSE" $ qUseMsg qs
                      -- no action needed for QTEST
                      -- any message in the new queue will mark it active and trigger deletion of the old queue
                      QTEST _ -> logServer "<--" c srv rId "MSG <QTEST>" >> ackDel msgId
                      where
                        qDuplex :: String -> (Connection 'CDuplex -> m ()) -> m ()
                        qDuplex name a = case conn of
                          DuplexConnection {} -> a conn >> ackDel msgId
                          _ -> qError $ name <> ": message must be sent to duplex connection"
                    Right _ -> prohibited >> ack
                    Left e@(AGENT A_DUPLICATE) -> do
                      withStore' c (\db -> getLastMsg db connId srvMsgId) >>= \case
                        Just RcvMsg {internalId, msgMeta, msgBody = agentMsgBody, userAck}
                          | userAck -> ackDel internalId
                          | otherwise -> do
                            liftEither (parse smpP (AGENT A_MESSAGE) agentMsgBody) >>= \case
                              AgentMessage _ (A_MSG body) -> do
                                logServer "<--" c srv rId "MSG <MSG>"
                                notify $ MSG msgMeta msgFlags body
                              _ -> pure ()
                        _ -> throwError e
                    Left e -> throwError e
                  where
                    agentClientMsg :: m (Maybe (InternalId, MsgMeta, AMessage))
                    agentClientMsg = withStore c $ \db -> runExceptT $ do
                      agentMsgBody <- agentRatchetDecrypt db connId encAgentMsg
                      liftEither (parse smpP (SEAgentError $ AGENT A_MESSAGE) agentMsgBody) >>= \case
                        agentMsg@(AgentMessage APrivHeader {sndMsgId, prevMsgHash} aMessage) -> do
                          let msgType = agentMessageType agentMsg
                              internalHash = C.sha256Hash agentMsgBody
                          internalTs <- liftIO getCurrentTime
                          (internalId, internalRcvId, prevExtSndId, prevRcvMsgHash) <- liftIO $ updateRcvIds db connId
                          let integrity = checkMsgIntegrity prevExtSndId sndMsgId prevRcvMsgHash prevMsgHash
                              recipient = (unId internalId, internalTs)
                              broker = (srvMsgId, systemToUTCTime srvTs)
                              msgMeta = MsgMeta {integrity, recipient, broker, sndMsgId}
                              rcvMsg = RcvMsgData {msgMeta, msgType, msgFlags, msgBody = agentMsgBody, internalRcvId, internalHash, externalPrevSndHash = prevMsgHash}
                          liftIO $ createRcvMsg db connId rq rcvMsg
                          pure $ Just (internalId, msgMeta, aMessage)
                        _ -> pure Nothing
                _ -> prohibited >> ack
            _ -> prohibited >> ack
          where
            ack :: m ()
            ack = enqueueCmd $ ICAck rId srvMsgId
            ackDel :: InternalId -> m ()
            ackDel = enqueueCmd . ICAckDel rId srvMsgId
            handleNotifyAck :: m () -> m ()
            handleNotifyAck m = m `catchError` \e -> notify (ERR e) >> ack
        SMP.END ->
          atomically (TM.lookup srv smpClients $>>= tryReadTMVar >>= processEND)
            >>= logServer "<--" c srv rId
          where
            processEND = \case
              Just (Right clnt)
                | sessId == sessionId clnt -> do
                  removeSubscription c connId
                  writeTBQueue subQ ("", connId, END)
                  pure "END"
                | otherwise -> ignored
              _ -> ignored
            ignored = pure "END from disconnected client - ignored"
        _ -> do
          logServer "<--" c srv rId $ "unexpected: " <> bshow cmd
          notify . ERR $ BROKER UNEXPECTED
      where
        notify :: ACommand 'Agent -> m ()
        notify msg = atomically $ writeTBQueue subQ ("", connId, msg)

        prohibited :: m ()
        prohibited = notify . ERR $ AGENT A_PROHIBITED

        enqueueCmd :: InternalCommand -> m ()
        enqueueCmd = enqueueCommand c "" connId (Just srv) . AInternalCommand

        decryptClientMessage :: C.DhSecretX25519 -> SMP.ClientMsgEnvelope -> m (SMP.PrivHeader, AgentMsgEnvelope)
        decryptClientMessage e2eDh SMP.ClientMsgEnvelope {cmNonce, cmEncBody} = do
          clientMsg <- agentCbDecrypt e2eDh cmNonce cmEncBody
          SMP.ClientMessage privHeader clientBody <- parseMessage clientMsg
          agentEnvelope <- parseMessage clientBody
          -- Version check is removed here, because when connecting via v1 contact address the agent still sends v2 message,
          -- to allow duplexHandshake mode, in case the receiving agent was updated to v2 after the address was created.
          -- aVRange <- asks $ smpAgentVRange . config
          -- if agentVersion agentEnvelope `isCompatible` aVRange
          --   then pure (privHeader, agentEnvelope)
          --   else throwError $ AGENT A_VERSION
          pure (privHeader, agentEnvelope)

        parseMessage :: Encoding a => ByteString -> m a
        parseMessage = liftEither . parse smpP (AGENT A_MESSAGE)

        smpConfirmation :: C.APublicVerifyKey -> C.PublicKeyX25519 -> Maybe (CR.E2ERatchetParams 'C.X448) -> ByteString -> Version -> Version -> m ()
        smpConfirmation senderKey e2ePubKey e2eEncryption encConnInfo smpClientVersion agentVersion = do
          logServer "<--" c srv rId "MSG <CONF>"
          AgentConfig {smpClientVRange, smpAgentVRange, e2eEncryptVRange} <- asks config
          unless
            (agentVersion `isCompatible` smpAgentVRange && smpClientVersion `isCompatible` smpClientVRange)
            (throwError $ AGENT A_VERSION)
          case status of
            New -> case (conn, e2eEncryption) of
              -- party initiating connection
              (RcvConnection {}, Just e2eSndParams@(CR.E2ERatchetParams e2eVersion _ _)) -> do
                unless (e2eVersion `isCompatible` e2eEncryptVRange) (throwError $ AGENT A_VERSION)
                (pk1, rcDHRs) <- withStore c (`getRatchetX3dhKeys` connId)
                let rc = CR.initRcvRatchet e2eEncryptVRange rcDHRs $ CR.x3dhRcv pk1 rcDHRs e2eSndParams
                (agentMsgBody_, rc', skipped) <- liftError cryptoError $ CR.rcDecrypt rc M.empty encConnInfo
                case (agentMsgBody_, skipped) of
                  (Right agentMsgBody, CR.SMDNoChange) ->
                    parseMessage agentMsgBody >>= \case
                      AgentConnInfo connInfo ->
                        processConf connInfo SMPConfirmation {senderKey, e2ePubKey, connInfo, smpReplyQueues = [], smpClientVersion} False
                      AgentConnInfoReply smpQueues connInfo ->
                        processConf connInfo SMPConfirmation {senderKey, e2ePubKey, connInfo, smpReplyQueues = L.toList smpQueues, smpClientVersion} True
                      _ -> prohibited
                    where
                      processConf connInfo senderConf duplexHS = do
                        let newConfirmation = NewConfirmation {connId, senderConf, ratchetState = rc'}
                        g <- asks idsDrg
                        confId <- withStore c $ \db -> do
                          setHandshakeVersion db connId agentVersion duplexHS
                          createConfirmation db g newConfirmation
                        let srvs = map qServer $ smpReplyQueues senderConf
                        notify $ CONF confId srvs connInfo
                  _ -> prohibited
              -- party accepting connection
              (DuplexConnection _ (RcvQueue {smpClientVersion = v'} :| _) _, Nothing) -> do
                withStore c (\db -> runExceptT $ agentRatchetDecrypt db connId encConnInfo) >>= parseMessage >>= \case
                  AgentConnInfo connInfo -> do
                    notify $ INFO connInfo
                    let dhSecret = C.dh' e2ePubKey e2ePrivKey
                    withStore' c $ \db -> setRcvQueueConfirmedE2E db rq dhSecret $ min v' smpClientVersion
                    enqueueCmd $ ICDuplexSecure rId senderKey
                  _ -> prohibited
              _ -> prohibited
            _ -> prohibited

        helloMsg :: m ()
        helloMsg = do
          logServer "<--" c srv rId "MSG <HELLO>"
          case status of
            Active -> prohibited
            _ ->
              case conn of
                DuplexConnection _ _ (sq@SndQueue {status = sndStatus} :| _)
                  -- `sndStatus == Active` when HELLO was previously sent, and this is the reply HELLO
                  -- this branch is executed by the accepting party in duplexHandshake mode (v2)
                  -- and by the initiating party in v1
                  -- Also see comment where HELLO is sent.
                  | sndStatus == Active -> atomically $ writeTBQueue subQ ("", connId, CON)
                  | duplexHandshake == Just True -> enqueueDuplexHello sq
                  | otherwise -> pure ()
                _ -> pure ()

        enqueueDuplexHello :: SndQueue -> m ()
        enqueueDuplexHello sq = void $ enqueueMessage c cData sq SMP.MsgFlags {notification = True} HELLO

        replyMsg :: NonEmpty SMPQueueInfo -> m ()
        replyMsg smpQueues = do
          logServer "<--" c srv rId "MSG <REPLY>"
          case duplexHandshake of
            Just True -> prohibited
            _ -> case conn of
              RcvConnection {} -> do
                AcceptedConfirmation {ownConnInfo} <- withStore c (`getAcceptedConfirmation` connId)
                connectReplyQueues c cData ownConnInfo smpQueues `catchError` (notify . ERR)
              _ -> prohibited

        -- processed by queue sender
        qAddMsg :: NonEmpty (SMPQueueUri, Maybe SndQAddr) -> Connection 'CDuplex -> m ()
        qAddMsg ((_, Nothing) :| _) _ = qError "adding queue without switching is not supported"
        qAddMsg ((qUri, Just addr) :| _) (DuplexConnection _ rqs sqs@(sq :| sqs_)) = do
          clientVRange <- asks $ smpClientVRange . config
          case qUri `compatibleVersion` clientVRange of
            Just qInfo@(Compatible sqInfo@SMPQueueInfo {queueAddress}) ->
              case (findQ (qAddress sqInfo) sqs, findQ addr sqs) of
                (Just _, _) -> qError "QADD: queue address is already used in connection"
                (_, Just _replaced@SndQueue {dbQueueId}) -> do
                  sq_@SndQueue {sndPublicKey, e2ePubKey} <- newSndQueue connId qInfo
                  let sq' = (sq_ :: SndQueue) {primary = True, dbReplaceQueueId = Just dbQueueId}
                  void . withStore c $ \db -> addConnSndQueue db connId sq'
                  case (sndPublicKey, e2ePubKey) of
                    (Just sndPubKey, Just dhPublicKey) -> do
                      logServer "<--" c srv rId $ "MSG <QADD> " <> logSecret (senderId queueAddress)
                      let sqInfo' = (sqInfo :: SMPQueueInfo) {queueAddress = queueAddress {dhPublicKey}}
                      void . enqueueMessages c cData sqs SMP.noMsgFlags $ QKEY [(sqInfo', sndPubKey)]
                      let conn' = DuplexConnection cData rqs (sq <| sq' :| sqs_)
                      notify . SWITCH QDSnd SPStarted $ connectionStats conn'
                    _ -> qError "absent sender keys"
                _ -> qError "QADD: replaced queue address is not found in connection"
            _ -> throwError $ AGENT A_VERSION

        -- processed by queue recipient
        qKeyMsg :: NonEmpty (SMPQueueInfo, SndPublicVerifyKey) -> Connection 'CDuplex -> m ()
        qKeyMsg ((qInfo, senderKey) :| _) (DuplexConnection _ rqs _) = do
          clientVRange <- asks $ smpClientVRange . config
          unless (qInfo `isCompatible` clientVRange) . throwError $ AGENT A_VERSION
          case findRQ (smpServer, senderId) rqs of
            Just rq'@RcvQueue {rcvId, e2ePrivKey = dhPrivKey, smpClientVersion = cVer, status = status'}
              | status' == New || status' == Confirmed -> do
                logServer "<--" c srv rId $ "MSG <QKEY> " <> logSecret senderId
                let dhSecret = C.dh' dhPublicKey dhPrivKey
                withStore' c $ \db -> setRcvQueueConfirmedE2E db rq' dhSecret $ min cVer cVer'
                enqueueCommand c "" connId (Just smpServer) $ AInternalCommand $ ICQSecure rcvId senderKey
                notify . SWITCH QDRcv SPConfirmed $ connectionStats conn
              | otherwise -> qError "QKEY: queue already secured"
            _ -> qError "QKEY: queue address not found in connection"
          where
            SMPQueueInfo cVer' SMPQueueAddress {smpServer, senderId, dhPublicKey} = qInfo

        -- processed by queue sender
        -- mark queue as Secured and to start sending messages to it
        qUseMsg :: NonEmpty ((SMPServer, SMP.SenderId), Bool) -> Connection 'CDuplex -> m ()
        -- NOTE: does not yet support the change of the primary status during the rotation
        qUseMsg ((addr, _primary) :| _) (DuplexConnection _ _ sqs) =
          case findQ addr sqs of
            Just sq' -> do
              logServer "<--" c srv rId $ "MSG <QUSE> " <> logSecret (snd addr)
              withStore' c $ \db -> setSndQueueStatus db sq' Secured
              let sq'' = (sq' :: SndQueue) {status = Secured}
              -- sending QTEST to the new queue only, the old one will be removed if sent successfully
              void $ enqueueMessages c cData [sq''] SMP.noMsgFlags $ QTEST [addr]
              notify . SWITCH QDSnd SPConfirmed $ connectionStats conn
            _ -> qError "QUSE: queue address not found in connection"

        qError :: String -> m ()
        qError = throwError . AGENT . A_QUEUE

        smpInvitation :: ConnectionRequestUri 'CMInvitation -> ConnInfo -> m ()
        smpInvitation connReq@(CRInvitationUri crData _) cInfo = do
          logServer "<--" c srv rId "MSG <KEY>"
          case conn of
            ContactConnection {} -> do
              g <- asks idsDrg
              let newInv = NewInvitation {contactConnId = connId, connReq, recipientConnInfo = cInfo}
              invId <- withStore c $ \db -> createInvitation db g newInv
              let srvs = L.map qServer $ crSmpQueues crData
              notify $ REQ invId srvs cInfo
            _ -> prohibited

        checkMsgIntegrity :: PrevExternalSndId -> ExternalSndId -> PrevRcvMsgHash -> ByteString -> MsgIntegrity
        checkMsgIntegrity prevExtSndId extSndId internalPrevMsgHash receivedPrevMsgHash
          | extSndId == prevExtSndId + 1 && internalPrevMsgHash == receivedPrevMsgHash = MsgOk
          | extSndId < prevExtSndId = MsgError $ MsgBadId extSndId
          | extSndId == prevExtSndId = MsgError MsgDuplicate -- ? deduplicate
          | extSndId > prevExtSndId + 1 = MsgError $ MsgSkipped (prevExtSndId + 1) (extSndId - 1)
          | internalPrevMsgHash /= receivedPrevMsgHash = MsgError MsgBadHash
          | otherwise = MsgError MsgDuplicate -- this case is not possible

connectReplyQueues :: AgentMonad m => AgentClient -> ConnData -> ConnInfo -> NonEmpty SMPQueueInfo -> m ()
connectReplyQueues c cData@ConnData {connId} ownConnInfo (qInfo :| _) = do
  clientVRange <- asks $ smpClientVRange . config
  case qInfo `proveCompatible` clientVRange of
    Nothing -> throwError $ AGENT A_VERSION
    Just qInfo' -> do
      sq <- newSndQueue connId qInfo'
      dbQueueId <- withStore c $ \db -> upgradeRcvConnToDuplex db connId sq
      enqueueConfirmation c cData sq {dbQueueId} ownConnInfo Nothing

confirmQueue :: forall m. AgentMonad m => Compatible Version -> AgentClient -> ConnData -> SndQueue -> SMPServerWithAuth -> ConnInfo -> Maybe (CR.E2ERatchetParams 'C.X448) -> m ()
confirmQueue (Compatible agentVersion) c cData@ConnData {connId} sq srv connInfo e2eEncryption = do
  aMessage <- mkAgentMessage agentVersion
  msg <- mkConfirmation aMessage
  sendConfirmation c sq msg
  withStore' c $ \db -> setSndQueueStatus db sq Confirmed
  where
    mkConfirmation :: AgentMessage -> m MsgBody
    mkConfirmation aMessage = withStore c $ \db -> runExceptT $ do
      void . liftIO $ updateSndIds db connId
      encConnInfo <- agentRatchetEncrypt db connId (smpEncode aMessage) e2eEncConnInfoLength
      pure . smpEncode $ AgentConfirmation {agentVersion, e2eEncryption, encConnInfo}
    mkAgentMessage :: Version -> m AgentMessage
    mkAgentMessage 1 = pure $ AgentConnInfo connInfo
    mkAgentMessage _ = do
      qInfo <- createReplyQueue c cData sq srv
      pure $ AgentConnInfoReply (qInfo :| []) connInfo

enqueueConfirmation :: forall m. AgentMonad m => AgentClient -> ConnData -> SndQueue -> ConnInfo -> Maybe (CR.E2ERatchetParams 'C.X448) -> m ()
enqueueConfirmation c cData@ConnData {connId, connAgentVersion} sq connInfo e2eEncryption = do
  resumeMsgDelivery c cData sq
  msgId <- storeConfirmation
  queuePendingMsgs c sq [msgId]
  where
    storeConfirmation :: m InternalId
    storeConfirmation = withStore c $ \db -> runExceptT $ do
      internalTs <- liftIO getCurrentTime
      (internalId, internalSndId, prevMsgHash) <- liftIO $ updateSndIds db connId
      let agentMsg = AgentConnInfo connInfo
          agentMsgStr = smpEncode agentMsg
          internalHash = C.sha256Hash agentMsgStr
      encConnInfo <- agentRatchetEncrypt db connId agentMsgStr e2eEncConnInfoLength
      let msgBody = smpEncode $ AgentConfirmation {agentVersion = connAgentVersion, e2eEncryption, encConnInfo}
          msgType = agentMessageType agentMsg
          msgData = SndMsgData {internalId, internalSndId, internalTs, msgType, msgBody, msgFlags = SMP.MsgFlags {notification = True}, internalHash, prevMsgHash}
      liftIO $ createSndMsg db connId msgData
      pure internalId

-- encoded AgentMessage -> encoded EncAgentMessage
agentRatchetEncrypt :: DB.Connection -> ConnId -> ByteString -> Int -> ExceptT StoreError IO ByteString
agentRatchetEncrypt db connId msg paddedLen = do
  rc <- ExceptT $ getRatchet db connId
  (encMsg, rc') <- liftE (SEAgentError . cryptoError) $ CR.rcEncrypt rc paddedLen msg
  liftIO $ updateRatchet db connId rc' CR.SMDNoChange
  pure encMsg

-- encoded EncAgentMessage -> encoded AgentMessage
agentRatchetDecrypt :: DB.Connection -> ConnId -> ByteString -> ExceptT StoreError IO ByteString
agentRatchetDecrypt db connId encAgentMsg = do
  rc <- ExceptT $ getRatchet db connId
  skipped <- liftIO $ getSkippedMsgKeys db connId
  (agentMsgBody_, rc', skippedDiff) <- liftE (SEAgentError . cryptoError) $ CR.rcDecrypt rc skipped encAgentMsg
  liftIO $ updateRatchet db connId rc' skippedDiff
  liftEither $ first (SEAgentError . cryptoError) agentMsgBody_

newSndQueue :: (MonadUnliftIO m, MonadReader Env m) => ConnId -> Compatible SMPQueueInfo -> m SndQueue
newSndQueue connId qInfo =
  asks (cmdSignAlg . config) >>= \case
    C.SignAlg a -> newSndQueue_ a connId qInfo

newSndQueue_ ::
  (C.SignatureAlgorithm a, C.AlgorithmI a, MonadUnliftIO m) =>
  C.SAlgorithm a ->
  ConnId ->
  Compatible SMPQueueInfo ->
  m SndQueue
newSndQueue_ a connId (Compatible (SMPQueueInfo smpClientVersion SMPQueueAddress {smpServer, senderId, dhPublicKey = rcvE2ePubDhKey})) = do
  -- this function assumes clientVersion is compatible - it was tested before
  (sndPublicKey, sndPrivateKey) <- liftIO $ C.generateSignatureKeyPair a
  (e2ePubKey, e2ePrivKey) <- liftIO C.generateKeyPair'
  pure
    SndQueue
      { connId,
        server = smpServer,
        sndId = senderId,
        sndPublicKey = Just sndPublicKey,
        sndPrivateKey,
        e2eDhSecret = C.dh' rcvE2ePubDhKey e2ePrivKey,
        e2ePubKey = Just e2ePubKey,
        status = New,
        dbQueueId = 0,
        primary = True,
        dbReplaceQueueId = Nothing,
        smpClientVersion
      }
