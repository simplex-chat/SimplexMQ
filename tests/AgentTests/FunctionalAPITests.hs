{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module AgentTests.FunctionalAPITests
  ( functionalAPITests,
    testServerMatrix2,
    withAgentClientsCfg2,
    withAgentClientsCfgServers2,
    getSMPAgentClient',
    withAgent,
    withAgentClients2,
    withAgentClients3,
    makeConnection,
    exchangeGreetings,
    switchComplete,
    createConnection,
    joinConnection,
    sendMessage,
    runRight,
    runRight_,
    inAnyOrder,
    get,
    get',
    rfGet,
    sfGet,
    nGet,
    getInAnyOrder,
    (##>),
    (=##>),
    pattern CON,
    pattern CONF,
    pattern INFO,
    pattern REQ,
    pattern Msg,
    pattern Msg',
    pattern SENT,
    agentCfgVPrevPQ,
  )
where

import AgentTests.ConnectionRequestTests (connReqData, queueAddr, testE2ERatchetParams12)
import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Bifunctor (first)
import qualified Data.ByteString.Base64 as B64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Either (isRight)
import Data.Int (Int64)
import Data.List (find, nub)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.Map as M
import Data.Maybe (isJust, isNothing)
import qualified Data.Set as S
import Data.Text.Encoding (decodeLatin1)
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Data.Word (Word16)
import GHC.Stack (withFrozenCallStack)
import SMPAgentClient
import SMPClient (cfg, prevRange, prevVersion, testPort, testPort2, testStoreLogFile2, testStoreMsgsDir2, withSmpServer, withSmpServerConfigOn, withSmpServerProxy, withSmpServerStoreLogOn, withSmpServerStoreMsgLogOn)
import Simplex.Messaging.Agent hiding (createConnection, joinConnection, sendMessage)
import qualified Simplex.Messaging.Agent as A
import Simplex.Messaging.Agent.Client (ProtocolTestFailure (..), ProtocolTestStep (..), ServerQueueInfo (..), UserNetworkInfo (..), UserNetworkType (..), waitForUserNetwork)
import Simplex.Messaging.Agent.Env.SQLite (AgentConfig (..), InitialAgentServers (..), createAgentStore)
import Simplex.Messaging.Agent.Protocol hiding (CON, CONF, INFO, REQ, SENT)
import qualified Simplex.Messaging.Agent.Protocol as A
import Simplex.Messaging.Agent.Store.Common (DBStore (..), withTransaction)
import Simplex.Messaging.Agent.Store.Interface
import qualified Simplex.Messaging.Agent.Store.DB as DB
import Simplex.Messaging.Agent.Store.Shared (MigrationConfirmation (..), MigrationError (..))
import Simplex.Messaging.Client (NetworkConfig (..), ProtocolClientConfig (..), SMPProxyFallback (..), SMPProxyMode (..), TransportSessionMode (..), defaultClientConfig)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet (InitialKeys (..), PQEncryption (..), PQSupport (..), pattern IKPQOff, pattern IKPQOn, pattern PQEncOff, pattern PQEncOn, pattern PQSupportOff, pattern PQSupportOn)
import qualified Simplex.Messaging.Crypto.Ratchet as CR
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Notifications.Transport (NTFVersion, pattern VersionNTF)
import Simplex.Messaging.Protocol (BasicAuth, ErrorType (..), MsgBody, ProtocolServer (..), SubscriptionMode (..), supportedSMPClientVRange)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Server.Env.STM (AStoreType (..), ServerConfig (..), SStoreType (..))
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Server.QueueStore.QueueInfo
import Simplex.Messaging.Transport (ATransport (..), SMPVersion, VersionSMP, authCmdsSMPVersion, currentServerSMPRelayVersion, minClientSMPRelayVersion, minServerSMPRelayVersion, sndAuthKeySMPVersion, supportedSMPHandshakes)
import Simplex.Messaging.Util (bshow, diffToMicroseconds)
import Simplex.Messaging.Version (VersionRange (..))
import qualified Simplex.Messaging.Version as V
import Simplex.Messaging.Version.Internal (Version (..))
import System.Directory (copyFile, renameFile)
import Test.Hspec
import UnliftIO
import Util
import XFTPClient (testXFTPServer)
#if defined(dbPostgres)
import Fixtures
#endif

type AEntityTransmission e = (ACorrId, ConnId, AEvent e)

-- deriving instance Eq (ValidFileDescription p)

shouldRespond :: (HasCallStack, MonadUnliftIO m, Eq a, Show a) => m a -> a -> m ()
a `shouldRespond` r = withFrozenCallStack $ withTimeout a (`shouldBe` r)

(##>) :: (HasCallStack, MonadUnliftIO m) => m (AEntityTransmission e) -> AEntityTransmission e -> m ()
a ##> t = a `shouldRespond` t

(=##>) :: (Show a, HasCallStack, MonadUnliftIO m) => m a -> (HasCallStack => a -> Bool) -> m ()
a =##> p =
  withTimeout a $ \r -> do
    unless (p r) $ liftIO $ putStrLn $ "value failed predicate: " <> show r
    r `shouldSatisfy` p

withTimeout :: (HasCallStack, MonadUnliftIO m) => m a -> (HasCallStack => a -> Expectation) -> m ()
withTimeout a test =
  timeout 10_000000 a >>= \case
    Nothing -> error "operation timed out"
    Just t -> liftIO $ test t

get :: (MonadIO m, HasCallStack) => AgentClient -> m (AEntityTransmission 'AEConn)
get c = withFrozenCallStack $ get' @'AEConn c

rfGet :: (MonadIO m, HasCallStack) => AgentClient -> m (AEntityTransmission 'AERcvFile)
rfGet c = withFrozenCallStack $ get' @'AERcvFile c

sfGet :: (MonadIO m, HasCallStack) => AgentClient -> m (AEntityTransmission 'AESndFile)
sfGet c = withFrozenCallStack $ get' @'AESndFile c

nGet :: (MonadIO m, HasCallStack) => AgentClient -> m (AEntityTransmission 'AENone)
nGet c = withFrozenCallStack $ get' @'AENone c

get' :: forall e m. (MonadIO m, AEntityI e, HasCallStack) => AgentClient -> m (AEntityTransmission e)
get' c = withFrozenCallStack $ do
  (corrId, connId, AEvt e cmd) <- pGet c
  case testEquality e (sAEntity @e) of
    Just Refl -> pure (corrId, connId, cmd)
    _ -> error $ "unexpected command " <> show cmd

pGet :: forall m. MonadIO m => AgentClient -> m ATransmission
pGet c = pGet' c True

pGet' :: forall m. MonadIO m => AgentClient -> Bool -> m ATransmission
pGet' c skipWarn = do
  t@(_, _, AEvt _ cmd) <- atomically (readTBQueue $ subQ c)
  case cmd of
    CONNECT {} -> pGet c
    DISCONNECT {} -> pGet c
    ERR (BROKER _ NETWORK) -> pGet c
    MWARN {} | skipWarn -> pGet c
    RFWARN {} | skipWarn -> pGet c
    SFWARN {} | skipWarn -> pGet c
    _ -> pure t

pattern CONF :: ConfirmationId -> [SMPServer] -> ConnInfo -> AEvent e
pattern CONF conId srvs connInfo <- A.CONF conId PQSupportOn srvs connInfo

pattern INFO :: ConnInfo -> AEvent 'AEConn
pattern INFO connInfo = A.INFO PQSupportOn connInfo

pattern REQ :: InvitationId -> NonEmpty SMPServer -> ConnInfo -> AEvent e
pattern REQ invId srvs connInfo <- A.REQ invId PQSupportOn srvs connInfo

pattern CON :: AEvent 'AEConn
pattern CON = A.CON PQEncOn

pattern Msg :: MsgBody -> AEvent e
pattern Msg msgBody <- MSG MsgMeta {integrity = MsgOk, pqEncryption = PQEncOn} _ msgBody

pattern Msg' :: AgentMsgId -> PQEncryption -> MsgBody -> AEvent e
pattern Msg' aMsgId pq msgBody <- MSG MsgMeta {integrity = MsgOk, recipient = (aMsgId, _), pqEncryption = pq} _ msgBody

pattern MsgErr :: AgentMsgId -> MsgErrorType -> MsgBody -> AEvent 'AEConn
pattern MsgErr msgId err msgBody <- MSG MsgMeta {recipient = (msgId, _), integrity = MsgError err} _ msgBody

pattern MsgErr' :: AgentMsgId -> MsgErrorType -> PQEncryption -> MsgBody -> AEvent 'AEConn
pattern MsgErr' msgId err pq msgBody <- MSG MsgMeta {recipient = (msgId, _), integrity = MsgError err, pqEncryption = pq} _ msgBody

pattern SENT :: AgentMsgId -> AEvent 'AEConn
pattern SENT msgId = A.SENT msgId Nothing

pattern Rcvd :: AgentMsgId -> AEvent 'AEConn
pattern Rcvd agentMsgId <- RCVD MsgMeta {integrity = MsgOk} [MsgReceipt {agentMsgId, msgRcptStatus = MROk}]

smpCfgVPrev :: ProtocolClientConfig SMPVersion
smpCfgVPrev = (smpCfg agentCfg) {clientALPN = Nothing, serverVRange = prevRange $ serverVRange $ smpCfg agentCfg}

ntfCfgVPrev :: ProtocolClientConfig NTFVersion
ntfCfgVPrev = (ntfCfg agentCfg) {clientALPN = Nothing, serverVRange = V.mkVersionRange (VersionNTF 1) (VersionNTF 1)}

agentCfgVPrev :: AgentConfig
agentCfgVPrev = agentCfgVPrevPQ {e2eEncryptVRange = prevRange $ e2eEncryptVRange agentCfg}

agentCfgVPrevPQ :: AgentConfig
agentCfgVPrevPQ =
  agentCfg
    { sndAuthAlg = C.AuthAlg C.SEd25519,
      smpAgentVRange = prevRange $ smpAgentVRange agentCfg,
      smpClientVRange = prevRange $ smpClientVRange agentCfg,
      smpCfg = smpCfgVPrev,
      ntfCfg = ntfCfgVPrev
    }

agentCfgRatchetVPrev :: AgentConfig
agentCfgRatchetVPrev = agentCfg {e2eEncryptVRange = prevRange $ e2eEncryptVRange agentCfg}

mkVersionRange :: Word16 -> Word16 -> VersionRange v
mkVersionRange v1 v2 = V.mkVersionRange (Version v1) (Version v2)

runRight_ :: (Eq e, Show e, HasCallStack) => ExceptT e IO () -> Expectation
runRight_ action = withFrozenCallStack $ runExceptT action `shouldReturn` Right ()

runRight :: (Show e, HasCallStack) => ExceptT e IO a -> IO a
runRight action =
  runExceptT action >>= \case
    Right x -> pure x
    Left e -> error $ "Unexpected error: " <> show e

getInAnyOrder :: HasCallStack => AgentClient -> [ATransmission -> Bool] -> Expectation
getInAnyOrder c ts = withFrozenCallStack $ inAnyOrder (pGet c) ts

inAnyOrder :: (Show a, MonadUnliftIO m, HasCallStack) => m a -> [a -> Bool] -> m ()
inAnyOrder _ [] = pure ()
inAnyOrder g rs = withFrozenCallStack $ do
  r <- 5000000 `timeout` g >>= maybe (error "inAnyOrder timeout") pure
  let rest = filter (not . expected r) rs
  if length rest < length rs
    then inAnyOrder g rest
    else error $ "unexpected event: " <> show r
  where
    expected :: a -> (a -> Bool) -> Bool
    expected r rp = rp r

createConnection :: AgentClient -> UserId -> Bool -> SConnectionMode c -> Maybe CRClientData -> SubscriptionMode -> AE (ConnId, ConnectionRequestUri c)
createConnection c userId enableNtfs cMode clientData = A.createConnection c userId enableNtfs cMode clientData (IKNoPQ PQSupportOn)

joinConnection :: AgentClient -> UserId -> Bool -> ConnectionRequestUri c -> ConnInfo -> SubscriptionMode -> AE (ConnId, SndQueueSecured)
joinConnection c userId enableNtfs cReq connInfo subMode = do
  connId <- A.prepareConnectionToJoin c userId enableNtfs cReq PQSupportOn
  (connId,) <$> A.joinConnection c userId connId enableNtfs cReq connInfo PQSupportOn subMode

sendMessage :: AgentClient -> ConnId -> SMP.MsgFlags -> MsgBody -> AE AgentMsgId
sendMessage c connId msgFlags msgBody = do
  (msgId, pqEnc) <- A.sendMessage c connId PQEncOn msgFlags msgBody
  liftIO $ pqEnc `shouldBe` PQEncOn
  pure msgId

functionalAPITests :: ATransport -> Spec
functionalAPITests t = do
  describe "Establishing duplex connection" $ do
    testMatrix2 t runAgentClientTest
    it "should connect when server with multiple identities is stored" $
      withSmpServer t testServerMultipleIdentities
    it "should connect with two peers" $
      withSmpServer t testAgentClient3
    it "should establish connection without PQ encryption and enable it" $
      withSmpServer t testEnablePQEncryption
  describe "Duplex connection - delivery stress test" $ do
    describe "one way (50)" $ testMatrix2Stress t $ runAgentClientStressTestOneWay 50
    xdescribe "one way (1000)" $ testMatrix2Stress t $ runAgentClientStressTestOneWay 1000
    describe "two way concurrently (50)" $ testMatrix2Stress t $ runAgentClientStressTestConc 25
    xdescribe "two way concurrently (1000)" $ testMatrix2Stress t $ runAgentClientStressTestConc 500
  describe "Establishing duplex connection, different PQ settings" $ do
    testPQMatrix2 t $ runAgentClientTestPQ False True
  describe "Establishing duplex connection v2, different Ratchet versions" $
    testRatchetMatrix2 t runAgentClientTest
  describe "Establish duplex connection via contact address" $
    testMatrix2 t runAgentClientContactTest
  describe "Establish duplex connection via contact address, different PQ settings" $ do
    testPQMatrix2NoInv t $ runAgentClientContactTestPQ False True PQSupportOn
  describe "Establish duplex connection via contact address v2, different Ratchet versions" $
    testRatchetMatrix2 t runAgentClientContactTest
  describe "Establish duplex connection via contact address, different PQ settings" $ do
    testPQMatrix3 t $ runAgentClientContactTestPQ3 True
  it "should support rejecting contact request" $
    withSmpServer t testRejectContactRequest
  describe "Changing connection user id" $ do
    it "should change user id for new connections" $ do
      withSmpServer t testUpdateConnectionUserId
  describe "Establishing connection asynchronously" $ do
    it "should connect with initiating client going offline" $
      withSmpServer t testAsyncInitiatingOffline
    it "should connect with joining client going offline before its queue activation" $
      withSmpServer t testAsyncJoiningOfflineBeforeActivation
    it "should connect with both clients going offline" $
      withSmpServer t testAsyncBothOffline
    it "should connect on the second attempt if server was offline" $
      testAsyncServerOffline t
    it "should restore confirmation after client restart" $
      testAllowConnectionClientRestart t
  describe "Message delivery" $ do
    describe "update connection agent version on received messages" $ do
      it "should increase if compatible, shouldn't decrease" $
        testIncreaseConnAgentVersion t
      it "should increase to max compatible version" $
        testIncreaseConnAgentVersionMaxCompatible t
      it "should increase when connection was negotiated on different versions" $
        testIncreaseConnAgentVersionStartDifferentVersion t
    -- TODO PQ tests for upgrading connection to PQ encryption
    it "should deliver message after client restart" $
      testDeliverClientRestart t
    it "should deliver messages to the user once, even if repeat delivery is made by the server (no ACK)" $
      testDuplicateMessage t
    it "should report error via msg integrity on skipped messages" $
      testSkippedMessages t
    it "should connect to the server when server goes up if it initially was down" $
      testDeliveryAfterSubscriptionError t
    it "should deliver messages if one of connections has quota exceeded" $
      testMsgDeliveryQuotaExceeded t
    describe "message expiration" $ do
      it "should expire one message" $ testExpireMessage t
      it "should expire multiple messages" $ testExpireManyMessages t
      it "should expire one message if quota is exceeded" $ testExpireMessageQuota t
      it "should expire multiple messages if quota is exceeded" $ testExpireManyMessagesQuota t
#if !defined(dbPostgres)
    -- TODO [postgres] restore from outdated db backup (we use copyFile/renameFile for sqlite)
    describe "Ratchet synchronization" $ do
      it "should report ratchet de-synchronization, synchronize ratchets" $
        testRatchetSync t
      it "should synchronize ratchets after server being offline" $
        testRatchetSyncServerOffline t
      it "should synchronize ratchets after client restart" $
        testRatchetSyncClientRestart t
      it "should synchronize ratchets after suspend/foreground" $
        testRatchetSyncSuspendForeground t
      it "should synchronize ratchets when clients start synchronization simultaneously" $
        testRatchetSyncSimultaneous t
#endif
    describe "Subscription mode OnlyCreate" $ do
      it "messages delivered only when polled (v8 - slow handshake)" $
        withSmpServer t testOnlyCreatePullSlowHandshake
      it "messages delivered only when polled" $
        withSmpServer t testOnlyCreatePull
  describe "Inactive client disconnection" $ do
    it "should disconnect clients without subs if they were inactive longer than TTL" $
      testInactiveNoSubs t
    it "should NOT disconnect inactive clients when they have subscriptions" $
      testInactiveWithSubs t
    it "should NOT disconnect active clients" $
      testActiveClientNotDisconnected t
  describe "Suspending agent" $ do
    it "should update client when agent is suspended" $
      withSmpServer t testSuspendingAgent
    it "should complete sending messages when agent is suspended" $
      testSuspendingAgentCompleteSending t
    it "should suspend agent on timeout, even if pending messages not sent" $
      testSuspendingAgentTimeout t
  describe "Batching SMP commands" $ do
    it "should subscribe to multiple (200) subscriptions with batching" $
      testBatchedSubscriptions 200 10 t
    skip "faster version of the previous test (200 subscriptions gets very slow with test coverage)" $
      it "should subscribe to multiple (6) subscriptions with batching" $
        testBatchedSubscriptions 6 3 t
    it "should subscribe to multiple connections with pending messages" $
      withSmpServer t $
        testBatchedPendingMessages 10 5
  describe "Batch send messages" $ do
    it "should send multiple messages to the same connection" $ withSmpServer t testSendMessagesB
    it "should send messages to the 2 connections" $ withSmpServer t testSendMessagesB2
  describe "Async agent commands" $ do
    describe "connect using async agent commands" $
      testBasicMatrix2 t testAsyncCommands
    it "should restore and complete async commands on restart" $
      testAsyncCommandsRestore t
    describe "accept connection using async command" $
      testBasicMatrix2 t testAcceptContactAsync
    it "should delete connections using async command when server connection fails" $
      testDeleteConnectionAsync t
    it "join connection when reply queue creation fails (v8 - slow handshake)" $
      testJoinConnectionAsyncReplyErrorV8 t
    it "join connection when reply queue creation fails" $
      testJoinConnectionAsyncReplyError t
    describe "delete connection waiting for delivery" $ do
      it "should delete connection immediately if there are no pending messages" $
        testWaitDeliveryNoPending t
      it "should delete connection after waiting for delivery to complete" $
        testWaitDelivery t
      it "should delete connection if message can't be delivered due to AUTH error" $
        testWaitDeliveryAUTHErr t
      it "should delete connection by timeout even if message wasn't delivered" $
        testWaitDeliveryTimeout t
      it "should delete connection by timeout, message in progress can be delivered" $
        testWaitDeliveryTimeout2 t
  describe "Users" $ do
    it "should create and delete user with connections" $
      withSmpServer t testUsers
    it "should create and delete user without connections" $
      withSmpServer t testDeleteUserQuietly
    it "should create and delete user with connections when server connection fails" $
      testUsersNoServer t
    it "should connect two users and switch session mode" $
      withSmpServer t testTwoUsers
  describe "Connection switch" $ do
    describe "should switch delivery to the new queue" $
      testServerMatrix2 t testSwitchConnection
    describe "should switch to new queue asynchronously" $
      testServerMatrix2 t testSwitchAsync
    describe "should delete connection during switch" $
      testServerMatrix2 t testSwitchDelete
    describe "should abort switch in Started phase" $
      testServerMatrix2 t testAbortSwitchStarted
    describe "should abort switch in Started phase, reinitiate immediately" $
      testServerMatrix2 t testAbortSwitchStartedReinitiate
    describe "should prohibit to abort switch in Secured phase" $
      testServerMatrix2 t testCannotAbortSwitchSecured
    describe "should switch two connections simultaneously" $
      testServerMatrix2 t testSwitch2Connections
    describe "should switch two connections simultaneously, abort one" $
      testServerMatrix2 t testSwitch2ConnectionsAbort1
  describe "SMP basic auth" $ do
    forM_ (nub [prevVersion authCmdsSMPVersion, authCmdsSMPVersion, currentServerSMPRelayVersion]) $ \v -> do
      let baseId = if v >= sndAuthKeySMPVersion then 1 else 3
          sqSecured = if v >= sndAuthKeySMPVersion then True else False
      describe ("v" <> show v <> ": with server auth") $ do
        --                                       allow NEW | server auth, v | clnt1 auth, v  | clnt2 auth, v    |  2 - success, 1 - JOIN fail, 0 - NEW fail
        it "success                " $ testBasicAuth t True (Just "abcd", v) (Just "abcd", v) (Just "abcd", v) sqSecured baseId `shouldReturn` 2
        it "disabled               " $ testBasicAuth t False (Just "abcd", v) (Just "abcd", v) (Just "abcd", v) sqSecured baseId `shouldReturn` 0
        it "NEW fail, no auth      " $ testBasicAuth t True (Just "abcd", v) (Nothing, v) (Just "abcd", v) sqSecured baseId `shouldReturn` 0
        it "NEW fail, bad auth     " $ testBasicAuth t True (Just "abcd", v) (Just "wrong", v) (Just "abcd", v) sqSecured baseId `shouldReturn` 0
        it "JOIN fail, no auth     " $ testBasicAuth t True (Just "abcd", v) (Just "abcd", v) (Nothing, v) sqSecured baseId `shouldReturn` 1
        it "JOIN fail, bad auth    " $ testBasicAuth t True (Just "abcd", v) (Just "abcd", v) (Just "wrong", v) sqSecured baseId `shouldReturn` 1
      describe ("v" <> show v <> ": no server auth") $ do
        it "success     " $ testBasicAuth t True (Nothing, v) (Nothing, v) (Nothing, v) sqSecured baseId `shouldReturn` 2
        it "srv disabled" $ testBasicAuth t False (Nothing, v) (Nothing, v) (Nothing, v) sqSecured baseId `shouldReturn` 0
        it "auth fst    " $ testBasicAuth t True (Nothing, v) (Just "abcd", v) (Nothing, v) sqSecured baseId `shouldReturn` 2
        it "auth snd    " $ testBasicAuth t True (Nothing, v) (Nothing, v) (Just "abcd", v) sqSecured baseId `shouldReturn` 2
        it "auth both   " $ testBasicAuth t True (Nothing, v) (Just "abcd", v) (Just "abcd", v) sqSecured baseId `shouldReturn` 2
        it "auth, disabled" $ testBasicAuth t False (Nothing, v) (Just "abcd", v) (Just "abcd", v) sqSecured baseId `shouldReturn` 0
  describe "SMP server test via agent API" $ do
    it "should pass without basic auth" $ testSMPServerConnectionTest t Nothing (noAuthSrv testSMPServer2) `shouldReturn` Nothing
    let srv1 = testSMPServer2 {keyHash = "1234"}
    it "should fail with incorrect fingerprint" $ do
      testSMPServerConnectionTest t Nothing (noAuthSrv srv1) `shouldReturn` Just (ProtocolTestFailure TSConnect $ BROKER (B.unpack $ strEncode srv1) NETWORK)
    describe "server with password" $ do
      let auth = Just "abcd"
          srv = ProtoServerWithAuth testSMPServer2
          authErr = Just (ProtocolTestFailure TSCreateQueue $ SMP (B.unpack $ strEncode testSMPServer2) AUTH)
      it "should pass with correct password" $ testSMPServerConnectionTest t auth (srv auth) `shouldReturn` Nothing
      it "should fail without password" $ testSMPServerConnectionTest t auth (srv Nothing) `shouldReturn` authErr
      it "should fail with incorrect password" $ testSMPServerConnectionTest t auth (srv $ Just "wrong") `shouldReturn` authErr
  describe "getRatchetAdHash" $
    it "should return the same data for both peers" $
      withSmpServer t testRatchetAdHash
  describe "Delivery receipts" $ do
    it "should send and receive delivery receipt" $ withSmpServer t testDeliveryReceipts
    it "should send delivery receipt only in connection v3+" $ testDeliveryReceiptsVersion t
    it "send delivery receipts concurrently with messages" $ testDeliveryReceiptsConcurrent t
  describe "user network info" $ do
    it "should wait for user network" testWaitForUserNetwork
    it "should not reset online to offline if happens too quickly" testDoNotResetOnlineToOffline
    it "should resume multiple threads" testResumeMultipleThreads
  describe "SMP queue info" $ do
    it "server should respond with queue and subscription information" $
      withSmpServer t testServerQueueInfo

testBasicAuth :: ATransport -> Bool -> (Maybe BasicAuth, VersionSMP) -> (Maybe BasicAuth, VersionSMP) -> (Maybe BasicAuth, VersionSMP) -> SndQueueSecured -> AgentMsgId -> IO Int
testBasicAuth t allowNewQueues srv@(srvAuth, srvVersion) clnt1 clnt2 sqSecured baseId = do
  let testCfg = cfg {allowNewQueues, newQueueBasicAuth = srvAuth, smpServerVRange = V.mkVersionRange minServerSMPRelayVersion srvVersion}
      canCreate1 = canCreateQueue allowNewQueues srv clnt1
      canCreate2 = canCreateQueue allowNewQueues srv clnt2
      expected
        | canCreate1 && canCreate2 = 2
        | canCreate1 = 1
        | otherwise = 0
  created <- withSmpServerConfigOn t testCfg testPort $ \_ -> testCreateQueueAuth srvVersion clnt1 clnt2 sqSecured baseId
  created `shouldBe` expected
  pure created

canCreateQueue :: Bool -> (Maybe BasicAuth, VersionSMP) -> (Maybe BasicAuth, VersionSMP) -> Bool
canCreateQueue allowNew (srvAuth, _) (clntAuth, _) =
  allowNew && (isNothing srvAuth || srvAuth == clntAuth)

testMatrix2 :: HasCallStack => ATransport -> (PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testMatrix2 t runTest = do
  it "current, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 agentCfg agentCfg (initAgentServersProxy SPMAlways SPFProhibit) 1 $ runTest PQSupportOn True True
  it "v8, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 agentProxyCfgV8 agentProxyCfgV8 (initAgentServersProxy SPMAlways SPFProhibit) 3 $ runTest PQSupportOn False True
  it "current" $ withSmpServer t $ runTestCfg2 agentCfg agentCfg 1 $ runTest PQSupportOn True False
  it "prev" $ withSmpServer t $ runTestCfg2 agentCfgVPrev agentCfgVPrev 3 $ runTest PQSupportOff False False
  it "prev to current" $ withSmpServer t $ runTestCfg2 agentCfgVPrev agentCfg 3 $ runTest PQSupportOff False False
  it "current to prev" $ withSmpServer t $ runTestCfg2 agentCfg agentCfgVPrev 3 $ runTest PQSupportOff False False

testMatrix2Stress :: HasCallStack => ATransport -> (PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testMatrix2Stress t runTest = do
  it "current, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 aCfg aCfg (initAgentServersProxy SPMAlways SPFProhibit) 1 $ runTest PQSupportOn True True
  it "v8, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 aProxyCfgV8 aProxyCfgV8 (initAgentServersProxy SPMAlways SPFProhibit) 3 $ runTest PQSupportOn False True
  it "current" $ withSmpServer t $ runTestCfg2 aCfg aCfg 1 $ runTest PQSupportOn True False
  it "prev" $ withSmpServer t $ runTestCfg2 aCfgVPrev aCfgVPrev 3 $ runTest PQSupportOff False False
  it "prev to current" $ withSmpServer t $ runTestCfg2 aCfgVPrev aCfg 3 $ runTest PQSupportOff False False
  it "current to prev" $ withSmpServer t $ runTestCfg2 aCfg aCfgVPrev 3 $ runTest PQSupportOff False False
  where
    aCfg = agentCfg {messageRetryInterval = fastMessageRetryInterval}
    aProxyCfgV8 = agentProxyCfgV8 {messageRetryInterval = fastMessageRetryInterval}
    aCfgVPrev = agentCfgVPrev {messageRetryInterval = fastMessageRetryInterval}

testBasicMatrix2 :: HasCallStack => ATransport -> (SndQueueSecured -> AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testBasicMatrix2 t runTest = do
  it "current" $ withSmpServer t $ runTestCfg2 agentCfg agentCfg 1 $ runTest True
  it "prev" $ withSmpServer t $ runTestCfg2 agentCfgVPrevPQ agentCfgVPrevPQ 3 $ runTest False
  it "prev to current" $ withSmpServer t $ runTestCfg2 agentCfgVPrevPQ agentCfg 3 $ runTest False
  it "current to prev" $ withSmpServer t $ runTestCfg2 agentCfg agentCfgVPrevPQ 3 $ runTest False

testRatchetMatrix2 :: HasCallStack => ATransport -> (PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testRatchetMatrix2 t runTest = do
  it "current, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 agentCfg agentCfg (initAgentServersProxy SPMAlways SPFProhibit) 1 $ runTest PQSupportOn True True
  it "v8, via proxy" $ withSmpServerProxy t $ runTestCfgServers2 agentProxyCfgV8 agentProxyCfgV8 (initAgentServersProxy SPMAlways SPFProhibit) 3 $ runTest PQSupportOn False True
  it "ratchet current" $ withSmpServer t $ runTestCfg2 agentCfg agentCfg 1 $ runTest PQSupportOn True False
  it "ratchet prev" $ withSmpServer t $ runTestCfg2 agentCfgRatchetVPrev agentCfgRatchetVPrev 1 $ runTest PQSupportOff True False
  it "ratchets prev to current" $ withSmpServer t $ runTestCfg2 agentCfgRatchetVPrev agentCfg 1 $ runTest PQSupportOff True False
  it "ratchets current to prev" $ withSmpServer t $ runTestCfg2 agentCfg agentCfgRatchetVPrev 1 $ runTest PQSupportOff True False

testServerMatrix2 :: HasCallStack => ATransport -> (InitialAgentServers -> IO ()) -> Spec
testServerMatrix2 t runTest = do
  it "1 server" $ withSmpServer t $ runTest initAgentServers
  it "2 servers" $ withSmpServer t $ withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \_ -> runTest initAgentServers2

testPQMatrix2 :: HasCallStack => ATransport -> (HasCallStack => (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()) -> Spec
testPQMatrix2 = pqMatrix2_ True

testPQMatrix2NoInv :: HasCallStack => ATransport -> (HasCallStack => (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()) -> Spec
testPQMatrix2NoInv = pqMatrix2_ False

pqMatrix2_ :: HasCallStack => Bool -> ATransport -> (HasCallStack => (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()) -> Spec
pqMatrix2_ pqInv t test = do
  it "dh/dh handshake" $ runTest $ \a b -> test (a, IKPQOff) (b, PQSupportOff)
  it "dh/pq handshake" $ runTest $ \a b -> test (a, IKPQOff) (b, PQSupportOn)
  it "pq/dh handshake" $ runTest $ \a b -> test (a, IKPQOn) (b, PQSupportOff)
  it "pq/pq handshake" $ runTest $ \a b -> test (a, IKPQOn) (b, PQSupportOn)
  when pqInv $ do
    it "pq-inv/dh handshake" $ runTest $ \a b -> test (a, IKUsePQ) (b, PQSupportOff)
    it "pq-inv/pq handshake" $ runTest $ \a b -> test (a, IKUsePQ) (b, PQSupportOn)
  where
    runTest = withSmpServerProxy t . runTestCfgServers2 agentProxyCfgV8 agentProxyCfgV8 (initAgentServersProxy SPMAlways SPFProhibit) 3

testPQMatrix3 ::
  HasCallStack =>
  ATransport ->
  (HasCallStack => (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()) ->
  Spec
testPQMatrix3 t test = do
  it "dh" $ runTest $ \a b c -> test (a, IKPQOff) (b, PQSupportOff) (c, PQSupportOff)
  it "dh/dh/pq" $ runTest $ \a b c -> test (a, IKPQOff) (b, PQSupportOff) (c, PQSupportOn)
  it "dh/pq/dh" $ runTest $ \a b c -> test (a, IKPQOff) (b, PQSupportOn) (c, PQSupportOff)
  it "dh/pq/pq" $ runTest $ \a b c -> test (a, IKPQOff) (b, PQSupportOn) (c, PQSupportOn)
  it "pq/dh/dh" $ runTest $ \a b c -> test (a, IKPQOn) (b, PQSupportOff) (c, PQSupportOff)
  it "pq/dh/pq" $ runTest $ \a b c -> test (a, IKPQOn) (b, PQSupportOff) (c, PQSupportOn)
  it "pq/pq/dh" $ runTest $ \a b c -> test (a, IKPQOn) (b, PQSupportOn) (c, PQSupportOff)
  it "pq" $ runTest $ \a b c -> test (a, IKPQOn) (b, PQSupportOn) (c, PQSupportOn)
  where
    runTest test' =
      withSmpServerProxy t $
        runTestCfgServers2 agentProxyCfgV8 agentProxyCfgV8 servers 3 $ \a b baseMsgId ->
          withAgent 3 agentProxyCfgV8 servers testDB3 $ \c -> test' a b c baseMsgId
    servers = initAgentServersProxy SPMAlways SPFProhibit

runTestCfg2 :: HasCallStack => AgentConfig -> AgentConfig -> AgentMsgId -> (HasCallStack => AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> IO ()
runTestCfg2 aCfg bCfg = runTestCfgServers2 aCfg bCfg initAgentServers
{-# INLINE runTestCfg2 #-}

runTestCfgServers2 :: HasCallStack => AgentConfig -> AgentConfig -> InitialAgentServers -> AgentMsgId -> (HasCallStack => AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> IO ()
runTestCfgServers2 aCfg bCfg servers baseMsgId runTest =
  withAgentClientsCfgServers2 aCfg bCfg servers $ \a b -> runTest a b baseMsgId
{-# INLINE runTestCfgServers2 #-}

withAgentClientsCfgServers2 :: HasCallStack => AgentConfig -> AgentConfig -> InitialAgentServers -> (HasCallStack => AgentClient -> AgentClient -> IO a) -> IO a
withAgentClientsCfgServers2 aCfg bCfg servers runTest =
  withAgent 1 aCfg servers testDB $ \a ->
    withAgent 2 bCfg servers testDB2 $ \b ->
      runTest a b

withAgentClientsCfg2 :: HasCallStack => AgentConfig -> AgentConfig -> (HasCallStack => AgentClient -> AgentClient -> IO a) -> IO a
withAgentClientsCfg2 aCfg bCfg = withAgentClientsCfgServers2 aCfg bCfg initAgentServers
{-# INLINE withAgentClientsCfg2 #-}

withAgentClients2 :: HasCallStack => (HasCallStack => AgentClient -> AgentClient -> IO a) -> IO a
withAgentClients2 = withAgentClientsCfg2 agentCfg agentCfg
{-# INLINE withAgentClients2 #-}

withAgentClients3 :: HasCallStack => (HasCallStack => AgentClient -> AgentClient -> AgentClient -> IO ()) -> IO ()
withAgentClients3 runTest =
  withAgentClients2 $ \a b ->
    withAgent 3 agentCfg initAgentServers testDB3 $ \c ->
      runTest a b c

runAgentClientTest :: HasCallStack => PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientTest pqSupport sqSecured viaProxy alice bob baseId =
  runAgentClientTestPQ sqSecured viaProxy (alice, IKNoPQ pqSupport) (bob, pqSupport) baseId

runAgentClientTestPQ :: HasCallStack => SndQueueSecured -> Bool -> (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()
runAgentClientTestPQ sqSecured viaProxy (alice, aPQ) (bob, bPQ) baseId =
  runRight_ $ do
    (bobId, qInfo) <- A.createConnection alice 1 True SCMInvitation Nothing aPQ SMSubscribe
    aliceId <- A.prepareConnectionToJoin bob 1 True qInfo bPQ
    sqSecured' <- A.joinConnection bob 1 aliceId True qInfo "bob's connInfo" bPQ SMSubscribe
    liftIO $ sqSecured' `shouldBe` sqSecured
    ("", _, A.CONF confId pqSup' _ "bob's connInfo") <- get alice
    liftIO $ pqSup' `shouldBe` CR.connPQEncryption aPQ
    allowConnection alice bobId confId "alice's connInfo"
    let pqEnc = PQEncryption $ pqConnectionMode aPQ bPQ
    get alice ##> ("", bobId, A.CON pqEnc)
    get bob ##> ("", aliceId, A.INFO bPQ "alice's connInfo")
    get bob ##> ("", aliceId, A.CON pqEnc)
    -- message IDs 1 to 3 (or 1 to 4 in v1) get assigned to control messages, so first MSG is assigned ID 4
    let proxySrv = if viaProxy then Just testSMPServer else Nothing
    1 <- msgId <$> A.sendMessage alice bobId pqEnc SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, A.SENT (baseId + 1) proxySrv)
    2 <- msgId <$> A.sendMessage alice bobId pqEnc SMP.noMsgFlags "how are you?"
    get alice ##> ("", bobId, A.SENT (baseId + 2) proxySrv)
    get bob =##> \case ("", c, Msg' _ pq "hello") -> c == aliceId && pq == pqEnc; _ -> False
    ackMessage bob aliceId (baseId + 1) Nothing
    get bob =##> \case ("", c, Msg' _ pq "how are you?") -> c == aliceId && pq == pqEnc; _ -> False
    ackMessage bob aliceId (baseId + 2) Nothing
    3 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "hello too"
    get bob ##> ("", aliceId, A.SENT (baseId + 3) proxySrv)
    4 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "message 1"
    get bob ##> ("", aliceId, A.SENT (baseId + 4) proxySrv)
    get alice =##> \case ("", c, Msg' _ pq "hello too") -> c == bobId && pq == pqEnc; _ -> False
    ackMessage alice bobId (baseId + 3) Nothing
    get alice =##> \case ("", c, Msg' _ pq "message 1") -> c == bobId && pq == pqEnc; _ -> False
    ackMessage alice bobId (baseId + 4) Nothing
    suspendConnection alice bobId
    5 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "message 2"
    get bob =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == aliceId && mId == (baseId + 5); _ -> False
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId . fst

pqConnectionMode :: InitialKeys -> PQSupport -> Bool
pqConnectionMode pqMode1 pqMode2 = supportPQ (CR.connPQEncryption pqMode1) && supportPQ pqMode2

runAgentClientStressTestOneWay :: HasCallStack => Int64 -> PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientStressTestOneWay n pqSupport sqSecured viaProxy alice bob baseId = runRight_ $ do
  let pqEnc = PQEncryption $ supportPQ pqSupport
  (aliceId, bobId) <- makeConnection_ pqSupport sqSecured alice bob
  let proxySrv = if viaProxy then Just testSMPServer else Nothing
      message i = "message " <> bshow i
  concurrently_
    ( forM_ ([1 .. n] :: [Int64]) $ \i -> do
        mId <- msgId <$> A.sendMessage alice bobId pqEnc SMP.noMsgFlags (message i)
        liftIO $ do
          mId >= i `shouldBe` True
          let getEvent =
                get alice >>= \case
                  ("", c, A.SENT mId' srv) -> c == bobId && mId' >= baseId + i && srv == proxySrv `shouldBe` True
                  ("", c, QCONT) -> do
                    c == bobId `shouldBe` True
                    getEvent
                  r -> expectationFailure $ "wrong message: " <> show r
          getEvent
    )
    ( forM_ ([1 .. n] :: [Int64]) $ \i -> do
        get bob >>= \case
          ("", c, Msg' mId pq msg) -> do
            liftIO $ c == aliceId && mId >= baseId + i && pq == pqEnc && msg == message i `shouldBe` True
            ackMessage bob aliceId mId Nothing
          r -> liftIO $ expectationFailure $ "wrong message: " <> show r
    )
  liftIO $ noMessagesIngoreQCONT alice "nothing else should be delivered to alice"
  liftIO $ noMessagesIngoreQCONT bob "nothing else should be delivered to bob"
  where
    msgId = subtract baseId . fst

runAgentClientStressTestConc :: HasCallStack => Int64 -> PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientStressTestConc n pqSupport sqSecured viaProxy alice bob baseId = runRight_ $ do
  let pqEnc = PQEncryption $ supportPQ pqSupport
  (aliceId, bobId) <- makeConnection_ pqSupport sqSecured alice bob
  let proxySrv = if viaProxy then Just testSMPServer else Nothing
      message i = "message " <> bshow i
      loop a bId mIdVar i = do
        when (i <= n) $ do
          mId <- msgId <$> A.sendMessage a bId pqEnc SMP.noMsgFlags (message i)
          liftIO $ mId >= i `shouldBe` True
        let getEvent = do
              get a >>= \case
                ("", c, A.SENT _ srv) -> liftIO $ c == bId && srv == proxySrv `shouldBe` True
                ("", c, QCONT) -> do
                  liftIO $ c == bId `shouldBe` True
                  getEvent
                ("", c, Msg' mId pq msg) -> do
                  -- tests that mId increases
                  liftIO $ (mId >) <$> atomically (swapTVar mIdVar mId) `shouldReturn` True
                  liftIO $ c == bId && pq == pqEnc && ("message " `B.isPrefixOf` msg) `shouldBe` True
                  ackMessage a bId mId Nothing
                r -> liftIO $ expectationFailure $ "wrong message: " <> show r
        getEvent
  amId <- newTVarIO 0
  bmId <- newTVarIO 0
  concurrently_
    (forM_ ([1 .. n * 2] :: [Int64]) $ loop alice bobId amId)
    (forM_ ([1 .. n * 2] :: [Int64]) $ loop bob aliceId bmId)
  liftIO $ noMessagesIngoreQCONT alice "nothing else should be delivered to alice"
  liftIO $ noMessagesIngoreQCONT bob "nothing else should be delivered to bob"
  where
    msgId = subtract baseId . fst

testEnablePQEncryption :: HasCallStack => IO ()
testEnablePQEncryption =
  withAgentClients2 $ \ca cb -> runRight_ $ do
    g <- liftIO C.newRandom
    (aId, bId) <- makeConnection_ PQSupportOff True ca cb
    let a = (ca, aId)
        b = (cb, bId)
    (a, 2, "msg 1") \#>\ b
    (b, 3, "msg 2") \#>\ a
    -- 45 bytes is used by agent message envelope inside double ratchet message envelope
    let largeMsg g' pqEnc = atomically $ C.randomBytes (e2eEncAgentMsgLength pqdrSMPAgentVersion pqEnc - 45) g'
    lrg <- largeMsg g PQSupportOff
    (a, 4, lrg) \#>\ b
    (b, 5, lrg) \#>\ a
    -- switched to smaller envelopes (before reporting PQ encryption enabled)
    sml <- largeMsg g PQSupportOn
    -- fail because of message size
    Left (A.CMD LARGE _) <- tryError $ A.sendMessage ca bId PQEncOn SMP.noMsgFlags lrg
    (7, PQEncOff) <- A.sendMessage ca bId PQEncOn SMP.noMsgFlags sml
    get ca =##> \case ("", connId, SENT 7) -> connId == bId; _ -> False
    get cb =##> \case ("", connId, MsgErr' 6 MsgSkipped {} PQEncOff msg') -> connId == aId && msg' == sml; _ -> False
    ackMessage cb aId 6 Nothing
    -- -- fail in reply to sync IDss
    Left (A.CMD LARGE _) <- tryError $ A.sendMessage cb aId PQEncOn SMP.noMsgFlags lrg
    (8, PQEncOff) <- A.sendMessage cb aId PQEncOn SMP.noMsgFlags sml
    get cb =##> \case ("", connId, SENT 8) -> connId == aId; _ -> False
    get ca =##> \case ("", connId, MsgErr' 8 MsgSkipped {} PQEncOff msg') -> connId == bId && msg' == sml; _ -> False
    ackMessage ca bId 8 Nothing
    (a, 9, sml) \#>! b
    -- PQ encryption now enabled
    (b, 10, sml) !#>! a
    (a, 11, sml) !#>! b
    (b, 12, sml) !#>! a
    -- disabling PQ encryption
    (a, 13, sml) !#>\ b
    (b, 14, sml) !#>\ a
    (a, 15, sml) \#>\ b
    (b, 16, sml) \#>\ a
    -- enabling PQ encryption again
    (a, 17, sml) \#>! b
    (b, 18, sml) \#>! a
    (a, 19, sml) \#>! b
    (b, 20, sml) !#>! a
    (a, 21, sml) !#>! b
    -- disabling PQ encryption again
    (b, 22, sml) !#>\ a
    (a, 23, sml) !#>\ b
    (b, 24, sml) \#>\ a
    (a, 25, sml) \#>\ b
    -- PQ encryption is now disabled, but support remained enabled, so we still cannot send larger messages
    Left (A.CMD LARGE _) <- tryError $ A.sendMessage ca bId PQEncOff SMP.noMsgFlags (sml <> "123456")
    Left (A.CMD LARGE _) <- tryError $ A.sendMessage cb aId PQEncOff SMP.noMsgFlags (sml <> "123456")
    pure ()
  where
    (\#>\) = PQEncOff `sndRcv` PQEncOff
    (\#>!) = PQEncOff `sndRcv` PQEncOn
    (!#>!) = PQEncOn `sndRcv` PQEncOn
    (!#>\) = PQEncOn `sndRcv` PQEncOff

sndRcv :: PQEncryption -> PQEncryption -> ((AgentClient, ConnId), AgentMsgId, MsgBody) -> (AgentClient, ConnId) -> ExceptT AgentErrorType IO ()
sndRcv pqEnc pqEnc' ((c1, id1), mId, msg) (c2, id2) = do
  r <- A.sendMessage c1 id2 pqEnc' SMP.noMsgFlags msg
  liftIO $ r `shouldBe` (mId, pqEnc)
  get c1 =##> \case ("", connId, SENT mId') -> connId == id2 && mId' == mId; _ -> False
  get c2 =##> \case ("", connId, Msg' mId' pq msg') -> connId == id1 && mId' == mId && msg' == msg && pq == pqEnc; _ -> False
  ackMessage c2 id1 mId Nothing

testAgentClient3 :: HasCallStack => IO ()
testAgentClient3 =
  withAgentClients3 $ \a b c -> runRight_ $ do
    (aIdForB, bId) <- makeConnection a b
    (aIdForC, cId) <- makeConnection a c

    2 <- sendMessage a bId SMP.noMsgFlags "b4"
    2 <- sendMessage a cId SMP.noMsgFlags "c4"
    3 <- sendMessage a bId SMP.noMsgFlags "b5"
    3 <- sendMessage a cId SMP.noMsgFlags "c5"
    get a =##> \case ("", connId, SENT 2) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 2) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 3) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 3) -> connId == bId || connId == cId; _ -> False
    get b =##> \case ("", connId, Msg "b4") -> connId == aIdForB; _ -> False
    ackMessage b aIdForB 2 Nothing
    get b =##> \case ("", connId, Msg "b5") -> connId == aIdForB; _ -> False
    ackMessage b aIdForB 3 Nothing
    get c =##> \case ("", connId, Msg "c4") -> connId == aIdForC; _ -> False
    ackMessage c aIdForC 2 Nothing
    get c =##> \case ("", connId, Msg "c5") -> connId == aIdForC; _ -> False
    ackMessage c aIdForC 3 Nothing

runAgentClientContactTest :: HasCallStack => PQSupport -> SndQueueSecured -> Bool -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientContactTest pqSupport sqSecured viaProxy alice bob baseId =
  runAgentClientContactTestPQ sqSecured viaProxy pqSupport (alice, IKNoPQ pqSupport) (bob, pqSupport) baseId

runAgentClientContactTestPQ :: HasCallStack => SndQueueSecured -> Bool -> PQSupport -> (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()
runAgentClientContactTestPQ sqSecured viaProxy reqPQSupport (alice, aPQ) (bob, bPQ) baseId =
  runRight_ $ do
    (_, qInfo) <- A.createConnection alice 1 True SCMContact Nothing aPQ SMSubscribe
    aliceId <- A.prepareConnectionToJoin bob 1 True qInfo bPQ
    sqSecuredJoin <- A.joinConnection bob 1 aliceId True qInfo "bob's connInfo" bPQ SMSubscribe
    liftIO $ sqSecuredJoin `shouldBe` False -- joining via contact address connection
    ("", _, A.REQ invId pqSup' _ "bob's connInfo") <- get alice
    liftIO $ pqSup' `shouldBe` reqPQSupport
    bobId <- A.prepareConnectionToAccept alice True invId (CR.connPQEncryption aPQ)
    sqSecured' <- acceptContact alice bobId True invId "alice's connInfo" (CR.connPQEncryption aPQ) SMSubscribe
    liftIO $ sqSecured' `shouldBe` sqSecured
    ("", _, A.CONF confId pqSup'' _ "alice's connInfo") <- get bob
    liftIO $ pqSup'' `shouldBe` bPQ
    allowConnection bob aliceId confId "bob's connInfo"
    let pqEnc = PQEncryption $ pqConnectionMode aPQ bPQ
    get alice ##> ("", bobId, A.INFO (CR.connPQEncryption aPQ) "bob's connInfo")
    get alice ##> ("", bobId, A.CON pqEnc)
    get bob ##> ("", aliceId, A.CON pqEnc)
    -- message IDs 1 to 3 (or 1 to 4 in v1) get assigned to control messages, so first MSG is assigned ID 4
    let proxySrv = if viaProxy then Just testSMPServer else Nothing
    1 <- msgId <$> A.sendMessage alice bobId pqEnc SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, A.SENT (baseId + 1) proxySrv)
    2 <- msgId <$> A.sendMessage alice bobId pqEnc SMP.noMsgFlags "how are you?"
    get alice ##> ("", bobId, A.SENT (baseId + 2) proxySrv)
    get bob =##> \case ("", c, Msg' _ pq "hello") -> c == aliceId && pq == pqEnc; _ -> False
    ackMessage bob aliceId (baseId + 1) Nothing
    get bob =##> \case ("", c, Msg' _ pq "how are you?") -> c == aliceId && pq == pqEnc; _ -> False
    ackMessage bob aliceId (baseId + 2) Nothing
    3 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "hello too"
    get bob ##> ("", aliceId, A.SENT (baseId + 3) proxySrv)
    4 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "message 1"
    get bob ##> ("", aliceId, A.SENT (baseId + 4) proxySrv)
    get alice =##> \case ("", c, Msg' _ pq "hello too") -> c == bobId && pq == pqEnc; _ -> False
    ackMessage alice bobId (baseId + 3) Nothing
    get alice =##> \case ("", c, Msg' _ pq "message 1") -> c == bobId && pq == pqEnc; _ -> False
    ackMessage alice bobId (baseId + 4) Nothing
    suspendConnection alice bobId
    5 <- msgId <$> A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "message 2"
    get bob =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == aliceId && mId == (baseId + 5); _ -> False
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId . fst

runAgentClientContactTestPQ3 :: HasCallStack => Bool -> (AgentClient, InitialKeys) -> (AgentClient, PQSupport) -> (AgentClient, PQSupport) -> AgentMsgId -> IO ()
runAgentClientContactTestPQ3 viaProxy (alice, aPQ) (bob, bPQ) (tom, tPQ) baseId = runRight_ $ do
  (_, qInfo) <- A.createConnection alice 1 True SCMContact Nothing aPQ SMSubscribe
  (bAliceId, bobId, abPQEnc) <- connectViaContact bob bPQ qInfo
  sentMessages abPQEnc alice bobId bob bAliceId
  (tAliceId, tomId, atPQEnc) <- connectViaContact tom tPQ qInfo
  sentMessages atPQEnc alice tomId tom tAliceId
  where
    msgId = subtract baseId . fst
    connectViaContact b pq qInfo = do
      aId <- A.prepareConnectionToJoin b 1 True qInfo pq
      sqSecuredJoin <- A.joinConnection b 1 aId True qInfo "bob's connInfo" pq SMSubscribe
      liftIO $ sqSecuredJoin `shouldBe` False -- joining via contact address connection
      ("", _, A.REQ invId pqSup' _ "bob's connInfo") <- get alice
      liftIO $ pqSup' `shouldBe` PQSupportOn
      bId <- A.prepareConnectionToAccept alice True invId (CR.connPQEncryption aPQ)
      sqSecuredAccept <- acceptContact alice bId True invId "alice's connInfo" (CR.connPQEncryption aPQ) SMSubscribe
      liftIO $ sqSecuredAccept `shouldBe` False -- agent cfg is v8
      ("", _, A.CONF confId pqSup'' _ "alice's connInfo") <- get b
      liftIO $ pqSup'' `shouldBe` pq
      allowConnection b aId confId "bob's connInfo"
      let pqEnc = PQEncryption $ pqConnectionMode aPQ pq
      get alice ##> ("", bId, A.INFO (CR.connPQEncryption aPQ) "bob's connInfo")
      get alice ##> ("", bId, A.CON pqEnc)
      get b ##> ("", aId, A.CON pqEnc)
      pure (aId, bId, pqEnc)
    sentMessages pqEnc a bId b aId = do
      let proxySrv = if viaProxy then Just testSMPServer else Nothing
      1 <- msgId <$> A.sendMessage a bId pqEnc SMP.noMsgFlags "hello"
      get a ##> ("", bId, A.SENT (baseId + 1) proxySrv)
      get b =##> \case ("", c, Msg' _ pq "hello") -> c == aId && pq == pqEnc; _ -> False
      ackMessage b aId (baseId + 1) Nothing
      2 <- msgId <$> A.sendMessage b aId pqEnc SMP.noMsgFlags "hello too"
      get b ##> ("", aId, A.SENT (baseId + 2) proxySrv)
      get a =##> \case ("", c, Msg' _ pq "hello too") -> c == bId && pq == pqEnc; _ -> False
      ackMessage a bId (baseId + 2) Nothing

noMessages :: HasCallStack => AgentClient -> String -> Expectation
noMessages = noMessages_ False

noMessagesIngoreQCONT :: AgentClient -> String -> Expectation
noMessagesIngoreQCONT = noMessages_ True

noMessages_ :: Bool -> HasCallStack => AgentClient -> String -> Expectation
noMessages_ ingoreQCONT c err = tryGet `shouldReturn` ()
  where
    tryGet =
      10000 `timeout` get c >>= \case
        Just (_, _, QCONT) | ingoreQCONT -> noMessages_ ingoreQCONT c err
        Just msg -> error $ err <> ": " <> show msg
        _ -> return ()

testRejectContactRequest :: HasCallStack => IO ()
testRejectContactRequest =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (addrConnId, qInfo) <- A.createConnection alice 1 True SCMContact Nothing IKPQOn SMSubscribe
    aliceId <- A.prepareConnectionToJoin bob 1 True qInfo PQSupportOn
    sqSecured <- A.joinConnection bob 1 aliceId True qInfo "bob's connInfo" PQSupportOn SMSubscribe
    liftIO $ sqSecured `shouldBe` False -- joining via contact address connection
    ("", _, A.REQ invId PQSupportOn _ "bob's connInfo") <- get alice
    liftIO $ runExceptT (rejectContact alice "abcd" invId) `shouldReturn` Left (CONN NOT_FOUND)
    rejectContact alice addrConnId invId
    liftIO $ noMessages bob "nothing delivered to bob"

testUpdateConnectionUserId :: HasCallStack => IO ()
testUpdateConnectionUserId =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (connId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    newUserId <- createUser alice [noAuthSrvCfg testSMPServer] [noAuthSrvCfg testXFTPServer]
    _ <- changeConnectionUser alice 1 connId newUserId
    aliceId <- A.prepareConnectionToJoin bob 1 True qInfo PQSupportOn
    sqSecured' <- A.joinConnection bob 1 aliceId True qInfo "bob's connInfo" PQSupportOn SMSubscribe
    liftIO $ sqSecured' `shouldBe` True
    ("", _, A.CONF confId pqSup' _ "bob's connInfo") <- get alice
    liftIO $ pqSup' `shouldBe` PQSupportOn
    allowConnection alice connId confId "alice's connInfo"
    let pqEnc = CR.pqSupportToEnc PQSupportOn
    get alice ##> ("", connId, A.CON pqEnc)
    get bob ##> ("", aliceId, A.INFO PQSupportOn "alice's connInfo")
    get bob ##> ("", aliceId, A.CON pqEnc)

testAsyncInitiatingOffline :: HasCallStack => IO ()
testAsyncInitiatingOffline =
  withAgent 2 agentCfg initAgentServers testDB2 $ \bob -> runRight_ $ do
    alice <- liftIO $ getSMPAgentClient' 1 agentCfg initAgentServers testDB
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    liftIO $ disposeAgentClient alice

    (aliceId, sqSecured) <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True

    -- send messages
    msgId1 <- A.sendMessage bob aliceId PQEncOn SMP.noMsgFlags "can send 1"
    liftIO $ msgId1 `shouldBe` (2, PQEncOff)
    get bob ##> ("", aliceId, SENT 2)
    msgId2 <- A.sendMessage bob aliceId PQEncOn SMP.noMsgFlags "can send 2"
    liftIO $ msgId2 `shouldBe` (3, PQEncOff)
    get bob ##> ("", aliceId, SENT 3)

    alice' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB
    subscribeConnection alice' bobId
    ("", _, CONF confId _ "bob's connInfo") <- get alice'
    -- receive messages
    get alice' =##> \case ("", c, Msg' mId pq "can send 1") -> c == bobId && mId == 1 && pq == PQEncOff; _ -> False
    ackMessage alice' bobId 1 Nothing
    get alice' =##> \case ("", c, Msg' mId pq "can send 2") -> c == bobId && mId == 2 && pq == PQEncOff; _ -> False
    ackMessage alice' bobId 2 Nothing
    -- for alice msg id 3 is sent confirmation, then they're matched with bob at msg id 4

    -- allow connection
    allowConnection alice' bobId confId "alice's connInfo"
    get alice' ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetingsMsgId 4 alice' bobId bob aliceId
    liftIO $ disposeAgentClient alice'

testAsyncJoiningOfflineBeforeActivation :: HasCallStack => IO ()
testAsyncJoiningOfflineBeforeActivation =
  withAgent 1 agentCfg initAgentServers testDB $ \alice -> runRight_ $ do
    bob <- liftIO $ getSMPAgentClient' 2 agentCfg initAgentServers testDB2
    (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    (aliceId, sqSecured) <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True
    liftIO $ disposeAgentClient bob
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    bob' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    subscribeConnection bob' aliceId
    get alice ##> ("", bobId, CON)
    get bob' ##> ("", aliceId, INFO "alice's connInfo")
    get bob' ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob' aliceId
    liftIO $ disposeAgentClient bob'

testAsyncBothOffline :: HasCallStack => IO ()
testAsyncBothOffline = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    liftIO $ disposeAgentClient alice
    (aliceId, sqSecured) <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True
    liftIO $ disposeAgentClient bob
    alice' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB
    subscribeConnection alice' bobId
    ("", _, CONF confId _ "bob's connInfo") <- get alice'
    allowConnection alice' bobId confId "alice's connInfo"
    bob' <- liftIO $ getSMPAgentClient' 4 agentCfg initAgentServers testDB2
    subscribeConnection bob' aliceId
    get alice' ##> ("", bobId, CON)
    get bob' ##> ("", aliceId, INFO "alice's connInfo")
    get bob' ##> ("", aliceId, CON)
    exchangeGreetings alice' bobId bob' aliceId
    liftIO $ disposeAgentClient alice'
    liftIO $ disposeAgentClient bob'

testAsyncServerOffline :: HasCallStack => ATransport -> IO ()
testAsyncServerOffline t = withAgentClients2 $ \alice bob -> do
  -- create connection and shutdown the server
  (bobId, cReq) <- withSmpServerStoreLogOn t testPort $ \_ ->
    runRight $ createConnection alice 1 True SCMInvitation Nothing SMSubscribe
  -- connection fails
  Left (BROKER _ NETWORK) <- runExceptT $ joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
  ("", "", DOWN srv conns) <- nGet alice
  srv `shouldBe` testSMPServer
  conns `shouldBe` [bobId]
  -- connection succeeds after server start
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    ("", "", UP srv1 conns1) <- nGet alice
    liftIO $ do
      srv1 `shouldBe` testSMPServer
      conns1 `shouldBe` [bobId]
    (aliceId, sqSecured) <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob aliceId

testAllowConnectionClientRestart :: HasCallStack => ATransport -> IO ()
testAllowConnectionClientRestart t = do
  let initAgentServersSrv2 = initAgentServers {smp = userServers [testSMPServer2]}
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServersSrv2 testDB2
  withSmpServerStoreLogOn t testPort $ \_ -> do
    (aliceId, bobId, confId) <-
      withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \_ -> do
        runRight $ do
          (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
          (aliceId, sqSecured) <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
          liftIO $ sqSecured `shouldBe` True
          ("", _, CONF confId _ "bob's connInfo") <- get alice
          pure (aliceId, bobId, confId)

    ("", "", DOWN _ _) <- nGet bob

    runRight_ $ do
      allowConnectionAsync alice "1" bobId confId "alice's connInfo"
      get alice ##> ("1", bobId, OK)
      pure ()

    threadDelay 100000 -- give time to enqueue confirmation (enqueueConfirmation)
    disposeAgentClient alice
    threadDelay 250000

    alice2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB
    runRight_ $ subscribeConnection alice2 bobId
    threadDelay 500000
    withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \_ -> do
      runRight $ do
        ("", "", UP _ _) <- nGet bob
        get alice2 ##> ("", bobId, CON)
        get bob ##> ("", aliceId, INFO "alice's connInfo")
        get bob ##> ("", aliceId, CON)
        exchangeGreetings alice2 bobId bob aliceId
    disposeAgentClient alice2
    disposeAgentClient bob

testIncreaseConnAgentVersion :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersion t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection_ PQSupportOff False alice bob
      exchangeGreetingsMsgId_ PQEncOff 2 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version doesn't increase if incompatible

    disposeAgentClient alice
    threadDelay 250000
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice2 bobId
      exchangeGreetingsMsgId_ PQEncOff 4 alice2 bobId bob aliceId
      checkVersion alice2 bobId 2
      checkVersion bob aliceId 2

    -- version increases if compatible

    disposeAgentClient bob
    threadDelay 250000
    bob2 <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection bob2 aliceId
      exchangeGreetingsMsgId_ PQEncOff 6 alice2 bobId bob2 aliceId
      checkVersion alice2 bobId 3
      checkVersion bob2 aliceId 3

    -- version doesn't decrease, even if incompatible

    disposeAgentClient alice2
    threadDelay 250000
    alice3 <- getSMPAgentClient' 5 agentCfg {smpAgentVRange = mkVersionRange 2 2} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice3 bobId
      exchangeGreetingsMsgId_ PQEncOff 8 alice3 bobId bob2 aliceId
      checkVersion alice3 bobId 3
      checkVersion bob2 aliceId 3

    disposeAgentClient bob2
    threadDelay 250000
    bob3 <- getSMPAgentClient' 6 agentCfg {smpAgentVRange = mkVersionRange 1 1} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection bob3 aliceId
      exchangeGreetingsMsgId_ PQEncOff 10 alice3 bobId bob3 aliceId
      checkVersion alice3 bobId 3
      checkVersion bob3 aliceId 3
    disposeAgentClient alice3
    disposeAgentClient bob3

checkVersion :: AgentClient -> ConnId -> Word16 -> ExceptT AgentErrorType IO ()
checkVersion c connId v = do
  ConnectionStats {connAgentVersion} <- getConnectionServers c connId
  liftIO $ connAgentVersion `shouldBe` VersionSMPA v

testIncreaseConnAgentVersionMaxCompatible :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersionMaxCompatible t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection_ PQSupportOff False alice bob
      exchangeGreetingsMsgId_ PQEncOff 2 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version increases to max compatible

    disposeAgentClient alice
    threadDelay 250000
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB
    disposeAgentClient bob
    threadDelay 250000
    bob2 <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = supportedSMPAgentVRange} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection alice2 bobId
      subscribeConnection bob2 aliceId
      exchangeGreetingsMsgId_ PQEncOff 4 alice2 bobId bob2 aliceId
      checkVersion alice2 bobId 3
      checkVersion bob2 aliceId 3
    disposeAgentClient alice2
    disposeAgentClient bob2

testIncreaseConnAgentVersionStartDifferentVersion :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersionStartDifferentVersion t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection_ PQSupportOff False alice bob
      exchangeGreetingsMsgId_ PQEncOff 2 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version increases to max compatible

    disposeAgentClient alice
    threadDelay 250000
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice2 bobId
      exchangeGreetingsMsgId_ PQEncOff 4 alice2 bobId bob aliceId
      checkVersion alice2 bobId 3
      checkVersion bob aliceId 3
    disposeAgentClient alice2
    disposeAgentClient bob

testDeliverClientRestart :: HasCallStack => ATransport -> IO ()
testDeliverClientRestart t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2

  (aliceId, bobId) <- withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight $ do
      (aliceId, bobId) <- makeConnection alice bob
      exchangeGreetings alice bobId bob aliceId
      pure (aliceId, bobId)

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob

  4 <- runRight $ sendMessage bob aliceId SMP.noMsgFlags "hello"

  disposeAgentClient bob

  bob2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      ("", "", UP _ _) <- nGet alice

      subscribeConnection bob2 aliceId

      get bob2 ##> ("", aliceId, SENT 4)
      get alice =##> \case ("", c, Msg "hello") -> c == bobId; _ -> False
  disposeAgentClient alice
  disposeAgentClient bob2

testDuplicateMessage :: HasCallStack => ATransport -> IO ()
testDuplicateMessage t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob1) <- withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ makeConnection alice bob
    runRight_ $ do
      2 <- sendMessage alice bobId SMP.noMsgFlags "hello"
      get alice ##> ("", bobId, SENT 2)
      get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    disposeAgentClient bob
    threadDelay 250000

    -- if the agent user did not send ACK, the message will be delivered again
    bob1 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    runRight_ $ do
      subscribeConnection bob1 aliceId
      get bob1 =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
      ackMessage bob1 aliceId 2 Nothing
      3 <- sendMessage alice bobId SMP.noMsgFlags "hello 2"
      get alice ##> ("", bobId, SENT 3)
      get bob1 =##> \case ("", c, Msg "hello 2") -> c == aliceId; _ -> False

    pure (aliceId, bobId, bob1)

  nGet alice =##> \case ("", "", DOWN _ [c]) -> c == bobId; _ -> False
  nGet bob1 =##> \case ("", "", DOWN _ [c]) -> c == aliceId; _ -> False
  -- commenting two lines below and uncommenting further two lines would also runRight_,
  -- it is the scenario tested above, when the message was not acknowledged by the user
  threadDelay 200000
  Left (BROKER _ NETWORK) <- runExceptT $ ackMessage bob1 aliceId 3 Nothing

  disposeAgentClient alice
  disposeAgentClient bob1
  threadDelay 250000

  alice2 <- getSMPAgentClient' 4 agentCfg initAgentServers testDB
  bob2 <- getSMPAgentClient' 5 agentCfg initAgentServers testDB2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      subscribeConnection bob2 aliceId
      subscribeConnection alice2 bobId
      -- get bob2 =##> \case ("", c, Msg "hello 2") -> c == aliceId; _ -> False
      -- ackMessage bob2 aliceId 5 Nothing
      -- message 2 is not delivered again, even though it was delivered to the agent
      4 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 3"
      get alice2 ##> ("", bobId, SENT 4)
      get bob2 =##> \case ("", c, Msg "hello 3") -> c == aliceId; _ -> False
  disposeAgentClient alice2
  disposeAgentClient bob2

testSkippedMessages :: HasCallStack => ATransport -> IO ()
testSkippedMessages t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId) <- withSmpServerConfigOn t cfg' testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ makeConnection alice bob
    runRight_ $ do
      2 <- sendMessage alice bobId SMP.noMsgFlags "hello"
      get alice ##> ("", bobId, SENT 2)
      get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
      ackMessage bob aliceId 2 Nothing

    disposeAgentClient bob

    runRight_ $ do
      3 <- sendMessage alice bobId SMP.noMsgFlags "hello 2"
      get alice ##> ("", bobId, SENT 3)
      4 <- sendMessage alice bobId SMP.noMsgFlags "hello 3"
      get alice ##> ("", bobId, SENT 4)
      5 <- sendMessage alice bobId SMP.noMsgFlags "hello 4"
      get alice ##> ("", bobId, SENT 5)

    pure (aliceId, bobId)

  nGet alice =##> \case ("", "", DOWN _ [c]) -> c == bobId; _ -> False
  threadDelay 200000

  disposeAgentClient alice

  alice2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB
  bob2 <- getSMPAgentClient' 4 agentCfg initAgentServers testDB2

  withSmpServerConfigOn t cfg' testPort $ \_ -> do
    runRight_ $ do
      subscribeConnection bob2 aliceId
      subscribeConnection alice2 bobId

      6 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 5"
      get alice2 ##> ("", bobId, SENT 6)
      get bob2 =##> \case ("", c, MSG MsgMeta {integrity = MsgError {errorInfo = MsgSkipped {fromMsgId = 3, toMsgId = 5}}} _ "hello 5") -> c == aliceId; _ -> False
      ackMessage bob2 aliceId 3 Nothing

      7 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 6"
      get alice2 ##> ("", bobId, SENT 7)
      get bob2 =##> \case ("", c, Msg "hello 6") -> c == aliceId; _ -> False
      ackMessage bob2 aliceId 4 Nothing
  disposeAgentClient alice2
  disposeAgentClient bob2
  where
    cfg' = cfg {msgStoreType = ASType SSTMemory, storeMsgsFile = Nothing}

testDeliveryAfterSubscriptionError :: HasCallStack => ATransport -> IO ()
testDeliveryAfterSubscriptionError t = do
  (aId, bId) <- withAgentClients2 $ \a b -> do
    (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ makeConnection a b
    nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
    nGet b =##> \case ("", "", DOWN _ [c]) -> c == aId; _ -> False
    2 <- runRight $ sendMessage a bId SMP.noMsgFlags "hello"
    liftIO $ noMessages b "not delivered"
    pure (aId, bId)

  withAgentClients2 $ \a b -> do
    Left (BROKER _ NETWORK) <- runExceptT $ subscribeConnection a bId
    Left (BROKER _ NETWORK) <- runExceptT $ subscribeConnection b aId
    withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
      withUP a bId $ \case ("", c, SENT 2) -> c == bId; _ -> False
      withUP b aId $ \case ("", c, Msg "hello") -> c == aId; _ -> False
      ackMessage b aId 2 Nothing

testMsgDeliveryQuotaExceeded :: HasCallStack => ATransport -> IO ()
testMsgDeliveryQuotaExceeded t =
  withAgentClients2 $ \a b -> withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    (aId', bId') <- makeConnection a b
    forM_ ([1 .. 4] :: [Int]) $ \i -> do
      mId <- sendMessage a bId SMP.noMsgFlags $ "message " <> bshow i
      get a =##> \case ("", c, SENT mId') -> bId == c && mId == mId'; _ -> False
    6 <- sendMessage a bId SMP.noMsgFlags "over quota"
    pGet' a False =##> \case ("", c, AEvt _ (MWARN 6 (SMP _ QUOTA))) -> bId == c; _ -> False
    2 <- sendMessage a bId' SMP.noMsgFlags "hello"
    get a =##> \case ("", c, SENT 2) -> bId' == c; _ -> False
    get b =##> \case ("", c, Msg "message 1") -> aId == c; _ -> False
    get b =##> \case ("", c, Msg "hello") -> aId' == c; _ -> False
    ackMessage b aId' 2 Nothing
    ackMessage b aId 2 Nothing
    get b =##> \case ("", c, Msg "message 2") -> aId == c; _ -> False
    ackMessage b aId 3 Nothing
    get b =##> \case ("", c, Msg "message 3") -> aId == c; _ -> False
    ackMessage b aId 4 Nothing
    get b =##> \case ("", c, Msg "message 4") -> aId == c; _ -> False
    ackMessage b aId 5 Nothing
    get a =##> \case ("", c, QCONT) -> bId == c; _ -> False
    get b =##> \case ("", c, Msg "over quota") -> aId == c; _ -> False
    ackMessage b aId 7 Nothing -- msg 8 was QCONT
    get a =##> \case ("", c, SENT 6) -> bId == c; _ -> False
    liftIO $ concurrently_ (noMessages a "no more events") (noMessages b "no more events")

testExpireMessage :: HasCallStack => ATransport -> IO ()
testExpireMessage t =
  withAgent 1 agentCfg {messageTimeout = 1.5, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB $ \a ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \b -> do
      (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ makeConnection a b
      nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
      nGet b =##> \case ("", "", DOWN _ [c]) -> c == aId; _ -> False
      2 <- runRight $ sendMessage a bId SMP.noMsgFlags "1"
      threadDelay 1500000
      3 <- runRight $ sendMessage a bId SMP.noMsgFlags "2" -- this won't expire
      get a =##> \case ("", c, MERR 2 (BROKER _ e)) -> bId == c && (e == TIMEOUT || e == NETWORK); _ -> False
      withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
        withUP a bId $ \case ("", _, SENT 3) -> True; _ -> False
        withUP b aId $ \case ("", _, MsgErr 2 (MsgSkipped 2 2) "2") -> True; _ -> False
        ackMessage b aId 2 Nothing

testExpireManyMessages :: HasCallStack => ATransport -> IO ()
testExpireManyMessages t =
  withAgent 1 agentCfg {messageTimeout = 2, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB $ \a ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \b -> do
      (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ makeConnection a b
      runRight_ $ do
        nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
        nGet b =##> \case ("", "", DOWN _ [c]) -> c == aId; _ -> False
        2 <- sendMessage a bId SMP.noMsgFlags "1"
        3 <- sendMessage a bId SMP.noMsgFlags "2"
        4 <- sendMessage a bId SMP.noMsgFlags "3"
        liftIO $ threadDelay 2000000
        5 <- sendMessage a bId SMP.noMsgFlags "4" -- this won't expire
        get a =##> \case ("", c, MERR 2 (BROKER _ e)) -> bId == c && (e == TIMEOUT || e == NETWORK); _ -> False
        let expected c e = bId == c && (e == TIMEOUT || e == NETWORK)
        get a >>= \case
          ("", c, MERR 3 (BROKER _ e)) -> do
            liftIO $ expected c e `shouldBe` True
            get a =##> \case ("", c', MERR 4 (BROKER _ e')) -> expected c' e'; ("", c', MERRS [4] (BROKER _ e')) -> expected c' e'; _ -> False
          ("", c, MERRS [3] (BROKER _ e)) -> do
            liftIO $ expected c e `shouldBe` True
            get a =##> \case ("", c', MERR 4 (BROKER _ e')) -> expected c' e'; _ -> False
          ("", c, MERRS [3, 4] (BROKER _ e)) ->
            liftIO $ expected c e `shouldBe` True
          r -> error $ show r
      withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
        withUP a bId $ \case ("", _, SENT 5) -> True; _ -> False
        withUP b aId $ \case ("", _, MsgErr 2 (MsgSkipped 2 4) "4") -> True; _ -> False
        ackMessage b aId 2 Nothing

withUP :: HasCallStack => AgentClient -> ConnId -> (AEntityTransmission 'AEConn -> Bool) -> ExceptT AgentErrorType IO ()
withUP a bId p =
  liftIO $
    getInAnyOrder
      a
      [ \case ("", "", AEvt SAENone (UP _ [c])) -> c == bId; _ -> False,
        \case (corrId, c, AEvt SAEConn cmd) -> c == bId && p (corrId, c, cmd); _ -> False
      ]

testExpireMessageQuota :: HasCallStack => ATransport -> IO ()
testExpireMessageQuota t = withSmpServerConfigOn t cfg {msgQueueQuota = 1, maxJournalMsgCount = 2} testPort $ \_ -> do
  a <- getSMPAgentClient' 1 agentCfg {quotaExceededTimeout = 1, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- runRight $ do
    (aId, bId) <- makeConnection a b
    liftIO $ threadDelay 500000 >> disposeAgentClient b
    2 <- sendMessage a bId SMP.noMsgFlags "1"
    get a ##> ("", bId, SENT 2)
    3 <- sendMessage a bId SMP.noMsgFlags "2"
    liftIO $ threadDelay 1000000
    4 <- sendMessage a bId SMP.noMsgFlags "3" -- this won't expire
    get a =##> \case ("", c, MERR 3 (SMP _ QUOTA)) -> bId == c; _ -> False
    pure (aId, bId)
  withAgent 3 agentCfg initAgentServers testDB2 $ \b' -> runRight_ $ do
    subscribeConnection b' aId
    get b' =##> \case ("", c, Msg "1") -> c == aId; _ -> False
    ackMessage b' aId 2 Nothing
    liftIO . getInAnyOrder a $
      [ \case ("", c, AEvt SAEConn (SENT 4)) -> c == bId; _ -> False,
        \case ("", c, AEvt SAEConn QCONT) -> c == bId; _ -> False
      ]
    get b' =##> \case ("", c, MsgErr 4 (MsgSkipped 3 3) "3") -> c == aId; _ -> False
    ackMessage b' aId 4 Nothing
  disposeAgentClient a

testExpireManyMessagesQuota :: ATransport -> IO ()
testExpireManyMessagesQuota t = withSmpServerConfigOn t cfg {msgQueueQuota = 1, maxJournalMsgCount = 2} testPort $ \_ -> do
  a <- getSMPAgentClient' 1 agentCfg {quotaExceededTimeout = 2, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- runRight $ do
    (aId, bId) <- makeConnection a b
    liftIO $ threadDelay 500000 >> disposeAgentClient b
    2 <- sendMessage a bId SMP.noMsgFlags "1"
    get a ##> ("", bId, SENT 2)
    3 <- sendMessage a bId SMP.noMsgFlags "2"
    4 <- sendMessage a bId SMP.noMsgFlags "3"
    5 <- sendMessage a bId SMP.noMsgFlags "4"
    liftIO $ threadDelay 2000000
    6 <- sendMessage a bId SMP.noMsgFlags "5" -- this won't expire
    get a =##> \case ("", c, MERR 3 (SMP _ QUOTA)) -> bId == c; _ -> False
    get a >>= \case
      ("", c, MERR 4 (SMP _ QUOTA)) -> do
        liftIO $ bId `shouldBe` c
        get a =##> \case ("", c', MERR 5 (SMP _ QUOTA)) -> bId == c'; ("", c', MERRS [5] (SMP _ QUOTA)) -> bId == c'; _ -> False
      ("", c, MERRS [4] (SMP _ QUOTA)) -> do
        liftIO $ bId `shouldBe` c
        get a =##> \case ("", c', MERR 5 (SMP _ QUOTA)) -> bId == c'; _ -> False
      ("", c, MERRS [4, 5] (SMP _ QUOTA)) -> liftIO $ bId `shouldBe` c
      r -> error $ show r
    pure (aId, bId)
  withAgent 3 agentCfg initAgentServers testDB2 $ \b' -> runRight_ $ do
    subscribeConnection b' aId
    get b' =##> \case ("", c, Msg "1") -> c == aId; _ -> False
    ackMessage b' aId 2 Nothing
    liftIO . getInAnyOrder a $
      [ \case ("", c, AEvt SAEConn (SENT 6)) -> c == bId; _ -> False,
        \case ("", c, AEvt SAEConn QCONT) -> c == bId; _ -> False
      ]
    get b' =##> \case ("", c, MsgErr 4 (MsgSkipped 3 5) "5") -> c == aId; _ -> False
    ackMessage b' aId 4 Nothing
  disposeAgentClient a

testRatchetSync :: HasCallStack => ATransport -> IO ()
testRatchetSync t = withAgentClients2 $ \alice bob ->
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId, bob2) <- setupDesynchronizedRatchet alice bob
    runRight $ do
      ConnectionStats {ratchetSyncState} <- synchronizeRatchet bob2 aliceId PQSupportOn False
      liftIO $ ratchetSyncState `shouldBe` RSStarted
      get alice =##> ratchetSyncP bobId RSAgreed
      get bob2 =##> ratchetSyncP aliceId RSAgreed
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 10 bob2 aliceId 7
    disposeAgentClient bob2

setupDesynchronizedRatchet :: HasCallStack => AgentClient -> AgentClient -> IO (ConnId, ConnId, AgentClient)
setupDesynchronizedRatchet alice bob = do
  (aliceId, bobId) <- runRight $ makeConnection alice bob
  runRight_ $ do
    2 <- sendMessage alice bobId SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, SENT 2)
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    ackMessage bob aliceId 2 Nothing

    3 <- sendMessage bob aliceId SMP.noMsgFlags "hello 2"
    get bob ##> ("", aliceId, SENT 3)
    get alice =##> \case ("", c, Msg "hello 2") -> c == bobId; _ -> False
    ackMessage alice bobId 3 Nothing

    liftIO $ copyFile testDB2 (testDB2 <> ".bak")

    4 <- sendMessage alice bobId SMP.noMsgFlags "hello 3"
    get alice ##> ("", bobId, SENT 4)
    get bob =##> \case ("", c, Msg "hello 3") -> c == aliceId; _ -> False
    ackMessage bob aliceId 4 Nothing

    5 <- sendMessage bob aliceId SMP.noMsgFlags "hello 4"
    get bob ##> ("", aliceId, SENT 5)
    get alice =##> \case ("", c, Msg "hello 4") -> c == bobId; _ -> False
    ackMessage alice bobId 5 Nothing

  disposeAgentClient bob
  threadDelay 250000

  -- importing database backup after progressing ratchet de-synchronizes ratchet
  liftIO $ renameFile (testDB2 <> ".bak") testDB2

  bob2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2

  runRight_ $ do
    subscribeConnection bob2 aliceId

    Left A.CMD {cmdErr = PROHIBITED} <- liftIO . runExceptT $ synchronizeRatchet bob2 aliceId PQSupportOn False

    6 <- sendMessage alice bobId SMP.noMsgFlags "hello 5"
    get alice ##> ("", bobId, SENT 6)
    get bob2 =##> ratchetSyncP aliceId RSRequired

    Left A.CMD {cmdErr = PROHIBITED} <- liftIO . runExceptT $ sendMessage bob2 aliceId SMP.noMsgFlags "hello 6"
    pure ()

  pure (aliceId, bobId, bob2)

ratchetSyncP :: ConnId -> RatchetSyncState -> AEntityTransmission 'AEConn -> Bool
ratchetSyncP cId rss = \case
  (_, cId', RSYNC rss' _ ConnectionStats {ratchetSyncState}) ->
    cId' == cId && rss' == rss && ratchetSyncState == rss
  _ -> False

ratchetSyncP' :: ConnId -> RatchetSyncState -> ATransmission -> Bool
ratchetSyncP' cId rss = \case
  (_, cId', AEvt SAEConn (RSYNC rss' _ ConnectionStats {ratchetSyncState})) ->
    cId' == cId && rss' == rss && ratchetSyncState == rss
  _ -> False

testRatchetSyncServerOffline :: HasCallStack => ATransport -> IO ()
testRatchetSyncServerOffline t = withAgentClients2 $ \alice bob -> do
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId PQSupportOn False
  liftIO $ ratchetSyncState `shouldBe` RSStarted

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    concurrently_
      (getInAnyOrder alice [ratchetSyncP' bobId RSAgreed, serverUpP])
      (getInAnyOrder bob2 [ratchetSyncP' aliceId RSAgreed, serverUpP])
    runRight_ $ do
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 10 bob2 aliceId 7
  disposeAgentClient bob2

serverUpP :: ATransmission -> Bool
serverUpP = \case
  ("", "", AEvt SAENone (UP _ _)) -> True
  _ -> False

testRatchetSyncClientRestart :: HasCallStack => ATransport -> IO ()
testRatchetSyncClientRestart t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob
  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2
  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId PQSupportOn False
  ratchetSyncState `shouldBe` RSStarted
  disposeAgentClient bob2
  bob3 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      ("", "", UP _ _) <- nGet alice
      subscribeConnection bob3 aliceId
      get alice =##> ratchetSyncP bobId RSAgreed
      get bob3 =##> ratchetSyncP aliceId RSAgreed
      get alice =##> ratchetSyncP bobId RSOk
      get bob3 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 10 bob3 aliceId 7
  disposeAgentClient alice
  disposeAgentClient bob
  disposeAgentClient bob3

testRatchetSyncSuspendForeground :: HasCallStack => ATransport -> IO ()
testRatchetSyncSuspendForeground t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId PQSupportOn False
  liftIO $ ratchetSyncState `shouldBe` RSStarted

  suspendAgent bob2 0
  threadDelay 100000
  foregroundAgent bob2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    concurrently_
      (getInAnyOrder alice [ratchetSyncP' bobId RSAgreed, serverUpP])
      (getInAnyOrder bob2 [ratchetSyncP' aliceId RSAgreed, serverUpP])
    runRight_ $ do
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 10 bob2 aliceId 7
  disposeAgentClient alice
  disposeAgentClient bob
  disposeAgentClient bob2

testRatchetSyncSimultaneous :: HasCallStack => ATransport -> IO ()
testRatchetSyncSimultaneous t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState = bRSS} <- runRight $ synchronizeRatchet bob2 aliceId PQSupportOn False
  liftIO $ bRSS `shouldBe` RSStarted

  ConnectionStats {ratchetSyncState = aRSS} <- runRight $ synchronizeRatchet alice bobId PQSupportOn True
  liftIO $ aRSS `shouldBe` RSStarted

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    concurrently_
      (getInAnyOrder alice [ratchetSyncP' bobId RSAgreed, serverUpP])
      (getInAnyOrder bob2 [ratchetSyncP' aliceId RSAgreed, serverUpP])
    runRight_ $ do
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 10 bob2 aliceId 7
  disposeAgentClient alice
  disposeAgentClient bob
  disposeAgentClient bob2

testOnlyCreatePullSlowHandshake :: IO ()
testOnlyCreatePullSlowHandshake = withAgentClientsCfg2 agentProxyCfgV8 agentProxyCfgV8 $ \alice bob -> runRight_ $ do
  (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMOnlyCreate
  (aliceId, sqSecured) <- joinConnection bob 1 True qInfo "bob's connInfo" SMOnlyCreate
  liftIO $ sqSecured `shouldBe` False
  Just ("", _, CONF confId _ "bob's connInfo") <- getMsg alice bobId $ timeout 5_000000 $ get alice
  getMSGNTF alice bobId
  allowConnection alice bobId confId "alice's connInfo"
  liftIO $ threadDelay 1_000000
  getMsg bob aliceId $
    get bob ##> ("", aliceId, INFO "alice's connInfo")
  getMSGNTF bob aliceId
  liftIO $ threadDelay 1_000000
  getMsg alice bobId $ pure ()
  inAnyOrder
    (get alice)
    [ \case ("", c, CON) -> c == bobId; _ -> False,
      \case ("", c, MSGNTF {}) -> c == bobId; _ -> False
    ]
  getMsg bob aliceId $
    get bob ##> ("", aliceId, CON)
  getMSGNTF bob aliceId
  -- exchange messages
  4 <- sendMessage alice bobId SMP.noMsgFlags "hello"
  get alice ##> ("", bobId, SENT 4)
  getMsg bob aliceId $
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  ackMessage bob aliceId 4 Nothing
  getMSGNTF bob aliceId
  5 <- sendMessage bob aliceId SMP.noMsgFlags "hello too"
  get bob ##> ("", aliceId, SENT 5)
  getMsg alice bobId $
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
  ackMessage alice bobId 5 Nothing
  getMSGNTF alice bobId

getMsg :: AgentClient -> ConnId -> ExceptT AgentErrorType IO a -> ExceptT AgentErrorType IO a
getMsg c cId action = do
  liftIO $ noMessages c "nothing should be delivered before GET"
  [Just _] <- lift $ getConnectionMessages c [cId]
  action

getMSGNTF :: AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
getMSGNTF c cId =
  get c =##> \case ("", c', MSGNTF {}) -> c' == cId; _ -> False

testOnlyCreatePull :: IO ()
testOnlyCreatePull = withAgentClients2 $ \alice bob -> runRight_ $ do
  (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMOnlyCreate
  (aliceId, sqSecured) <- joinConnection bob 1 True qInfo "bob's connInfo" SMOnlyCreate
  liftIO $ sqSecured `shouldBe` True
  Just ("", _, CONF confId _ "bob's connInfo") <- getMsg alice bobId $ timeout 5_000000 $ get alice
  getMSGNTF alice bobId
  allowConnection alice bobId confId "alice's connInfo"
  liftIO $ threadDelay 1_000000
  getMsg bob aliceId $ do
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
  getMSGNTF bob aliceId
  liftIO $ threadDelay 1_000000
  get alice ##> ("", bobId, CON) -- sent to initiating party after sending confirmation
  -- exchange messages
  2 <- sendMessage alice bobId SMP.noMsgFlags "hello"
  get alice ##> ("", bobId, SENT 2)
  getMsg bob aliceId $
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  ackMessage bob aliceId 2 Nothing
  getMSGNTF bob aliceId
  3 <- sendMessage bob aliceId SMP.noMsgFlags "hello too"
  get bob ##> ("", aliceId, SENT 3)
  getMsg alice bobId $
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
  ackMessage alice bobId 3 Nothing
  getMSGNTF alice bobId

makeConnection :: AgentClient -> AgentClient -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnection = makeConnection_ PQSupportOn True

makeConnection_ :: PQSupport -> SndQueueSecured -> AgentClient -> AgentClient -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnection_ pqEnc sqSecured alice bob = makeConnectionForUsers_ pqEnc sqSecured alice 1 bob 1

makeConnectionForUsers :: HasCallStack => AgentClient -> UserId -> AgentClient -> UserId -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnectionForUsers = makeConnectionForUsers_ PQSupportOn True

makeConnectionForUsers_ :: HasCallStack => PQSupport -> SndQueueSecured -> AgentClient -> UserId -> AgentClient -> UserId -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnectionForUsers_ pqSupport sqSecured alice aliceUserId bob bobUserId = do
  (bobId, qInfo) <- A.createConnection alice aliceUserId True SCMInvitation Nothing (CR.IKNoPQ pqSupport) SMSubscribe
  aliceId <- A.prepareConnectionToJoin bob bobUserId True qInfo pqSupport
  sqSecured' <- A.joinConnection bob bobUserId aliceId True qInfo "bob's connInfo" pqSupport SMSubscribe
  liftIO $ sqSecured' `shouldBe` sqSecured
  ("", _, A.CONF confId pqSup' _ "bob's connInfo") <- get alice
  liftIO $ pqSup' `shouldBe` pqSupport
  allowConnection alice bobId confId "alice's connInfo"
  let pqEnc = CR.pqSupportToEnc pqSupport
  get alice ##> ("", bobId, A.CON pqEnc)
  get bob ##> ("", aliceId, A.INFO pqSupport "alice's connInfo")
  get bob ##> ("", aliceId, A.CON pqEnc)
  pure (aliceId, bobId)

testInactiveNoSubs :: ATransport -> IO ()
testInactiveNoSubs t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ ->
    withAgent 1 agentCfg initAgentServers testDB $ \alice -> do
      runRight_ . void $ createConnection alice 1 True SCMInvitation Nothing SMOnlyCreate -- do not subscribe to pass noSubscriptions check
      Just (_, _, AEvt SAENone (CONNECT _ _)) <- timeout 2000000 $ atomically (readTBQueue $ subQ alice)
      Just (_, _, AEvt SAENone (DISCONNECT _ _)) <- timeout 5000000 $ atomically (readTBQueue $ subQ alice)
      pure ()

testInactiveWithSubs :: ATransport -> IO ()
testInactiveWithSubs t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ ->
    withAgent 1 agentCfg initAgentServers testDB $ \alice -> do
      runRight_ . void $ createConnection alice 1 True SCMInvitation Nothing SMSubscribe
      Nothing <- 800000 `timeout` get alice
      liftIO $ threadDelay 1200000
      -- and after 2 sec of inactivity no DOWN is sent as we have a live subscription
      liftIO $ timeout 1200000 (get alice) `shouldReturn` Nothing

testActiveClientNotDisconnected :: ATransport -> IO ()
testActiveClientNotDisconnected t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ ->
    withAgent 1 agentCfg initAgentServers testDB $ \alice -> do
      ts <- getSystemTime
      runRight_ $ do
        (connId, _cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
        keepSubscribing alice connId ts
  where
    keepSubscribing :: AgentClient -> ConnId -> SystemTime -> ExceptT AgentErrorType IO ()
    keepSubscribing alice connId ts = do
      ts' <- liftIO getSystemTime
      if milliseconds ts' - milliseconds ts < 2200
        then do
          -- keep sending SUB for 2.2 seconds
          liftIO $ threadDelay 200000
          subscribeConnection alice connId
          keepSubscribing alice connId ts
        else do
          -- check that nothing is sent from agent
          Nothing <- 800000 `timeout` get alice
          liftIO $ threadDelay 1200000
          -- and after 2 sec of inactivity no DOWN is sent as we have a live subscription
          liftIO $ timeout 1200000 (get alice) `shouldReturn` Nothing
    milliseconds ts = systemSeconds ts * 1000 + fromIntegral (systemNanoseconds ts `div` 1000000)

testSuspendingAgent :: IO ()
testSuspendingAgent =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    2 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 2)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 2 Nothing
    liftIO $ suspendAgent b 1000000
    get' b ##> ("", "", SUSPENDED)
    3 <- sendMessage a bId SMP.noMsgFlags "hello 2"
    get a ##> ("", bId, SENT 3)
    Nothing <- 100000 `timeout` get b
    liftIO $ foregroundAgent b
    get b =##> \case ("", c, Msg "hello 2") -> c == aId; _ -> False

testSuspendingAgentCompleteSending :: ATransport -> IO ()
testSuspendingAgentCompleteSending t = withAgentClients2 $ \a b -> do
  (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    (aId, bId) <- makeConnection a b
    2 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 2)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 2 Nothing
    pure (aId, bId)
  runRight_ $ do
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet b
    3 <- sendMessage b aId SMP.noMsgFlags "hello too"
    4 <- sendMessage b aId SMP.noMsgFlags "how are you?"
    liftIO $ threadDelay 100000
    liftIO $ suspendAgent b 5000000
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ @AgentErrorType $ do
    -- there will be no UP event for b, because re-subscriptions are suspended until the agent is in foreground
    get b =##> \case ("", c, SENT 3) -> c == aId; _ -> False
    get b =##> \case ("", c, SENT 4) -> c == aId; _ -> False
    nGet b ##> ("", "", SUSPENDED)
    liftIO $
      getInAnyOrder
        a
        [ \case ("", c, AEvt _ (Msg "hello too")) -> c == bId; _ -> False,
          \case ("", "", AEvt _ UP {}) -> True; _ -> False
        ]
    ackMessage a bId 3 Nothing
    get a =##> \case ("", c, Msg "how are you?") -> c == bId; _ -> False
    ackMessage a bId 4 Nothing

testSuspendingAgentTimeout :: ATransport -> IO ()
testSuspendingAgentTimeout t = withAgentClients2 $ \a b -> do
  (aId, _) <- withSmpServer t . runRight $ do
    (aId, bId) <- makeConnection a b
    2 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 2)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 2 Nothing
    pure (aId, bId)

  runRight_ $ do
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet b
    3 <- sendMessage b aId SMP.noMsgFlags "hello too"
    4 <- sendMessage b aId SMP.noMsgFlags "how are you?"
    liftIO $ suspendAgent b 100000
    ("", "", SUSPENDED) <- nGet b
    pure ()

testBatchedSubscriptions :: Int -> Int -> ATransport -> IO ()
testBatchedSubscriptions nCreate nDel t =
  withAgentClientsCfgServers2 agentCfg agentCfg initAgentServers2 $ \a b -> do
    conns <- runServers $ do
      conns <- replicateM nCreate $ makeConnection_ PQSupportOff True a b
      forM_ conns $ \(aId, bId) -> exchangeGreetings_ PQEncOff a bId b aId
      let (aIds', bIds') = unzip $ take nDel conns
      delete a bIds'
      delete b aIds'
      liftIO $ threadDelay 1000000
      pure conns
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet b
    ("", "", DOWN {}) <- nGet b
    runServers $ do
      ("", "", UP {}) <- nGet a
      ("", "", UP {}) <- nGet a
      ("", "", UP {}) <- nGet b
      ("", "", UP {}) <- nGet b
      liftIO $ threadDelay 1000000
      let (aIds, bIds) = unzip conns
          conns' = drop nDel conns
          (aIds', bIds') = unzip conns'
      subscribe a bIds
      subscribe b aIds
      forM_ conns' $ \(aId, bId) -> exchangeGreetingsMsgId_ PQEncOff 4 a bId b aId
      void $ resubscribeConnections a bIds
      void $ resubscribeConnections b aIds
      forM_ conns' $ \(aId, bId) -> exchangeGreetingsMsgId_ PQEncOff 6 a bId b aId
      delete a bIds'
      delete b aIds'
      deleteFail a bIds'
      deleteFail b aIds'
  where
    subscribe :: AgentClient -> [ConnId] -> ExceptT AgentErrorType IO ()
    subscribe c cs = do
      r <- subscribeConnections c cs
      liftIO $ do
        let dc = S.fromList $ take nDel cs
        all isRight (M.withoutKeys r dc) `shouldBe` True
        all (== Left (CONN NOT_FOUND)) (M.restrictKeys r dc) `shouldBe` True
        M.keys r `shouldMatchList` cs
    delete :: AgentClient -> [ConnId] -> ExceptT AgentErrorType IO ()
    delete c cs = do
      r <- deleteConnections c cs
      liftIO $ do
        all isRight r `shouldBe` True
        M.keys r `shouldMatchList` cs
    deleteFail :: AgentClient -> [ConnId] -> ExceptT AgentErrorType IO ()
    deleteFail c cs = do
      r <- deleteConnections c cs
      liftIO $ do
        all (== Left (CONN NOT_FOUND)) r `shouldBe` True
        M.keys r `shouldMatchList` cs
    runServers :: ExceptT AgentErrorType IO a -> IO a
    runServers a = do
      withSmpServerStoreLogOn t testPort $ \t1 -> do
        res <- withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \t2 ->
          runRight a `finally` killThread t2
        killThread t1
        pure res

testBatchedPendingMessages :: Int -> Int -> IO ()
testBatchedPendingMessages nCreate nMsgs =
  withA $ \a -> do
    conns <- withB $ \b -> runRight $ do
      replicateM nCreate $ makeConnection a b
    let msgConns = take nMsgs conns
    runRight_ $ forM_ msgConns $ \(_, bId) -> sendMessage a bId SMP.noMsgFlags "hello"
    replicateM_ nMsgs $ get a =##> \case ("", cId, SENT _) -> isJust $ find ((cId ==) . snd) msgConns; _ -> False
    withB $ \b -> runRight_ $ do
      r <- subscribeConnections b $ map fst conns
      liftIO $ all isRight r `shouldBe` True
      replicateM_ nMsgs $ do
        ("", cId, Msg' msgId _ "hello") <- get b
        liftIO $ isJust (find ((cId ==) . fst) msgConns) `shouldBe` True
        ackMessage b cId msgId Nothing
  where
    withA = withAgent 1 agentCfg initAgentServers testDB
    withB = withAgent 2 agentCfg initAgentServers testDB2

testSendMessagesB :: IO ()
testSendMessagesB = withAgentClients2 $ \a b -> runRight_ $ do
  (aId, bId) <- makeConnection a b
  let msg cId body = Right (cId, PQEncOn, SMP.noMsgFlags, body)
  [SentB 2, SentB 3, SentB 4] <- sendMessagesB a ([msg bId "msg 1", msg "" "msg 2", msg "" "msg 3"] :: [Either AgentErrorType MsgReq])
  get a ##> ("", bId, SENT 2)
  get a ##> ("", bId, SENT 3)
  get a ##> ("", bId, SENT 4)
  receiveMsg b aId 2 "msg 1"
  receiveMsg b aId 3 "msg 2"
  receiveMsg b aId 4 "msg 3"

testSendMessagesB2 :: IO ()
testSendMessagesB2 = withAgentClients3 $ \a b c -> runRight_ $ do
  (abId, bId) <- makeConnection a b
  (acId, cId) <- makeConnection a c
  let msg connId body = Right (connId, PQEncOn, SMP.noMsgFlags, body)
  [SentB 2, SentB 3, SentB 4, SentB 2, SentB 3] <-
    sendMessagesB a ([msg bId "msg 1", msg "" "msg 2", msg "" "msg 3", msg cId "msg 4", msg "" "msg 5"] :: [Either AgentErrorType MsgReq])
  liftIO $
    getInAnyOrder
      a
      [ \case ("", cId', AEvt SAEConn (SENT 2)) -> cId' == bId; _ -> False,
        \case ("", cId', AEvt SAEConn (SENT 3)) -> cId' == bId; _ -> False,
        \case ("", cId', AEvt SAEConn (SENT 4)) -> cId' == bId; _ -> False,
        \case ("", cId', AEvt SAEConn (SENT 2)) -> cId' == cId; _ -> False,
        \case ("", cId', AEvt SAEConn (SENT 3)) -> cId' == cId; _ -> False
      ]
  receiveMsg b abId 2 "msg 1"
  receiveMsg b abId 3 "msg 2"
  receiveMsg b abId 4 "msg 3"
  receiveMsg c acId 2 "msg 4"
  receiveMsg c acId 3 "msg 5"

pattern SentB :: AgentMsgId -> Either AgentErrorType (AgentMsgId, PQEncryption)
pattern SentB msgId <- Right (msgId, PQEncOn)

receiveMsg :: AgentClient -> ConnId -> AgentMsgId -> MsgBody -> ExceptT AgentErrorType IO ()
receiveMsg c cId msgId msg = do
  get c =##> \case ("", cId', Msg' mId' PQEncOn msg') -> cId' == cId && mId' == msgId && msg' == msg; _ -> False
  ackMessage c cId msgId Nothing

testAsyncCommands :: SndQueueSecured -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
testAsyncCommands sqSecured alice bob baseId =
  runRight_ $ do
    bobId <- createConnectionAsync alice 1 "1" True SCMInvitation (IKNoPQ PQSupportOn) SMSubscribe
    ("1", bobId', INV (ACR _ qInfo)) <- get alice
    liftIO $ bobId' `shouldBe` bobId
    aliceId <- joinConnectionAsync bob 1 "2" True qInfo "bob's connInfo" PQSupportOn SMSubscribe
    ("2", aliceId', JOINED sqSecured') <- get bob
    liftIO $ do
      aliceId' `shouldBe` aliceId
      sqSecured' `shouldBe` sqSecured
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnectionAsync alice "3" bobId confId "alice's connInfo"
    get alice =##> \case ("3", _, OK) -> True; _ -> False
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    -- message IDs 1 to 3 get assigned to control messages, so first MSG is assigned ID 4
    1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, SENT $ baseId + 1)
    2 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
    get alice ##> ("", bobId, SENT $ baseId + 2)
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    ackMessageAsync bob "4" aliceId (baseId + 1) Nothing
    inAnyOrder
      (get bob)
      [ \case ("4", _, OK) -> True; _ -> False,
        \case ("", c, Msg "how are you?") -> c == aliceId; _ -> False
      ]
    ackMessageAsync bob "5" aliceId (baseId + 2) Nothing
    get bob =##> \case ("5", _, OK) -> True; _ -> False
    3 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
    get bob ##> ("", aliceId, SENT $ baseId + 3)
    4 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "message 1"
    get bob ##> ("", aliceId, SENT $ baseId + 4)
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
    ackMessageAsync alice "6" bobId (baseId + 3) Nothing
    inAnyOrder
      (get alice)
      [ \case ("6", _, OK) -> True; _ -> False,
        \case ("", c, Msg "message 1") -> c == bobId; _ -> False
      ]
    ackMessageAsync alice "7" bobId (baseId + 4) Nothing
    get alice =##> \case ("7", _, OK) -> True; _ -> False
    deleteConnectionAsync alice False bobId
    get alice =##> \case ("", "", DEL_RCVQS [(c, _, _, Nothing)]) -> c == bobId; _ -> False
    get alice =##> \case ("", "", DEL_CONNS [c]) -> c == bobId; _ -> False
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId

testAsyncCommandsRestore :: ATransport -> IO ()
testAsyncCommandsRestore t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bobId <- runRight $ createConnectionAsync alice 1 "1" True SCMInvitation (IKNoPQ PQSupportOn) SMSubscribe
  liftIO $ noMessages alice "alice doesn't receive INV because server is down"
  disposeAgentClient alice
  withAgent 2 agentCfg initAgentServers testDB $ \alice' ->
    withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
      subscribeConnection alice' bobId
      get alice' =##> \case ("1", _, INV _) -> True; _ -> False
      pure ()

testAcceptContactAsync :: SndQueueSecured -> AgentClient -> AgentClient -> AgentMsgId -> IO ()
testAcceptContactAsync sqSecured alice bob baseId =
  runRight_ $ do
    (_, qInfo) <- createConnection alice 1 True SCMContact Nothing SMSubscribe
    (aliceId, sqSecuredJoin) <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    liftIO $ sqSecuredJoin `shouldBe` False -- joining via contact address connection
    ("", _, REQ invId _ "bob's connInfo") <- get alice
    bobId <- acceptContactAsync alice "1" True invId "alice's connInfo" PQSupportOn SMSubscribe
    get alice =##> \case ("1", c, JOINED sqSecured') -> c == bobId && sqSecured' == sqSecured; _ -> False
    ("", _, CONF confId _ "alice's connInfo") <- get bob
    allowConnection bob aliceId confId "bob's connInfo"
    get alice ##> ("", bobId, INFO "bob's connInfo")
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, CON)
    -- message IDs 1 to 3 (or 1 to 4 in v1) get assigned to control messages, so first MSG is assigned ID 4
    1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, SENT $ baseId + 1)
    2 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
    get alice ##> ("", bobId, SENT $ baseId + 2)
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    ackMessage bob aliceId (baseId + 1) Nothing
    get bob =##> \case ("", c, Msg "how are you?") -> c == aliceId; _ -> False
    ackMessage bob aliceId (baseId + 2) Nothing
    3 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
    get bob ##> ("", aliceId, SENT $ baseId + 3)
    4 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "message 1"
    get bob ##> ("", aliceId, SENT $ baseId + 4)
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
    ackMessage alice bobId (baseId + 3) Nothing
    get alice =##> \case ("", c, Msg "message 1") -> c == bobId; _ -> False
    ackMessage alice bobId (baseId + 4) Nothing
    suspendConnection alice bobId
    5 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "message 2"
    get bob =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == aliceId && mId == (baseId + 5); _ -> False
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId

testDeleteConnectionAsync :: ATransport -> IO ()
testDeleteConnectionAsync t =
  withAgent 1 agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB $ \a -> do
    connIds <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
      (bId1, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
      (bId2, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
      (bId3, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
      pure ([bId1, bId2, bId3] :: [ConnId])
    runRight_ $ do
      deleteConnectionsAsync a False connIds
      nGet a =##> \case ("", "", DOWN {}) -> True; _ -> False
      let delOk = \case (c, _, _, Just (BROKER _ e)) -> c `elem` connIds && (e == TIMEOUT || e == NETWORK); _ -> False
      get a =##> \case ("", "", DEL_RCVQS rs) -> length rs == 3 && all delOk rs; _ -> False
      get a =##> \case ("", "", DEL_CONNS cs) -> length cs == 3 && all (`elem` connIds) cs; _ -> False
      liftIO $ noMessages a "nothing else should be delivered to alice"

testWaitDeliveryNoPending :: ATransport -> IO ()
testWaitDeliveryNoPending t = withAgentClients2 $ \alice bob ->
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    (aliceId, bobId) <- makeConnection alice bob

    1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, SENT $ baseId + 1)
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    ackMessage bob aliceId (baseId + 1) Nothing

    2 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
    get bob ##> ("", aliceId, SENT $ baseId + 2)
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
    ackMessage alice bobId (baseId + 2) Nothing

    deleteConnectionsAsync alice True [bobId]
    get alice =##> \case ("", "", DEL_RCVQS [(cId, _, _, Nothing)]) -> cId == bobId; _ -> False
    get alice =##> \case ("", "", DEL_CONNS [cId]) -> cId == bobId; _ -> False

    3 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "message 2"
    get bob =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == aliceId && mId == (baseId + 3); _ -> False

    liftIO $ noMessages alice "nothing else should be delivered to alice"
    liftIO $ noMessages bob "nothing else should be delivered to bob"
  where
    baseId = 1
    msgId = subtract baseId

testWaitDelivery :: ATransport -> IO ()
testWaitDelivery t =
  withAgent 1 agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB $ \alice ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \bob -> do
      (aliceId, bobId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        (aliceId, bobId) <- makeConnection alice bob

        1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
        get alice ##> ("", bobId, SENT $ baseId + 1)
        get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
        ackMessage bob aliceId (baseId + 1) Nothing

        2 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
        get bob ##> ("", aliceId, SENT $ baseId + 2)
        get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
        ackMessage alice bobId (baseId + 2) Nothing

        pure (aliceId, bobId)

      runRight_ $ do
        ("", "", DOWN _ _) <- nGet alice
        ("", "", DOWN _ _) <- nGet bob
        3 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
        4 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "message 1"
        deleteConnectionsAsync alice True [bobId]
        get alice =##> \case ("", "", DEL_RCVQS [(cId, _, _, Just (BROKER _ e))]) -> cId == bobId && (e == TIMEOUT || e == NETWORK); _ -> False
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"

      withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
        get alice ##> ("", bobId, SENT $ baseId + 3)
        get alice ##> ("", bobId, SENT $ baseId + 4)
        get alice =##> \case ("", "", DEL_CONNS [cId]) -> cId == bobId; _ -> False

        liftIO $
          getInAnyOrder
            bob
            [ \case ("", "", AEvt SAENone (UP _ [cId])) -> cId == aliceId; _ -> False,
              \case ("", cId, AEvt SAEConn (Msg "how are you?")) -> cId == aliceId; _ -> False
            ]
        ackMessage bob aliceId (baseId + 3) Nothing
        get bob =##> \case ("", c, Msg "message 1") -> c == aliceId; _ -> False
        ackMessage bob aliceId (baseId + 4) Nothing

        -- queue wasn't deleted (DEL never reached server, see DEL_RCVQ with error), so bob can send message
        5 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "message 2"
        get bob ##> ("", aliceId, SENT $ baseId + 5)

        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"
  where
    baseId = 1
    msgId = subtract baseId

testWaitDeliveryAUTHErr :: ATransport -> IO ()
testWaitDeliveryAUTHErr t =
  withAgent 1 agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB $ \alice ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \bob -> do
      (_aliceId, bobId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        (aliceId, bobId) <- makeConnection alice bob

        1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
        get alice ##> ("", bobId, SENT $ baseId + 1)
        get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
        ackMessage bob aliceId (baseId + 1) Nothing

        2 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
        get bob ##> ("", aliceId, SENT $ baseId + 2)
        get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
        ackMessage alice bobId (baseId + 2) Nothing

        deleteConnectionsAsync bob False [aliceId]
        get bob =##> \case ("", "", DEL_RCVQS [(cId, _, _, Nothing)]) -> cId == aliceId; _ -> False
        get bob =##> \case ("", "", DEL_CONNS [cId]) -> cId == aliceId; _ -> False

        pure (aliceId, bobId)

      runRight_ $ do
        ("", "", DOWN _ _) <- nGet alice
        3 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
        4 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "message 1"
        deleteConnectionsAsync alice True [bobId]
        get alice =##> \case ("", "", DEL_RCVQS [(cId, _, _, Just (BROKER _ e))]) -> cId == bobId && (e == TIMEOUT || e == NETWORK); _ -> False
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"

      withSmpServerStoreLogOn t testPort $ \_ -> do
        get alice =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == bobId && mId == (baseId + 3); _ -> False
        get alice =##> \case ("", cId, MERR mId (SMP _ AUTH)) -> cId == bobId && mId == (baseId + 4); _ -> False
        get alice =##> \case ("", "", DEL_CONNS [cId]) -> cId == bobId; _ -> False

        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"
  where
    baseId = 1
    msgId = subtract baseId

testWaitDeliveryTimeout :: ATransport -> IO ()
testWaitDeliveryTimeout t =
  withAgent 1 agentCfg {connDeleteDeliveryTimeout = 1, initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB $ \alice ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \bob -> do
      (aliceId, bobId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        (aliceId, bobId) <- makeConnection alice bob

        1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
        get alice ##> ("", bobId, SENT $ baseId + 1)
        get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
        ackMessage bob aliceId (baseId + 1) Nothing

        2 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
        get bob ##> ("", aliceId, SENT $ baseId + 2)
        get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
        ackMessage alice bobId (baseId + 2) Nothing

        pure (aliceId, bobId)

      runRight_ $ do
        ("", "", DOWN _ _) <- nGet alice
        ("", "", DOWN _ _) <- nGet bob
        3 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
        4 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "message 1"
        deleteConnectionsAsync alice True [bobId]
        get alice =##> \case ("", "", DEL_RCVQS [(cId, _, _, Just (BROKER _ e))]) -> cId == bobId && (e == TIMEOUT || e == NETWORK); _ -> False
        get alice =##> \case ("", "", DEL_CONNS [cId]) -> cId == bobId; _ -> False
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"

      liftIO $ threadDelay 100000

      withSmpServerStoreLogOn t testPort $ \_ -> do
        nGet bob =##> \case ("", "", UP _ [cId]) -> cId == aliceId; _ -> False
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"
  where
    baseId = 1
    msgId = subtract baseId

testWaitDeliveryTimeout2 :: ATransport -> IO ()
testWaitDeliveryTimeout2 t =
  withAgent 1 agentCfg {connDeleteDeliveryTimeout = 2, messageRetryInterval = fastMessageRetryInterval, initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB $ \alice ->
    withAgent 2 agentCfg initAgentServers testDB2 $ \bob -> do
      (aliceId, bobId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        (aliceId, bobId) <- makeConnection alice bob

        1 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "hello"
        get alice ##> ("", bobId, SENT $ baseId + 1)
        get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
        ackMessage bob aliceId (baseId + 1) Nothing

        2 <- msgId <$> sendMessage bob aliceId SMP.noMsgFlags "hello too"
        get bob ##> ("", aliceId, SENT $ baseId + 2)
        get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
        ackMessage alice bobId (baseId + 2) Nothing

        pure (aliceId, bobId)

      runRight_ $ do
        ("", "", DOWN _ _) <- nGet alice
        ("", "", DOWN _ _) <- nGet bob
        3 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "how are you?"
        4 <- msgId <$> sendMessage alice bobId SMP.noMsgFlags "message 1"
        deleteConnectionsAsync alice True [bobId]
        get alice =##> \case ("", "", DEL_RCVQS [(cId, _, _, Just (BROKER _ e))]) -> cId == bobId && (e == TIMEOUT || e == NETWORK); _ -> False
        get alice =##> \case ("", "", DEL_CONNS [cId]) -> cId == bobId; _ -> False
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"

      withSmpServerStoreLogOn t testPort $ \_ -> do
        get alice ##> ("", bobId, SENT $ baseId + 3)
        -- "message 1" not delivered

        liftIO $
          getInAnyOrder
            bob
            [ \case ("", "", AEvt SAENone (UP _ [cId])) -> cId == aliceId; _ -> False,
              \case ("", cId, AEvt SAEConn (Msg "how are you?")) -> cId == aliceId; _ -> False
            ]
        liftIO $ noMessages alice "nothing else should be delivered to alice"
        liftIO $ noMessages bob "nothing else should be delivered to bob"
  where
    baseId = 1
    msgId = subtract baseId

testJoinConnectionAsyncReplyErrorV8 :: HasCallStack => ATransport -> IO ()
testJoinConnectionAsyncReplyErrorV8 t = do
  let initAgentServersSrv2 = initAgentServers {smp = userServers [testSMPServer2]}
  withAgent 1 agentCfgVPrevPQ initAgentServers testDB $ \a ->
    withAgent 2 agentCfgVPrevPQ initAgentServersSrv2 testDB2 $ \b -> do
      (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        bId <- createConnectionAsync a 1 "1" True SCMInvitation (IKNoPQ PQSupportOn) SMSubscribe
        ("1", bId', INV (ACR _ qInfo)) <- get a
        liftIO $ bId' `shouldBe` bId
        aId <- joinConnectionAsync b 1 "2" True qInfo "bob's connInfo" PQSupportOn SMSubscribe
        liftIO $ threadDelay 500000
        ConnectionStats {rcvQueuesInfo = [], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
        pure (aId, bId)
      nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
      withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \_ -> do
        get b =##> \case ("2", c, JOINED sqSecured) -> c == aId && not sqSecured; _ -> False
        confId <- withSmpServerStoreLogOn t testPort $ \_ -> do
          pGet a >>= \case
            ("", "", AEvt _ (UP _ [_])) -> do
              ("", _, CONF confId _ "bob's connInfo") <- get a
              pure confId
            ("", _, AEvt _ (CONF confId _ "bob's connInfo")) -> do
              ("", "", UP _ [_]) <- nGet a
              pure confId
            r -> error $ "unexpected response " <> show r
        nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
        runRight_ $ do
          allowConnectionAsync a "3" bId confId "alice's connInfo"
          get a ##> ("3", bId, OK)
          liftIO $ threadDelay 500000
          ConnectionStats {rcvQueuesInfo = [RcvQueueInfo {}], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
          pure ()
        withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
          nGet a =##> \case ("", "", UP _ [c]) -> c == bId; _ -> False
          get a ##> ("", bId, CON)
          get b ##> ("", aId, INFO "alice's connInfo")
          get b ##> ("", aId, CON)
          exchangeGreetingsMsgId 4 a bId b aId

testJoinConnectionAsyncReplyError :: HasCallStack => ATransport -> IO ()
testJoinConnectionAsyncReplyError t = do
  let initAgentServersSrv2 = initAgentServers {smp = userServers [testSMPServer2]}
  withAgent 1 agentCfg initAgentServers testDB $ \a ->
    withAgent 2 agentCfg initAgentServersSrv2 testDB2 $ \b -> do
      (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
        bId <- createConnectionAsync a 1 "1" True SCMInvitation (IKNoPQ PQSupportOn) SMSubscribe
        ("1", bId', INV (ACR _ qInfo)) <- get a
        liftIO $ bId' `shouldBe` bId
        aId <- joinConnectionAsync b 1 "2" True qInfo "bob's connInfo" PQSupportOn SMSubscribe
        liftIO $ threadDelay 500000
        ConnectionStats {rcvQueuesInfo = [], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
        pure (aId, bId)
      nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
      withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2, storeMsgsFile = Just testStoreMsgsDir2} testPort2 $ \_ -> do
        confId <- withSmpServerStoreLogOn t testPort $ \_ -> do
          -- both servers need to be online for connection to progress because of SKEY
          get b =##> \case ("2", c, JOINED sqSecured) -> c == aId && sqSecured; _ -> False
          pGet a >>= \case
            ("", "", AEvt _ (UP _ [_])) -> do
              ("", _, CONF confId _ "bob's connInfo") <- get a
              pure confId
            ("", _, AEvt _ (CONF confId _ "bob's connInfo")) -> do
              ("", "", UP _ [_]) <- nGet a
              pure confId
            r -> error $ "unexpected response " <> show r
        nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
        runRight_ $ do
          allowConnectionAsync a "3" bId confId "alice's connInfo"
          get a ##> ("3", bId, OK)
          get a ##> ("", bId, CON)
          liftIO $ threadDelay 500000
          ConnectionStats {rcvQueuesInfo = [RcvQueueInfo {}], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
          pure ()
        withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
          nGet a =##> \case ("", "", UP _ [c]) -> c == bId; _ -> False
          get b ##> ("", aId, INFO "alice's connInfo")
          get b ##> ("", aId, CON)
          exchangeGreetings a bId b aId

testUsers :: IO ()
testUsers =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    auId <- createUser a [noAuthSrvCfg testSMPServer] [noAuthSrvCfg testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetings a bId' b aId'
    deleteUser a auId True
    get a =##> \case ("", "", DEL_RCVQS [(c, _, _, Nothing)]) -> c == bId'; _ -> False
    get a =##> \case ("", "", DEL_CONNS [c]) -> c == bId'; _ -> False
    nGet a =##> \case ("", "", DEL_USER u) -> u == auId; _ -> False
    exchangeGreetingsMsgId 4 a bId b aId
    liftIO $ noMessages a "nothing else should be delivered to alice"

testDeleteUserQuietly :: IO ()
testDeleteUserQuietly =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    auId <- createUser a [noAuthSrvCfg testSMPServer] [noAuthSrvCfg testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetings a bId' b aId'
    deleteUser a auId False
    exchangeGreetingsMsgId 4 a bId b aId
    liftIO $ noMessages a "nothing else should be delivered to alice"

testUsersNoServer :: HasCallStack => ATransport -> IO ()
testUsersNoServer t = withAgentClientsCfg2 aCfg agentCfg $ \a b -> do
  (aId, bId, auId, _aId', bId') <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    auId <- createUser a [noAuthSrvCfg testSMPServer] [noAuthSrvCfg testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetings a bId' b aId'
    pure (aId, bId, auId, aId', bId')
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId || c == bId'; _ -> False
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId || c == bId'; _ -> False
  nGet b =##> \case ("", "", DOWN _ cs) -> length cs == 2; _ -> False
  runRight_ $ do
    deleteUser a auId True
    get a =##> \case ("", "", DEL_RCVQS [(c, _, _, Just (BROKER _ e))]) -> c == bId' && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", "", DEL_CONNS [c]) -> c == bId'; _ -> False
    nGet a =##> \case ("", "", DEL_USER u) -> u == auId; _ -> False
    liftIO $ noMessages a "nothing else should be delivered to alice"
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    nGet a =##> \case ("", "", UP _ [c]) -> c == bId; _ -> False
    nGet b =##> \case ("", "", UP _ cs) -> length cs == 2; _ -> False
    exchangeGreetingsMsgId 4 a bId b aId
  where
    aCfg = agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3}

testSwitchConnection :: InitialAgentServers -> IO ()
testSwitchConnection servers =
  withAgentClientsCfgServers2 agentCfg agentCfg servers $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    testFullSwitch a bId b aId 8
    testFullSwitch a bId b aId 14

testFullSwitch :: AgentClient -> ByteString -> AgentClient -> ByteString -> Int64 -> ExceptT AgentErrorType IO ()
testFullSwitch a bId b aId msgId = do
  stats <- switchConnectionAsync a "" bId
  liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
  switchComplete a bId b aId
  exchangeGreetingsMsgId msgId a bId b aId

switchComplete :: AgentClient -> ByteString -> AgentClient -> ByteString -> ExceptT AgentErrorType IO ()
switchComplete a bId b aId = do
  phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
  phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
  phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]
  phaseRcv a bId SPConfirmed [Just RSSendingQADD, Nothing]
  phaseRcv a bId SPSecured [Just RSSendingQUSE, Nothing]
  phaseSnd b aId SPSecured [Just SSSendingQTEST, Nothing]
  phaseSnd b aId SPCompleted [Nothing]
  phaseRcv a bId SPCompleted [Nothing]

phaseRcv :: AgentClient -> ByteString -> SwitchPhase -> [Maybe RcvSwitchStatus] -> ExceptT AgentErrorType IO ()
phaseRcv c connId p swchStatuses = phase c connId QDRcv p (\stats -> rcvSwchStatuses' stats `shouldMatchList` swchStatuses)

rcvSwchStatuses' :: ConnectionStats -> [Maybe RcvSwitchStatus]
rcvSwchStatuses' ConnectionStats {rcvQueuesInfo} = map (\RcvQueueInfo {rcvSwitchStatus} -> rcvSwitchStatus) rcvQueuesInfo

phaseSnd :: AgentClient -> ByteString -> SwitchPhase -> [Maybe SndSwitchStatus] -> ExceptT AgentErrorType IO ()
phaseSnd c connId p swchStatuses = phase c connId QDSnd p (\stats -> sndSwchStatuses' stats `shouldMatchList` swchStatuses)

sndSwchStatuses' :: ConnectionStats -> [Maybe SndSwitchStatus]
sndSwchStatuses' ConnectionStats {sndQueuesInfo} = map (\SndQueueInfo {sndSwitchStatus} -> sndSwitchStatus) sndQueuesInfo

phase :: AgentClient -> ByteString -> QueueDirection -> SwitchPhase -> (ConnectionStats -> Expectation) -> ExceptT AgentErrorType IO ()
phase c connId d p statsExpectation =
  get c >>= \(_, connId', msg) -> do
    liftIO $ connId `shouldBe` connId'
    case msg of
      SWITCH d' p' stats -> liftIO $ do
        d `shouldBe` d'
        p `shouldBe` p'
        statsExpectation stats
      ERR (AGENT A_DUPLICATE) -> phase c connId d p statsExpectation
      r -> do
        liftIO . putStrLn $ "expected: " <> show p <> ", received: " <> show r
        SWITCH {} <- pure r
        pure ()

testSwitchAsync :: HasCallStack => InitialAgentServers -> IO ()
testSwitchAsync servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    pure (aId, bId)
  let withA' = sessionSubscribe withA [bId]
      withB' = sessionSubscribe withB [aId]
  withA' $ \a -> do
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
  withB' $ \b -> do
    phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
    phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]
  withA' $ \a -> do
    phaseRcv a bId SPConfirmed [Just RSSendingQADD, Nothing]
    phaseRcv a bId SPSecured [Just RSSendingQUSE, Nothing]
  withB' $ \b -> do
    phaseSnd b aId SPSecured [Just SSSendingQTEST, Nothing]
    phaseSnd b aId SPCompleted [Nothing]
  withA' $ \a -> phaseRcv a bId SPCompleted [Nothing]
  withA $ \a -> withB $ \b -> runRight_ $ do
    subscribeConnection a bId
    subscribeConnection b aId
    exchangeGreetingsMsgId 8 a bId b aId
    testFullSwitch a bId b aId 14
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

withAgent :: HasCallStack => Int -> AgentConfig -> InitialAgentServers -> String -> (HasCallStack => AgentClient -> IO a) -> IO a
withAgent clientId cfg' servers dbPath = bracket (getSMPAgentClient' clientId cfg' servers dbPath) (\a -> disposeAgentClient a >> threadDelay 100000)

sessionSubscribe :: (forall a. (AgentClient -> IO a) -> IO a) -> [ConnId] -> (AgentClient -> ExceptT AgentErrorType IO ()) -> IO ()
sessionSubscribe withC connIds a =
  withC $ \c -> runRight_ $ do
    void $ subscribeConnections c connIds
    r <- a c
    liftIO $ threadDelay 500000
    liftIO $ noMessages c "nothing else should be delivered"
    pure r

testSwitchDelete :: InitialAgentServers -> IO ()
testSwitchDelete servers =
  withAgentClientsCfgServers2 agentCfg agentCfg servers $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    liftIO $ disposeAgentClient b
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
    deleteConnectionAsync a False bId
    get a =##> \case ("", "", DEL_RCVQS [(c, _, _, Nothing), (c', _, _, Nothing)]) -> c == bId && c' == bId; _ -> False
    get a =##> \case ("", "", DEL_CONNS [c]) -> c == bId; _ -> False
    liftIO $ noMessages a "nothing else should be delivered to alice"

testAbortSwitchStarted :: HasCallStack => InitialAgentServers -> IO ()
testAbortSwitchStarted servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    pure (aId, bId)
  let withA' = sessionSubscribe withA [bId]
      withB' = sessionSubscribe withB [aId]
  withA' $ \a -> do
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
    -- repeat switch is prohibited
    Left A.CMD {cmdErr = PROHIBITED} <- liftIO . runExceptT $ switchConnectionAsync a "" bId
    -- abort current switch
    stats' <- abortConnectionSwitch a bId
    liftIO $ rcvSwchStatuses' stats' `shouldMatchList` [Nothing]
  withB' $ \b -> do
    phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
    phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]
  withA' $ \a -> do
    get a ##> ("", bId, ERR (AGENT {agentErr = A_QUEUE {queueErr = "QKEY: queue address not found in connection"}}))
    -- repeat switch
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
  withA $ \a -> withB $ \b -> runRight_ $ do
    subscribeConnection a bId
    subscribeConnection b aId

    phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
    phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]

    phaseRcv a bId SPConfirmed [Just RSSendingQADD, Nothing]
    phaseRcv a bId SPSecured [Just RSSendingQUSE, Nothing]

    phaseSnd b aId SPSecured [Just SSSendingQTEST, Nothing]
    phaseSnd b aId SPCompleted [Nothing]

    phaseRcv a bId SPCompleted [Nothing]

    exchangeGreetingsMsgId 10 a bId b aId

    testFullSwitch a bId b aId 16
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testAbortSwitchStartedReinitiate :: HasCallStack => InitialAgentServers -> IO ()
testAbortSwitchStartedReinitiate servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    pure (aId, bId)
  let withA' = sessionSubscribe withA [bId]
      withB' = sessionSubscribe withB [aId]
  withA' $ \a -> do
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
    -- abort current switch
    stats' <- abortConnectionSwitch a bId
    liftIO $ rcvSwchStatuses' stats' `shouldMatchList` [Nothing]
    -- repeat switch
    stats'' <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats'' `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
  withB' $ \b -> do
    phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
    liftIO . getInAnyOrder b $
      [ switchPhaseSndP aId SPStarted [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId SPConfirmed [Just SSSendingQKEY, Nothing]
      ]
    phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]
  withA $ \a -> withB $ \b -> runRight_ $ do
    subscribeConnection a bId
    subscribeConnection b aId

    liftIO . getInAnyOrder a $
      [ errQueueNotFoundP bId,
        switchPhaseRcvP bId SPConfirmed [Just RSSendingQADD, Nothing]
      ]

    phaseRcv a bId SPSecured [Just RSSendingQUSE, Nothing]

    phaseSnd b aId SPSecured [Just SSSendingQTEST, Nothing]
    phaseSnd b aId SPCompleted [Nothing]

    phaseRcv a bId SPCompleted [Nothing]

    exchangeGreetingsMsgId 10 a bId b aId

    testFullSwitch a bId b aId 16
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

switchPhaseRcvP :: ConnId -> SwitchPhase -> [Maybe RcvSwitchStatus] -> ATransmission -> Bool
switchPhaseRcvP cId sphase swchStatuses = switchPhaseP cId QDRcv sphase (\stats -> rcvSwchStatuses' stats == swchStatuses)

switchPhaseSndP :: ConnId -> SwitchPhase -> [Maybe SndSwitchStatus] -> ATransmission -> Bool
switchPhaseSndP cId sphase swchStatuses = switchPhaseP cId QDSnd sphase (\stats -> sndSwchStatuses' stats == swchStatuses)

switchPhaseP :: ConnId -> QueueDirection -> SwitchPhase -> (ConnectionStats -> Bool) -> ATransmission -> Bool
switchPhaseP cId qd sphase statsP = \case
  (_, cId', AEvt SAEConn (SWITCH qd' sphase' stats)) -> cId' == cId && qd' == qd && sphase' == sphase && statsP stats
  _ -> False

errQueueNotFoundP :: ConnId -> ATransmission -> Bool
errQueueNotFoundP cId = \case
  (_, cId', AEvt SAEConn (ERR AGENT {agentErr = A_QUEUE {queueErr = "QKEY: queue address not found in connection"}})) -> cId' == cId
  _ -> False

testCannotAbortSwitchSecured :: HasCallStack => InitialAgentServers -> IO ()
testCannotAbortSwitchSecured servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetings a bId b aId
    pure (aId, bId)
  let withA' = sessionSubscribe withA [bId]
      withB' = sessionSubscribe withB [aId]
  withA' $ \a -> do
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
  withB' $ \b -> do
    phaseSnd b aId SPStarted [Just SSSendingQKEY, Nothing]
    phaseSnd b aId SPConfirmed [Just SSSendingQKEY, Nothing]
  withA' $ \a -> do
    phaseRcv a bId SPConfirmed [Just RSSendingQADD, Nothing]
    phaseRcv a bId SPSecured [Just RSSendingQUSE, Nothing]
    Left A.CMD {cmdErr = PROHIBITED} <- liftIO . runExceptT $ abortConnectionSwitch a bId
    pure ()
  withA $ \a -> withB $ \b -> runRight_ $ do
    subscribeConnection a bId
    subscribeConnection b aId

    phaseSnd b aId SPSecured [Just SSSendingQTEST, Nothing]
    phaseSnd b aId SPCompleted [Nothing]

    phaseRcv a bId SPCompleted [Nothing]

    exchangeGreetingsMsgId 8 a bId b aId

    testFullSwitch a bId b aId 14
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testSwitch2Connections :: HasCallStack => InitialAgentServers -> IO ()
testSwitch2Connections servers = do
  (aId1, bId1, aId2, bId2) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId1, bId1) <- makeConnection a b
    exchangeGreetings a bId1 b aId1
    (aId2, bId2) <- makeConnection a b
    exchangeGreetings a bId2 b aId2
    pure (aId1, bId1, aId2, bId2)
  let withA' = sessionSubscribe withA [bId1, bId2]
      withB' = sessionSubscribe withB [aId1, aId2]
  withA' $ \a -> do
    stats1 <- switchConnectionAsync a "" bId1
    liftIO $ rcvSwchStatuses' stats1 `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId1 SPStarted [Just RSSendingQADD, Nothing]
    stats2 <- switchConnectionAsync a "" bId2
    liftIO $ rcvSwchStatuses' stats2 `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId2 SPStarted [Just RSSendingQADD, Nothing]
  withB' $ \b -> do
    liftIO . getInAnyOrder b $
      [ switchPhaseSndP aId1 SPStarted [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId1 SPConfirmed [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId2 SPStarted [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId2 SPConfirmed [Just SSSendingQKEY, Nothing]
      ]
  withA' $ \a -> do
    liftIO . getInAnyOrder a $
      [ switchPhaseRcvP bId1 SPConfirmed [Just RSSendingQADD, Nothing],
        switchPhaseRcvP bId1 SPSecured [Just RSSendingQUSE, Nothing],
        switchPhaseRcvP bId2 SPConfirmed [Just RSSendingQADD, Nothing],
        switchPhaseRcvP bId2 SPSecured [Just RSSendingQUSE, Nothing]
      ]
  withB' $ \b -> do
    liftIO . getInAnyOrder b $
      [ switchPhaseSndP aId1 SPSecured [Just SSSendingQTEST, Nothing],
        switchPhaseSndP aId1 SPCompleted [Nothing],
        switchPhaseSndP aId2 SPSecured [Just SSSendingQTEST, Nothing],
        switchPhaseSndP aId2 SPCompleted [Nothing]
      ]
  withA' $ \a -> do
    liftIO . getInAnyOrder a $
      [ switchPhaseRcvP bId1 SPCompleted [Nothing],
        switchPhaseRcvP bId2 SPCompleted [Nothing]
      ]
  withA $ \a -> withB $ \b -> runRight_ $ do
    void $ subscribeConnections a [bId1, bId2]
    void $ subscribeConnections b [aId1, aId2]

    exchangeGreetingsMsgId 8 a bId1 b aId1
    exchangeGreetingsMsgId 8 a bId2 b aId2

    testFullSwitch a bId1 b aId1 14
    testFullSwitch a bId2 b aId2 14
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testSwitch2ConnectionsAbort1 :: HasCallStack => InitialAgentServers -> IO ()
testSwitch2ConnectionsAbort1 servers = do
  (aId1, bId1, aId2, bId2) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId1, bId1) <- makeConnection a b
    exchangeGreetings a bId1 b aId1
    (aId2, bId2) <- makeConnection a b
    exchangeGreetings a bId2 b aId2
    pure (aId1, bId1, aId2, bId2)
  let withA' = sessionSubscribe withA [bId1, bId2]
      withB' = sessionSubscribe withB [aId1, aId2]
  withA' $ \a -> do
    stats1 <- switchConnectionAsync a "" bId1
    liftIO $ rcvSwchStatuses' stats1 `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId1 SPStarted [Just RSSendingQADD, Nothing]
    stats2 <- switchConnectionAsync a "" bId2
    liftIO $ rcvSwchStatuses' stats2 `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId2 SPStarted [Just RSSendingQADD, Nothing]
    -- abort switch of second connection
    stats2' <- abortConnectionSwitch a bId2
    liftIO $ rcvSwchStatuses' stats2' `shouldMatchList` [Nothing]
  withB' $ \b -> do
    liftIO . getInAnyOrder b $
      [ switchPhaseSndP aId1 SPStarted [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId1 SPConfirmed [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId2 SPStarted [Just SSSendingQKEY, Nothing],
        switchPhaseSndP aId2 SPConfirmed [Just SSSendingQKEY, Nothing]
      ]
  withA' $ \a -> do
    liftIO . getInAnyOrder a $
      [ switchPhaseRcvP bId1 SPConfirmed [Just RSSendingQADD, Nothing],
        switchPhaseRcvP bId1 SPSecured [Just RSSendingQUSE, Nothing],
        errQueueNotFoundP bId2
      ]
  withA $ \a -> withB $ \b -> runRight_ $ do
    void $ subscribeConnections a [bId1, bId2]
    void $ subscribeConnections b [aId1, aId2]

    phaseSnd b aId1 SPSecured [Just SSSendingQTEST, Nothing]
    phaseSnd b aId1 SPCompleted [Nothing]

    phaseRcv a bId1 SPCompleted [Nothing]

    exchangeGreetingsMsgId 8 a bId1 b aId1
    exchangeGreetingsMsgId 6 a bId2 b aId2

    testFullSwitch a bId1 b aId1 14
    testFullSwitch a bId2 b aId2 12
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testCreateQueueAuth :: HasCallStack => VersionSMP -> (Maybe BasicAuth, VersionSMP) -> (Maybe BasicAuth, VersionSMP) -> SndQueueSecured -> AgentMsgId -> IO Int
testCreateQueueAuth srvVersion clnt1 clnt2 sqSecured baseId = do
  a <- getClient 1 clnt1 testDB
  b <- getClient 2 clnt2 testDB2
  r <- runRight $ do
    tryError (createConnection a 1 True SCMInvitation Nothing SMSubscribe) >>= \case
      Left (SMP _ AUTH) -> pure 0
      Left e -> throwError e
      Right (bId, qInfo) ->
        tryError (joinConnection b 1 True qInfo "bob's connInfo" SMSubscribe) >>= \case
          Left (SMP _ AUTH) -> pure 1
          Left e -> throwError e
          Right (aId, sqSecured') -> do
            liftIO $ sqSecured' `shouldBe` sqSecured
            ("", _, CONF confId _ "bob's connInfo") <- get a
            allowConnection a bId confId "alice's connInfo"
            get a ##> ("", bId, CON)
            get b ##> ("", aId, INFO "alice's connInfo")
            get b ##> ("", aId, CON)
            exchangeGreetingsMsgId (baseId + 1) a bId b aId
            pure 2
  disposeAgentClient a
  disposeAgentClient b
  pure r
  where
    getClient clientId (clntAuth, clntVersion) db =
      let servers = initAgentServers {smp = userServers' [ProtoServerWithAuth testSMPServer clntAuth]}
          alpn_ = if clntVersion >= authCmdsSMPVersion then Just supportedSMPHandshakes else Nothing
          smpCfg = defaultClientConfig alpn_ False $ V.mkVersionRange minClientSMPRelayVersion clntVersion
          sndAuthAlg = if srvVersion >= authCmdsSMPVersion && clntVersion >= authCmdsSMPVersion then C.AuthAlg C.SX25519 else C.AuthAlg C.SEd25519
       in getSMPAgentClient' clientId agentCfg {smpCfg, sndAuthAlg} servers db

testSMPServerConnectionTest :: ATransport -> Maybe BasicAuth -> SMPServerWithAuth -> IO (Maybe ProtocolTestFailure)
testSMPServerConnectionTest t newQueueBasicAuth srv =
  withSmpServerConfigOn t cfg {newQueueBasicAuth} testPort2 $ \_ -> do
    -- initially passed server is not running
    withAgent 1 agentCfg initAgentServers testDB $ \a ->
      testProtocolServer a 1 srv

testRatchetAdHash :: HasCallStack => IO ()
testRatchetAdHash =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    ad1 <- getConnectionRatchetAdHash a bId
    ad2 <- getConnectionRatchetAdHash b aId
    liftIO $ ad1 `shouldBe` ad2

testDeliveryReceipts :: HasCallStack => IO ()
testDeliveryReceipts =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    -- a sends, b receives and sends delivery receipt
    2 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 2)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 2 $ Just ""
    get a =##> \case ("", c, Rcvd 2) -> c == bId; _ -> False
    ackMessage a bId 3 Nothing
    -- b sends, a receives and sends delivery receipt
    4 <- sendMessage b aId SMP.noMsgFlags "hello too"
    get b ##> ("", aId, SENT 4)
    get a =##> \case ("", c, Msg "hello too") -> c == bId; _ -> False
    ackMessage a bId 4 $ Just ""
    get b =##> \case ("", c, Rcvd 4) -> c == aId; _ -> False
    ackMessage b aId 5 (Just "") `catchError` \case (A.CMD PROHIBITED _) -> pure (); e -> liftIO $ expectationFailure ("unexpected error " <> show e)
    ackMessage b aId 5 Nothing

testDeliveryReceiptsVersion :: HasCallStack => ATransport -> IO ()
testDeliveryReceiptsVersion t = do
  a <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aId, bId) <- runRight $ do
      (aId, bId) <- makeConnection_ PQSupportOff False a b
      checkVersion a bId 3
      checkVersion b aId 3
      (2, _) <- A.sendMessage a bId PQEncOff SMP.noMsgFlags "hello"
      get a ##> ("", bId, SENT 2)
      get b =##> \case ("", c, Msg' 2 PQEncOff "hello") -> c == aId; _ -> False
      ackMessage b aId 2 $ Just ""
      liftIO $ noMessages a "no delivery receipt (unsupported version)"
      (3, _) <- A.sendMessage b aId PQEncOff SMP.noMsgFlags "hello too"
      get b ##> ("", aId, SENT 3)
      get a =##> \case ("", c, Msg' 3 PQEncOff "hello too") -> c == bId; _ -> False
      ackMessage a bId 3 $ Just ""
      liftIO $ noMessages b "no delivery receipt (unsupported version)"
      pure (aId, bId)

    disposeAgentClient a
    disposeAgentClient b
    a' <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = supportedSMPAgentVRange} initAgentServers testDB
    b' <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = supportedSMPAgentVRange} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection a' bId
      subscribeConnection b' aId
      exchangeGreetingsMsgId_ PQEncOff 4 a' bId b' aId
      checkVersion a' bId 7
      checkVersion b' aId 7
      (6, PQEncOff) <- A.sendMessage a' bId PQEncOn SMP.noMsgFlags "hello"
      get a' ##> ("", bId, SENT 6)
      get b' =##> \case ("", c, Msg' 6 PQEncOff "hello") -> c == aId; _ -> False
      ackMessage b' aId 6 $ Just ""
      get a' =##> \case ("", c, Rcvd 6) -> c == bId; _ -> False
      ackMessage a' bId 7 Nothing
      (8, PQEncOff) <- A.sendMessage b' aId PQEncOn SMP.noMsgFlags "hello too"
      get b' ##> ("", aId, SENT 8)
      get a' =##> \case ("", c, Msg' 8 PQEncOff "hello too") -> c == bId; _ -> False
      ackMessage a' bId 8 $ Just ""
      get b' =##> \case ("", c, Rcvd 8) -> c == aId; _ -> False
      ackMessage b' aId 9 Nothing
      (10, _) <- A.sendMessage a' bId PQEncOn SMP.noMsgFlags "hello 2"
      get a' ##> ("", bId, SENT 10)
      get b' =##> \case ("", c, Msg' 10 PQEncOff "hello 2") -> c == aId; _ -> False
      ackMessage b' aId 10 $ Just ""
      get a' =##> \case ("", c, Rcvd 10) -> c == bId; _ -> False
      ackMessage a' bId 11 Nothing
    disposeAgentClient a'
    disposeAgentClient b'

testDeliveryReceiptsConcurrent :: HasCallStack => ATransport -> IO ()
testDeliveryReceiptsConcurrent t =
  withSmpServerConfigOn t cfg {msgQueueQuota = 256, maxJournalMsgCount = 512} testPort $ \_ -> do
    withAgentClients2 $ \a b -> do
      (aId, bId) <- runRight $ makeConnection a b
      t1 <- liftIO getCurrentTime
      concurrently_ (runClient "a" a bId) (runClient "b" b aId)
      t2 <- liftIO getCurrentTime
      diffUTCTime t2 t1 `shouldSatisfy` (< 60)
      liftIO $ noMessages a "nothing else should be delivered to alice"
      liftIO $ noMessages b "nothing else should be delivered to bob"
  where
    runClient :: String -> AgentClient -> ConnId -> IO ()
    runClient _cName client connId = do
      concurrently_ send receive
      where
        numMsgs = 100
        send = runRight_ $
          replicateM_ numMsgs $ do
            void $ sendMessage client connId SMP.noMsgFlags "hello"
        receive =
          runRight_ $
            -- for each sent message: 1 SENT, 1 RCVD, 1 OK for acknowledging RCVD
            -- for each received message: 1 MSG, 1 OK for acknowledging MSG
            receiveLoop (numMsgs * 5)
        receiveLoop :: Int -> ExceptT AgentErrorType IO ()
        receiveLoop 0 = pure ()
        receiveLoop n = do
          r <- getWithTimeout
          case r of
            (_, _, SENT _) -> do
              -- liftIO $ print $ cName <> ": SENT"
              pure ()
            (_, _, MSG MsgMeta {recipient = (msgId, _), integrity = MsgOk} _ _) -> do
              -- liftIO $ print $ cName <> ": MSG " <> show msgId
              ackMessageAsync client (B.pack . show $ n) connId msgId (Just "")
            (_, _, RCVD MsgMeta {recipient = (msgId, _), integrity = MsgOk} _) -> do
              -- liftIO $ print $ cName <> ": RCVD " <> show msgId
              ackMessageAsync client (B.pack . show $ n) connId msgId Nothing
            (_, _, OK) -> do
              -- liftIO $ print $ cName <> ": OK"
              pure ()
            r' -> error $ "unexpected event: " <> show r'
          receiveLoop (n - 1)
        getWithTimeout :: ExceptT AgentErrorType IO (AEntityTransmission 'AEConn)
        getWithTimeout = do
          3000000 `timeout` get client >>= \case
            Just r -> pure r
            _ -> error "timeout"

testTwoUsers :: HasCallStack => IO ()
testTwoUsers = withAgentClients2 $ \a b -> do
  let nc = netCfg initAgentServers
  sessionMode nc `shouldBe` TSMSession
  runRight_ $ do
    (aId1, bId1) <- makeConnectionForUsers a 1 b 1
    exchangeGreetings a bId1 b aId1
    (aId1', bId1') <- makeConnectionForUsers a 1 b 1
    exchangeGreetings a bId1' b aId1'
    a `hasClients` 1
    b `hasClients` 1
    liftIO $ setNetworkConfig a nc {sessionMode = TSMEntity}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 2

    exchangeGreetingsMsgId 4 a bId1 b aId1
    exchangeGreetingsMsgId 4 a bId1' b aId1'
    liftIO $ threadDelay 250000
    liftIO $ setNetworkConfig a nc {sessionMode = TSMUser}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 1

    aUserId2 <- createUser a [noAuthSrvCfg testSMPServer] [noAuthSrvCfg testXFTPServer]
    (aId2, bId2) <- makeConnectionForUsers a aUserId2 b 1
    exchangeGreetings a bId2 b aId2
    (aId2', bId2') <- makeConnectionForUsers a aUserId2 b 1
    exchangeGreetings a bId2' b aId2'
    a `hasClients` 2
    b `hasClients` 1
    liftIO $ setNetworkConfig a nc {sessionMode = TSMEntity}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 4
    exchangeGreetingsMsgId 6 a bId1 b aId1
    exchangeGreetingsMsgId 6 a bId1' b aId1'
    exchangeGreetingsMsgId 4 a bId2 b aId2
    exchangeGreetingsMsgId 4 a bId2' b aId2'
    liftIO $ threadDelay 250000
    liftIO $ setNetworkConfig a nc {sessionMode = TSMUser}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    -- to avoice race condition
    nGet a =##> \case ("", "", DOWN _ _) -> True; ("", "", UP _ _) -> True; _ -> False
    nGet a =##> \case ("", "", UP _ _) -> True; ("", "", DOWN _ _) -> True; _ -> False
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 2
    exchangeGreetingsMsgId 8 a bId1 b aId1
    exchangeGreetingsMsgId 8 a bId1' b aId1'
    exchangeGreetingsMsgId 6 a bId2 b aId2
    exchangeGreetingsMsgId 6 a bId2' b aId2'
  where
    hasClients :: HasCallStack => AgentClient -> Int -> ExceptT AgentErrorType IO ()
    hasClients c n = liftIO $ M.size <$> readTVarIO (smpClients c) `shouldReturn` n

getSMPAgentClient' :: Int -> AgentConfig -> InitialAgentServers -> String -> IO AgentClient
getSMPAgentClient' clientId cfg' initServers dbPath = do
  Right st <- liftIO $ createStore dbPath
  c <- getSMPAgentClient_ clientId cfg' initServers st False
  when (dbNew st) $ insertUser st
  pure c

#if defined(dbPostgres)
createStore :: String -> IO (Either MigrationError DBStore)
createStore schema = createAgentStore (DBOpts testDBConnstr schema) MCError

insertUser :: DBStore -> IO ()
insertUser st = withTransaction st (`DB.execute_` "INSERT INTO users DEFAULT VALUES")
#else
createStore :: String -> IO (Either MigrationError DBStore)
createStore dbPath = createAgentStore (DBOpts dbPath "" False True DB.TQOff) MCError

insertUser :: DBStore -> IO ()
insertUser st = withTransaction st (`DB.execute_` "INSERT INTO users (user_id) VALUES (1)")
#endif

testServerMultipleIdentities :: HasCallStack => IO ()
testServerMultipleIdentities =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    (aliceId, sqSecured) <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob aliceId
    -- this saves queue with second server identity
    bob' <- liftIO $ do
      Left (BROKER _ NETWORK) <- runExceptT $ joinConnection bob 1 True secondIdentityCReq "bob's connInfo" SMSubscribe
      disposeAgentClient bob
      threadDelay 250000
      getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    subscribeConnection bob' aliceId
    exchangeGreetingsMsgId 4 alice bobId bob' aliceId
    liftIO $ disposeAgentClient bob'
  where
    secondIdentityCReq :: ConnectionRequestUri 'CMInvitation
    secondIdentityCReq =
      CRInvitationUri
        connReqData
          { crSmpQueues =
              [ SMPQueueUri
                  supportedSMPClientVRange
                  queueAddr
                    { smpServer = SMPServer "localhost" "5001" (C.KeyHash "\215m\248\251")
                    }
              ]
          }
        testE2ERatchetParams12

testWaitForUserNetwork :: IO ()
testWaitForUserNetwork = do
  a <- getSMPAgentClient' 1 aCfg initAgentServers testDB
  noNetworkDelay a
  setUserNetworkInfo a $ UserNetworkInfo UNNone False
  networkDelay a 100000
  networkDelay a 100000
  setUserNetworkInfo a $ UserNetworkInfo UNCellular True
  noNetworkDelay a
  setUserNetworkInfo a $ UserNetworkInfo UNCellular False
  networkDelay a 100000
  concurrently_
    (threadDelay 50000 >> setUserNetworkInfo a (UserNetworkInfo UNCellular True))
    (networkDelay a 50000)
  noNetworkDelay a
  where
    aCfg = agentCfg {userNetworkInterval = 100000, userOfflineDelay = 0}

testDoNotResetOnlineToOffline :: IO ()
testDoNotResetOnlineToOffline = do
  a <- getSMPAgentClient' 1 aCfg initAgentServers testDB
  noNetworkDelay a
  setUserNetworkInfo a $ UserNetworkInfo UNWifi False
  networkDelay a 100000
  setUserNetworkInfo a $ UserNetworkInfo UNWifi False
  setUserNetworkInfo a $ UserNetworkInfo UNWifi True
  noNetworkDelay a
  setUserNetworkInfo a $ UserNetworkInfo UNWifi False -- ingnored
  noNetworkDelay a
  threadDelay 100000
  setUserNetworkInfo a $ UserNetworkInfo UNWifi False
  networkDelay a 100000
  setUserNetworkInfo a $ UserNetworkInfo UNNone False
  networkDelay a 100000
  setUserNetworkInfo a $ UserNetworkInfo UNWifi True
  setUserNetworkInfo a $ UserNetworkInfo UNNone False -- ingnored
  noNetworkDelay a
  where
    aCfg = agentCfg {userNetworkInterval = 100000, userOfflineDelay = 0.1}

testResumeMultipleThreads :: IO ()
testResumeMultipleThreads = do
  a <- getSMPAgentClient' 1 aCfg initAgentServers testDB
  noNetworkDelay a
  setUserNetworkInfo a $ UserNetworkInfo UNNone False
  vs <-
    replicateM 50000 $ do
      v <- newEmptyTMVarIO
      void . forkIO $ waitNetwork a >>= atomically . putTMVar v
      pure v
  threadDelay 1000000
  setUserNetworkInfo a $ UserNetworkInfo UNCellular True
  ts <- mapM (atomically . readTMVar) vs
  -- print $ minimum ts
  -- print $ maximum ts
  -- print $ sum ts `div` fromIntegral (length ts)
  let average = sum ts `div` fromIntegral (length ts)
  average < 3000000 `shouldBe` True
  maximum ts < 4000000 `shouldBe` True
  where
    aCfg = agentCfg {userOfflineDelay = 0}

testServerQueueInfo :: IO ()
testServerQueueInfo = do
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    liftIO $ threadDelay 200000
    checkEmptyQ alice bobId False
    (aliceId, sqSecured) <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    liftIO $ sqSecured `shouldBe` True
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    liftIO $ threadDelay 200000
    checkEmptyQ alice bobId True -- secured by sender
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    liftIO $ threadDelay 200000
    checkEmptyQ alice bobId True
    checkEmptyQ bob aliceId True
    let msgId = 2
    (msgId', PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello"
    liftIO $ msgId' `shouldBe` msgId
    get alice ##> ("", bobId, SENT msgId)
    liftIO $ threadDelay 200000
    Just srvMsgId <- checkMsgQ bob aliceId 1
    get bob =##> \case
      ("", c, MSG MsgMeta {integrity = MsgOk, broker = (smId, _), recipient = (mId, _), pqEncryption = PQEncOn} _ "hello") ->
        c == aliceId && decodeLatin1 (B64.encode smId) == srvMsgId && mId == msgId
      _ -> False
    ackMessage bob aliceId msgId Nothing
    liftIO $ threadDelay 200000
    checkEmptyQ bob aliceId True
    (msgId1, PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello 1"
    get alice ##> ("", bobId, SENT msgId1)
    Just _ <- checkMsgQ bob aliceId 1
    (msgId2, PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello 2"
    get alice ##> ("", bobId, SENT msgId2)
    (msgId3, PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello 3"
    get alice ##> ("", bobId, SENT msgId3)
    (msgId4, PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello 4"
    get alice ##> ("", bobId, SENT msgId4)
    Just _ <- checkMsgQ bob aliceId 4
    (msgId5, PQEncOn) <- A.sendMessage alice bobId PQEncOn SMP.noMsgFlags "hello: quota exceeded"
    liftIO $ threadDelay 200000
    Just _ <- checkMsgQ bob aliceId 5
    get bob =##> \case ("", c, Msg' mId PQEncOn "hello 1") -> c == aliceId && mId == msgId1; _ -> False
    ackMessage bob aliceId msgId1 Nothing
    liftIO $ threadDelay 200000
    Just _ <- checkMsgQ bob aliceId 4
    get bob =##> \case ("", c, Msg' mId PQEncOn "hello 2") -> c == aliceId && mId == msgId2; _ -> False
    ackMessage bob aliceId msgId2 Nothing
    get bob =##> \case ("", c, Msg' mId PQEncOn "hello 3") -> c == aliceId && mId == msgId3; _ -> False
    ackMessage bob aliceId msgId3 Nothing
    liftIO $ threadDelay 200000
    Just _ <- checkMsgQ bob aliceId 2
    get bob =##> \case ("", c, Msg' mId PQEncOn "hello 4") -> c == aliceId && mId == msgId4; _ -> False
    ackMessage bob aliceId msgId4 Nothing
    liftIO $ threadDelay 200000
    Just _ <- checkMsgQ bob aliceId 1 -- the one that did not fit now accepted
    get alice ##> ("", bobId, QCONT)
    get alice ##> ("", bobId, SENT msgId5)
    liftIO $ threadDelay 200000
    Just _srvMsgId <- checkQ bob aliceId True (Just QNoSub) 1 (Just MTMessage)
    get bob =##> \case ("", c, Msg' mId PQEncOn "hello: quota exceeded") -> c == aliceId && mId == msgId5 + 1; _ -> False
    ackMessage bob aliceId (msgId5 + 1) Nothing
    liftIO $ threadDelay 200000
    checkEmptyQ bob aliceId True
    pure ()
  where
    checkEmptyQ c cId qiSnd' = do
      r <- checkQ c cId qiSnd' (Just QNoSub) 0 Nothing
      liftIO $ r `shouldBe` Nothing
    checkMsgQ c cId qiSize' = do
      r <- checkQ c cId True (Just QNoSub) qiSize' (Just MTMessage)
      liftIO $ isJust r `shouldBe` True
      pure r
    checkQ c cId qiSnd' qiSubThread_ qiSize' msgType_ = do
      ServerQueueInfo {info = QueueInfo {qiSnd, qiNtf, qiSub, qiSize, qiMsg}} <- getConnectionQueueInfo c cId
      liftIO $ do
        qiSnd `shouldBe` qiSnd'
        qiNtf `shouldBe` False
        qSubThread <$> qiSub `shouldBe` qiSubThread_
        qiSize `shouldBe` qiSize'
        msgId_ <- forM qiMsg $ \MsgInfo {msgId, msgType} -> msgId <$ (Just msgType `shouldBe` msgType_)
        qDelivered <$> qiSub `shouldBe` Just msgId_
        pure msgId_

noNetworkDelay :: AgentClient -> IO ()
noNetworkDelay a = do
  d <- waitNetwork a
  unless (d < 10000) $ expectationFailure $ "expected no delay, d = " <> show d

networkDelay :: AgentClient -> Int64 -> IO ()
networkDelay a d' = do
  d <- waitNetwork a
  unless (d' - 1000 < d && d < d' + 15000) $ expectationFailure $ "expected delay " <> show d' <> ", d = " <> show d

waitNetwork :: AgentClient -> IO Int64
waitNetwork a = do
  t <- getCurrentTime
  waitForUserNetwork a
  t' <- getCurrentTime
  pure $ diffToMicroseconds $ diffUTCTime t' t

exchangeGreetings :: HasCallStack => AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetings = exchangeGreetings_ PQEncOn

exchangeGreetings_ :: HasCallStack => PQEncryption -> AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetings_ pqEnc = exchangeGreetingsMsgId_ pqEnc 2

exchangeGreetingsMsgId :: HasCallStack => Int64 -> AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetingsMsgId = exchangeGreetingsMsgId_ PQEncOn

exchangeGreetingsMsgId_ :: HasCallStack => PQEncryption -> Int64 -> AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetingsMsgId_ pqEnc msgId alice bobId bob aliceId = do
  msgId1 <- A.sendMessage alice bobId pqEnc SMP.noMsgFlags "hello"
  liftIO $ msgId1 `shouldBe` (msgId, pqEnc)
  get alice ##> ("", bobId, SENT msgId)
  get bob =##> \case ("", c, Msg' mId pq "hello") -> c == aliceId && mId == msgId && pq == pqEnc; _ -> False
  ackMessage bob aliceId msgId Nothing
  msgId2 <- A.sendMessage bob aliceId pqEnc SMP.noMsgFlags "hello too"
  let msgId' = msgId + 1
  liftIO $ msgId2 `shouldBe` (msgId', pqEnc)
  get bob ##> ("", aliceId, SENT msgId')
  get alice =##> \case ("", c, Msg' mId pq "hello too") -> c == bobId && mId == msgId' && pq == pqEnc; _ -> False
  ackMessage alice bobId msgId' Nothing

exchangeGreetingsMsgIds :: HasCallStack => AgentClient -> ConnId -> Int64 -> AgentClient -> ConnId -> Int64 -> ExceptT AgentErrorType IO ()
exchangeGreetingsMsgIds alice bobId aliceMsgId bob aliceId bobMsgId = do
  msgId1 <- sendMessage alice bobId SMP.noMsgFlags "hello"
  liftIO $ msgId1 `shouldBe` aliceMsgId
  get alice ##> ("", bobId, SENT aliceMsgId)
  get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  ackMessage bob aliceId bobMsgId Nothing
  msgId2 <- sendMessage bob aliceId SMP.noMsgFlags "hello too"
  let aliceMsgId' = aliceMsgId + 1
      bobMsgId' = bobMsgId + 1
  liftIO $ msgId2 `shouldBe` bobMsgId'
  get bob ##> ("", aliceId, SENT bobMsgId')
  get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
  ackMessage alice bobId aliceMsgId' Nothing

newtype InternalException e = InternalException {unInternalException :: e}
  deriving (Eq, Show)

instance Exception e => Exception (InternalException e)

instance Exception e => MonadUnliftIO (ExceptT e IO) where
  {-# INLINE withRunInIO #-}
  withRunInIO :: ((forall a. ExceptT e IO a -> IO a) -> IO b) -> ExceptT e IO b
  withRunInIO inner =
    ExceptT . fmap (first unInternalException) . try $
      withRunInIO $ \run ->
        inner $ run . (either (throwIO . InternalException) pure <=< runExceptT)
