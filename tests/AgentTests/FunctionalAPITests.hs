{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}

module AgentTests.FunctionalAPITests
  ( functionalAPITests,
    testServerMatrix2,
    withAgentClientsCfg2,
    getSMPAgentClient',
    makeConnection,
    exchangeGreetingsMsgId,
    switchComplete,
    runRight,
    runRight_,
    get,
    get',
    rfGet,
    sfGet,
    nGet,
    (##>),
    (=##>),
    pattern Msg,
    agentCfgV7,
  )
where

import AgentTests.ConnectionRequestTests (connReqData, queueAddr, testE2ERatchetParams12)
import Control.Concurrent (killThread, threadDelay)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Either (isRight)
import Data.Int (Int64)
import qualified Data.Map as M
import Data.Maybe (isNothing)
import qualified Data.Set as S
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Data.Type.Equality
import qualified Database.SQLite.Simple as SQL
import SMPAgentClient
import SMPClient (cfg, testPort, testPort2, testStoreLogFile2, withSmpServer, withSmpServerV7, withSmpServerConfigOn, withSmpServerOn, withSmpServerStoreLogOn, withSmpServerStoreMsgLogOn)
import Simplex.Messaging.Agent
import Simplex.Messaging.Agent.Client (ProtocolTestFailure (..), ProtocolTestStep (..))
import Simplex.Messaging.Agent.Env.SQLite (AgentConfig (..), InitialAgentServers (..), createAgentStore)
import Simplex.Messaging.Agent.Protocol as Agent
import Simplex.Messaging.Agent.Store.SQLite (MigrationConfirmation (..), SQLiteStore (dbNew))
import Simplex.Messaging.Agent.Store.SQLite.Common (withTransaction')
import Simplex.Messaging.Client (NetworkConfig (..), ProtocolClientConfig (..), TransportSessionMode (TSMEntity, TSMUser), defaultSMPClientConfig)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Notifications.Transport (authBatchCmdsNTFVersion)
import Simplex.Messaging.Protocol (BasicAuth, ErrorType (..), MsgBody, ProtocolServer (..), SubscriptionMode (..), supportedSMPClientVRange)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Server.Env.STM (ServerConfig (..))
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Transport (ATransport (..), authCmdsSMPVersion)
import Simplex.Messaging.Version
import System.Directory (copyFile, renameFile)
import Test.Hspec
import UnliftIO
import XFTPClient (testXFTPServer)

type AEntityTransmission e = (ACorrId, ConnId, ACommand 'Agent e)

(##>) :: (HasCallStack, MonadUnliftIO m) => m (AEntityTransmission e) -> AEntityTransmission e -> m ()
a ##> t = withTimeout a (`shouldBe` t)

(=##>) :: (Show a, HasCallStack, MonadUnliftIO m) => m a -> (a -> Bool) -> m ()
a =##> p =
  withTimeout a $ \r -> do
    unless (p r) $ liftIO $ putStrLn $ "value failed predicate: " <> show r
    r `shouldSatisfy` p

withTimeout :: (HasCallStack, MonadUnliftIO m) => m a -> (a -> Expectation) -> m ()
withTimeout a test =
  timeout 10_000000 a >>= \case
    Nothing -> error "operation timed out"
    Just t -> liftIO $ test t

get :: MonadIO m => AgentClient -> m (AEntityTransmission 'AEConn)
get = get' @'AEConn

rfGet :: MonadIO m => AgentClient -> m (AEntityTransmission 'AERcvFile)
rfGet = get' @'AERcvFile

sfGet :: MonadIO m => AgentClient -> m (AEntityTransmission 'AESndFile)
sfGet = get' @'AESndFile

nGet :: MonadIO m => AgentClient -> m (AEntityTransmission 'AENone)
nGet = get' @'AENone

get' :: forall e m. (MonadIO m, AEntityI e) => AgentClient -> m (AEntityTransmission e)
get' c = do
  (corrId, connId, APC e cmd) <- pGet c
  case testEquality e (sAEntity @e) of
    Just Refl -> pure (corrId, connId, cmd)
    _ -> error $ "unexpected command " <> show cmd

pGet :: forall m. MonadIO m => AgentClient -> m (ATransmission 'Agent)
pGet c = do
  t@(_, _, APC _ cmd) <- atomically (readTBQueue $ subQ c)
  case cmd of
    CONNECT {} -> pGet c
    DISCONNECT {} -> pGet c
    _ -> pure t

pattern Msg :: MsgBody -> ACommand 'Agent e
pattern Msg msgBody <- MSG MsgMeta {integrity = MsgOk} _ msgBody

pattern MsgErr :: AgentMsgId -> MsgErrorType -> MsgBody -> ACommand 'Agent e
pattern MsgErr msgId err msgBody <- MSG MsgMeta {recipient = (msgId, _), integrity = MsgError err} _ msgBody

pattern Rcvd :: AgentMsgId -> ACommand 'Agent e
pattern Rcvd agentMsgId <- RCVD MsgMeta {integrity = MsgOk} [MsgReceipt {agentMsgId, msgRcptStatus = MROk}]

smpCfgVPrev :: ProtocolClientConfig
smpCfgVPrev = (smpCfg agentCfg) {serverVRange = prevRange $ serverVRange $ smpCfg agentCfg}

smpCfgV7 :: ProtocolClientConfig
smpCfgV7 = (smpCfg agentCfg) {serverVRange = mkVersionRange 4 authCmdsSMPVersion}

ntfCfgV2 :: ProtocolClientConfig
ntfCfgV2 = (smpCfg agentCfg) {serverVRange = mkVersionRange 1 authBatchCmdsNTFVersion}

agentCfgVPrev :: AgentConfig
agentCfgVPrev =
  agentCfg
    { smpAgentVRange = prevRange $ smpAgentVRange agentCfg,
      smpClientVRange = prevRange $ smpClientVRange agentCfg,
      e2eEncryptVRange = prevRange $ e2eEncryptVRange agentCfg,
      smpCfg = smpCfgVPrev
    }

agentCfgV7 :: AgentConfig
agentCfgV7 = 
  agentCfg
    { sndAuthAlg = C.AuthAlg C.SX25519,
      smpCfg = smpCfgV7,
      ntfCfg = ntfCfgV2
    }

agentCfgRatchetVPrev :: AgentConfig
agentCfgRatchetVPrev = agentCfg {e2eEncryptVRange = prevRange $ e2eEncryptVRange agentCfg}

prevRange :: VersionRange -> VersionRange
prevRange vr = vr {maxVersion = max (minVersion vr) (maxVersion vr - 1)}

runRight_ :: (Eq e, Show e, HasCallStack) => ExceptT e IO () -> Expectation
runRight_ action = runExceptT action `shouldReturn` Right ()

runRight :: (Show e, HasCallStack) => ExceptT e IO a -> IO a
runRight action =
  runExceptT action >>= \case
    Right x -> pure x
    Left e -> error $ "Unexpected error: " <> show e

getInAnyOrder :: HasCallStack => AgentClient -> [ATransmission 'Agent -> Bool] -> Expectation
getInAnyOrder c = inAnyOrder (pGet c)

inAnyOrder :: (Show a, MonadIO m, HasCallStack) => m a -> [a -> Bool] -> m ()
inAnyOrder _ [] = pure ()
inAnyOrder g rs = do
  r <- g
  let rest = filter (not . expected r) rs
  if length rest < length rs
    then inAnyOrder g rest
    else error $ "unexpected event: " <> show r
  where
    expected :: a -> (a -> Bool) -> Bool
    expected r rp = rp r

functionalAPITests :: ATransport -> Spec
functionalAPITests t = do
  describe "Establishing duplex connection" $ do
    testMatrix2 t runAgentClientTest
    it "should connect when server with multiple identities is stored" $
      withSmpServer t testServerMultipleIdentities
    it "should connect with two peers" $
      withSmpServer t testAgentClient3
  describe "Establishing duplex connection v2, different Ratchet versions" $
    testRatchetMatrix2 t runAgentClientTest
  describe "Establish duplex connection via contact address" $
    testMatrix2 t runAgentClientContactTest
  describe "Establish duplex connection via contact address v2, different Ratchet versions" $
    testRatchetMatrix2 t runAgentClientContactTest
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
    it "should deliver message after client restart" $
      testDeliverClientRestart t
    it "should deliver messages to the user once, even if repeat delivery is made by the server (no ACK)" $
      testDuplicateMessage t
    it "should report error via msg integrity on skipped messages" $
      testSkippedMessages t
    describe "message expiration" $ do
      it "should expire one message" $ testExpireMessage t
      it "should expire multiple messages" $ testExpireManyMessages t
      it "should expire one message if quota is exceeded" $ testExpireMessageQuota t
      it "should expire multiple messages if quota is exceeded" $ testExpireManyMessagesQuota t
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
    describe "Subscription mode OnlyCreate" $ do
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
    -- 200 subscriptions gets very slow with test coverage, use below test instead
    xit "should subscribe to multiple (6) subscriptions with batching" $
      testBatchedSubscriptions 6 3 t
  describe "Async agent commands" $ do
    it "should connect using async agent commands" $
      withSmpServer t testAsyncCommands
    it "should restore and complete async commands on restart" $
      testAsyncCommandsRestore t
    it "should accept connection using async command" $
      withSmpServer t testAcceptContactAsync
    it "should delete connections using async command when server connection fails" $
      testDeleteConnectionAsync t
    it "join connection when reply queue creation fails" $
      testJoinConnectionAsyncReplyError t
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
    describe "with server auth" $ do
      --                                       allow NEW | server auth, v | clnt1 auth, v  | clnt2 auth, v    |  2 - success, 1 - JOIN fail, 0 - NEW fail
      it "success                " $ testBasicAuth t True (Just "abcd", 5) (Just "abcd", 5) (Just "abcd", 5) `shouldReturn` 2
      it "disabled               " $ testBasicAuth t False (Just "abcd", 5) (Just "abcd", 5) (Just "abcd", 5) `shouldReturn` 0
      it "NEW fail, no auth      " $ testBasicAuth t True (Just "abcd", 5) (Nothing, 5) (Just "abcd", 5) `shouldReturn` 0
      it "NEW fail, bad auth     " $ testBasicAuth t True (Just "abcd", 5) (Just "wrong", 5) (Just "abcd", 5) `shouldReturn` 0
      it "NEW fail, version      " $ testBasicAuth t True (Just "abcd", 5) (Just "abcd", 4) (Just "abcd", 5) `shouldReturn` 0
      it "JOIN fail, no auth     " $ testBasicAuth t True (Just "abcd", 5) (Just "abcd", 5) (Nothing, 5) `shouldReturn` 1
      it "JOIN fail, bad auth    " $ testBasicAuth t True (Just "abcd", 5) (Just "abcd", 5) (Just "wrong", 5) `shouldReturn` 1
      it "JOIN fail, version     " $ testBasicAuth t True (Just "abcd", 5) (Just "abcd", 5) (Just "abcd", 4) `shouldReturn` 1
    describe "no server auth" $ do
      it "success     " $ testBasicAuth t True (Nothing, 5) (Nothing, 5) (Nothing, 5) `shouldReturn` 2
      it "srv disabled" $ testBasicAuth t False (Nothing, 5) (Nothing, 5) (Nothing, 5) `shouldReturn` 0
      it "version srv " $ testBasicAuth t True (Nothing, 4) (Nothing, 5) (Nothing, 5) `shouldReturn` 2
      it "version fst " $ testBasicAuth t True (Nothing, 5) (Nothing, 4) (Nothing, 5) `shouldReturn` 2
      it "version snd " $ testBasicAuth t True (Nothing, 5) (Nothing, 5) (Nothing, 4) `shouldReturn` 2
      it "version both" $ testBasicAuth t True (Nothing, 5) (Nothing, 4) (Nothing, 4) `shouldReturn` 2
      it "version all " $ testBasicAuth t True (Nothing, 4) (Nothing, 4) (Nothing, 4) `shouldReturn` 2
      it "auth fst    " $ testBasicAuth t True (Nothing, 5) (Just "abcd", 5) (Nothing, 5) `shouldReturn` 2
      it "auth fst 2  " $ testBasicAuth t True (Nothing, 4) (Just "abcd", 5) (Nothing, 5) `shouldReturn` 2
      it "auth snd    " $ testBasicAuth t True (Nothing, 5) (Nothing, 5) (Just "abcd", 5) `shouldReturn` 2
      it "auth both   " $ testBasicAuth t True (Nothing, 5) (Just "abcd", 5) (Just "abcd", 5) `shouldReturn` 2
      it "auth, disabled" $ testBasicAuth t False (Nothing, 5) (Just "abcd", 5) (Just "abcd", 5) `shouldReturn` 0
  describe "SMP server test via agent API" $ do
    it "should pass without basic auth" $ testSMPServerConnectionTest t Nothing (noAuthSrv testSMPServer2) `shouldReturn` Nothing
    let srv1 = testSMPServer2 {keyHash = "1234"}
    it "should fail with incorrect fingerprint" $ do
      testSMPServerConnectionTest t Nothing (noAuthSrv srv1) `shouldReturn` Just (ProtocolTestFailure TSConnect $ BROKER (B.unpack $ strEncode srv1) NETWORK)
    describe "server with password" $ do
      let auth = Just "abcd"
          srv = ProtoServerWithAuth testSMPServer2
          authErr = Just (ProtocolTestFailure TSCreateQueue $ SMP AUTH)
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

testBasicAuth :: ATransport -> Bool -> (Maybe BasicAuth, Version) -> (Maybe BasicAuth, Version) -> (Maybe BasicAuth, Version) -> IO Int
testBasicAuth t allowNewQueues srv@(srvAuth, srvVersion) clnt1 clnt2 = do
  let testCfg = cfg {allowNewQueues, newQueueBasicAuth = srvAuth, smpServerVRange = mkVersionRange 4 srvVersion}
      canCreate1 = canCreateQueue allowNewQueues srv clnt1
      canCreate2 = canCreateQueue allowNewQueues srv clnt2
      expected
        | canCreate1 && canCreate2 = 2
        | canCreate1 = 1
        | otherwise = 0
  created <- withSmpServerConfigOn t testCfg testPort $ \_ -> testCreateQueueAuth clnt1 clnt2
  created `shouldBe` expected
  pure created

canCreateQueue :: Bool -> (Maybe BasicAuth, Version) -> (Maybe BasicAuth, Version) -> Bool
canCreateQueue allowNew (srvAuth, srvVersion) (clntAuth, clntVersion) =
  allowNew && (isNothing srvAuth || (srvVersion == 5 && clntVersion == 5 && srvAuth == clntAuth))

testMatrix2 :: ATransport -> (AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testMatrix2 t runTest = do
  it "v7" $ withSmpServerV7 t $ runTestCfg2 agentCfgV7 agentCfgV7 3 runTest
  it "v7 to current" $ withSmpServerV7 t $ runTestCfg2 agentCfgV7 agentCfg 3 runTest
  it "current to v7" $ withSmpServerV7 t $ runTestCfg2 agentCfg agentCfgV7 3 runTest
  it "current with v7 server" $ withSmpServerV7 t $ runTestCfg2 agentCfg agentCfg 3 runTest
  it "current" $ withSmpServer t $ runTestCfg2 agentCfg agentCfg 3 runTest
  it "prev" $ withSmpServer t $ runTestCfg2 agentCfgVPrev agentCfgVPrev 3 runTest
  it "prev to current" $ withSmpServer t $ runTestCfg2 agentCfgVPrev agentCfg 3 runTest
  it "current to prev" $ withSmpServer t $ runTestCfg2 agentCfg agentCfgVPrev 3 runTest

testRatchetMatrix2 :: ATransport -> (AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> Spec
testRatchetMatrix2 t runTest = do
  it "ratchet current" $ withSmpServer t $ runTestCfg2 agentCfg agentCfg 3 runTest
  pendingV "ratchet prev" $ withSmpServer t $ runTestCfg2 agentCfgRatchetVPrev agentCfgRatchetVPrev 3 runTest
  pendingV "ratchets prev to current" $ withSmpServer t $ runTestCfg2 agentCfgRatchetVPrev agentCfg 3 runTest
  pendingV "ratchets current to prev" $ withSmpServer t $ runTestCfg2 agentCfg agentCfgRatchetVPrev 3 runTest
  where
    pendingV = 
      let vr = e2eEncryptVRange agentCfg
       in if minVersion vr == maxVersion vr then xit else it

testServerMatrix2 :: ATransport -> (InitialAgentServers -> IO ()) -> Spec
testServerMatrix2 t runTest = do
  it "1 server" $ withSmpServer t $ runTest initAgentServers
  it "2 servers" $ withSmpServer t . withSmpServerOn t testPort2 $ runTest initAgentServers2

runTestCfg2 :: AgentConfig -> AgentConfig -> AgentMsgId -> (AgentClient -> AgentClient -> AgentMsgId -> IO ()) -> IO ()
runTestCfg2 aCfg bCfg baseMsgId runTest =
  withAgentClientsCfg2 aCfg bCfg $ \a b -> runTest a b baseMsgId

withAgentClientsCfg2 :: AgentConfig -> AgentConfig -> (AgentClient -> AgentClient -> IO ()) -> IO ()
withAgentClientsCfg2 aCfg bCfg runTest = do
  a <- getSMPAgentClient' 1 aCfg initAgentServers testDB
  b <- getSMPAgentClient' 2 bCfg initAgentServers testDB2
  runTest a b
  disconnectAgentClient a
  disconnectAgentClient b

withAgentClients2 :: (AgentClient -> AgentClient -> IO ()) -> IO ()
withAgentClients2 = withAgentClientsCfg2 agentCfg agentCfg

runAgentClientTest :: HasCallStack => AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientTest alice@AgentClient {} bob baseId =
  runRight_ $ do
    (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
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
    get bob ##> ("", aliceId, MERR (baseId + 5) (SMP AUTH))
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId

testAgentClient3 :: HasCallStack => IO ()
testAgentClient3 = do
  a <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  c <- getSMPAgentClient' 3 agentCfg initAgentServers testDB3
  runRight_ $ do
    (aIdForB, bId) <- makeConnection a b
    (aIdForC, cId) <- makeConnection a c

    4 <- sendMessage a bId SMP.noMsgFlags "b4"
    4 <- sendMessage a cId SMP.noMsgFlags "c4"
    5 <- sendMessage a bId SMP.noMsgFlags "b5"
    5 <- sendMessage a cId SMP.noMsgFlags "c5"
    get a =##> \case ("", connId, SENT 4) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 4) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 5) -> connId == bId || connId == cId; _ -> False
    get a =##> \case ("", connId, SENT 5) -> connId == bId || connId == cId; _ -> False
    get b =##> \case ("", connId, Msg "b4") -> connId == aIdForB; _ -> False
    ackMessage b aIdForB 4 Nothing
    get b =##> \case ("", connId, Msg "b5") -> connId == aIdForB; _ -> False
    ackMessage b aIdForB 5 Nothing
    get c =##> \case ("", connId, Msg "c4") -> connId == aIdForC; _ -> False
    ackMessage c aIdForC 4 Nothing
    get c =##> \case ("", connId, Msg "c5") -> connId == aIdForC; _ -> False
    ackMessage c aIdForC 5 Nothing

runAgentClientContactTest :: HasCallStack => AgentClient -> AgentClient -> AgentMsgId -> IO ()
runAgentClientContactTest alice bob baseId =
  runRight_ $ do
    (_, qInfo) <- createConnection alice 1 True SCMContact Nothing SMSubscribe
    aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    ("", _, REQ invId _ "bob's connInfo") <- get alice
    bobId <- acceptContact alice True invId "alice's connInfo" SMSubscribe
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
    get bob ##> ("", aliceId, MERR (baseId + 5) (SMP AUTH))
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    msgId = subtract baseId

noMessages :: HasCallStack => AgentClient -> String -> Expectation
noMessages c err = tryGet `shouldReturn` ()
  where
    tryGet =
      10000 `timeout` get c >>= \case
        Just msg -> error $ err <> ": " <> show msg
        _ -> return ()

testAsyncInitiatingOffline :: HasCallStack => IO ()
testAsyncInitiatingOffline =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    disconnectAgentClient alice
    aliceId <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    alice' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB
    subscribeConnection alice' bobId
    ("", _, CONF confId _ "bob's connInfo") <- get alice'
    allowConnection alice' bobId confId "alice's connInfo"
    get alice' ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice' bobId bob aliceId

testAsyncJoiningOfflineBeforeActivation :: HasCallStack => IO ()
testAsyncJoiningOfflineBeforeActivation =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    disconnectAgentClient bob
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    bob' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    subscribeConnection bob' aliceId
    get alice ##> ("", bobId, CON)
    get bob' ##> ("", aliceId, INFO "alice's connInfo")
    get bob' ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob' aliceId

testAsyncBothOffline :: HasCallStack => IO ()
testAsyncBothOffline =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    disconnectAgentClient alice
    aliceId <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    disconnectAgentClient bob
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
    aliceId <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob aliceId

testAllowConnectionClientRestart :: HasCallStack => ATransport -> IO ()
testAllowConnectionClientRestart t = do
  let initAgentServersSrv2 = initAgentServers {smp = userServers [noAuthSrv testSMPServer2]}
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServersSrv2 testDB2
  withSmpServerStoreLogOn t testPort $ \_ -> do
    (aliceId, bobId, confId) <-
      withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2} testPort2 $ \_ -> do
        runRight $ do
          (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
          aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
          ("", _, CONF confId _ "bob's connInfo") <- get alice
          pure (aliceId, bobId, confId)

    ("", "", DOWN _ _) <- nGet bob

    runRight_ $ do
      allowConnectionAsync alice "1" bobId confId "alice's connInfo"
      get alice =##> \case ("1", _, OK) -> True; _ -> False
      pure ()

    threadDelay 100000 -- give time to enqueue confirmation (enqueueConfirmation)
    disconnectAgentClient alice

    alice2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB

    withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2} testPort2 $ \_ -> do
      runRight $ do
        ("", "", UP _ _) <- nGet bob

        subscribeConnection alice2 bobId

        get alice2 ##> ("", bobId, CON)
        get bob ##> ("", aliceId, INFO "alice's connInfo")
        get bob ##> ("", aliceId, CON)

        exchangeGreetingsMsgId 4 alice2 bobId bob aliceId
    disconnectAgentClient alice2
    disconnectAgentClient bob

testIncreaseConnAgentVersion :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersion t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection alice bob
      exchangeGreetingsMsgId 4 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version doesn't increase if incompatible

    disconnectAgentClient alice
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice2 bobId
      exchangeGreetingsMsgId 6 alice2 bobId bob aliceId
      checkVersion alice2 bobId 2
      checkVersion bob aliceId 2

    -- version increases if compatible

    disconnectAgentClient bob
    bob2 <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection bob2 aliceId
      exchangeGreetingsMsgId 8 alice2 bobId bob2 aliceId
      checkVersion alice2 bobId 3
      checkVersion bob2 aliceId 3

    -- version doesn't decrease, even if incompatible

    disconnectAgentClient alice2
    alice3 <- getSMPAgentClient' 5 agentCfg {smpAgentVRange = mkVersionRange 2 2} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice3 bobId
      exchangeGreetingsMsgId 10 alice3 bobId bob2 aliceId
      checkVersion alice3 bobId 3
      checkVersion bob2 aliceId 3

    disconnectAgentClient bob2
    bob3 <- getSMPAgentClient' 6 agentCfg {smpAgentVRange = mkVersionRange 1 1} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection bob3 aliceId
      exchangeGreetingsMsgId 12 alice3 bobId bob3 aliceId
      checkVersion alice3 bobId 3
      checkVersion bob3 aliceId 3
    disconnectAgentClient alice3
    disconnectAgentClient bob3

checkVersion :: AgentClient -> ConnId -> Version -> ExceptT AgentErrorType IO ()
checkVersion c connId v = do
  ConnectionStats {connAgentVersion} <- getConnectionServers c connId
  liftIO $ connAgentVersion `shouldBe` v

testIncreaseConnAgentVersionMaxCompatible :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersionMaxCompatible t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection alice bob
      exchangeGreetingsMsgId 4 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version increases to max compatible

    disconnectAgentClient alice
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB
    disconnectAgentClient bob
    bob2 <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = mkVersionRange 1 4} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection alice2 bobId
      subscribeConnection bob2 aliceId
      exchangeGreetingsMsgId 6 alice2 bobId bob2 aliceId
      checkVersion alice2 bobId 3
      checkVersion bob2 aliceId 3
    disconnectAgentClient alice2
    disconnectAgentClient bob2

testIncreaseConnAgentVersionStartDifferentVersion :: HasCallStack => ATransport -> IO ()
testIncreaseConnAgentVersionStartDifferentVersion t = do
  alice <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 2} initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ do
      (aliceId, bobId) <- makeConnection alice bob
      exchangeGreetingsMsgId 4 alice bobId bob aliceId
      checkVersion alice bobId 2
      checkVersion bob aliceId 2
      pure (aliceId, bobId)

    -- version increases to max compatible

    disconnectAgentClient alice
    alice2 <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB

    runRight_ $ do
      subscribeConnection alice2 bobId
      exchangeGreetingsMsgId 6 alice2 bobId bob aliceId
      checkVersion alice2 bobId 3
      checkVersion bob aliceId 3
    disconnectAgentClient alice2
    disconnectAgentClient bob

testDeliverClientRestart :: HasCallStack => ATransport -> IO ()
testDeliverClientRestart t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2

  (aliceId, bobId) <- withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight $ do
      (aliceId, bobId) <- makeConnection alice bob
      exchangeGreetingsMsgId 4 alice bobId bob aliceId
      pure (aliceId, bobId)

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob

  6 <- runRight $ sendMessage bob aliceId SMP.noMsgFlags "hello"

  disconnectAgentClient bob

  bob2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      ("", "", UP _ _) <- nGet alice

      subscribeConnection bob2 aliceId

      get bob2 ##> ("", aliceId, SENT 6)
      get alice =##> \case ("", c, Msg "hello") -> c == bobId; _ -> False
  disconnectAgentClient alice
  disconnectAgentClient bob2

testDuplicateMessage :: HasCallStack => ATransport -> IO ()
testDuplicateMessage t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob1) <- withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ makeConnection alice bob
    runRight_ $ do
      4 <- sendMessage alice bobId SMP.noMsgFlags "hello"
      get alice ##> ("", bobId, SENT 4)
      get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    disconnectAgentClient bob

    -- if the agent user did not send ACK, the message will be delivered again
    bob1 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    runRight_ $ do
      subscribeConnection bob1 aliceId
      get bob1 =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
      ackMessage bob1 aliceId 4 Nothing
      5 <- sendMessage alice bobId SMP.noMsgFlags "hello 2"
      get alice ##> ("", bobId, SENT 5)
      get bob1 =##> \case ("", c, Msg "hello 2") -> c == aliceId; _ -> False

    pure (aliceId, bobId, bob1)

  nGet alice =##> \case ("", "", DOWN _ [c]) -> c == bobId; _ -> False
  nGet bob1 =##> \case ("", "", DOWN _ [c]) -> c == aliceId; _ -> False
  -- commenting two lines below and uncommenting further two lines would also runRight_,
  -- it is the scenario tested above, when the message was not acknowledged by the user
  threadDelay 200000
  Left (BROKER _ NETWORK) <- runExceptT $ ackMessage bob1 aliceId 5 Nothing

  disconnectAgentClient alice
  disconnectAgentClient bob1

  alice2 <- getSMPAgentClient' 4 agentCfg initAgentServers testDB
  bob2 <- getSMPAgentClient' 5 agentCfg initAgentServers testDB2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      subscribeConnection bob2 aliceId
      subscribeConnection alice2 bobId
      -- get bob2 =##> \case ("", c, Msg "hello 2") -> c == aliceId; _ -> False
      -- ackMessage bob2 aliceId 5 Nothing
      -- message 2 is not delivered again, even though it was delivered to the agent
      6 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 3"
      get alice2 ##> ("", bobId, SENT 6)
      get bob2 =##> \case ("", c, Msg "hello 3") -> c == aliceId; _ -> False
  disconnectAgentClient alice2
  disconnectAgentClient bob2

testSkippedMessages :: HasCallStack => ATransport -> IO ()
testSkippedMessages t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId) <- withSmpServerStoreLogOn t testPort $ \_ -> do
    (aliceId, bobId) <- runRight $ makeConnection alice bob
    runRight_ $ do
      4 <- sendMessage alice bobId SMP.noMsgFlags "hello"
      get alice ##> ("", bobId, SENT 4)
      get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
      ackMessage bob aliceId 4 Nothing

    disconnectAgentClient bob

    runRight_ $ do
      5 <- sendMessage alice bobId SMP.noMsgFlags "hello 2"
      get alice ##> ("", bobId, SENT 5)
      6 <- sendMessage alice bobId SMP.noMsgFlags "hello 3"
      get alice ##> ("", bobId, SENT 6)
      7 <- sendMessage alice bobId SMP.noMsgFlags "hello 4"
      get alice ##> ("", bobId, SENT 7)

    pure (aliceId, bobId)

  nGet alice =##> \case ("", "", DOWN _ [c]) -> c == bobId; _ -> False
  threadDelay 200000

  disconnectAgentClient alice

  alice2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB
  bob2 <- getSMPAgentClient' 4 agentCfg initAgentServers testDB2

  withSmpServerStoreLogOn t testPort $ \_ -> do
    runRight_ $ do
      subscribeConnection bob2 aliceId
      subscribeConnection alice2 bobId

      8 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 5"
      get alice2 ##> ("", bobId, SENT 8)
      get bob2 =##> \case ("", c, MSG MsgMeta {integrity = MsgError {errorInfo = MsgSkipped {fromMsgId = 4, toMsgId = 6}}} _ "hello 5") -> c == aliceId; _ -> False
      ackMessage bob2 aliceId 5 Nothing

      9 <- sendMessage alice2 bobId SMP.noMsgFlags "hello 6"
      get alice2 ##> ("", bobId, SENT 9)
      get bob2 =##> \case ("", c, Msg "hello 6") -> c == aliceId; _ -> False
      ackMessage bob2 aliceId 6 Nothing
  disconnectAgentClient alice2
  disconnectAgentClient bob2

testExpireMessage :: HasCallStack => ATransport -> IO ()
testExpireMessage t = do
  a <- getSMPAgentClient' 1 agentCfg {messageTimeout = 1, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ makeConnection a b
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
  nGet b =##> \case ("", "", DOWN _ [c]) -> c == aId; _ -> False
  4 <- runRight $ sendMessage a bId SMP.noMsgFlags "1"
  threadDelay 1000000
  5 <- runRight $ sendMessage a bId SMP.noMsgFlags "2" -- this won't expire
  get a =##> \case ("", c, MERR 4 (BROKER _ e)) -> bId == c && (e == TIMEOUT || e == NETWORK); _ -> False
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    withUP a bId $ \case ("", _, SENT 5) -> True; _ -> False
    withUP b aId $ \case ("", _, MsgErr 4 (MsgSkipped 3 3) "2") -> True; _ -> False
    ackMessage b aId 4 Nothing

testExpireManyMessages :: HasCallStack => ATransport -> IO ()
testExpireManyMessages t = do
  a <- getSMPAgentClient' 1 agentCfg {messageTimeout = 1, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ makeConnection a b
  runRight_ $ do
    nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
    nGet b =##> \case ("", "", DOWN _ [c]) -> c == aId; _ -> False
    4 <- sendMessage a bId SMP.noMsgFlags "1"
    5 <- sendMessage a bId SMP.noMsgFlags "2"
    6 <- sendMessage a bId SMP.noMsgFlags "3"
    liftIO $ threadDelay 1000000
    7 <- sendMessage a bId SMP.noMsgFlags "4" -- this won't expire
    get a =##> \case ("", c, MERR 4 (BROKER _ e)) -> bId == c && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", c, MERRS [5, 6] (BROKER _ e)) -> bId == c && (e == TIMEOUT || e == NETWORK); _ -> False
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    withUP a bId $ \case ("", _, SENT 7) -> True; _ -> False
    withUP b aId $ \case ("", _, MsgErr 4 (MsgSkipped 3 5) "4") -> True; _ -> False
    ackMessage b aId 4 Nothing

withUP :: AgentClient -> ConnId -> (AEntityTransmission 'AEConn -> Bool) -> ExceptT AgentErrorType IO ()
withUP a bId p =
  liftIO $
    getInAnyOrder
      a
      [ \case ("", "", APC SAENone (UP _ [c])) -> c == bId; _ -> False,
        \case (corrId, c, APC SAEConn cmd) -> c == bId && p (corrId, c, cmd); _ -> False
      ]

testExpireMessageQuota :: HasCallStack => ATransport -> IO ()
testExpireMessageQuota t = withSmpServerConfigOn t cfg {msgQueueQuota = 1} testPort $ \_ -> do
  a <- getSMPAgentClient' 1 agentCfg {quotaExceededTimeout = 1, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- runRight $ do
    (aId, bId) <- makeConnection a b
    liftIO $ threadDelay 500000
    disconnectAgentClient b
    4 <- sendMessage a bId SMP.noMsgFlags "1"
    get a ##> ("", bId, SENT 4)
    5 <- sendMessage a bId SMP.noMsgFlags "2"
    liftIO $ threadDelay 1000000
    6 <- sendMessage a bId SMP.noMsgFlags "3" -- this won't expire
    get a =##> \case ("", c, MERR 5 (SMP QUOTA)) -> bId == c; _ -> False
    pure (aId, bId)
  b' <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
  runRight_ $ do
    subscribeConnection b' aId
    get b' =##> \case ("", c, Msg "1") -> c == aId; _ -> False
    ackMessage b' aId 4 Nothing
    get a ##> ("", bId, SENT 6)
    get b' =##> \case ("", c, MsgErr 6 (MsgSkipped 4 4) "3") -> c == aId; _ -> False
    ackMessage b' aId 6 Nothing

testExpireManyMessagesQuota :: HasCallStack => ATransport -> IO ()
testExpireManyMessagesQuota t = withSmpServerConfigOn t cfg {msgQueueQuota = 1} testPort $ \_ -> do
  a <- getSMPAgentClient' 1 agentCfg {quotaExceededTimeout = 1, messageRetryInterval = fastMessageRetryInterval} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aId, bId) <- runRight $ do
    (aId, bId) <- makeConnection a b
    liftIO $ threadDelay 500000
    disconnectAgentClient b
    4 <- sendMessage a bId SMP.noMsgFlags "1"
    get a ##> ("", bId, SENT 4)
    5 <- sendMessage a bId SMP.noMsgFlags "2"
    6 <- sendMessage a bId SMP.noMsgFlags "3"
    7 <- sendMessage a bId SMP.noMsgFlags "4"
    liftIO $ threadDelay 1000000
    8 <- sendMessage a bId SMP.noMsgFlags "5" -- this won't expire
    get a =##> \case ("", c, MERR 5 (SMP QUOTA)) -> bId == c; _ -> False
    get a =##> \case ("", c, MERRS [6, 7] (SMP QUOTA)) -> bId == c; _ -> False
    pure (aId, bId)
  b' <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
  runRight_ $ do
    subscribeConnection b' aId
    get b' =##> \case ("", c, Msg "1") -> c == aId; _ -> False
    ackMessage b' aId 4 Nothing
    get a ##> ("", bId, SENT 8)
    get b' =##> \case ("", c, MsgErr 6 (MsgSkipped 4 6) "5") -> c == aId; _ -> False
    ackMessage b' aId 6 Nothing

testRatchetSync :: HasCallStack => ATransport -> IO ()
testRatchetSync t = withAgentClients2 $ \alice bob ->
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aliceId, bobId, bob2) <- setupDesynchronizedRatchet alice bob
    runRight $ do
      ConnectionStats {ratchetSyncState} <- synchronizeRatchet bob2 aliceId False
      liftIO $ ratchetSyncState `shouldBe` RSStarted
      get alice =##> ratchetSyncP bobId RSAgreed
      get bob2 =##> ratchetSyncP aliceId RSAgreed
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 12 bob2 aliceId 9

setupDesynchronizedRatchet :: HasCallStack => AgentClient -> AgentClient -> IO (ConnId, ConnId, AgentClient)
setupDesynchronizedRatchet alice bob = do
  (aliceId, bobId) <- runRight $ makeConnection alice bob
  runRight_ $ do
    4 <- sendMessage alice bobId SMP.noMsgFlags "hello"
    get alice ##> ("", bobId, SENT 4)
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    ackMessage bob aliceId 4 Nothing

    5 <- sendMessage bob aliceId SMP.noMsgFlags "hello 2"
    get bob ##> ("", aliceId, SENT 5)
    get alice =##> \case ("", c, Msg "hello 2") -> c == bobId; _ -> False
    ackMessage alice bobId 5 Nothing

    liftIO $ copyFile testDB2 (testDB2 <> ".bak")

    6 <- sendMessage alice bobId SMP.noMsgFlags "hello 3"
    get alice ##> ("", bobId, SENT 6)
    get bob =##> \case ("", c, Msg "hello 3") -> c == aliceId; _ -> False
    ackMessage bob aliceId 6 Nothing

    7 <- sendMessage bob aliceId SMP.noMsgFlags "hello 4"
    get bob ##> ("", aliceId, SENT 7)
    get alice =##> \case ("", c, Msg "hello 4") -> c == bobId; _ -> False
    ackMessage alice bobId 7 Nothing

  disconnectAgentClient bob

  -- importing database backup after progressing ratchet de-synchronizes ratchet
  liftIO $ renameFile (testDB2 <> ".bak") testDB2

  bob2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2

  runRight_ $ do
    subscribeConnection bob2 aliceId

    Left Agent.CMD {cmdErr = PROHIBITED} <- runExceptT $ synchronizeRatchet bob2 aliceId False

    8 <- sendMessage alice bobId SMP.noMsgFlags "hello 5"
    get alice ##> ("", bobId, SENT 8)
    get bob2 =##> ratchetSyncP aliceId RSRequired

    Left Agent.CMD {cmdErr = PROHIBITED} <- runExceptT $ sendMessage bob2 aliceId SMP.noMsgFlags "hello 6"
    pure ()

  pure (aliceId, bobId, bob2)

ratchetSyncP :: ConnId -> RatchetSyncState -> AEntityTransmission 'AEConn -> Bool
ratchetSyncP cId rss = \case
  (_, cId', RSYNC rss' _ ConnectionStats {ratchetSyncState}) ->
    cId' == cId && rss' == rss && ratchetSyncState == rss
  _ -> False

ratchetSyncP' :: ConnId -> RatchetSyncState -> ATransmission 'Agent -> Bool
ratchetSyncP' cId rss = \case
  (_, cId', APC SAEConn (RSYNC rss' _ ConnectionStats {ratchetSyncState})) ->
    cId' == cId && rss' == rss && ratchetSyncState == rss
  _ -> False

testRatchetSyncServerOffline :: HasCallStack => ATransport -> IO ()
testRatchetSyncServerOffline t = withAgentClients2 $ \alice bob -> do
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId False
  liftIO $ ratchetSyncState `shouldBe` RSStarted

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      liftIO . getInAnyOrder alice $
        [ ratchetSyncP' bobId RSAgreed,
          serverUpP
        ]
      liftIO . getInAnyOrder bob2 $
        [ ratchetSyncP' aliceId RSAgreed,
          serverUpP
        ]
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 12 bob2 aliceId 9

serverUpP :: ATransmission 'Agent -> Bool
serverUpP = \case
  ("", "", APC SAENone (UP _ _)) -> True
  _ -> False

testRatchetSyncClientRestart :: HasCallStack => ATransport -> IO ()
testRatchetSyncClientRestart t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob
  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2
  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId False
  liftIO $ ratchetSyncState `shouldBe` RSStarted
  disconnectAgentClient bob2
  bob3 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      ("", "", UP _ _) <- nGet alice
      subscribeConnection bob3 aliceId
      get alice =##> ratchetSyncP bobId RSAgreed
      get bob3 =##> ratchetSyncP aliceId RSAgreed
      get alice =##> ratchetSyncP bobId RSOk
      get bob3 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 12 bob3 aliceId 9
  disconnectAgentClient alice
  disconnectAgentClient bob
  disconnectAgentClient bob3

testRatchetSyncSuspendForeground :: HasCallStack => ATransport -> IO ()
testRatchetSyncSuspendForeground t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState} <- runRight $ synchronizeRatchet bob2 aliceId False
  liftIO $ ratchetSyncState `shouldBe` RSStarted

  suspendAgent bob2 0
  threadDelay 100000
  foregroundAgent bob2

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      liftIO . getInAnyOrder alice $
        [ ratchetSyncP' bobId RSAgreed,
          serverUpP
        ]
      liftIO . getInAnyOrder bob2 $
        [ ratchetSyncP' aliceId RSAgreed,
          serverUpP
        ]
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 12 bob2 aliceId 9
  disconnectAgentClient alice
  disconnectAgentClient bob
  disconnectAgentClient bob2

testRatchetSyncSimultaneous :: HasCallStack => ATransport -> IO ()
testRatchetSyncSimultaneous t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bob <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  (aliceId, bobId, bob2) <- withSmpServerStoreMsgLogOn t testPort $ \_ ->
    setupDesynchronizedRatchet alice bob

  ("", "", DOWN _ _) <- nGet alice
  ("", "", DOWN _ _) <- nGet bob2

  ConnectionStats {ratchetSyncState = bRSS} <- runRight $ synchronizeRatchet bob2 aliceId False
  liftIO $ bRSS `shouldBe` RSStarted

  ConnectionStats {ratchetSyncState = aRSS} <- runRight $ synchronizeRatchet alice bobId True
  liftIO $ aRSS `shouldBe` RSStarted

  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    runRight_ $ do
      liftIO . getInAnyOrder alice $
        [ ratchetSyncP' bobId RSAgreed,
          serverUpP
        ]
      liftIO . getInAnyOrder bob2 $
        [ ratchetSyncP' aliceId RSAgreed,
          serverUpP
        ]
      get alice =##> ratchetSyncP bobId RSOk
      get bob2 =##> ratchetSyncP aliceId RSOk
      exchangeGreetingsMsgIds alice bobId 12 bob2 aliceId 9
  disconnectAgentClient alice
  disconnectAgentClient bob
  disconnectAgentClient bob2

testOnlyCreatePull :: IO ()
testOnlyCreatePull = withAgentClients2 $ \alice bob -> runRight_ $ do
  (bobId, qInfo) <- createConnection alice 1 True SCMInvitation Nothing SMOnlyCreate
  aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMOnlyCreate
  Just ("", _, CONF confId _ "bob's connInfo") <- getMsg alice bobId $ timeout 5_000000 $ get alice
  allowConnection alice bobId confId "alice's connInfo"
  liftIO $ threadDelay 1_000000
  getMsg bob aliceId $
    get bob ##> ("", aliceId, INFO "alice's connInfo")
  liftIO $ threadDelay 1_000000
  getMsg alice bobId $ pure ()
  get alice ##> ("", bobId, CON)
  getMsg bob aliceId $
    get bob ##> ("", aliceId, CON)
  -- exchange messages
  4 <- sendMessage alice bobId SMP.noMsgFlags "hello"
  get alice ##> ("", bobId, SENT 4)
  getMsg bob aliceId $
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  ackMessage bob aliceId 4 Nothing
  5 <- sendMessage bob aliceId SMP.noMsgFlags "hello too"
  get bob ##> ("", aliceId, SENT 5)
  getMsg alice bobId $
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
  ackMessage alice bobId 5 Nothing
  where
    getMsg :: AgentClient -> ConnId -> ExceptT AgentErrorType IO a -> ExceptT AgentErrorType IO a
    getMsg c cId action = do
      liftIO $ noMessages c "nothing should be delivered before GET"
      Just _ <- getConnectionMessage c cId
      r <- action
      get c =##> \case ("", cId', MSGNTF _) -> cId == cId'; _ -> False
      pure r

makeConnection :: AgentClient -> AgentClient -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnection alice bob = makeConnectionForUsers alice 1 bob 1

makeConnectionForUsers :: AgentClient -> UserId -> AgentClient -> UserId -> ExceptT AgentErrorType IO (ConnId, ConnId)
makeConnectionForUsers alice aliceUserId bob bobUserId = do
  (bobId, qInfo) <- createConnection alice aliceUserId True SCMInvitation Nothing SMSubscribe
  aliceId <- joinConnection bob bobUserId True qInfo "bob's connInfo" SMSubscribe
  ("", _, CONF confId _ "bob's connInfo") <- get alice
  allowConnection alice bobId confId "alice's connInfo"
  get alice ##> ("", bobId, CON)
  get bob ##> ("", aliceId, INFO "alice's connInfo")
  get bob ##> ("", aliceId, CON)
  pure (aliceId, bobId)

testInactiveNoSubs :: ATransport -> IO ()
testInactiveNoSubs t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ -> do
    alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    runRight_ . void $ createConnection alice 1 True SCMInvitation Nothing SMOnlyCreate -- do not subscribe to pass noSubscriptions check
    Just (_, _, APC SAENone (CONNECT _ _)) <- timeout 2000000 $ atomically (readTBQueue $ subQ alice)
    Just (_, _, APC SAENone (DISCONNECT _ _)) <- timeout 5000000 $ atomically (readTBQueue $ subQ alice)
    disconnectAgentClient alice

testInactiveWithSubs :: ATransport -> IO ()
testInactiveWithSubs t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ -> do
    alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    runRight_ . void $ createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    Nothing <- 800000 `timeout` get alice
    liftIO $ threadDelay 1200000
    -- and after 2 sec of inactivity no DOWN is sent as we have a live subscription
    liftIO $ timeout 1200000 (get alice) `shouldReturn` Nothing
    disconnectAgentClient alice

testActiveClientNotDisconnected :: ATransport -> IO ()
testActiveClientNotDisconnected t = do
  let cfg' = cfg {inactiveClientExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}}
  withSmpServerConfigOn t cfg' testPort $ \_ -> do
    alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    ts <- getSystemTime
    runRight_ $ do
      (connId, _cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
      keepSubscribing alice connId ts
    disconnectAgentClient alice
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
    4 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 4)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 4 Nothing
    suspendAgent b 1000000
    get' b ##> ("", "", SUSPENDED)
    5 <- sendMessage a bId SMP.noMsgFlags "hello 2"
    get a ##> ("", bId, SENT 5)
    Nothing <- 100000 `timeout` get b
    foregroundAgent b
    get b =##> \case ("", c, Msg "hello 2") -> c == aId; _ -> False

testSuspendingAgentCompleteSending :: ATransport -> IO ()
testSuspendingAgentCompleteSending t = withAgentClients2 $ \a b -> do
  (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    (aId, bId) <- makeConnection a b
    4 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 4)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 4 Nothing
    pure (aId, bId)

  runRight_ $ do
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet b
    5 <- sendMessage b aId SMP.noMsgFlags "hello too"
    6 <- sendMessage b aId SMP.noMsgFlags "how are you?"
    liftIO $ threadDelay 100000
    suspendAgent b 5000000

  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ @AgentErrorType $ do
    pGet b =##> \case ("", c, APC _ (SENT 5)) -> c == aId; ("", "", APC _ UP {}) -> True; _ -> False
    pGet b =##> \case ("", c, APC _ (SENT 5)) -> c == aId; ("", "", APC _ UP {}) -> True; _ -> False
    pGet b =##> \case ("", c, APC _ (SENT 6)) -> c == aId; ("", "", APC _ UP {}) -> True; _ -> False
    ("", "", SUSPENDED) <- nGet b

    pGet a =##> \case ("", c, APC _ (Msg "hello too")) -> c == bId; ("", "", APC _ UP {}) -> True; _ -> False
    pGet a =##> \case ("", c, APC _ (Msg "hello too")) -> c == bId; ("", "", APC _ UP {}) -> True; _ -> False
    ackMessage a bId 5 Nothing
    get a =##> \case ("", c, Msg "how are you?") -> c == bId; _ -> False
    ackMessage a bId 6 Nothing

testSuspendingAgentTimeout :: ATransport -> IO ()
testSuspendingAgentTimeout t = withAgentClients2 $ \a b -> do
  (aId, _) <- withSmpServer t . runRight $ do
    (aId, bId) <- makeConnection a b
    4 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 4)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 4 Nothing
    pure (aId, bId)

  runRight_ $ do
    ("", "", DOWN {}) <- nGet a
    ("", "", DOWN {}) <- nGet b
    5 <- sendMessage b aId SMP.noMsgFlags "hello too"
    6 <- sendMessage b aId SMP.noMsgFlags "how are you?"
    suspendAgent b 100000
    ("", "", SUSPENDED) <- nGet b
    pure ()

testBatchedSubscriptions :: Int -> Int -> ATransport -> IO ()
testBatchedSubscriptions nCreate nDel t = do
  a <- getSMPAgentClient' 1 agentCfg initAgentServers2 testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServers2 testDB2
  conns <- runServers $ do
    conns <- replicateM (nCreate :: Int) $ makeConnection a b
    forM_ conns $ \(aId, bId) -> exchangeGreetings a bId b aId
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
    forM_ conns' $ \(aId, bId) -> exchangeGreetingsMsgId 6 a bId b aId
    void $ resubscribeConnections a bIds
    void $ resubscribeConnections b aIds
    forM_ conns' $ \(aId, bId) -> exchangeGreetingsMsgId 8 a bId b aId
    delete a bIds'
    delete b aIds'
    deleteFail a bIds'
    deleteFail b aIds'
  disconnectAgentClient a
  disconnectAgentClient b
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
        res <- withSmpServerConfigOn t cfg {storeLogFile = Just testStoreLogFile2} testPort2 $ \t2 ->
          runRight a `finally` killThread t2
        killThread t1
        pure res

testAsyncCommands :: IO ()
testAsyncCommands =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    bobId <- createConnectionAsync alice 1 "1" True SCMInvitation SMSubscribe
    ("1", bobId', INV (ACR _ qInfo)) <- get alice
    liftIO $ bobId' `shouldBe` bobId
    aliceId <- joinConnectionAsync bob 1 "2" True qInfo "bob's connInfo" SMSubscribe
    ("2", aliceId', OK) <- get bob
    liftIO $ aliceId' `shouldBe` aliceId
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
    deleteConnectionAsync alice bobId
    get alice =##> \case ("", c, DEL_RCVQ _ _ Nothing) -> c == bobId; _ -> False
    get alice =##> \case ("", c, DEL_CONN) -> c == bobId; _ -> False
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    baseId = 3
    msgId = subtract baseId

testAsyncCommandsRestore :: ATransport -> IO ()
testAsyncCommandsRestore t = do
  alice <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  bobId <- runRight $ createConnectionAsync alice 1 "1" True SCMInvitation SMSubscribe
  liftIO $ noMessages alice "alice doesn't receive INV because server is down"
  disconnectAgentClient alice
  alice' <- liftIO $ getSMPAgentClient' 2 agentCfg initAgentServers testDB
  withSmpServerStoreLogOn t testPort $ \_ -> do
    runRight_ $ do
      subscribeConnection alice' bobId
      get alice' =##> \case ("1", _, INV _) -> True; _ -> False
      pure ()
  disconnectAgentClient alice'

testAcceptContactAsync :: IO ()
testAcceptContactAsync =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (_, qInfo) <- createConnection alice 1 True SCMContact Nothing SMSubscribe
    aliceId <- joinConnection bob 1 True qInfo "bob's connInfo" SMSubscribe
    ("", _, REQ invId _ "bob's connInfo") <- get alice
    bobId <- acceptContactAsync alice "1" True invId "alice's connInfo" SMSubscribe
    get alice =##> \case ("1", c, OK) -> c == bobId; _ -> False
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
    get bob ##> ("", aliceId, MERR (baseId + 5) (SMP AUTH))
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  where
    baseId = 3
    msgId = subtract baseId

testDeleteConnectionAsync :: ATransport -> IO ()
testDeleteConnectionAsync t = do
  a <- getSMPAgentClient' 1 agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3} initAgentServers testDB
  connIds <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    (bId1, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
    (bId2, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
    (bId3, _inv) <- createConnection a 1 True SCMInvitation Nothing SMSubscribe
    pure ([bId1, bId2, bId3] :: [ConnId])
  runRight_ $ do
    deleteConnectionsAsync a connIds
    get a =##> \case ("", c, DEL_RCVQ _ _ (Just (BROKER _ e))) -> c `elem` connIds && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", c, DEL_RCVQ _ _ (Just (BROKER _ e))) -> c `elem` connIds && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", c, DEL_RCVQ _ _ (Just (BROKER _ e))) -> c `elem` connIds && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c `elem` connIds; _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c `elem` connIds; _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c `elem` connIds; _ -> False
    liftIO $ noMessages a "nothing else should be delivered to alice"
  disconnectAgentClient a

testJoinConnectionAsyncReplyError :: HasCallStack => ATransport -> IO ()
testJoinConnectionAsyncReplyError t = do
  let initAgentServersSrv2 = initAgentServers {smp = userServers [noAuthSrv testSMPServer2]}
  a <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg initAgentServersSrv2 testDB2
  (aId, bId) <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    bId <- createConnectionAsync a 1 "1" True SCMInvitation SMSubscribe
    ("1", bId', INV (ACR _ qInfo)) <- get a
    liftIO $ bId' `shouldBe` bId
    aId <- joinConnectionAsync b 1 "2" True qInfo "bob's connInfo" SMSubscribe
    liftIO $ threadDelay 500000
    ConnectionStats {rcvQueuesInfo = [], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
    pure (aId, bId)
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
  withSmpServerOn t testPort2 $ do
    get b =##> \case ("2", c, OK) -> c == aId; _ -> False
    confId <- withSmpServerStoreLogOn t testPort $ \_ -> do
      pGet a >>= \case
        ("", "", APC _ (UP _ [_])) -> do
          ("", _, CONF confId _ "bob's connInfo") <- get a
          pure confId
        ("", _, APC _ (CONF confId _ "bob's connInfo")) -> do
          ("", "", UP _ [_]) <- nGet a
          pure confId
        r -> error $ "unexpected response " <> show r
    nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId; _ -> False
    runRight_ $ do
      allowConnectionAsync a "3" bId confId "alice's connInfo"
      liftIO $ threadDelay 500000
      ConnectionStats {rcvQueuesInfo = [RcvQueueInfo {}], sndQueuesInfo = [SndQueueInfo {}]} <- getConnectionServers b aId
      pure ()
    withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
      pGet a =##> \case ("3", c, APC _ OK) -> c == bId; ("", "", APC _ (UP _ [c])) -> c == bId; _ -> False
      pGet a =##> \case ("3", c, APC _ OK) -> c == bId; ("", "", APC _ (UP _ [c])) -> c == bId; _ -> False
      get a ##> ("", bId, CON)
      get b ##> ("", aId, INFO "alice's connInfo")
      get b ##> ("", aId, CON)
      exchangeGreetings a bId b aId
  disconnectAgentClient a
  disconnectAgentClient b

testUsers :: IO ()
testUsers =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    auId <- createUser a [noAuthSrv testSMPServer] [noAuthSrv testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetingsMsgId 4 a bId' b aId'
    deleteUser a auId True
    get a =##> \case ("", c, DEL_RCVQ _ _ Nothing) -> c == bId'; _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c == bId'; _ -> False
    nGet a =##> \case ("", "", DEL_USER u) -> u == auId; _ -> False
    exchangeGreetingsMsgId 6 a bId b aId
    liftIO $ noMessages a "nothing else should be delivered to alice"

testDeleteUserQuietly :: IO ()
testDeleteUserQuietly =
  withAgentClients2 $ \a b -> runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    auId <- createUser a [noAuthSrv testSMPServer] [noAuthSrv testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetingsMsgId 4 a bId' b aId'
    deleteUser a auId False
    exchangeGreetingsMsgId 6 a bId b aId
    liftIO $ noMessages a "nothing else should be delivered to alice"

testUsersNoServer :: HasCallStack => ATransport -> IO ()
testUsersNoServer t = withAgentClientsCfg2 aCfg agentCfg $ \a b -> do
  (aId, bId, auId, _aId', bId') <- withSmpServerStoreLogOn t testPort $ \_ -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    auId <- createUser a [noAuthSrv testSMPServer] [noAuthSrv testXFTPServer]
    (aId', bId') <- makeConnectionForUsers a auId b 1
    exchangeGreetingsMsgId 4 a bId' b aId'
    pure (aId, bId, auId, aId', bId')
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId || c == bId'; _ -> False
  nGet a =##> \case ("", "", DOWN _ [c]) -> c == bId || c == bId'; _ -> False
  nGet b =##> \case ("", "", DOWN _ cs) -> length cs == 2; _ -> False
  runRight_ $ do
    deleteUser a auId True
    get a =##> \case ("", c, DEL_RCVQ _ _ (Just (BROKER _ e))) -> c == bId' && (e == TIMEOUT || e == NETWORK); _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c == bId'; _ -> False
    nGet a =##> \case ("", "", DEL_USER u) -> u == auId; _ -> False
    liftIO $ noMessages a "nothing else should be delivered to alice"
  withSmpServerStoreLogOn t testPort $ \_ -> runRight_ $ do
    nGet a =##> \case ("", "", UP _ [c]) -> c == bId; _ -> False
    nGet b =##> \case ("", "", UP _ cs) -> length cs == 2; _ -> False
    exchangeGreetingsMsgId 6 a bId b aId
  where
    aCfg = agentCfg {initialCleanupDelay = 10000, cleanupInterval = 10000, deleteErrorCount = 3}

testSwitchConnection :: InitialAgentServers -> IO ()
testSwitchConnection servers = do
  a <- getSMPAgentClient' 1 agentCfg servers testDB
  b <- getSMPAgentClient' 2 agentCfg servers testDB2
  runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    testFullSwitch a bId b aId 10
    testFullSwitch a bId b aId 16
  disconnectAgentClient a
  disconnectAgentClient b

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
    exchangeGreetingsMsgId 4 a bId b aId
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
    exchangeGreetingsMsgId 10 a bId b aId
    testFullSwitch a bId b aId 16
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

withAgent :: Int -> AgentConfig -> InitialAgentServers -> FilePath -> (AgentClient -> IO a) -> IO a
withAgent clientId cfg' servers dbPath = bracket (getSMPAgentClient' clientId cfg' servers dbPath) disconnectAgentClient

sessionSubscribe :: (forall a. (AgentClient -> IO a) -> IO a) -> [ConnId] -> (AgentClient -> ExceptT AgentErrorType IO ()) -> IO ()
sessionSubscribe withC connIds a =
  withC $ \c -> runRight_ $ do
    void $ subscribeConnections c connIds
    r <- a c
    liftIO $ threadDelay 500000
    liftIO $ noMessages c "nothing else should be delivered"
    pure r

testSwitchDelete :: InitialAgentServers -> IO ()
testSwitchDelete servers = do
  a <- getSMPAgentClient' 1 agentCfg servers testDB
  b <- getSMPAgentClient' 2 agentCfg servers testDB2
  runRight_ $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    disconnectAgentClient b
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
    deleteConnectionAsync a bId
    get a =##> \case ("", c, DEL_RCVQ _ _ Nothing) -> c == bId; _ -> False
    get a =##> \case ("", c, DEL_RCVQ _ _ Nothing) -> c == bId; _ -> False
    get a =##> \case ("", c, DEL_CONN) -> c == bId; _ -> False
    liftIO $ noMessages a "nothing else should be delivered to alice"
  disconnectAgentClient a
  disconnectAgentClient b

testAbortSwitchStarted :: HasCallStack => InitialAgentServers -> IO ()
testAbortSwitchStarted servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
    pure (aId, bId)
  let withA' = sessionSubscribe withA [bId]
      withB' = sessionSubscribe withB [aId]
  withA' $ \a -> do
    stats <- switchConnectionAsync a "" bId
    liftIO $ rcvSwchStatuses' stats `shouldMatchList` [Just RSSwitchStarted]
    phaseRcv a bId SPStarted [Just RSSendingQADD, Nothing]
    -- repeat switch is prohibited
    Left Agent.CMD {cmdErr = PROHIBITED} <- runExceptT $ switchConnectionAsync a "" bId
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

    exchangeGreetingsMsgId 12 a bId b aId

    testFullSwitch a bId b aId 18
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testAbortSwitchStartedReinitiate :: HasCallStack => InitialAgentServers -> IO ()
testAbortSwitchStartedReinitiate servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
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

    exchangeGreetingsMsgId 12 a bId b aId

    testFullSwitch a bId b aId 18
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

switchPhaseRcvP :: ConnId -> SwitchPhase -> [Maybe RcvSwitchStatus] -> ATransmission 'Agent -> Bool
switchPhaseRcvP cId sphase swchStatuses = switchPhaseP cId QDRcv sphase (\stats -> rcvSwchStatuses' stats == swchStatuses)

switchPhaseSndP :: ConnId -> SwitchPhase -> [Maybe SndSwitchStatus] -> ATransmission 'Agent -> Bool
switchPhaseSndP cId sphase swchStatuses = switchPhaseP cId QDSnd sphase (\stats -> sndSwchStatuses' stats == swchStatuses)

switchPhaseP :: ConnId -> QueueDirection -> SwitchPhase -> (ConnectionStats -> Bool) -> ATransmission 'Agent -> Bool
switchPhaseP cId qd sphase statsP = \case
  (_, cId', APC SAEConn (SWITCH qd' sphase' stats)) -> cId' == cId && qd' == qd && sphase' == sphase && statsP stats
  _ -> False

errQueueNotFoundP :: ConnId -> ATransmission 'Agent -> Bool
errQueueNotFoundP cId = \case
  (_, cId', APC SAEConn (ERR AGENT {agentErr = A_QUEUE {queueErr = "QKEY: queue address not found in connection"}})) -> cId' == cId
  _ -> False

testCannotAbortSwitchSecured :: HasCallStack => InitialAgentServers -> IO ()
testCannotAbortSwitchSecured servers = do
  (aId, bId) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId, bId) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId b aId
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
    Left Agent.CMD {cmdErr = PROHIBITED} <- runExceptT $ abortConnectionSwitch a bId
    pure ()
  withA $ \a -> withB $ \b -> runRight_ $ do
    subscribeConnection a bId
    subscribeConnection b aId

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

testSwitch2Connections :: HasCallStack => InitialAgentServers -> IO ()
testSwitch2Connections servers = do
  (aId1, bId1, aId2, bId2) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId1, bId1) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId1 b aId1
    (aId2, bId2) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId2 b aId2
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

    exchangeGreetingsMsgId 10 a bId1 b aId1
    exchangeGreetingsMsgId 10 a bId2 b aId2

    testFullSwitch a bId1 b aId1 16
    testFullSwitch a bId2 b aId2 16
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testSwitch2ConnectionsAbort1 :: HasCallStack => InitialAgentServers -> IO ()
testSwitch2ConnectionsAbort1 servers = do
  (aId1, bId1, aId2, bId2) <- withA $ \a -> withB $ \b -> runRight $ do
    (aId1, bId1) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId1 b aId1
    (aId2, bId2) <- makeConnection a b
    exchangeGreetingsMsgId 4 a bId2 b aId2
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

    exchangeGreetingsMsgId 10 a bId1 b aId1
    exchangeGreetingsMsgId 8 a bId2 b aId2

    testFullSwitch a bId1 b aId1 16
    testFullSwitch a bId2 b aId2 14
  where
    withA :: (AgentClient -> IO a) -> IO a
    withA = withAgent 1 agentCfg servers testDB
    withB :: (AgentClient -> IO a) -> IO a
    withB = withAgent 2 agentCfg servers testDB2

testCreateQueueAuth :: HasCallStack => (Maybe BasicAuth, Version) -> (Maybe BasicAuth, Version) -> IO Int
testCreateQueueAuth clnt1 clnt2 = do
  a <- getClient 1 clnt1 testDB
  b <- getClient 2 clnt2 testDB2
  r <- runRight $ do
    tryError (createConnection a 1 True SCMInvitation Nothing SMSubscribe) >>= \case
      Left (SMP AUTH) -> pure 0
      Left e -> throwError e
      Right (bId, qInfo) ->
        tryError (joinConnection b 1 True qInfo "bob's connInfo" SMSubscribe) >>= \case
          Left (SMP AUTH) -> pure 1
          Left e -> throwError e
          Right aId -> do
            ("", _, CONF confId _ "bob's connInfo") <- get a
            allowConnection a bId confId "alice's connInfo"
            get a ##> ("", bId, CON)
            get b ##> ("", aId, INFO "alice's connInfo")
            get b ##> ("", aId, CON)
            exchangeGreetings a bId b aId
            pure 2
  disconnectAgentClient a
  disconnectAgentClient b
  pure r
  where
    getClient clientId (clntAuth, clntVersion) db =
      let servers = initAgentServers {smp = userServers [ProtoServerWithAuth testSMPServer clntAuth]}
          smpCfg = (defaultSMPClientConfig :: ProtocolClientConfig) {serverVRange = mkVersionRange 4 clntVersion}
       in getSMPAgentClient' clientId agentCfg {smpCfg} servers db

testSMPServerConnectionTest :: ATransport -> Maybe BasicAuth -> SMPServerWithAuth -> IO (Maybe ProtocolTestFailure)
testSMPServerConnectionTest t newQueueBasicAuth srv =
  withSmpServerConfigOn t cfg {newQueueBasicAuth} testPort2 $ \_ -> do
    a <- getSMPAgentClient' 1 agentCfg initAgentServers testDB -- initially passed server is not running
    runRight $ testProtocolServer a 1 srv

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
    4 <- sendMessage a bId SMP.noMsgFlags "hello"
    get a ##> ("", bId, SENT 4)
    get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
    ackMessage b aId 4 $ Just ""
    get a =##> \case ("", c, Rcvd 4) -> c == bId; _ -> False
    ackMessage a bId 5 Nothing
    -- b sends, a receives and sends delivery receipt
    6 <- sendMessage b aId SMP.noMsgFlags "hello too"
    get b ##> ("", aId, SENT 6)
    get a =##> \case ("", c, Msg "hello too") -> c == bId; _ -> False
    ackMessage a bId 6 $ Just ""
    get b =##> \case ("", c, Rcvd 6) -> c == aId; _ -> False
    ackMessage b aId 7 (Just "") `catchError` \e -> liftIO $ e `shouldBe` Agent.CMD PROHIBITED
    ackMessage b aId 7 Nothing

testDeliveryReceiptsVersion :: HasCallStack => ATransport -> IO ()
testDeliveryReceiptsVersion t = do
  a <- getSMPAgentClient' 1 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB
  b <- getSMPAgentClient' 2 agentCfg {smpAgentVRange = mkVersionRange 1 3} initAgentServers testDB2
  withSmpServerStoreMsgLogOn t testPort $ \_ -> do
    (aId, bId) <- runRight $ do
      (aId, bId) <- makeConnection a b
      checkVersion a bId 3
      checkVersion b aId 3
      4 <- sendMessage a bId SMP.noMsgFlags "hello"
      get a ##> ("", bId, SENT 4)
      get b =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
      ackMessage b aId 4 $ Just ""
      liftIO $ noMessages a "no delivery receipt (unsupported version)"
      5 <- sendMessage b aId SMP.noMsgFlags "hello too"
      get b ##> ("", aId, SENT 5)
      get a =##> \case ("", c, Msg "hello too") -> c == bId; _ -> False
      ackMessage a bId 5 $ Just ""
      liftIO $ noMessages b "no delivery receipt (unsupported version)"
      pure (aId, bId)

    disconnectAgentClient a
    disconnectAgentClient b
    a' <- getSMPAgentClient' 3 agentCfg {smpAgentVRange = mkVersionRange 1 4} initAgentServers testDB
    b' <- getSMPAgentClient' 4 agentCfg {smpAgentVRange = mkVersionRange 1 4} initAgentServers testDB2

    runRight_ $ do
      subscribeConnection a' bId
      subscribeConnection b' aId
      exchangeGreetingsMsgId 6 a' bId b' aId
      checkVersion a' bId 4
      checkVersion b' aId 4
      8 <- sendMessage a' bId SMP.noMsgFlags "hello"
      get a' ##> ("", bId, SENT 8)
      get b' =##> \case ("", c, Msg "hello") -> c == aId; _ -> False
      ackMessage b' aId 8 $ Just ""
      get a' =##> \case ("", c, Rcvd 8) -> c == bId; _ -> False
      ackMessage a' bId 9 Nothing
      10 <- sendMessage b' aId SMP.noMsgFlags "hello too"
      get b' ##> ("", aId, SENT 10)
      get a' =##> \case ("", c, Msg "hello too") -> c == bId; _ -> False
      ackMessage a' bId 10 $ Just ""
      get b' =##> \case ("", c, Rcvd 10) -> c == aId; _ -> False
      ackMessage b' aId 11 Nothing
    disconnectAgentClient a'
    disconnectAgentClient b'

testDeliveryReceiptsConcurrent :: HasCallStack => ATransport -> IO ()
testDeliveryReceiptsConcurrent t =
  withSmpServerConfigOn t cfg {msgQueueQuota = 128} testPort $ \_ -> do
    withAgentClients2 $ \a b -> do
      (aId, bId) <- runRight $ makeConnection a b
      t1 <- liftIO getCurrentTime
      concurrently_ (runClient "a" a bId) (runClient "b" b aId)
      t2 <- liftIO getCurrentTime
      diffUTCTime t2 t1 `shouldSatisfy` (< 15)
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
            -- liftIO $ print $ cName <> ": sendMessage"
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
          1000000 `timeout` get client >>= \case
            Just r -> pure r
            _ -> error "timeout"

testTwoUsers :: HasCallStack => IO ()
testTwoUsers = withAgentClients2 $ \a b -> do
  let nc = netCfg initAgentServers
  sessionMode nc `shouldBe` TSMUser
  runRight_ $ do
    (aId1, bId1) <- makeConnectionForUsers a 1 b 1
    exchangeGreetings a bId1 b aId1
    (aId1', bId1') <- makeConnectionForUsers a 1 b 1
    exchangeGreetings a bId1' b aId1'
    a `hasClients` 1
    b `hasClients` 1
    setNetworkConfig a nc {sessionMode = TSMEntity}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 2

    exchangeGreetingsMsgId 6 a bId1 b aId1
    exchangeGreetingsMsgId 6 a bId1' b aId1'
    liftIO $ threadDelay 250000
    setNetworkConfig a nc {sessionMode = TSMUser}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 1

    aUserId2 <- createUser a [noAuthSrv testSMPServer] [noAuthSrv testXFTPServer]
    (aId2, bId2) <- makeConnectionForUsers a aUserId2 b 1
    exchangeGreetings a bId2 b aId2
    (aId2', bId2') <- makeConnectionForUsers a aUserId2 b 1
    exchangeGreetings a bId2' b aId2'
    a `hasClients` 2
    b `hasClients` 1
    setNetworkConfig a nc {sessionMode = TSMEntity}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 4
    exchangeGreetingsMsgId 8 a bId1 b aId1
    exchangeGreetingsMsgId 8 a bId1' b aId1'
    exchangeGreetingsMsgId 6 a bId2 b aId2
    exchangeGreetingsMsgId 6 a bId2' b aId2'
    liftIO $ threadDelay 250000
    setNetworkConfig a nc {sessionMode = TSMUser}
    liftIO $ threadDelay 250000
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", DOWN _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    ("", "", UP _ _) <- nGet a
    a `hasClients` 2
    exchangeGreetingsMsgId 10 a bId1 b aId1
    exchangeGreetingsMsgId 10 a bId1' b aId1'
    exchangeGreetingsMsgId 8 a bId2 b aId2
    exchangeGreetingsMsgId 8 a bId2' b aId2'
  where
    hasClients :: HasCallStack => AgentClient -> Int -> ExceptT AgentErrorType IO ()
    hasClients c n = liftIO $ M.size <$> readTVarIO (smpClients c) `shouldReturn` n

getSMPAgentClient' :: Int -> AgentConfig -> InitialAgentServers -> FilePath -> IO AgentClient
getSMPAgentClient' clientId cfg' initServers dbPath = do
  Right st <- liftIO $ createAgentStore dbPath "" False MCError
  c <- getSMPAgentClient_ clientId cfg' initServers st False
  when (dbNew st) $ withTransaction' st (`SQL.execute_` "INSERT INTO users (user_id) VALUES (1)")
  pure c

testServerMultipleIdentities :: HasCallStack => IO ()
testServerMultipleIdentities =
  withAgentClients2 $ \alice bob -> runRight_ $ do
    (bobId, cReq) <- createConnection alice 1 True SCMInvitation Nothing SMSubscribe
    aliceId <- joinConnection bob 1 True cReq "bob's connInfo" SMSubscribe
    ("", _, CONF confId _ "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob aliceId
    -- this saves queue with second server identity
    Left (BROKER _ NETWORK) <- runExceptT $ joinConnection bob 1 True secondIdentityCReq "bob's connInfo" SMSubscribe
    disconnectAgentClient bob
    bob' <- liftIO $ getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    subscribeConnection bob' aliceId
    exchangeGreetingsMsgId 6 alice bobId bob' aliceId
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

exchangeGreetings :: HasCallStack => AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetings = exchangeGreetingsMsgId 4

exchangeGreetingsMsgId :: HasCallStack => Int64 -> AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetingsMsgId msgId alice bobId bob aliceId = do
  msgId1 <- sendMessage alice bobId SMP.noMsgFlags "hello"
  liftIO $ msgId1 `shouldBe` msgId
  get alice ##> ("", bobId, SENT msgId)
  get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  ackMessage bob aliceId msgId Nothing
  msgId2 <- sendMessage bob aliceId SMP.noMsgFlags "hello too"
  let msgId' = msgId + 1
  liftIO $ msgId2 `shouldBe` msgId'
  get bob ##> ("", aliceId, SENT msgId')
  get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
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
