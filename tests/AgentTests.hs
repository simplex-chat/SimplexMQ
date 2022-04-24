{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PostfixOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}

module AgentTests (agentTests) where

import AgentTests.ConnectionRequestTests
import AgentTests.DoubleRatchetTests (doubleRatchetTests)
import AgentTests.FunctionalAPITests (functionalAPITests)
import AgentTests.SQLiteTests (storeTests)
import Control.Concurrent
import Control.Monad (forM_)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Network.HTTP.Types (urlEncode)
import SMPAgentClient
import SMPClient (testKeyHash, testPort, testPort2, testStoreLogFile, withSmpServer, withSmpServerStoreLogOn)
import Simplex.Messaging.Agent.Protocol
import qualified Simplex.Messaging.Agent.Protocol as A
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (ErrorType (..), MsgBody)
import Simplex.Messaging.Transport (ATransport (..), TProxy (..), Transport (..))
import Simplex.Messaging.Util (bshow)
import System.Directory (removeFile)
import System.Timeout
import Test.Hspec

agentTests :: ATransport -> Spec
agentTests (ATransport t) = do
  describe "Connection request" connectionRequestTests
  describe "Double ratchet tests" doubleRatchetTests
  describe "Functional API" $ functionalAPITests (ATransport t)
  describe "SQLite store" storeTests
  describe "SMP agent protocol syntax" $ syntaxTests t
  describe "Establishing duplex connection" $ do
    it "should connect via one server and one agent" $
      smpAgentTest2_1_1 $ testDuplexConnection t
    it "should connect via one server and one agent (random IDs)" $
      smpAgentTest2_1_1 $ testDuplexConnRandomIds t
    it "should connect via one server and 2 agents" $
      smpAgentTest2_2_1 $ testDuplexConnection t
    it "should connect via one server and 2 agents (random IDs)" $
      smpAgentTest2_2_1 $ testDuplexConnRandomIds t
    it "should connect via 2 servers and 2 agents" $
      smpAgentTest2_2_2 $ testDuplexConnection t
    it "should connect via 2 servers and 2 agents (random IDs)" $
      smpAgentTest2_2_2 $ testDuplexConnRandomIds t
  describe "Establishing connections via `contact connection`" $ do
    it "should connect via contact connection with one server and 3 agents" $
      smpAgentTest3 $ testContactConnection t
    it "should connect via contact connection with one server and 2 agents (random IDs)" $
      smpAgentTest2_2_1 $ testContactConnRandomIds t
    it "should support rejecting contact request" $
      smpAgentTest2_2_1 $ testRejectContactRequest t
  describe "Connection subscriptions" $ do
    it "should connect via one server and one agent" $
      smpAgentTest3_1_1 $ testSubscription t
    it "should send notifications to client when server disconnects" $
      smpAgentServerTest $ testSubscrNotification t
  describe "Message delivery and server reconnection" $ do
    it "should deliver messages after losing server connection and re-connecting" $
      smpAgentTest2_2_2_needs_server $ testMsgDeliveryServerRestart t
    it "should connect to the server when server goes up if it initially was down" $
      smpAgentTestN [] $ testServerConnectionAfterError t
    it "should deliver pending messages after agent restarting" $
      smpAgentTest1_1_1 $ testMsgDeliveryAgentRestart t
    it "should concurrently deliver messages to connections without blocking" $
      smpAgentTest2_2_1 $ testConcurrentMsgDelivery t
    it "should deliver messages if one of connections has quota exceeded" $
      smpAgentTest2_2_1 $ testMsgDeliveryQuotaExceeded t

-- | receive message to handle `h`
(<#:) :: Transport c => c -> IO (ATransmissionOrError 'Agent)
(<#:) = tGet SAgent

-- | send transmission `t` to handle `h` and get response
(#:) :: Transport c => c -> (ByteString, ByteString, ByteString) -> IO (ATransmissionOrError 'Agent)
h #: t = tPutRaw h t >> (<#:) h

-- | action and expected response
-- `h #:t #> r` is the test that sends `t` to `h` and validates that the response is `r`
(#>) :: IO (ATransmissionOrError 'Agent) -> ATransmission 'Agent -> Expectation
action #> (corrId, connId, cmd) = action `shouldReturn` (corrId, connId, Right cmd)

-- | action and predicate for the response
-- `h #:t =#> p` is the test that sends `t` to `h` and validates the response using `p`
(=#>) :: IO (ATransmissionOrError 'Agent) -> (ATransmission 'Agent -> Bool) -> Expectation
action =#> p = action >>= (`shouldSatisfy` p . correctTransmission)

correctTransmission :: ATransmissionOrError a -> ATransmission a
correctTransmission (corrId, connId, cmdOrErr) = case cmdOrErr of
  Right cmd -> (corrId, connId, cmd)
  Left e -> error $ show e

-- | receive message to handle `h` and validate that it is the expected one
(<#) :: Transport c => c -> ATransmission 'Agent -> Expectation
h <# (corrId, connId, cmd) = (h <#:) `shouldReturn` (corrId, connId, Right cmd)

-- | receive message to handle `h` and validate it using predicate `p`
(<#=) :: Transport c => c -> (ATransmission 'Agent -> Bool) -> Expectation
h <#= p = (h <#:) >>= (`shouldSatisfy` p . correctTransmission)

-- | test that nothing is delivered to handle `h` during 10ms
(#:#) :: Transport c => c -> String -> Expectation
h #:# err = tryGet `shouldReturn` ()
  where
    tryGet =
      10000 `timeout` tGet SAgent h >>= \case
        Just _ -> error err
        _ -> return ()

pattern Msg :: MsgBody -> ACommand 'Agent
pattern Msg msgBody <- MSG MsgMeta {integrity = MsgOk} msgBody

testDuplexConnection :: Transport c => TProxy c -> c -> c -> IO ()
testDuplexConnection _ alice bob = do
  ("1", "bob", Right (INV cReq)) <- alice #: ("1", "bob", "NEW INV")
  let cReq' = strEncode cReq
  bob #: ("11", "alice", "JOIN " <> cReq' <> " 14\nbob's connInfo") #> ("11", "alice", OK)
  ("", "bob", Right (CONF confId "bob's connInfo")) <- (alice <#:)
  alice #: ("2", "bob", "LET " <> confId <> " 16\nalice's connInfo") #> ("2", "bob", OK)
  bob <# ("", "alice", INFO "alice's connInfo")
  bob <# ("", "alice", CON)
  alice <# ("", "bob", CON)
  -- message IDs 1 to 3 get assigned to control messages, so first MSG is assigned ID 4
  alice #: ("3", "bob", "SEND :hello") #> ("3", "bob", MID 5)
  alice <# ("", "bob", SENT 5)
  bob <#= \case ("", "alice", Msg "hello") -> True; _ -> False
  bob #: ("12", "alice", "ACK 5") #> ("12", "alice", OK)
  alice #: ("4", "bob", "SEND :how are you?") #> ("4", "bob", MID 6)
  alice <# ("", "bob", SENT 6)
  bob <#= \case ("", "alice", Msg "how are you?") -> True; _ -> False
  bob #: ("13", "alice", "ACK 6") #> ("13", "alice", OK)
  bob #: ("14", "alice", "SEND 9\nhello too") #> ("14", "alice", MID 7)
  bob <# ("", "alice", SENT 7)
  alice <#= \case ("", "bob", Msg "hello too") -> True; _ -> False
  alice #: ("3a", "bob", "ACK 7") #> ("3a", "bob", OK)
  bob #: ("15", "alice", "SEND 9\nmessage 1") #> ("15", "alice", MID 8)
  bob <# ("", "alice", SENT 8)
  alice <#= \case ("", "bob", Msg "message 1") -> True; _ -> False
  alice #: ("4a", "bob", "ACK 8") #> ("4a", "bob", OK)
  alice #: ("5", "bob", "OFF") #> ("5", "bob", OK)
  bob #: ("17", "alice", "SEND 9\nmessage 3") #> ("17", "alice", MID 9)
  bob <# ("", "alice", MERR 9 (SMP AUTH))
  alice #: ("6", "bob", "DEL") #> ("6", "bob", OK)
  alice #:# "nothing else should be delivered to alice"

testDuplexConnRandomIds :: Transport c => TProxy c -> c -> c -> IO ()
testDuplexConnRandomIds _ alice bob = do
  ("1", bobConn, Right (INV cReq)) <- alice #: ("1", "", "NEW INV")
  let cReq' = strEncode cReq
  ("11", aliceConn, Right OK) <- bob #: ("11", "", "JOIN " <> cReq' <> " 14\nbob's connInfo")
  ("", bobConn', Right (CONF confId "bob's connInfo")) <- (alice <#:)
  bobConn' `shouldBe` bobConn
  alice #: ("2", bobConn, "LET " <> confId <> " 16\nalice's connInfo") =#> \case ("2", c, OK) -> c == bobConn; _ -> False
  bob <# ("", aliceConn, INFO "alice's connInfo")
  bob <# ("", aliceConn, CON)
  alice <# ("", bobConn, CON)
  alice #: ("2", bobConn, "SEND :hello") #> ("2", bobConn, MID 5)
  alice <# ("", bobConn, SENT 5)
  bob <#= \case ("", c, Msg "hello") -> c == aliceConn; _ -> False
  bob #: ("12", aliceConn, "ACK 5") #> ("12", aliceConn, OK)
  alice #: ("3", bobConn, "SEND :how are you?") #> ("3", bobConn, MID 6)
  alice <# ("", bobConn, SENT 6)
  bob <#= \case ("", c, Msg "how are you?") -> c == aliceConn; _ -> False
  bob #: ("13", aliceConn, "ACK 6") #> ("13", aliceConn, OK)
  bob #: ("14", aliceConn, "SEND 9\nhello too") #> ("14", aliceConn, MID 7)
  bob <# ("", aliceConn, SENT 7)
  alice <#= \case ("", c, Msg "hello too") -> c == bobConn; _ -> False
  alice #: ("3a", bobConn, "ACK 7") #> ("3a", bobConn, OK)
  bob #: ("15", aliceConn, "SEND 9\nmessage 1") #> ("15", aliceConn, MID 8)
  bob <# ("", aliceConn, SENT 8)
  alice <#= \case ("", c, Msg "message 1") -> c == bobConn; _ -> False
  alice #: ("4a", bobConn, "ACK 8") #> ("4a", bobConn, OK)
  alice #: ("5", bobConn, "OFF") #> ("5", bobConn, OK)
  bob #: ("17", aliceConn, "SEND 9\nmessage 3") #> ("17", aliceConn, MID 9)
  bob <# ("", aliceConn, MERR 9 (SMP AUTH))
  alice #: ("6", bobConn, "DEL") #> ("6", bobConn, OK)
  alice #:# "nothing else should be delivered to alice"

testContactConnection :: Transport c => TProxy c -> c -> c -> c -> IO ()
testContactConnection _ alice bob tom = do
  ("1", "alice_contact", Right (INV cReq)) <- alice #: ("1", "alice_contact", "NEW CON")
  let cReq' = strEncode cReq

  bob #: ("11", "alice", "JOIN " <> cReq' <> " 14\nbob's connInfo") #> ("11", "alice", OK)
  ("", "alice_contact", Right (REQ aInvId "bob's connInfo")) <- (alice <#:)
  alice #: ("2", "bob", "ACPT " <> aInvId <> " 16\nalice's connInfo") #> ("2", "bob", OK)
  ("", "alice", Right (CONF bConfId "alice's connInfo")) <- (bob <#:)
  bob #: ("12", "alice", "LET " <> bConfId <> " 16\nbob's connInfo 2") #> ("12", "alice", OK)
  alice <# ("", "bob", INFO "bob's connInfo 2")
  alice <# ("", "bob", CON)
  bob <# ("", "alice", CON)
  alice #: ("3", "bob", "SEND :hi") #> ("3", "bob", MID 5)
  alice <# ("", "bob", SENT 5)
  bob <#= \case ("", "alice", Msg "hi") -> True; _ -> False
  bob #: ("13", "alice", "ACK 5") #> ("13", "alice", OK)

  tom #: ("21", "alice", "JOIN " <> cReq' <> " 14\ntom's connInfo") #> ("21", "alice", OK)
  ("", "alice_contact", Right (REQ aInvId' "tom's connInfo")) <- (alice <#:)
  alice #: ("4", "tom", "ACPT " <> aInvId' <> " 16\nalice's connInfo") #> ("4", "tom", OK)
  ("", "alice", Right (CONF tConfId "alice's connInfo")) <- (tom <#:)
  tom #: ("22", "alice", "LET " <> tConfId <> " 16\ntom's connInfo 2") #> ("22", "alice", OK)
  alice <# ("", "tom", INFO "tom's connInfo 2")
  alice <# ("", "tom", CON)
  tom <# ("", "alice", CON)
  alice #: ("5", "tom", "SEND :hi there") #> ("5", "tom", MID 5)
  alice <# ("", "tom", SENT 5)
  tom <#= \case ("", "alice", Msg "hi there") -> True; _ -> False
  tom #: ("23", "alice", "ACK 5") #> ("23", "alice", OK)

testContactConnRandomIds :: Transport c => TProxy c -> c -> c -> IO ()
testContactConnRandomIds _ alice bob = do
  ("1", aliceContact, Right (INV cReq)) <- alice #: ("1", "", "NEW CON")
  let cReq' = strEncode cReq

  ("11", aliceConn, Right OK) <- bob #: ("11", "", "JOIN " <> cReq' <> " 14\nbob's connInfo")
  ("", aliceContact', Right (REQ aInvId "bob's connInfo")) <- (alice <#:)
  aliceContact' `shouldBe` aliceContact

  ("2", bobConn, Right OK) <- alice #: ("2", "", "ACPT " <> aInvId <> " 16\nalice's connInfo")
  ("", aliceConn', Right (CONF bConfId "alice's connInfo")) <- (bob <#:)
  aliceConn' `shouldBe` aliceConn

  bob #: ("12", aliceConn, "LET " <> bConfId <> " 16\nbob's connInfo 2") #> ("12", aliceConn, OK)
  alice <# ("", bobConn, INFO "bob's connInfo 2")
  alice <# ("", bobConn, CON)
  bob <# ("", aliceConn, CON)

  alice #: ("3", bobConn, "SEND :hi") #> ("3", bobConn, MID 5)
  alice <# ("", bobConn, SENT 5)
  bob <#= \case ("", c, Msg "hi") -> c == aliceConn; _ -> False
  bob #: ("13", aliceConn, "ACK 5") #> ("13", aliceConn, OK)

testRejectContactRequest :: Transport c => TProxy c -> c -> c -> IO ()
testRejectContactRequest _ alice bob = do
  ("1", "a_contact", Right (INV cReq)) <- alice #: ("1", "a_contact", "NEW CON")
  let cReq' = strEncode cReq
  bob #: ("11", "alice", "JOIN " <> cReq' <> " 10\nbob's info") #> ("11", "alice", OK)
  ("", "a_contact", Right (REQ aInvId "bob's info")) <- (alice <#:)
  -- RJCT must use correct contact connection
  alice #: ("2a", "bob", "RJCT " <> aInvId) #> ("2a", "bob", ERR $ CONN NOT_FOUND)
  alice #: ("2b", "a_contact", "RJCT " <> aInvId) #> ("2b", "a_contact", OK)
  alice #: ("3", "bob", "ACPT " <> aInvId <> " 12\nalice's info") #> ("3", "bob", ERR $ A.CMD PROHIBITED)
  bob #:# "nothing should be delivered to bob"

testSubscription :: Transport c => TProxy c -> c -> c -> c -> IO ()
testSubscription _ alice1 alice2 bob = do
  (alice1, "alice") `connect` (bob, "bob")
  bob #: ("12", "alice", "SEND 5\nhello") #> ("12", "alice", MID 5)
  bob <# ("", "alice", SENT 5)
  alice1 <#= \case ("", "bob", Msg "hello") -> True; _ -> False
  alice1 #: ("1", "bob", "ACK 5") #> ("1", "bob", OK)
  bob #: ("13", "alice", "SEND 11\nhello again") #> ("13", "alice", MID 6)
  bob <# ("", "alice", SENT 6)
  alice1 <#= \case ("", "bob", Msg "hello again") -> True; _ -> False
  alice1 #: ("2", "bob", "ACK 6") #> ("2", "bob", OK)
  alice2 #: ("21", "bob", "SUB") #> ("21", "bob", OK)
  alice1 <# ("", "bob", END)
  bob #: ("14", "alice", "SEND 2\nhi") #> ("14", "alice", MID 7)
  bob <# ("", "alice", SENT 7)
  alice2 <#= \case ("", "bob", Msg "hi") -> True; _ -> False
  alice2 #: ("22", "bob", "ACK 7") #> ("22", "bob", OK)
  alice1 #:# "nothing else should be delivered to alice1"

testSubscrNotification :: Transport c => TProxy c -> (ThreadId, ThreadId) -> c -> IO ()
testSubscrNotification t (server, _) client = do
  client #: ("1", "conn1", "NEW INV") =#> \case ("1", "conn1", INV {}) -> True; _ -> False
  client #:# "nothing should be delivered to client before the server is killed"
  killThread server
  client <# ("", "", DOWN testSMPServer ["conn1"])
  withSmpServer (ATransport t) $
    client <# ("", "conn1", ERR (SMP AUTH)) -- this new server does not have the queue

testMsgDeliveryServerRestart :: Transport c => TProxy c -> c -> c -> IO ()
testMsgDeliveryServerRestart t alice bob = do
  withServer $ do
    connect (alice, "alice") (bob, "bob")
    bob #: ("1", "alice", "SEND 2\nhi") #> ("1", "alice", MID 5)
    bob <# ("", "alice", SENT 5)
    alice <#= \case ("", "bob", Msg "hi") -> True; _ -> False
    alice #: ("11", "bob", "ACK 5") #> ("11", "bob", OK)
    alice #:# "nothing else delivered before the server is killed"

  let server = (SMPServer "localhost" testPort2 testKeyHash)
  alice <# ("", "", DOWN server ["bob"])
  bob #: ("2", "alice", "SEND 11\nhello again") #> ("2", "alice", MID 6)
  bob #:# "nothing else delivered before the server is restarted"
  alice #:# "nothing else delivered before the server is restarted"

  withServer $ do
    bob <# ("", "alice", SENT 6)
    alice <# ("", "", UP server ["bob"])
    alice <#= \case ("", "bob", Msg "hello again") -> True; _ -> False
    alice #: ("12", "bob", "ACK 6") #> ("12", "bob", OK)

  removeFile testStoreLogFile
  where
    withServer test' = withSmpServerStoreLogOn (ATransport t) testPort2 (const test') `shouldReturn` ()

testServerConnectionAfterError :: forall c. Transport c => TProxy c -> [c] -> IO ()
testServerConnectionAfterError t _ = do
  withAgent1 $ \bob -> do
    withAgent2 $ \alice -> do
      withServer $ do
        connect (bob, "bob") (alice, "alice")

      bob <# ("", "", DOWN server ["alice"])
      alice <# ("", "", DOWN server ["bob"])
      alice #: ("1", "bob", "SEND 5\nhello") #> ("1", "bob", MID 5)
      alice #:# "nothing else delivered before the server is restarted"
      bob #:# "nothing else delivered before the server is restarted"

  withAgent1 $ \bob -> do
    withAgent2 $ \alice -> do
      bob #: ("1", "alice", "SUB") #> ("1", "alice", ERR (BROKER NETWORK))
      alice #: ("1", "bob", "SUB") #> ("1", "bob", ERR (BROKER NETWORK))
      withServer $ do
        alice <# ("", "bob", SENT 5)
        bob <# ("", "", UP server ["alice"])
        bob <#= \case ("", "alice", Msg "hello") -> True; _ -> False
        bob #: ("2", "alice", "ACK 5") #> ("2", "alice", OK)
        alice <# ("", "", UP server ["bob"])
        alice #: ("1", "bob", "SEND 11\nhello again") #> ("1", "bob", MID 6)
        alice <# ("", "bob", SENT 6)
        bob <#= \case ("", "alice", Msg "hello again") -> True; _ -> False

  removeFile testStoreLogFile
  removeFile testDB
  removeFile testDB2
  where
    server = SMPServer "localhost" testPort2 testKeyHash
    withServer test' = withSmpServerStoreLogOn (ATransport t) testPort2 (const test') `shouldReturn` ()
    withAgent1 = withAgent agentTestPort testDB
    withAgent2 = withAgent agentTestPort2 testDB2
    withAgent :: String -> String -> (c -> IO a) -> IO a
    withAgent agentPort agentDB = withSmpAgentThreadOn_ (ATransport t) (agentPort, testPort2, agentDB) (pure ()) . const . testSMPAgentClientOn agentPort

testMsgDeliveryAgentRestart :: Transport c => TProxy c -> c -> IO ()
testMsgDeliveryAgentRestart t bob = do
  let server = SMPServer "localhost" testPort2 testKeyHash
  withAgent $ \alice -> do
    withServer $ do
      connect (bob, "bob") (alice, "alice")
      alice #: ("1", "bob", "SEND 5\nhello") #> ("1", "bob", MID 5)
      alice <# ("", "bob", SENT 5)
      bob <#= \case ("", "alice", Msg "hello") -> True; _ -> False
      bob #: ("11", "alice", "ACK 5") #> ("11", "alice", OK)
      bob #:# "nothing else delivered before the server is down"

    bob <# ("", "", DOWN server ["alice"])
    alice #: ("2", "bob", "SEND 11\nhello again") #> ("2", "bob", MID 6)
    alice #:# "nothing else delivered before the server is restarted"
    bob #:# "nothing else delivered before the server is restarted"

  withAgent $ \alice -> do
    withServer $ do
      tPutRaw alice ("3", "bob", "SUB")
      alice <#= \case
        (corrId, "bob", cmd) ->
          (corrId == "3" && cmd == OK)
            || (corrId == "" && cmd == SENT 6)
        _ -> False
      bob <# ("", "", UP server ["alice"])
      bob <#= \case ("", "alice", Msg "hello again") -> True; _ -> False
      bob #: ("12", "alice", "ACK 6") #> ("12", "alice", OK)

  removeFile testStoreLogFile
  removeFile testDB
  where
    withServer test' = withSmpServerStoreLogOn (ATransport t) testPort2 (const test') `shouldReturn` ()
    withAgent = withSmpAgentThreadOn_ (ATransport t) (agentTestPort, testPort, testDB) (pure ()) . const . testSMPAgentClientOn agentTestPort

testConcurrentMsgDelivery :: Transport c => TProxy c -> c -> c -> IO ()
testConcurrentMsgDelivery _ alice bob = do
  connect (alice, "alice") (bob, "bob")

  ("1", "bob2", Right (INV cReq)) <- alice #: ("1", "bob2", "NEW INV")
  let cReq' = strEncode cReq
  bob #: ("11", "alice2", "JOIN " <> cReq' <> " 14\nbob's connInfo") #> ("11", "alice2", OK)
  ("", "bob2", Right (CONF _confId "bob's connInfo")) <- (alice <#:)
  -- below commands would be needed to accept bob's connection, but alice does not
  -- alice #: ("2", "bob", "LET " <> _confId <> " 16\nalice's connInfo") #> ("2", "bob", OK)
  -- bob <# ("", "alice", INFO "alice's connInfo")
  -- bob <# ("", "alice", CON)
  -- alice <# ("", "bob", CON)

  -- the first connection should not be blocked by the second one
  sendMessage (alice, "alice") (bob, "bob") "hello"
  -- alice #: ("2", "bob", "SEND :hello") #> ("2", "bob", MID 1)
  -- alice <# ("", "bob", SENT 1)
  -- bob <#= \case ("", "alice", Msg "hello") -> True; _ -> False
  -- bob #: ("12", "alice", "ACK 1") #> ("12", "alice", OK)
  bob #: ("14", "alice", "SEND 9\nhello too") #> ("14", "alice", MID 6)
  bob <# ("", "alice", SENT 6)
  -- if delivery is blocked it won't go further
  alice <#= \case ("", "bob", Msg "hello too") -> True; _ -> False
  alice #: ("3", "bob", "ACK 6") #> ("3", "bob", OK)

testMsgDeliveryQuotaExceeded :: Transport c => TProxy c -> c -> c -> IO ()
testMsgDeliveryQuotaExceeded _ alice bob = do
  connect (alice, "alice") (bob, "bob")
  connect (alice, "alice2") (bob, "bob2")
  forM_ [1 .. 4 :: Int] $ \i -> do
    let corrId = bshow i
        msg = "message " <> bshow i
    (_, "bob", Right (MID mId)) <- alice #: (corrId, "bob", "SEND :" <> msg)
    alice <#= \case ("", "bob", SENT m) -> m == mId; _ -> False
  (_, "bob", Right (MID _)) <- alice #: ("5", "bob", "SEND :over quota")

  alice #: ("1", "bob2", "SEND :hello") #> ("1", "bob2", MID 5)
  -- if delivery is blocked it won't go further
  alice <# ("", "bob2", SENT 5)

connect :: forall c. Transport c => (c, ByteString) -> (c, ByteString) -> IO ()
connect (h1, name1) (h2, name2) = do
  ("c1", _, Right (INV cReq)) <- h1 #: ("c1", name2, "NEW INV")
  let cReq' = strEncode cReq
  h2 #: ("c2", name1, "JOIN " <> cReq' <> " 5\ninfo2") #> ("c2", name1, OK)
  ("", _, Right (CONF connId "info2")) <- (h1 <#:)
  h1 #: ("c3", name2, "LET " <> connId <> " 5\ninfo1") #> ("c3", name2, OK)
  h2 <# ("", name1, INFO "info1")
  h2 <# ("", name1, CON)
  h1 <# ("", name2, CON)

sendMessage :: Transport c => (c, ConnId) -> (c, ConnId) -> ByteString -> IO ()
sendMessage (h1, name1) (h2, name2) msg = do
  ("m1", name2', Right (MID mId)) <- h1 #: ("m1", name2, "SEND :" <> msg)
  name2' `shouldBe` name2
  h1 <#= \case ("", n, SENT m) -> n == name2 && m == mId; _ -> False
  ("", name1', Right (MSG MsgMeta {recipient = (msgId', _)} msg')) <- (h2 <#:)
  name1' `shouldBe` name1
  msg' `shouldBe` msg
  h2 #: ("m2", name1, "ACK " <> bshow msgId') =#> \case ("m2", n, OK) -> n == name1; _ -> False

-- connect' :: forall c. Transport c => c -> c -> IO (ByteString, ByteString)
-- connect' h1 h2 = do
--   ("c1", conn2, Right (INV cReq)) <- h1 #: ("c1", "", "NEW INV")
--   let cReq' = strEncode cReq
--   ("c2", conn1, Right OK) <- h2 #: ("c2", "", "JOIN " <> cReq' <> " 5\ninfo2")
--   ("", _, Right (REQ connId "info2")) <- (h1 <#:)
--   h1 #: ("c3", conn2, "ACPT " <> connId <> " 5\ninfo1") =#> \case ("c3", c, OK) -> c == conn2; _ -> False
--   h2 <# ("", conn1, INFO "info1")
--   h2 <# ("", conn1, CON)
--   h1 <# ("", conn2, CON)
--   pure (conn1, conn2)

sampleDhKey :: ByteString
sampleDhKey = "MCowBQYDK2VuAyEAjiswwI3O_NlS8Fk3HJUW870EY2bAwmttMBsvRB9eV3o="

syntaxTests :: forall c. Transport c => TProxy c -> Spec
syntaxTests t = do
  it "unknown command" $ ("1", "5678", "HELLO") >#> ("1", "5678", "ERR CMD SYNTAX")
  describe "NEW" $ do
    describe "valid" $ do
      -- TODO: add tests with defined connection id
      it "with correct parameter" $ ("211", "", "NEW INV") >#>= \case ("211", _, "INV" : _) -> True; _ -> False
    describe "invalid" $ do
      -- TODO: add tests with defined connection id
      it "with incorrect parameter" $ ("222", "", "NEW hi") >#> ("222", "", "ERR CMD SYNTAX")

  describe "JOIN" $ do
    describe "valid" $ do
      it "using same server as in invitation" $
        ( "311",
          "a",
          "JOIN https://simpex.chat/invitation#/?smp=smp%3A%2F%2F"
            <> urlEncode True "LcJUMfVhwD8yxjAiSaDzzGF3-kLG4Uh0Fl_ZIjrRwjI="
            <> "%40localhost%3A5001%2F3456-w%3D%3D%23"
            <> urlEncode True sampleDhKey
            <> "&v=1"
            <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> " 14\nbob's connInfo"
        )
          >#> ("311", "a", "ERR SMP AUTH")
    describe "invalid" $ do
      it "no parameters" $ ("321", "", "JOIN") >#> ("321", "", "ERR CMD SYNTAX")
  where
    -- simple test for one command with the expected response
    (>#>) :: ARawTransmission -> ARawTransmission -> Expectation
    command >#> response = smpAgentTest t command `shouldReturn` response

    -- simple test for one command with a predicate for the expected response
    (>#>=) :: ARawTransmission -> ((ByteString, ByteString, [ByteString]) -> Bool) -> Expectation
    command >#>= p = smpAgentTest t command >>= (`shouldSatisfy` p . \(cId, connId, cmd) -> (cId, connId, B.words cmd))
