{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

module AgentTests where

import AgentTests.SQLiteTests (storeTests)
import Control.Concurrent
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import SMPAgentClient
import Simplex.Messaging.Agent.Transmission
import Simplex.Messaging.Protocol (ErrorType (..), MsgBody)
import System.IO (Handle)
import System.Timeout
import Test.Hspec

agentTests :: Spec
agentTests = do
  describe "SQLite store" storeTests
  describe "SMP agent protocol syntax" syntaxTests
  describe "Establishing duplex connection" do
    it "should connect via one server and one agent" $
      smpAgentTest2_1 testDuplexConnection
    it "should connect via one server and 2 agents" $
      smpAgentTest2 testDuplexConnection
  describe "Connection subscriptions" do
    -- TODO replace delays with a permanent fix, this often fails in github build
    xit "should connect via one server and one agent" $
      smpAgentTest3_1 testSubscription
    it "should send notifications to client when server disconnects" $
      smpAgentServerTest testSubscrNotification

-- | simple test for one command with the expected response
(>#>) :: ARawTransmission -> ARawTransmission -> Expectation
command >#> response = smpAgentTest command `shouldReturn` response

-- | simple test for one command with a predicate for the expected response
(>#>=) :: ARawTransmission -> ((ByteString, ByteString, [ByteString]) -> Bool) -> Expectation
command >#>= p = smpAgentTest command >>= (`shouldSatisfy` p . \(cId, cAlias, cmd) -> (cId, cAlias, B.words cmd))

-- | send transmission `t` to handle `h` and get response
(#:) :: Handle -> (ByteString, ByteString, ByteString) -> IO (ATransmissionOrError 'Agent)
h #: t = tPutRaw h t >> tGet SAgent h

-- | action and expected response
-- `h #:t #> r` is the test that sends `t` to `h` and validates that the response is `r`
(#>) :: IO (ATransmissionOrError 'Agent) -> ATransmission 'Agent -> Expectation
action #> (corrId, cAlias, cmd) = action `shouldReturn` (corrId, cAlias, Right cmd)

-- | action and predicate for the response
-- `h #:t =#> p` is the test that sends `t` to `h` and validates the response using `p`
(=#>) :: IO (ATransmissionOrError 'Agent) -> (ATransmission 'Agent -> Bool) -> Expectation
action =#> p = action >>= (`shouldSatisfy` p . correctTransmission)

correctTransmission :: ATransmissionOrError a -> ATransmission a
correctTransmission (corrId, cAlias, cmdOrErr) = case cmdOrErr of
  Right cmd -> (corrId, cAlias, cmd)
  Left e -> error $ show e

-- | receive message to handle `h` and validate that it is the expected one
(<#) :: Handle -> ATransmission 'Agent -> Expectation
h <# (corrId, cAlias, cmd) = tGet SAgent h `shouldReturn` (corrId, cAlias, Right cmd)

-- | receive message to handle `h` and validate it using predicate `p`
(<#=) :: Handle -> (ATransmission 'Agent -> Bool) -> Expectation
h <#= p = tGet SAgent h >>= (`shouldSatisfy` p . correctTransmission)

-- | test that nothing is delivered to handle `h` during 10ms
(#:#) :: Handle -> String -> Expectation
h #:# err = tryGet `shouldReturn` ()
  where
    tryGet =
      10000 `timeout` tGet SAgent h >>= \case
        Just _ -> error err
        _ -> return ()

pattern Msg :: MsgBody -> ACommand 'Agent
pattern Msg m_body <- MSG {m_body}

testDuplexConnection :: Handle -> Handle -> IO ()
testDuplexConnection alice bob = do
  ("1", "bob", Right (INV qInfo)) <- alice #: ("1", "bob", "NEW localhost:5000")
  let qInfo' = serializeSmpQueueInfo qInfo
  bob #: ("11", "alice", "JOIN " <> qInfo') #> ("11", "alice", CON)
  alice <# ("", "bob", CON)
  alice #: ("2", "bob", "SEND :hello") =#> \case ("2", "bob", SENT _) -> True; _ -> False
  alice #: ("3", "bob", "SEND :how are you?") =#> \case ("3", "bob", SENT _) -> True; _ -> False
  bob <#= \case ("", "alice", Msg "hello") -> True; _ -> False
  bob <#= \case ("", "alice", Msg "how are you?") -> True; _ -> False
  bob #: ("14", "alice", "SEND 9\nhello too") =#> \case ("14", "alice", SENT _) -> True; _ -> False
  alice <#= \case ("", "bob", Msg "hello too") -> True; _ -> False
  bob #: ("15", "alice", "SEND 9\nmessage 1") =#> \case ("15", "alice", SENT _) -> True; _ -> False
  alice <#= \case ("", "bob", Msg "message 1") -> True; _ -> False
  alice #: ("5", "bob", "OFF") #> ("5", "bob", OK)
  bob #: ("17", "alice", "SEND 9\nmessage 3") #> ("17", "alice", ERR (SMP AUTH))
  alice #: ("6", "bob", "DEL") #> ("6", "bob", OK)
  alice #:# "nothing else should be delivered to alice"

testSubscription :: Handle -> Handle -> Handle -> IO ()
testSubscription alice1 alice2 bob = do
  ("1", "bob", Right (INV qInfo)) <- alice1 #: ("1", "bob", "NEW localhost:5000")
  let qInfo' = serializeSmpQueueInfo qInfo
  bob #: ("11", "alice", "JOIN " <> qInfo') #> ("11", "alice", CON)
  bob #: ("12", "alice", "SEND 5\nhello") =#> \case ("12", "alice", SENT _) -> True; _ -> False
  bob #: ("13", "alice", "SEND 11\nhello again") =#> \case ("13", "alice", SENT _) -> True; _ -> False
  alice1 <# ("", "bob", CON)
  alice1 <#= \case ("", "bob", Msg "hello") -> True; _ -> False
  -- alice1 <#= \case ("", "bob", Msg "hello again") -> True; _ -> False
  t <- tGet SAgent alice1
  print t
  t `shouldSatisfy` (\case ("", "bob", Msg "hello again") -> True; _ -> False) . correctTransmission
  alice2 #: ("21", "bob", "SUB") #> ("21", "bob", OK)
  alice1 <# ("", "bob", END)
  bob #: ("14", "alice", "SEND 2\nhi") =#> \case ("14", "alice", SENT _) -> True; _ -> False
  alice2 <#= \case ("", "bob", Msg "hi") -> True; _ -> False
  alice1 #:# "nothing else should be delivered to alice1"

testSubscrNotification :: (ThreadId, ThreadId) -> Handle -> IO ()
testSubscrNotification (server, _) client = do
  client #: ("1", "conn1", "NEW localhost:5000") =#> \case ("1", "conn1", INV _) -> True; _ -> False
  client #:# "nothing should be delivered to client before the server is killed"
  killThread server
  client <# ("", "conn1", END)

samplePublicKey :: ByteString
samplePublicKey = "256,ppr3DCweAD3RTVFhU2j0u+DnYdqJl1qCdKLHIKsPl1xBzfmnzK0o9GEDlaIClbK39KzPJMljcpnYb2KlSoZ51AhwF5PH2CS+FStc3QzajiqfdOQPet23Hd9YC6pqyTQ7idntqgPrE7yKJF44lUhKlq8QS9KQcbK7W6t7F9uQFw44ceWd2eVf81UV04kQdKWJvC5Sz6jtSZNEfs9mVI8H0wi1amUvS6+7EDJbxikhcCRnFShFO9dUKRYXj6L2JVqXqO5cZgY9BScyneWIg6mhhsTcdDbITM6COlL+pF1f3TjDN+slyV+IzE+ap/9NkpsrCcI8KwwDpqEDmUUV/JQfmQ==,gj2UAiWzSj7iun0iXvI5iz5WEjaqngmB3SzQ5+iarixbaG15LFDtYs3pijG3eGfB1wIFgoP4D2z97vIWn8olT4uCTUClf29zGDDve07h/B3QG/4i0IDnio7MX3AbE8O6PKouqy/GLTfT4WxFUn423g80rpsVYd5oj+SCL2eaxIc="

syntaxTests :: Spec
syntaxTests = do
  it "unknown command" $ ("1", "5678", "HELLO") >#> ("1", "5678", "ERR SYNTAX 11")
  describe "NEW" do
    describe "valid" do
      -- TODO: ERROR no connection alias in the response (it does not generate it yet if not provided)
      -- TODO: add tests with defined connection alias
      xit "only server" $ ("211", "", "NEW localhost") >#>= \case ("211", "", "INV" : _) -> True; _ -> False
      it "with port" $ ("212", "", "NEW localhost:5000") >#>= \case ("212", "", "INV" : _) -> True; _ -> False
      xit "with keyHash" $ ("213", "", "NEW localhost#1234") >#>= \case ("213", "", "INV" : _) -> True; _ -> False
      it "with port and keyHash" $ ("214", "", "NEW localhost:5000#1234") >#>= \case ("214", "", "INV" : _) -> True; _ -> False
    describe "invalid" do
      -- TODO: add tests with defined connection alias
      it "no parameters" $ ("221", "", "NEW") >#> ("221", "", "ERR SYNTAX 11")
      it "many parameters" $ ("222", "", "NEW localhost:5000 hi") >#> ("222", "", "ERR SYNTAX 11")
      it "invalid server keyHash" $ ("223", "", "NEW localhost:5000#1") >#> ("223", "", "ERR SYNTAX 11")

  describe "JOIN" do
    describe "valid" do
      -- TODO: ERROR no connection alias in the response (it does not generate it yet if not provided)
      -- TODO: add tests with defined connection alias
      it "using same server as in invitation" $
        ("311", "", "JOIN smp::localhost:5000::1234::" <> samplePublicKey) >#> ("311", "", "ERR SMP AUTH")
    describe "invalid" do
      -- TODO: JOIN is not merged yet - to be added
      it "no parameters" $ ("321", "", "JOIN") >#> ("321", "", "ERR SYNTAX 11")
