{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}

module AgentTests.FunctionalAPITests (functionalAPITests) where

import Control.Concurrent
import Control.Monad.Except (ExceptT, catchError, runExceptT)
import Control.Monad.IO.Unlift
import SMPAgentClient
import SMPClient (withSmpServer)
import Simplex.Messaging.Agent
import Simplex.Messaging.Agent.Env.SQLite (AgentConfig, dbFile)
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.Store (InternalId (..))
import Simplex.Messaging.Protocol (ErrorType (..), MsgBody)
import Simplex.Messaging.Transport (ATransport (..))
import System.Timeout
import Test.Hspec
import UnliftIO.STM

(##>) :: MonadIO m => m (ATransmission 'Agent) -> ATransmission 'Agent -> m ()
a ##> t = a >>= \t' -> liftIO (t' `shouldBe` t)

(=##>) :: MonadIO m => m (ATransmission 'Agent) -> (ATransmission 'Agent -> Bool) -> m ()
a =##> p = a >>= \t -> liftIO (t `shouldSatisfy` p)

get :: MonadIO m => AgentClient -> m (ATransmission 'Agent)
get c = atomically (readTBQueue $ subQ c)

pattern Msg :: MsgBody -> ACommand 'Agent
pattern Msg msgBody <- MSG MsgMeta {integrity = MsgOk} msgBody

functionalAPITests :: ATransport -> Spec
functionalAPITests t = do
  describe "Establishing duplex connection" $
    it "should connect via one server using SMP agent clients" $
      withSmpServer t testAgentClient
  describe "Establishing connection asynchronously" do
    it "should connect with initiating client going offline" $
      withSmpServer t testAsyncInitiatingOffline
    it "should connect with joining client going offline before its queue activation" $
      withSmpServer t testAsyncJoiningOfflineBeforeActivation
    -- TODO a valid test case but not trivial to implement, probably requires some agent rework
    xit "should connect with joining client going offline after its queue activation" $
      withSmpServer t testAsyncJoiningOfflineAfterActivation
    it "should connect with both clients going offline" $
      withSmpServer t testAsyncBothOffline

testAgentClient :: IO ()
testAgentClient = do
  alice <- getSMPAgentClient cfg
  bob <- getSMPAgentClient cfg {dbFile = testDB2}
  Right () <- runExceptT $ do
    (bobId, qInfo) <- createConnection alice
    aliceId <- joinConnection bob qInfo "bob's connInfo"
    ("", _, CONF confId "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    get alice ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    InternalId 1 <- sendMessage alice bobId "hello"
    InternalId 2 <- sendMessage alice bobId "how are you?"
    get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
    get bob =##> \case ("", c, Msg "how are you?") -> c == aliceId; _ -> False
    InternalId 3 <- sendMessage bob aliceId "hello too"
    InternalId 4 <- sendMessage bob aliceId "message 1"
    get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
    get alice =##> \case ("", c, Msg "message 1") -> c == bobId; _ -> False
    suspendConnection alice bobId
    InternalId 0 <- sendMessage bob aliceId "message 2" `catchError` \(SMP AUTH) -> pure $ InternalId 0
    deleteConnection alice bobId
    liftIO $ noMessages alice "nothing else should be delivered to alice"
  pure ()
  where
    noMessages :: AgentClient -> String -> Expectation
    noMessages c err = tryGet `shouldReturn` ()
      where
        tryGet =
          10000 `timeout` get c >>= \case
            Just _ -> error err
            _ -> return ()

testAsyncInitiatingOffline :: IO ()
testAsyncInitiatingOffline = do
  alice <- getSMPAgentClient cfg
  bob <- getSMPAgentClient cfg {dbFile = testDB2}
  Right () <- runExceptT do
    (bobId, qInfo) <- createConnection alice
    disconnectAgentClient alice
    aliceId <- joinConnection bob qInfo "bob's connInfo"
    alice' <- waitAndComeOnline 3 cfg bobId
    ("", _, CONF confId "bob's connInfo") <- get alice'
    allowConnection alice' bobId confId "alice's connInfo"
    get alice' ##> ("", bobId, CON)
    get bob ##> ("", aliceId, INFO "alice's connInfo")
    get bob ##> ("", aliceId, CON)
    exchangeGreetings alice' bobId bob aliceId
  pure ()

testAsyncJoiningOfflineBeforeActivation :: IO ()
testAsyncJoiningOfflineBeforeActivation = do
  alice <- getSMPAgentClient cfg
  bob <- getSMPAgentClient cfg {dbFile = testDB2}
  Right () <- runExceptT do
    (bobId, qInfo) <- createConnection alice
    aliceId <- joinConnection bob qInfo "bob's connInfo"
    disconnectAgentClient bob
    ("", _, CONF confId "bob's connInfo") <- get alice
    allowConnection alice bobId confId "alice's connInfo"
    bob' <- waitAndComeOnline 3 cfg {dbFile = testDB2} aliceId
    get alice ##> ("", bobId, CON)
    get bob' ##> ("", aliceId, INFO "alice's connInfo")
    get bob' ##> ("", aliceId, CON)
    exchangeGreetings alice bobId bob' aliceId
  pure ()

testAsyncJoiningOfflineAfterActivation :: IO ()
testAsyncJoiningOfflineAfterActivation = error "not implemented"

testAsyncBothOffline :: IO ()
testAsyncBothOffline = do
  alice <- getSMPAgentClient cfg
  bob <- getSMPAgentClient cfg {dbFile = testDB2}
  Right () <- runExceptT do
    (bobId, qInfo) <- createConnection alice
    disconnectAgentClient alice
    aliceId <- joinConnection bob qInfo "bob's connInfo"
    disconnectAgentClient bob
    alice' <- waitAndComeOnline 2 cfg bobId
    ("", _, CONF confId "bob's connInfo") <- get alice'
    allowConnection alice' bobId confId "alice's connInfo"
    bob' <- waitAndComeOnline 1 cfg {dbFile = testDB2} aliceId
    get alice' ##> ("", bobId, CON)
    get bob' ##> ("", aliceId, INFO "alice's connInfo")
    get bob' ##> ("", aliceId, CON)
    exchangeGreetings alice' bobId bob' aliceId
  pure ()

waitAndComeOnline :: Int -> AgentConfig -> ConnId -> ExceptT AgentErrorType IO AgentClient
waitAndComeOnline delaySec agentCfg connId = do
  liftIO $ threadDelay $ delaySec * 1_000_000
  c <- liftIO $ getSMPAgentClient agentCfg
  subscribeConnection c connId
  pure c

exchangeGreetings :: AgentClient -> ConnId -> AgentClient -> ConnId -> ExceptT AgentErrorType IO ()
exchangeGreetings alice bobId bob aliceId = do
  InternalId 1 <- sendMessage alice bobId "hello"
  get bob =##> \case ("", c, Msg "hello") -> c == aliceId; _ -> False
  InternalId 2 <- sendMessage bob aliceId "hello too"
  get alice =##> \case ("", c, Msg "hello too") -> c == bobId; _ -> False
