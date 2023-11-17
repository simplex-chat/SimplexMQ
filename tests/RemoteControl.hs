{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module RemoteControl where

import AgentTests.FunctionalAPITests (runRight)
import Control.Logger.Simple
import Crypto.Random (ChaChaDRG, drgNew)
import qualified Data.Aeson as J
import Data.List.NonEmpty (NonEmpty (..))
import qualified Simplex.RemoteControl.Client as RC
import Simplex.RemoteControl.Invitation (RCSignedInvitation, verifySignedInvitation)
import Simplex.RemoteControl.Types
import Test.Hspec
import UnliftIO
import UnliftIO.Concurrent

remoteControlTests :: Spec
remoteControlTests = do
  describe "New controller/host pairing" $ do
    it "should connect to new pairing" testNewPairing
    it "should connect to existing pairing" testExistingPairing
  describe "Multicast discovery" $ do
    it "should find paired host and connect" testMulticast

testNewPairing :: IO ()
testNewPairing = do
  drg <- drgNew >>= newTVarIO
  hp <- RC.newRCHostPairing
  invVar <- newEmptyMVar
  ctrlSessId <- async . runRight $ do
    logNote "c 1"
    (inv, hc, r) <- RC.connectRCHost drg hp (J.String "app") False
    logNote "c 2"
    putMVar invVar (inv, hc)
    logNote "c 3"
    Right (sessId, _tls, r') <- atomically $ takeTMVar r
    logNote "c 4"
    Right (_rcHostSession, _rcHelloBody, _hp') <- atomically $ takeTMVar r'
    logNote "c 5"
    threadDelay 250000
    logNote "ctrl: ciao"
    liftIO $ RC.cancelHostClient hc
    pure sessId

  (signedInv, hc) <- takeMVar invVar
  -- logNote $ decodeUtf8 $ strEncode inv
  inv <- maybe (fail "bad invite") pure $ verifySignedInvitation signedInv

  hostSessId <- async . runRight $ do
    logNote "h 1"
    (rcCtrlClient, r) <- RC.connectRCCtrl drg inv Nothing (J.String "app")
    logNote "h 2"
    Right (sessId', _tls, r') <- atomically $ takeTMVar r
    logNote "h 3"
    liftIO $ RC.confirmCtrlSession rcCtrlClient True
    logNote "h 4"
    Right (_rcCtrlSession, _rcCtrlPairing) <- atomically $ takeTMVar r'
    logNote "h 5"
    threadDelay 250000
    logNote "ctrl: adios"
    pure sessId'

  waitCatch hc.action >>= \case
    Left err -> fromException err `shouldBe` Just AsyncCancelled
    Right () -> fail "Unexpected controller finish"

  timeout 5000000 (waitBoth ctrlSessId hostSessId) >>= \case
    Just (sessId, sessId') -> sessId `shouldBe` sessId'
    _ -> fail "timeout"

testExistingPairing :: IO ()
testExistingPairing = do
  drg <- drgNew >>= newTVarIO
  invVar <- newEmptyMVar
  hp <- liftIO $ RC.newRCHostPairing
  ctrl <- runCtrl drg False hp invVar
  inv <- takeMVar invVar
  let cp_ = Nothing
  host <- runHostURI drg cp_ inv
  timeout 5000000 (waitBoth ctrl host) >>= \case
    Nothing -> fail "timeout"
    Just (hp', cp') -> do
      ctrl' <- runCtrl drg False hp' invVar
      inv' <- takeMVar invVar
      host' <- runHostURI drg (Just cp') inv'
      timeout 5000000 (waitBoth ctrl' host') >>= \case
        Nothing -> fail "timeout"
        Just (_hp2, cp2) -> do
          ctrl2 <- runCtrl drg False hp' invVar -- old host pairing used to test controller not updating state
          inv2 <- takeMVar invVar
          host2 <- runHostURI drg (Just cp2) inv2
          timeout 5000000 (waitBoth ctrl2 host2) >>= \case
            Nothing -> fail "timeout"
            Just (hp3, cp3) -> do
              ctrl3 <- runCtrl drg False hp3 invVar
              inv3 <- takeMVar invVar
              host3 <- runHostURI drg (Just cp3) inv3
              timeout 5000000 (waitBoth ctrl3 host3) >>= \case
                Nothing -> fail "timeout"
                Just _ -> pure ()

testMulticast :: IO ()
testMulticast = do
  drg <- drgNew >>= newTVarIO
  subscribers <- newTMVarIO 0
  invVar <- newEmptyMVar
  hp <- liftIO RC.newRCHostPairing
  ctrl <- runCtrl drg False hp invVar
  inv <- takeMVar invVar
  let cp_ = Nothing
  host <- runHostURI drg cp_ inv
  timeout 5000000 (waitBoth ctrl host) >>= \case
    Nothing -> fail "timeout"
    Just (hp', cp') -> do
      ctrl' <- runCtrl drg True hp' invVar
      _inv <- takeMVar invVar
      host' <- runHostMulticast drg subscribers cp'
      timeout 5000000 (waitBoth ctrl' host') >>= \case
        Nothing -> fail "timeout"
        Just _ -> pure ()

runCtrl :: TVar ChaChaDRG -> Bool -> RCHostPairing -> MVar RCSignedInvitation -> IO (Async RCHostPairing)
runCtrl drg multicast hp invVar = async . runRight $ do
  (inv, hc, r) <- RC.connectRCHost drg hp (J.String "app") multicast
  putMVar invVar inv
  Right (_sessId, _tls, r') <- atomically $ takeTMVar r
  Right (_rcHostSession, _rcHelloBody, hp') <- atomically $ takeTMVar r'
  threadDelay 250000
  liftIO $ RC.cancelHostClient hc
  pure hp'

runHostURI :: TVar ChaChaDRG -> Maybe RCCtrlPairing -> RCSignedInvitation -> IO (Async RCCtrlPairing)
runHostURI drg cp_ signedInv = async . runRight $ do
  inv <- maybe (fail "bad invite") pure $ verifySignedInvitation signedInv
  (rcCtrlClient, r) <- RC.connectRCCtrl drg inv cp_ (J.String "app")
  Right (_sessId', _tls, r') <- atomically $ takeTMVar r
  liftIO $ RC.confirmCtrlSession rcCtrlClient True
  Right (_rcCtrlSession, cp') <- atomically $ takeTMVar r'
  threadDelay 250000
  pure cp'

runHostMulticast :: TVar ChaChaDRG -> TMVar Int -> RCCtrlPairing -> IO (Async RCCtrlPairing)
runHostMulticast drg subscribers cp = async . runRight $ do
  (pairing, inv) <- RC.discoverRCCtrl subscribers (cp :| [])
  (rcCtrlClient, r) <- RC.connectRCCtrl drg inv (Just pairing) (J.String "app")
  Right (_sessId', _tls, r') <- atomically $ takeTMVar r
  liftIO $ RC.confirmCtrlSession rcCtrlClient True
  Right (_rcCtrlSession, cp') <- atomically $ takeTMVar r'
  threadDelay 250000
  pure cp'
