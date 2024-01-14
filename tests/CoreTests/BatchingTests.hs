{-# LANGUAGE LambdaCase #-}

module CoreTests.BatchingTests (batchingTests) where

import Control.Concurrent.STM
import Control.Monad
import Data.ByteString.Builder (Builder, toLazyByteString)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as LB
import qualified Data.List.NonEmpty as L
import Simplex.Messaging.Client
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol
import Simplex.Messaging.Transport
import Simplex.Messaging.Version (VersionRange (..))
import Test.Hspec

batchingTests :: Spec
batchingTests = do
  describe "batchTransmissions" $ do
    it "should batch with 90 subscriptions per batch" testBatchSubscriptions
    it "should break on message that does not fit" testBatchWithMessage
    it "should break on large message" testBatchWithLargeMessage
  describe "batchTransmissions'" $ do
    it "should batch with 90 subscriptions per batch" testClientBatchSubscriptions
    it "should break on message that does not fit" testClientBatchWithMessage
    it "should break on large message" testClientBatchWithLargeMessage

testBatchSubscriptions :: IO ()
testBatchSubscriptions = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  subs <- replicateM 200 $ randomSUB sessId
  let batches1 = batchTransmissions False smpBlockSize $ L.fromList subs
  all lenOk1 batches1 `shouldBe` True
  length batches1 `shouldBe` 200
  let batches = batchTransmissions True smpBlockSize $ L.fromList subs
  length batches `shouldBe` 3
  [TBTransmissions s1 n1 _, TBTransmissions s2 n2 _, TBTransmissions s3 n3 _] <- pure batches
  (n1, n2, n3) `shouldBe` (20, 90, 90)
  all lenOk [s1, s2, s3] `shouldBe` True

testBatchWithMessage :: IO ()
testBatchWithMessage = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  subs1 <- replicateM 60 $ randomSUB sessId
  send <- randomSEND sessId 8000
  subs2 <- replicateM 40 $ randomSUB sessId
  let cmds = subs1 <> [send] <> subs2
      batches1 = batchTransmissions False smpBlockSize $ L.fromList cmds
  all lenOk1 batches1 `shouldBe` True
  length batches1 `shouldBe` 101
  let batches = batchTransmissions True smpBlockSize $ L.fromList cmds
  length batches `shouldBe` 2
  [TBTransmissions s1 n1 _, TBTransmissions s2 n2 _] <- pure batches
  (n1, n2) `shouldBe` (55, 46)
  all lenOk [s1, s2] `shouldBe` True

testBatchWithLargeMessage :: IO ()
testBatchWithLargeMessage = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  subs1 <- replicateM 60 $ randomSUB sessId
  send <- randomSEND sessId 17000
  subs2 <- replicateM 100 $ randomSUB sessId
  let cmds = subs1 <> [send] <> subs2
      batches1 = batchTransmissions False smpBlockSize $ L.fromList cmds
  all lenOk1 batches1 `shouldBe` False
  length batches1 `shouldBe` 161
  let batches1' = take 60 batches1 <> drop 61 batches1
  all lenOk1 batches1' `shouldBe` True
  length batches1' `shouldBe` 160
  let batches = batchTransmissions True smpBlockSize $ L.fromList cmds
  length batches `shouldBe` 4
  [TBTransmissions s1 n1 _, TBLargeTransmission _, TBTransmissions s2 n2 _, TBTransmissions s3 n3 _] <- pure batches
  (n1, n2, n3) `shouldBe` (60, 10, 90)
  all lenOk [s1, s2, s3] `shouldBe` True

testClientBatchSubscriptions :: IO ()
testClientBatchSubscriptions = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  client <- atomically $ clientStub sessId
  subs <- replicateM 200 $ randomSUBCmd client
  let batches1 = batchTransmissions' False smpBlockSize $ L.fromList subs
  all lenOk1 batches1 `shouldBe` True
  let batches = batchTransmissions' True smpBlockSize $ L.fromList subs
  length batches `shouldBe` 3
  [TBTransmissions s1 n1 rs1, TBTransmissions s2 n2 rs2, TBTransmissions s3 n3 rs3] <- pure batches
  (n1, n2, n3) `shouldBe` (20, 90, 90)
  (length rs1, length rs2, length rs3) `shouldBe` (20, 90, 90)
  all lenOk [s1, s2, s3] `shouldBe` True

testClientBatchWithMessage :: IO ()
testClientBatchWithMessage = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  client <- atomically $ clientStub sessId
  subs1 <- replicateM 60 $ randomSUBCmd client
  send <- randomSENDCmd client 8000
  subs2 <- replicateM 40 $ randomSUBCmd client
  let cmds = subs1 <> [send] <> subs2
      batches1 = batchTransmissions' False smpBlockSize $ L.fromList cmds
  all lenOk1 batches1 `shouldBe` True
  length batches1 `shouldBe` 101
  let batches = batchTransmissions' True smpBlockSize $ L.fromList cmds
  length batches `shouldBe` 2
  [TBTransmissions s1 n1 rs1, TBTransmissions s2 n2 rs2] <- pure batches
  (n1, n2) `shouldBe` (55, 46)
  (length rs1, length rs2) `shouldBe` (55, 46)
  all lenOk [s1, s2] `shouldBe` True

testClientBatchWithLargeMessage :: IO ()
testClientBatchWithLargeMessage = do
  sessId <- atomically . C.randomBytes 32 =<< C.newRandom
  client <- atomically $ clientStub sessId
  subs1 <- replicateM 60 $ randomSUBCmd client
  send <- randomSENDCmd client 17000
  subs2 <- replicateM 100 $ randomSUBCmd client
  let cmds = subs1 <> [send] <> subs2
      batches1 = batchTransmissions' False smpBlockSize $ L.fromList cmds
  all lenOk1 batches1 `shouldBe` False
  length batches1 `shouldBe` 161
  let batches1' = take 60 batches1 <> drop 61 batches1
  all lenOk1 batches1' `shouldBe` True
  length batches1' `shouldBe` 160
  --
  let batches = batchTransmissions' True smpBlockSize $ L.fromList cmds
  length batches `shouldBe` 4
  [TBTransmissions s1 n1 rs1, TBLargeTransmission _, TBTransmissions s2 n2 rs2, TBTransmissions s3 n3 rs3] <- pure batches
  (n1, n2, n3) `shouldBe` (60, 10, 90)
  (length rs1, length rs2, length rs3) `shouldBe` (60, 10, 90)
  all lenOk [s1, s2, s3] `shouldBe` True
  --
  let cmds' = [send] <> subs1 <> subs2
  let batches' = batchTransmissions' True smpBlockSize $ L.fromList cmds'
  length batches' `shouldBe` 3
  [TBLargeTransmission _, TBTransmissions s1' n1' rs1', TBTransmissions s2' n2' rs2'] <- pure batches'
  (n1', n2') `shouldBe` (70, 90)
  (length rs1', length rs2') `shouldBe` (70, 90)
  all lenOk [s1', s2'] `shouldBe` True

randomSUB :: ByteString -> IO (Maybe C.ASignature, ByteString)
randomSUB sessId = do
  g <- C.newRandom
  rId <- atomically $ C.randomBytes 24 g
  corrId <- atomically $ CorrId <$> C.randomBytes 3 g
  (_, rpKey) <- atomically $ C.generateSignatureKeyPair C.SEd448 g
  let s = encodeTransmission (maxVersion supportedSMPServerVRange) sessId (corrId, rId, Cmd SRecipient SUB)
  pure (Just $ C.sign rpKey s, s)

randomSUBCmd :: ProtocolClient ErrorType BrokerMsg -> IO (PCTransmission ErrorType BrokerMsg)
randomSUBCmd c = do
  g <- C.newRandom
  rId <- atomically $ C.randomBytes 24 g
  (_, rpKey) <- atomically $ C.generateSignatureKeyPair C.SEd448 g
  mkTransmission c (Just rpKey, rId, Cmd SRecipient SUB)

randomSEND :: ByteString -> Int -> IO (Maybe C.ASignature, ByteString)
randomSEND sessId len = do
  g <- C.newRandom
  sId <- atomically $ C.randomBytes 24 g
  corrId <- atomically $ CorrId <$> C.randomBytes 3 g
  (_, rpKey) <- atomically $ C.generateSignatureKeyPair C.SEd448 g
  msg <- atomically $ C.randomBytes len g
  let s = encodeTransmission (maxVersion supportedSMPServerVRange) sessId (corrId, sId, Cmd SSender $ SEND noMsgFlags msg)
  pure (Just $ C.sign rpKey s, s)

randomSENDCmd :: ProtocolClient ErrorType BrokerMsg -> Int -> IO (PCTransmission ErrorType BrokerMsg)
randomSENDCmd c len = do
  g <- C.newRandom
  sId <- atomically $ C.randomBytes 24 g
  (_, rpKey) <- atomically $ C.generateSignatureKeyPair C.SEd448 g
  msg <- atomically $ C.randomBytes len g
  mkTransmission c (Just rpKey, sId, Cmd SSender $ SEND noMsgFlags msg)

lenOk :: Builder -> Bool
lenOk s = 0 < len && len <= smpBlockSize - 2
  where
    len = fromIntegral . LB.length $ toLazyByteString s

lenOk1 :: TransportBatch r -> Bool
lenOk1 = \case
  TBTransmission s _ -> lenOk s
  _ -> False
