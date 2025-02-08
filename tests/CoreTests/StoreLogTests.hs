{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module CoreTests.StoreLogTests where

import Control.Concurrent.STM
import Control.Monad
import CoreTests.MsgStoreTests
import Crypto.Random (ChaChaDRG)
import qualified Data.ByteString.Char8 as B
import Data.Either (partitionEithers)
import qualified Data.Map.Strict as M
import SMPClient
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.Env.STM (readWriteQueueStore)
import Simplex.Messaging.Server.MsgStore.Journal
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.STM (STMQueueStore (..))
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Server.StoreLog
import Test.Hspec

testPublicAuthKey :: C.APublicAuthKey
testPublicAuthKey = C.APublicAuthKey C.SEd25519 (C.publicKey "MC4CAQAwBQYDK2VwBCIEIDfEfevydXXfKajz3sRkcQ7RPvfWUPoq6pu1TYHV1DEe")

testNtfCreds :: TVar ChaChaDRG -> IO NtfCreds
testNtfCreds g = do
  (notifierKey, _) <- atomically $ C.generateAuthKeyPair C.SX25519 g
  (k, pk) <- atomically $ C.generateKeyPair @'C.X25519 g
  pure
    NtfCreds
      { notifierId = EntityId "ijkl",
        notifierKey,
        rcvNtfDhSecret = C.dh' k pk
      }

data StoreLogTestCase r s = SLTC {name :: String, saved :: [r], state :: s, compacted :: [r]}

type SMPStoreLogTestCase = StoreLogTestCase StoreLogRecord (M.Map RecipientId QueueRec)

deriving instance Eq QueueRec

deriving instance Eq StoreLogRecord

deriving instance Eq NtfCreds

storeLogTests :: Spec
storeLogTests =
  forM_ [False, True] $ \sndSecure -> do
    ((rId, qr), ntfCreds, date) <- runIO $ do
      g <- C.newRandom
      (,,) <$> testNewQueueRec g sndSecure <*> testNtfCreds g <*> getSystemDate
    testSMPStoreLog
      ("SMP server store log, sndSecure = " <> show sndSecure)
      [ SLTC
          { name = "create new queue",
            saved = [CreateQueue rId qr],
            compacted = [CreateQueue rId qr],
            state = M.fromList [(rId, qr)]
          },
        SLTC
          { name = "secure queue",
            saved = [CreateQueue rId qr, SecureQueue rId testPublicAuthKey],
            compacted = [CreateQueue rId qr {senderKey = Just testPublicAuthKey}],
            state = M.fromList [(rId, qr {senderKey = Just testPublicAuthKey})]
          },
        SLTC
          { name = "create and delete queue",
            saved = [CreateQueue rId qr, DeleteQueue rId],
            compacted = [],
            state = M.fromList []
          },
        SLTC
          { name = "create queue and add notifier",
            saved = [CreateQueue rId qr, AddNotifier rId ntfCreds],
            compacted = [CreateQueue rId qr {notifier = Just ntfCreds}],
            state = M.fromList [(rId, qr {notifier = Just ntfCreds})]
          },
        SLTC
          { name = "delete notifier",
            saved = [CreateQueue rId qr, AddNotifier rId ntfCreds, DeleteNotifier rId],
            compacted = [CreateQueue rId qr],
            state = M.fromList [(rId, qr)]
          },
        SLTC
          { name = "update time",
            saved = [CreateQueue rId qr, UpdateTime rId date],
            compacted = [CreateQueue rId qr {updatedAt = Just date}],
            state = M.fromList [(rId, qr {updatedAt = Just date})]
          }
      ]

testSMPStoreLog :: String -> [SMPStoreLogTestCase] -> Spec
testSMPStoreLog testSuite tests =
  describe testSuite $ forM_ tests $ \t@SLTC {name, saved} -> it name $ do
    l <- openWriteStoreLog testStoreLogFile
    mapM_ (writeStoreLogRecord l) saved
    closeStoreLog l
    replicateM_ 3 $ testReadWrite t
  where
    testReadWrite SLTC {compacted, state} = do
      st <- newMsgStore $ testJournalStoreCfg SQSMemory
      l <- readWriteQueueStore testStoreLogFile st
      storeState st `shouldReturn` state
      closeStoreLog l
      ([], compacted') <- partitionEithers . map strDecode . B.lines <$> B.readFile testStoreLogFile
      compacted' `shouldBe` compacted
    storeState :: JournalMsgStore 'QSMemory -> IO (M.Map RecipientId QueueRec)
    storeState st = M.mapMaybe id <$> (readTVarIO (queues $ stmQueueStore st) >>= mapM (readTVarIO . queueRec))
