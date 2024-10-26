{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module CoreTests.MsgStoreTests where

import AgentTests.FunctionalAPITests (runRight, runRight_)
import Control.Concurrent.STM
import Control.Exception (bracket)
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Base64.URL as B64
import Data.Time.Clock.System (getSystemTime)
import Simplex.Messaging.Crypto (pattern MaxLenBS)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (EntityId (..), Message (..), noMsgFlags)
import Simplex.Messaging.Server (MessageStats (..), exportMessages, importMessages, printMessageStats)
import Simplex.Messaging.Server.Env.STM (journalMsgStoreDepth)
import Simplex.Messaging.Server.MsgStore.Journal
import Simplex.Messaging.Server.MsgStore.STM
import Simplex.Messaging.Server.MsgStore.Types
import SMPClient (testStoreMsgsDir, testStoreMsgsDir2, testStoreMsgsFile, testStoreMsgsFile2)
import System.Directory (copyFile, createDirectoryIfMissing, listDirectory, removeFile, renameFile)
import System.FilePath ((</>))
import System.IO (IOMode (..), hClose, withFile)
import Test.Hspec

msgStoreTests :: Spec
msgStoreTests = do
  around (withMsgStore testSMTStoreConfig) $ describe "STM message store" someMsgStoreTests
  around (withMsgStore testJournalStoreCfg) $ describe "Journal message store" $ do
    someMsgStoreTests
    it "should export and import journal store" testExportImportStore
    describe "queue state" $ do
      it "should restore queue state from the last line" testQueueState
      it "should recover when message is written and state is not" testMessageState
  where
    someMsgStoreTests :: MsgStoreClass s => SpecWith s
    someMsgStoreTests = do
      it "should get queue and store/read messages" testGetQueue
      it "should not fail on EOF when changing read journal" testChangeReadJournal

withMsgStore :: MsgStoreClass s => MsgStoreConfig s -> (s -> IO ()) -> IO ()
withMsgStore cfg = bracket (newMsgStore cfg) closeMsgStore

testSMTStoreConfig :: STMStoreConfig
testSMTStoreConfig = STMStoreConfig {storePath = Nothing, quota = 3}

testJournalStoreCfg :: JournalStoreConfig
testJournalStoreCfg =
  JournalStoreConfig
    { storePath = testStoreMsgsDir,
      pathParts = journalMsgStoreDepth,
      quota = 3,
      maxMsgCount = 4,
      maxStateLines = 2
    }

mkMessage :: MonadIO m => ByteString -> m Message
mkMessage body = liftIO $ do
  g <- C.newRandom
  msgTs <- getSystemTime
  msgId <- atomically $ C.randomBytes 24 g
  pure Message {msgId, msgTs, msgFlags = noMsgFlags, msgBody = C.unsafeMaxLenBS body}

pattern Msg :: ByteString -> Maybe Message
pattern Msg s <- Just Message {msgBody = MaxLenBS s}

deriving instance Eq MsgQueueState

deriving instance Eq (JournalState t)

deriving instance Eq (SJournalType t)

testGetQueue :: MsgStoreClass s => s -> IO ()
testGetQueue ms = do
  g <- C.newRandom
  rId <- EntityId <$> atomically (C.randomBytes 24 g)
  runRight_ $ do
    q <- getMsgQueue ms rId
    let write s = writeMsg ms q True =<< mkMessage s
    Just (Message {msgId = mId1}, True) <- write "message 1"
    Just (Message {msgId = mId2}, False) <- write "message 2"
    Just (Message {msgId = mId3}, False) <- write "message 3"
    Msg "message 1" <- tryPeekMsg q
    Msg "message 1" <- tryPeekMsg q
    Nothing <- tryDelMsg q mId2
    Msg "message 1" <- tryDelMsg q mId1
    Nothing <- tryDelMsg q mId1
    Msg "message 2" <- tryPeekMsg q
    Nothing <- tryDelMsg q mId1
    (Nothing, Msg "message 2") <- tryDelPeekMsg q mId1
    (Msg "message 2", Msg "message 3") <- tryDelPeekMsg q mId2
    (Nothing, Msg "message 3") <- tryDelPeekMsg q mId2
    Msg "message 3" <- tryPeekMsg q
    (Msg "message 3", Nothing) <- tryDelPeekMsg q mId3
    Nothing <- tryDelMsg q mId2
    Nothing <- tryDelMsg q mId3
    Nothing <- tryPeekMsg q
    Just (Message {msgId = mId4}, True) <- write "message 4"
    Msg "message 4" <- tryPeekMsg q
    Just (Message {msgId = mId5}, False) <- write "message 5"
    (Nothing, Msg "message 4") <- tryDelPeekMsg q mId3
    (Msg "message 4", Msg "message 5") <- tryDelPeekMsg q mId4
    Just (Message {msgId = mId6}, False) <- write "message 6"
    Just (Message {msgId = mId7}, False) <- write "message 7"
    Nothing <- write "message 8"
    Msg "message 5" <- tryPeekMsg q
    (Nothing, Msg "message 5") <- tryDelPeekMsg q mId4
    (Msg "message 5", Msg "message 6") <- tryDelPeekMsg q mId5
    (Msg "message 6", Msg "message 7") <- tryDelPeekMsg q mId6
    (Msg "message 7", Just MessageQuota {msgId = mId8}) <- tryDelPeekMsg q mId7
    (Just MessageQuota {}, Nothing) <- tryDelPeekMsg q mId8
    (Nothing, Nothing) <- tryDelPeekMsg q mId8
    pure ()
  delMsgQueue ms rId

testChangeReadJournal :: MsgStoreClass s => s -> IO ()
testChangeReadJournal ms = do
  g <- C.newRandom
  rId <- EntityId <$> atomically (C.randomBytes 24 g)
  runRight_ $ do
    q <- getMsgQueue ms rId
    let write s = writeMsg ms q True =<< mkMessage s
    Just (Message {msgId = mId1}, True) <- write "message 1"
    (Msg "message 1", Nothing) <- tryDelPeekMsg q mId1
    Just (Message {msgId = mId2}, True) <- write "message 2"
    (Msg "message 2", Nothing) <- tryDelPeekMsg q mId2
    Just (Message {msgId = mId3}, True) <- write "message 3"
    (Msg "message 3", Nothing) <- tryDelPeekMsg q mId3
    Just (Message {msgId = mId4}, True) <- write "message 4"
    (Msg "message 4", Nothing) <- tryDelPeekMsg q mId4
    Just (Message {msgId = mId5}, True) <- write "message 5"
    (Msg "message 5", Nothing) <- tryDelPeekMsg q mId5
    pure ()
  delMsgQueue ms rId

testExportImportStore :: JournalMsgStore -> IO ()
testExportImportStore ms = do
  g <- C.newRandom
  rId1 <- EntityId <$> atomically (C.randomBytes 24 g)
  rId2 <- EntityId <$> atomically (C.randomBytes 24 g)
  runRight_ $ do
    let write q s = writeMsg ms q True =<< mkMessage s
    q1 <- getMsgQueue ms rId1
    Just (Message {}, True) <- write q1 "message 1"
    Just (Message {}, False) <- write q1 "message 2"
    q2 <- getMsgQueue ms rId2
    Just (Message {msgId = mId3}, True) <- write q2 "message 3"
    Just (Message {msgId = mId4}, False) <- write q2 "message 4"
    (Msg "message 3", Msg "message 4") <- tryDelPeekMsg q2 mId3
    (Msg "message 4", Nothing) <- tryDelPeekMsg q2 mId4
    Just (Message {}, True) <- write q2 "message 5"
    Just (Message {}, False) <- write q2 "message 6"
    Just (Message {}, False) <- write q2 "message 7"
    Nothing <- write q2 "message 8"
    pure ()
  length <$> listDirectory (msgQueueDirectory ms rId1) `shouldReturn` 2
  length <$> listDirectory (msgQueueDirectory ms rId2) `shouldReturn` 3
  exportMessages False ms testStoreMsgsFile False
  renameFile testStoreMsgsFile (testStoreMsgsFile <> ".copy")
  closeMsgStore ms
  exportMessages False ms testStoreMsgsFile False
  (B.readFile testStoreMsgsFile `shouldReturn`) =<< B.readFile (testStoreMsgsFile <> ".copy")
  let cfg = (testJournalStoreCfg :: JournalStoreConfig) {storePath = testStoreMsgsDir2}
  ms' <- newMsgStore cfg
  stats@MessageStats {storedMsgsCount = 5, expiredMsgsCount = 0, storedQueues = 2} <-
    importMessages False ms' testStoreMsgsFile Nothing
  printMessageStats "Messages" stats
  length <$> listDirectory (msgQueueDirectory ms rId1) `shouldReturn` 2
  length <$> listDirectory (msgQueueDirectory ms rId2) `shouldReturn` 4 -- state file is backed up, 2 message files
  exportMessages False ms' testStoreMsgsFile2 False
  (B.readFile testStoreMsgsFile2 `shouldReturn`) =<< B.readFile (testStoreMsgsFile <> ".bak")
  stmStore <- newMsgStore testSMTStoreConfig
  MessageStats {storedMsgsCount = 5, expiredMsgsCount = 0, storedQueues = 2} <-
    importMessages False stmStore testStoreMsgsFile2 Nothing
  exportMessages False stmStore testStoreMsgsFile False
  (B.sort <$> B.readFile testStoreMsgsFile `shouldReturn`) =<< (B.sort <$> B.readFile (testStoreMsgsFile2 <> ".bak"))

testQueueState :: JournalMsgStore -> IO ()
testQueueState ms = do
  g <- C.newRandom
  rId <- EntityId <$> atomically (C.randomBytes 24 g)
  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
  createDirectoryIfMissing True dir
  state <- newMsgQueueState <$> newJournalId (random ms)
  withFile statePath WriteMode (`appendState` state)
  length . lines <$> readFile statePath `shouldReturn` 1
  readQueueState statePath `shouldReturn` state
  length <$> listDirectory dir `shouldReturn` 1 -- no backup

  let state1 =
        state
          { size = 1,
            readState = (readState state) {msgCount = 1, byteCount = 100},
            writeState = (writeState state) {msgPos = 1, msgCount = 1, bytePos = 100, byteCount = 100}
          }
  withFile statePath AppendMode (`appendState` state1)
  length . lines <$> readFile statePath `shouldReturn` 2
  readQueueState statePath `shouldReturn` state1
  length <$> listDirectory dir `shouldReturn` 1 -- no backup

  let state2 =
        state
          { size = 2,
            readState = (readState state) {msgCount = 2, byteCount = 200},
            writeState = (writeState state) {msgPos = 2, msgCount = 2, bytePos = 200, byteCount = 200}
          }
  withFile statePath AppendMode (`appendState` state2)
  length . lines <$> readFile statePath `shouldReturn` 3
  copyFile statePath (statePath <> ".2")
  readQueueState statePath `shouldReturn` state2
  length <$> listDirectory dir `shouldReturn` 3 -- new state, copy + backup
  length . lines <$> readFile statePath `shouldReturn` 1

  -- corrupt the only line
  corruptFile statePath
  newState <- readQueueState statePath
  newState `shouldBe` newMsgQueueState (journalId $ writeState newState)

  -- corrupt the last line
  renameFile (statePath <> ".2") statePath
  removeOtherFiles dir statePath
  length . lines <$> readFile statePath `shouldReturn` 3
  corruptFile statePath
  readQueueState statePath `shouldReturn` state1
  length <$> listDirectory dir `shouldReturn` 2
  length . lines <$> readFile statePath `shouldReturn` 1
  where
    readQueueState statePath = do
      (state, h) <- readWriteQueueState ms statePath
      hClose h
      pure state
    corruptFile f = do
      s <- readFile f
      removeFile f
      writeFile f $ take (length s - 4) s
    removeOtherFiles dir keep = do
      names <- listDirectory dir
      forM_ names $ \name ->
        let f = dir </> name
         in unless (f == keep) $ removeFile f

testMessageState :: JournalMsgStore -> IO ()
testMessageState ms = do
  g <- C.newRandom
  rId <- EntityId <$> atomically (C.randomBytes 24 g)
  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
      write q s = writeMsg ms q True =<< mkMessage s

  mId1 <- runRight $ do
    q <- getMsgQueue ms rId
    Just (Message {msgId = mId1}, True) <- write q "message 1"
    Just (Message {}, False) <- write q "message 2"
    liftIO $ closeMsgQueue ms rId
    pure mId1

  ls <- B.lines <$> B.readFile statePath
  B.writeFile statePath $ B.unlines $ take (length ls - 1) ls

  runRight_ $ do
    q <- getMsgQueue ms rId
    Just (Message {msgId = mId3}, False) <- write q "message 3"
    (Msg "message 1", Msg "message 3") <- tryDelPeekMsg q mId1
    (Msg "message 3", Nothing) <- tryDelPeekMsg q mId3
    liftIO $ closeMsgQueueHandles q
