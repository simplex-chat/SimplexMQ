{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module CoreTests.MsgStoreTests where

import AgentTests.FunctionalAPITests (runRight, runRight_)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Exception (bracket)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Crypto.Random (ChaChaDRG)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Base64.URL as B64
import Data.List (isPrefixOf, isSuffixOf)
import Data.Maybe (fromJust)
import Data.Time.Clock (addUTCTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Simplex.Messaging.Crypto (pattern MaxLenBS)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (EntityId (..), Message (..), RecipientId, SParty (..), noMsgFlags)
import Simplex.Messaging.Server (MessageStats (..), exportMessages, importMessages, printMessageStats)
import Simplex.Messaging.Server.Env.STM (journalMsgStoreDepth, readWriteQueueStore)
import Simplex.Messaging.Server.Expiration (ExpirationConfig (..), expireBeforeEpoch)
import Simplex.Messaging.Server.MsgStore.Journal
import Simplex.Messaging.Server.MsgStore.STM
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Server.StoreLog (closeStoreLog, logCreateQueue)
import SMPClient (testStoreLogFile, testStoreMsgsDir, testStoreMsgsDir2, testStoreMsgsFile, testStoreMsgsFile2)
import System.Directory (copyFile, createDirectoryIfMissing, listDirectory, removeFile, renameFile)
import System.FilePath ((</>))
import System.IO (IOMode (..), withFile)
import Test.Hspec

msgStoreTests :: Spec
msgStoreTests = do
  around (withMsgStore testSMTStoreConfig) $ describe "STM message store" someMsgStoreTests
  around (withMsgStore $ testJournalStoreCfg MQStoreCfg) $ describe "Journal message store" $ do
    someMsgStoreTests
    it "should export and import journal store" testExportImportStore
    describe "queue state" $ do
      it "should restore queue state from the last line" testQueueState
      it "should recover when message is written and state is not" testMessageState
      it "should remove journal files when queue is empty" testRemoveJournals
    describe "missing files" $ do
      it "should create read file when missing" testReadFileMissing
      it "should switch to write file when read file missing" testReadFileMissingSwitch
      it "should create write file when missing" testWriteFileMissing
      it "should create read file when read and write files are missing" testReadAndWriteFilesMissing
  describe "Journal message store: queue state backup expiration" $ do
    it "should remove old queue state backups" testRemoveQueueStateBackups
    it "should expire messages in idle queues" testExpireIdleQueues
  where
    someMsgStoreTests :: MsgStoreClass s => SpecWith s
    someMsgStoreTests = do
      it "should get queue and store/read messages" testGetQueue
      it "should not fail on EOF when changing read journal" testChangeReadJournal

-- TODO constrain to STM stores?
withMsgStore :: MsgStoreClass s => MsgStoreConfig s -> (s -> IO ()) -> IO ()
withMsgStore cfg = bracket (newMsgStore cfg) closeMsgStore

testSMTStoreConfig :: STMStoreConfig
testSMTStoreConfig = STMStoreConfig {storePath = Nothing, quota = 3}

testJournalStoreCfg :: QStoreCfg s -> JournalStoreConfig s
testJournalStoreCfg queueStoreCfg =
  JournalStoreConfig
    { storePath = testStoreMsgsDir,
      pathParts = journalMsgStoreDepth,
      queueStoreCfg,
      quota = 3,
      maxMsgCount = 4,
      maxStateLines = 2,
      stateTailSize = 256,
      idleInterval = 21600,
      expireBackupsAfter = 0,
      keepMinBackups = 1
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

testNewQueueRec :: TVar ChaChaDRG -> Bool -> IO (RecipientId, QueueRec)
testNewQueueRec g sndSecure = do
  rId <- atomically $ EntityId <$> C.randomBytes 24 g
  senderId <- atomically $ EntityId <$> C.randomBytes 24 g
  (recipientKey, _) <- atomically $ C.generateAuthKeyPair C.SX25519 g
  (k, pk) <- atomically $ C.generateKeyPair @'C.X25519 g
  let qr =
        QueueRec
          { recipientKey,
            rcvDhSecret = C.dh' k pk,
            senderId,
            senderKey = Nothing,
            sndSecure,
            notifier = Nothing,
            status = EntityActive,
            updatedAt = Nothing
          }
  pure (rId, qr)

-- TODO constrain to STM stores
testGetQueue :: MsgStoreClass s => s -> IO ()
testGetQueue ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  runRight_ $ do
    q <- ExceptT $ addQueue ms rId qr
    let write s = writeMsg ms q True =<< mkMessage s
    Just (Message {msgId = mId1}, True) <- write "message 1"
    Just (Message {msgId = mId2}, False) <- write "message 2"
    Just (Message {msgId = mId3}, False) <- write "message 3"
    Msg "message 1" <- tryPeekMsg ms q
    Msg "message 1" <- tryPeekMsg ms q
    Nothing <- tryDelMsg ms q mId2
    Msg "message 1" <- tryDelMsg ms q mId1
    Nothing <- tryDelMsg ms q mId1
    Msg "message 2" <- tryPeekMsg ms q
    Nothing <- tryDelMsg ms q mId1
    (Nothing, Msg "message 2") <- tryDelPeekMsg ms q mId1
    (Msg "message 2", Msg "message 3") <- tryDelPeekMsg ms q mId2
    (Nothing, Msg "message 3") <- tryDelPeekMsg ms q mId2
    Msg "message 3" <- tryPeekMsg ms q
    (Msg "message 3", Nothing) <- tryDelPeekMsg ms q mId3
    Nothing <- tryDelMsg ms q mId2
    Nothing <- tryDelMsg ms q mId3
    Nothing <- tryPeekMsg ms q
    Just (Message {msgId = mId4}, True) <- write "message 4"
    Msg "message 4" <- tryPeekMsg ms q
    Just (Message {msgId = mId5}, False) <- write "message 5"
    (Nothing, Msg "message 4") <- tryDelPeekMsg ms q mId3
    (Msg "message 4", Msg "message 5") <- tryDelPeekMsg ms q mId4
    Just (Message {msgId = mId6}, False) <- write "message 6"
    Just (Message {msgId = mId7}, False) <- write "message 7"
    Nothing <- write "message 8"
    Msg "message 5" <- tryPeekMsg ms q
    (Nothing, Msg "message 5") <- tryDelPeekMsg ms q mId4
    (Msg "message 5", Msg "message 6") <- tryDelPeekMsg ms q mId5
    (Msg "message 6", Msg "message 7") <- tryDelPeekMsg ms q mId6
    (Msg "message 7", Just MessageQuota {msgId = mId8}) <- tryDelPeekMsg ms q mId7
    (Just MessageQuota {}, Nothing) <- tryDelPeekMsg ms q mId8
    (Nothing, Nothing) <- tryDelPeekMsg ms q mId8
    void $ ExceptT $ deleteQueue ms q

-- TODO constrain to STM stores
testChangeReadJournal :: MsgStoreClass s => s -> IO ()
testChangeReadJournal ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  runRight_ $ do
    q <- ExceptT $ addQueue ms rId qr
    let write s = writeMsg ms q True =<< mkMessage s
    Just (Message {msgId = mId1}, True) <- write "message 1"
    (Msg "message 1", Nothing) <- tryDelPeekMsg ms q mId1
    Just (Message {msgId = mId2}, True) <- write "message 2"
    (Msg "message 2", Nothing) <- tryDelPeekMsg ms q mId2
    Just (Message {msgId = mId3}, True) <- write "message 3"
    (Msg "message 3", Nothing) <- tryDelPeekMsg ms q mId3
    Just (Message {msgId = mId4}, True) <- write "message 4"
    (Msg "message 4", Nothing) <- tryDelPeekMsg ms q mId4
    Just (Message {msgId = mId5}, True) <- write "message 5"
    (Msg "message 5", Nothing) <- tryDelPeekMsg ms q mId5
    void $ ExceptT $ deleteQueue ms q

testExportImportStore :: JournalMsgStore 'QSMemory -> IO ()
testExportImportStore ms = do
  g <- C.newRandom
  (rId1, qr1) <- testNewQueueRec g True
  (rId2, qr2) <- testNewQueueRec g True
  sl <- readWriteQueueStore True (mkQueue ms) testStoreLogFile $ queueStore ms
  runRight_ $ do
    let write q s = writeMsg ms q True =<< mkMessage s
    q1 <- ExceptT $ addQueue ms rId1 qr1
    liftIO $ logCreateQueue sl rId1 qr1
    Just (Message {}, True) <- write q1 "message 1"
    Just (Message {}, False) <- write q1 "message 2"
    q2 <- ExceptT $ addQueue ms rId2 qr2
    liftIO $ logCreateQueue sl rId2 qr2
    Just (Message {msgId = mId3}, True) <- write q2 "message 3"
    Just (Message {msgId = mId4}, False) <- write q2 "message 4"
    (Msg "message 3", Msg "message 4") <- tryDelPeekMsg ms q2 mId3
    (Msg "message 4", Nothing) <- tryDelPeekMsg ms q2 mId4
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
  closeStoreLog sl
  exportMessages False ms testStoreMsgsFile False
  (B.readFile testStoreMsgsFile `shouldReturn`) =<< B.readFile (testStoreMsgsFile <> ".copy")
  let cfg = (testJournalStoreCfg MQStoreCfg :: JournalStoreConfig 'QSMemory) {storePath = testStoreMsgsDir2}
  ms' <- newMsgStore cfg
  readWriteQueueStore True (mkQueue ms') testStoreLogFile (queueStore ms') >>= closeStoreLog
  stats@MessageStats {storedMsgsCount = 5, expiredMsgsCount = 0, storedQueues = 2} <-
    importMessages False ms' testStoreMsgsFile Nothing
  printMessageStats "Messages" stats
  length <$> listDirectory (msgQueueDirectory ms rId1) `shouldReturn` 2
  length <$> listDirectory (msgQueueDirectory ms rId2) `shouldReturn` 4 -- state file is backed up, 2 message files
  exportMessages False ms' testStoreMsgsFile2 False
  (B.readFile testStoreMsgsFile2 `shouldReturn`) =<< B.readFile (testStoreMsgsFile <> ".bak")
  stmStore <- newMsgStore testSMTStoreConfig
  readWriteQueueStore True (mkQueue stmStore) testStoreLogFile (queueStore stmStore) >>= closeStoreLog
  MessageStats {storedMsgsCount = 5, expiredMsgsCount = 0, storedQueues = 2} <-
    importMessages False stmStore testStoreMsgsFile2 Nothing
  exportMessages False stmStore testStoreMsgsFile False
  (B.sort <$> B.readFile testStoreMsgsFile `shouldReturn`) =<< (B.sort <$> B.readFile (testStoreMsgsFile2 <> ".bak"))

testQueueState :: JournalMsgStore s -> IO ()
testQueueState ms = do
  g <- C.newRandom
  rId <- EntityId <$> atomically (C.randomBytes 24 g)
  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
  createDirectoryIfMissing True dir
  state <- newMsgQueueState <$> newJournalId (random ms)
  withFile statePath WriteMode (`appendState` state)
  length . lines <$> readFile statePath `shouldReturn` 1
  readQueueState ms statePath `shouldReturn` (Just state, False)
  length <$> listDirectory dir `shouldReturn` 1 -- no backup

  let state1 =
        state
          { size = 1,
            readState = (readState state) {msgCount = 1, byteCount = 100},
            writeState = (writeState state) {msgPos = 1, msgCount = 1, bytePos = 100, byteCount = 100}
          }
  withFile statePath AppendMode (`appendState` state1)
  length . lines <$> readFile statePath `shouldReturn` 2
  readQueueState ms statePath `shouldReturn` (Just state1, False)
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
  readQueueState ms statePath `shouldReturn` (Just state2, True)
  length <$> listDirectory dir `shouldReturn` 2 -- new state + copy
  ls <- lines <$> readFile statePath
  length ls `shouldBe` 3
  -- mock compacting file
  writeFile statePath $ last ls

  -- corrupt the only line
  corruptFile statePath
  (Nothing, True) <- readQueueState ms statePath

  -- corrupt the last line
  renameFile (statePath <> ".2") statePath
  removeOtherFiles dir statePath
  length . lines <$> readFile statePath `shouldReturn` 3
  corruptFile statePath
  readQueueState ms statePath `shouldReturn` (Just state1, True)
  length <$> listDirectory dir `shouldReturn` 1
  length . lines <$> readFile statePath `shouldReturn` 3
  where
    corruptFile f = do
      s <- readFile f
      removeFile f
      writeFile f $ take (length s - 4) s
    removeOtherFiles dir keep = do
      names <- listDirectory dir
      forM_ names $ \name ->
        let f = dir </> name
         in unless (f == keep) $ removeFile f

testMessageState :: JournalMsgStore s -> IO ()
testMessageState ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
      write q s = writeMsg ms q True =<< mkMessage s

  mId1 <- runRight $ do
    q <- ExceptT $ addQueue ms rId qr
    Just (Message {msgId = mId1}, True) <- write q "message 1"
    Just (Message {}, False) <- write q "message 2"
    liftIO $ closeMsgQueue q
    pure mId1

  ls <- B.lines <$> B.readFile statePath
  B.writeFile statePath $ B.unlines $ take (length ls - 1) ls

  runRight_ $ do
    q <- ExceptT $ getQueue ms SRecipient rId
    Just (Message {msgId = mId3}, False) <- write q "message 3"
    (Msg "message 1", Msg "message 3") <- tryDelPeekMsg ms q mId1
    (Msg "message 3", Nothing) <- tryDelPeekMsg ms q mId3
    liftIO $ closeMsgQueue q

testRemoveJournals :: JournalMsgStore s -> IO ()
testRemoveJournals ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
      write q s = writeMsg ms q True =<< mkMessage s

  runRight $ do
    q <- ExceptT $ addQueue ms rId qr
    Just (Message {msgId = mId1}, True) <- write q "message 1"
    Just (Message {msgId = mId2}, False) <- write q "message 2"    
    (Msg "message 1", Msg "message 2") <- tryDelPeekMsg ms q mId1
    (Msg "message 2", Nothing) <- tryDelPeekMsg ms q mId2
    liftIO $ closeMsgQueue q

  ls <- B.lines <$> B.readFile statePath
  length ls `shouldBe` 4
  journalFilesCount dir `shouldReturn` 1
  stateBackupCount dir `shouldReturn` 0

  runRight $ do
    q <- ExceptT $ getQueue ms SRecipient rId
    -- not removed yet
    liftIO $ journalFilesCount dir `shouldReturn` 1
    liftIO $ stateBackupCount dir `shouldReturn` 0
    Nothing <- tryPeekMsg ms q
    -- still not removed, queue is empty and not opened
    liftIO $ journalFilesCount dir `shouldReturn` 1
    _mq <- isolateQueue q "test" $ getMsgQueue ms q False
    -- journal is removed
    liftIO $ journalFilesCount dir `shouldReturn` 0
    liftIO $ stateBackupCount dir `shouldReturn` 1
    Just (Message {msgId = mId3}, True) <- write q "message 3"
    -- journal is created
    liftIO $ journalFilesCount dir `shouldReturn` 1
    Just (Message {msgId = mId4}, False) <- write q "message 4"
    (Msg "message 3", Msg "message 4") <- tryDelPeekMsg ms q mId3
    (Msg "message 4", Nothing) <- tryDelPeekMsg ms q mId4
    Just (Message {msgId = mId5}, True) <- write q "message 5"
    Just (Message {msgId = mId6}, False) <- write q "message 6"
    liftIO $ journalFilesCount dir `shouldReturn` 1
    Just (Message {msgId = mId7}, False) <- write q "message 7"
    -- separate write journal is created
    liftIO $ journalFilesCount dir `shouldReturn` 2
    Nothing <- write q "message 8"
    (Msg "message 5", Msg "message 6") <- tryDelPeekMsg ms q mId5
    liftIO $ journalFilesCount dir `shouldReturn` 2
    (Msg "message 6", Msg "message 7") <- tryDelPeekMsg ms q mId6
    -- read journal is removed
    liftIO $ journalFilesCount dir `shouldReturn` 1
    (Msg "message 7", Just MessageQuota {msgId = mId8}) <- tryDelPeekMsg ms q mId7
    (Just MessageQuota {}, Nothing) <- tryDelPeekMsg ms q mId8
    liftIO $ closeMsgQueue q

  journalFilesCount dir `shouldReturn` 1
  runRight $ do
    q <- ExceptT $ getQueue ms SRecipient rId
    Just (Message {}, True) <- write q "message 8"
    liftIO $ journalFilesCount dir `shouldReturn` 1
    liftIO $ stateBackupCount dir `shouldReturn` 2
    liftIO $ closeMsgQueue q
  where
    journalFilesCount dir = length . filter ("messages." `isPrefixOf`) <$> listDirectory dir
    stateBackupCount dir = length . filter (".bak" `isSuffixOf`) <$> listDirectory dir

testRemoveQueueStateBackups :: IO ()
testRemoveQueueStateBackups = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True

  ms' <- newMsgStore (testJournalStoreCfg MQStoreCfg) {maxStateLines = 1, expireBackupsAfter = 0, keepMinBackups = 0}
  -- set expiration time 1 second ahead
  let ms = ms' {expireBackupsBefore = addUTCTime 1 $ expireBackupsBefore ms'}

  let dir = msgQueueDirectory ms rId
      write q s = writeMsg ms q True =<< mkMessage s

  runRight $ do
    q <- ExceptT $ addQueue ms rId qr
    Just (Message {msgId = mId1}, True) <- write q "message 1"
    Just (Message {msgId = mId2}, False) <- write q "message 2"
    (Msg "message 1", Msg "message 2") <- tryDelPeekMsg ms q mId1
    (Msg "message 2", Nothing) <- tryDelPeekMsg ms q mId2
    liftIO $ closeMsgQueue q
    liftIO $ stateBackupCount dir `shouldReturn` 0

    q1 <- ExceptT $ getQueue ms SRecipient rId
    Just (Message {}, True) <- write q1 "message 3"
    Just (Message {}, False) <- write q1 "message 4"
    liftIO $ closeMsgQueue q1
    liftIO $ stateBackupCount dir `shouldReturn` 0

    liftIO $ threadDelay 1000000
    q2 <- ExceptT $ getQueue ms SRecipient rId
    Just (Message {}, False) <- write q2 "message 5"
    Nothing <- write q2 "message 5"
    liftIO $ closeMsgQueue q2
    liftIO $ stateBackupCount dir `shouldReturn` 1
  where
    stateBackupCount dir = length . filter (".bak" `isSuffixOf`) <$> listDirectory dir

testExpireIdleQueues :: IO ()
testExpireIdleQueues = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True

  ms <- newMsgStore (testJournalStoreCfg MQStoreCfg) {idleInterval = 0}

  let dir = msgQueueDirectory ms rId
      statePath = msgQueueStatePath dir $ B.unpack (B64.encode $ unEntityId rId)
      write q s = writeMsg ms q True =<< mkMessage s

  q <- runRight $ do
    q <- ExceptT $ addQueue ms rId qr
    Just (Message {msgId = mId1}, True) <- write q "message 1"
    Just (Message {msgId = mId2}, False) <- write q "message 2"
    (Msg "message 1", Msg "message 2") <- tryDelPeekMsg ms q mId1
    (Msg "message 2", Nothing) <- tryDelPeekMsg ms q mId2
    liftIO $ closeMsgQueue q
    pure q

  (Just MsgQueueState {size = 0, readState = rs, writeState = ws}, True) <- readQueueState ms statePath
  msgCount rs `shouldBe` 2
  msgCount ws `shouldBe` 2

  old <- expireBeforeEpoch ExpirationConfig {ttl = 1, checkInterval = 1} -- no old messages
  now <- systemSeconds <$> getSystemTime

  (expired_, stored) <- runRight $ idleDeleteExpiredMsgs now ms q old
  expired_ `shouldBe` Just 0
  stored `shouldBe` 0
  (Nothing, False) <- readQueueState ms statePath
  pure ()

testReadFileMissing :: JournalMsgStore s -> IO ()
testReadFileMissing ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  let write q s = writeMsg ms q True =<< mkMessage s
  q <- runRight $ do
    q <- ExceptT $ addQueue ms rId qr
    Just (Message {}, True) <- write q "message 1"
    Msg "message 1" <- tryPeekMsg ms q
    pure q

  mq <- fromJust <$> readTVarIO (msgQueue q)
  MsgQueueState {readState = rs} <- readTVarIO $ state mq
  closeMsgStore ms
  let path = journalFilePath (queueDirectory $ queue mq) $ journalId rs
  removeFile path

  runRight_ $ do
    q' <- ExceptT $ getQueue ms SRecipient rId
    Nothing <- tryPeekMsg ms q'
    Just (Message {}, True) <- write q' "message 2"
    Msg "message 2" <- tryPeekMsg ms q'
    pure ()

testReadFileMissingSwitch :: JournalMsgStore s -> IO ()
testReadFileMissingSwitch ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  q <- writeMessages ms rId qr

  mq <- fromJust <$> readTVarIO (msgQueue q)
  MsgQueueState {readState = rs} <- readTVarIO $ state mq
  closeMsgStore ms
  let path = journalFilePath (queueDirectory $ queue mq) $ journalId rs
  removeFile path

  runRight_ $ do
    q' <- ExceptT $ getQueue ms SRecipient rId
    Just (Message {}, False) <- writeMsg ms q' True =<< mkMessage "message 6"
    Msg "message 5" <- tryPeekMsg ms q'
    pure ()

testWriteFileMissing :: JournalMsgStore s -> IO ()
testWriteFileMissing ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  q <- writeMessages ms rId qr

  mq <- fromJust <$> readTVarIO (msgQueue q)
  MsgQueueState {writeState = ws} <- readTVarIO $ state mq
  closeMsgStore ms
  let path = journalFilePath (queueDirectory $ queue mq) $ journalId ws
  print path
  removeFile path

  runRight_ $ do
    q' <- ExceptT $ getQueue ms SRecipient rId
    Just Message {msgId = mId3} <- tryPeekMsg ms q'
    (Msg "message 3", Msg "message 4") <- tryDelPeekMsg ms q' mId3
    Just Message {msgId = mId4} <- tryPeekMsg ms q'
    (Msg "message 4", Nothing) <- tryDelPeekMsg ms q' mId4
    Just (Message {}, True) <- writeMsg ms q' True =<< mkMessage "message 6"
    Msg "message 6" <- tryPeekMsg ms q'
    pure ()

testReadAndWriteFilesMissing :: JournalMsgStore s -> IO ()
testReadAndWriteFilesMissing ms = do
  g <- C.newRandom
  (rId, qr) <- testNewQueueRec g True
  q <- writeMessages ms rId qr

  mq <- fromJust <$> readTVarIO (msgQueue q)
  MsgQueueState {readState = rs, writeState = ws} <- readTVarIO $ state mq
  closeMsgStore ms
  removeFile $ journalFilePath (queueDirectory $ queue mq) $ journalId rs
  removeFile $ journalFilePath (queueDirectory $ queue mq) $ journalId ws

  runRight_ $ do
    q' <- ExceptT $ getQueue ms SRecipient rId
    Nothing <- tryPeekMsg ms q'
    Just (Message {}, True) <- writeMsg ms q' True =<< mkMessage "message 6"
    Msg "message 6" <- tryPeekMsg ms q'
    pure ()

writeMessages :: JournalMsgStore s -> RecipientId -> QueueRec -> IO (JournalQueue s)
writeMessages ms rId qr = runRight $ do
  q <- ExceptT $ addQueue ms rId qr
  let write s = writeMsg ms q True =<< mkMessage s
  Just (Message {msgId = mId1}, True) <- write "message 1"
  Just (Message {msgId = mId2}, False) <- write "message 2"
  Just (Message {}, False) <- write "message 3"
  (Msg "message 1", Msg "message 2") <- tryDelPeekMsg ms q mId1
  (Msg "message 2", Msg "message 3") <- tryDelPeekMsg ms q mId2
  Just (Message {}, False) <- write "message 4"
  Just (Message {}, False) <- write "message 5"
  pure q
