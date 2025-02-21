{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Server.MsgStore.Journal
  ( JournalMsgStore (random, expireBackupsBefore),
    QStore (..),
    QStoreCfg (..),
    JournalQueue,
    JournalMsgQueue (queue, state),
    JMQueue (queueDirectory, statePath),
    JournalStoreConfig (..),
    closeMsgQueue,
    closeMsgQueueHandles,
    -- below are exported for tests
    MsgQueueState (..),
    JournalState (..),
    SJournalType (..),
    msgQueueDirectory,
    msgQueueStatePath,
    readQueueState,
    newMsgQueueState,
    newJournalId,
    appendState,
    queueLogFileName,
    journalFilePath,
    logFileExt,
    stmQueueStore,
    postgresQueueStore,
  )
where

import Control.Concurrent.STM
import qualified Control.Exception as E
import Control.Logger.Simple
import Control.Monad
import Control.Monad.Trans.Except
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import Data.Int (Int64)
import Data.List (intercalate, sort)
import Data.Maybe (catMaybes, fromMaybe, isNothing, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Data.Time.Format.ISO8601 (iso8601Show, iso8601ParseM)
import GHC.IO (catchAny)
import Simplex.Messaging.Agent.Client (getMapLock, withLockMap)
import Simplex.Messaging.Agent.Lock
import Simplex.Messaging.Agent.Store.Postgres.Common (DBOpts)
import Simplex.Messaging.Agent.Store.Shared (MigrationConfirmation)
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.Postgres
import Simplex.Messaging.Server.QueueStore.STM
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Util (ifM, tshow, whenM, ($>>=), (<$$>))
import System.Directory
import System.Exit
import System.FilePath (takeFileName, (</>))
import System.IO (BufferMode (..), Handle, IOMode (..), SeekMode (..), stdout)
import qualified System.IO as IO
import System.Random (StdGen, genByteString, newStdGen)

data JournalMsgStore s = JournalMsgStore
  { config :: JournalStoreConfig s,
    random :: TVar StdGen,
    queueLocks :: TMap RecipientId Lock,
    queueStore_ :: QStore s,
    expireBackupsBefore :: UTCTime
  }

data QStore (s :: QSType) where
  MQStore :: STMQueueStore (JournalQueue 'QSMemory) -> QStore 'QSMemory
  PQStore :: PostgresQueueStore (JournalQueue 'QSPostgres) -> QStore 'QSPostgres

stmQueueStore :: JournalMsgStore 'QSMemory -> STMQueueStore (JournalQueue 'QSMemory)
stmQueueStore st = case queueStore_ st of
  MQStore st' -> st'

postgresQueueStore :: JournalMsgStore 'QSPostgres -> PostgresQueueStore (JournalQueue 'QSPostgres)
postgresQueueStore st = case queueStore_ st of
  PQStore st' -> st'

data JournalStoreConfig s = JournalStoreConfig
  { storePath :: FilePath,
    pathParts :: Int,
    queueStoreCfg :: QStoreCfg s,
    quota :: Int,
    -- Max number of messages per journal file - ignored in STM store.
    -- When this limit is reached, the file will be changed.
    -- This number should be set bigger than queue quota.
    maxMsgCount :: Int,
    maxStateLines :: Int,
    stateTailSize :: Int,
    -- time in seconds after which the queue will be closed after message expiration
    idleInterval :: Int64,
    -- expire state backup files
    expireBackupsAfter :: NominalDiffTime,
    keepMinBackups :: Int
  }

data QStoreCfg s where
  MQStoreCfg :: QStoreCfg 'QSMemory
  PQStoreCfg :: DBOpts -> MigrationConfirmation -> QStoreCfg 'QSPostgres

data JournalQueue (s :: QSType) = JournalQueue
  { recipientId' :: RecipientId,
    queueLock :: Lock,
    -- To avoid race conditions and errors when restoring queues,
    -- Nothing is written to TVar when queue is deleted.
    queueRec' :: TVar (Maybe QueueRec),
    msgQueue' :: TVar (Maybe (JournalMsgQueue s)),
    -- system time in seconds since epoch
    activeAt :: TVar Int64,
    queueState :: TVar (Maybe QState) -- Nothing - unknown
  }

data QState = QState
  { hasPending :: Bool,
    hasStored :: Bool
  }

data JMQueue = JMQueue
  { queueDirectory :: FilePath,
    statePath :: FilePath
  }

data JournalMsgQueue (s :: QSType) = JournalMsgQueue
  { queue :: JMQueue,
    state :: TVar MsgQueueState,
    -- tipMsg contains last message and length incl. newline
    -- Nothing - unknown, Just Nothing - empty queue.
    -- It  prevents reading each message twice,
    -- and reading it after it was just written.
    tipMsg :: TVar (Maybe (Maybe (Message, Int64))),
    handles :: TVar (Maybe MsgQueueHandles)
  }

data MsgQueueState = MsgQueueState
  { readState :: JournalState 'JTRead,
    writeState :: JournalState 'JTWrite,
    canWrite :: Bool,
    size :: Int
  }
  deriving (Show)

data MsgQueueHandles = MsgQueueHandles
  { stateHandle :: Handle, -- handle to queue state log file, rotates and removes old backups when server is restarted
    readHandle :: Handle,
    writeHandle :: Maybe Handle -- optional, used when write file is different from read file
  }

data JournalState t = JournalState
  { journalType :: SJournalType t,
    journalId :: ByteString,
    msgPos :: Int,
    msgCount :: Int,
    bytePos :: Int64,
    byteCount :: Int64
  }
  deriving (Show)

qState :: MsgQueueState -> QState
qState MsgQueueState {size, readState = rs, writeState = ws} =
  let hasPending = size > 0
   in QState {hasPending, hasStored = hasPending || msgCount rs > 0 || msgCount ws > 0}
{-# INLINE qState #-}

data JournalType = JTRead | JTWrite

data SJournalType (t :: JournalType) where
  SJTRead :: SJournalType 'JTRead
  SJTWrite :: SJournalType 'JTWrite

class JournalTypeI t where sJournalType :: SJournalType t

instance JournalTypeI 'JTRead where sJournalType = SJTRead

instance JournalTypeI 'JTWrite where sJournalType = SJTWrite

deriving instance Show (SJournalType t)

newMsgQueueState :: ByteString -> MsgQueueState
newMsgQueueState journalId =
  MsgQueueState
    { writeState = newJournalState journalId,
      readState = newJournalState journalId,
      canWrite = True,
      size = 0
    }

newJournalState :: JournalTypeI t => ByteString -> JournalState t
newJournalState journalId = JournalState sJournalType journalId 0 0 0 0

journalFilePath :: FilePath -> ByteString -> FilePath
journalFilePath dir journalId = dir </> (msgLogFileName <> "." <> B.unpack journalId <> logFileExt)

instance StrEncoding MsgQueueState where
  strEncode MsgQueueState {writeState, readState, canWrite, size} =
    B.unwords
      [ "write=" <> strEncode writeState,
        "read=" <> strEncode readState,
        "canWrite=" <> strEncode canWrite,
        "size=" <> strEncode size
      ]
  strP = do
    writeState <- "write=" *> strP
    readState <- " read=" *> strP
    canWrite <- " canWrite=" *> strP
    size <- " size=" *> strP
    pure MsgQueueState {writeState, readState, canWrite, size}

instance JournalTypeI t => StrEncoding (JournalState t) where
  strEncode JournalState {journalId, msgPos, msgCount, bytePos, byteCount} =
    B.intercalate "," [journalId, e msgPos, e msgCount, e bytePos, e byteCount]
    where
      e :: StrEncoding a => a -> ByteString
      e = strEncode
  strP = do
    journalId <- A.takeTill (== ',')
    JournalState sJournalType journalId <$> i <*> i <*> i <*> i
    where
      i :: Integral a => A.Parser a
      i = A.char ',' *> A.decimal

queueLogFileName :: String
queueLogFileName = "queue_state"

msgLogFileName :: String
msgLogFileName = "messages"

logFileExt :: String
logFileExt = ".log"

newtype StoreIO (s :: QSType) a = StoreIO {unStoreIO :: IO a}
  deriving newtype (Functor, Applicative, Monad)

instance StoreQueueClass (JournalQueue s) where
  type MsgQueue (JournalQueue s) = JournalMsgQueue s
  recipientId = recipientId'
  {-# INLINE recipientId #-}
  queueRec = queueRec'
  {-# INLINE queueRec #-}
  msgQueue = msgQueue'
  {-# INLINE msgQueue #-}
  withQueueLock :: JournalQueue s -> String -> IO a -> IO a
  withQueueLock = withLock' . queueLock
  {-# INLINE withQueueLock #-}

instance QueueStoreClass (JournalQueue s) (QStore s) where
  type QueueStoreCfg (QStore s) = QStoreCfg s

  newQueueStore :: QStoreCfg s -> IO (QStore s)
  newQueueStore = \case
    MQStoreCfg -> MQStore <$> newQueueStore @(JournalQueue s) ()
    PQStoreCfg dbOpts confirmMigrations -> PQStore <$> newQueueStore @(JournalQueue s) (dbOpts, confirmMigrations)

  loadedQueues = \case
    MQStore st -> loadedQueues st
    PQStore st -> loadedQueues st
  {-# INLINE loadedQueues #-}

  queueCounts = \case
    MQStore st -> queueCounts @(JournalQueue s) st
    PQStore st -> queueCounts @(JournalQueue s) st
  {-# INLINE queueCounts #-}

  addQueue_ = \case
    MQStore st -> addQueue_ st
    PQStore st -> addQueue_ st
  {-# INLINE addQueue_ #-}

  getQueue_ = \case
    MQStore st -> getQueue_ st
    PQStore st -> getQueue_ st
  {-# INLINE getQueue_ #-}

  secureQueue = \case
    MQStore st -> secureQueue st
    PQStore st -> secureQueue st
  {-# INLINE secureQueue #-}

  addQueueNotifier = \case
    MQStore st -> addQueueNotifier st
    PQStore st -> addQueueNotifier st
  {-# INLINE addQueueNotifier #-}

  deleteQueueNotifier = \case
    MQStore st -> deleteQueueNotifier st
    PQStore st -> deleteQueueNotifier st
  {-# INLINE deleteQueueNotifier #-}

  suspendQueue = \case
    MQStore st -> suspendQueue st
    PQStore st -> suspendQueue st
  {-# INLINE suspendQueue #-}

  blockQueue = \case
    MQStore st -> blockQueue st
    PQStore st -> blockQueue st
  {-# INLINE blockQueue #-}

  unblockQueue = \case
    MQStore st -> unblockQueue st
    PQStore st -> unblockQueue st
  {-# INLINE unblockQueue #-}

  updateQueueTime = \case
    MQStore st -> updateQueueTime st
    PQStore st -> updateQueueTime st
  {-# INLINE updateQueueTime #-}

  deleteStoreQueue = \case
    MQStore st -> deleteStoreQueue st
    PQStore st -> deleteStoreQueue st
  {-# INLINE deleteStoreQueue #-}


instance MsgStoreClass (JournalMsgStore s) where
  type StoreMonad (JournalMsgStore s) = StoreIO s
  type QueueStore (JournalMsgStore s) = QStore s
  type StoreQueue (JournalMsgStore s) = JournalQueue s
  type MsgStoreConfig (JournalMsgStore s) = JournalStoreConfig s

  newMsgStore :: JournalStoreConfig s -> IO (JournalMsgStore s)
  newMsgStore config@JournalStoreConfig {queueStoreCfg} = do
    random <- newTVarIO =<< newStdGen
    queueLocks <- TM.emptyIO
    queueStore_ <- newQueueStore @(JournalQueue s) queueStoreCfg
    expireBackupsBefore <- addUTCTime (- expireBackupsAfter config) <$> getCurrentTime
    pure JournalMsgStore {config, random, queueLocks, queueStore_, expireBackupsBefore}

  closeMsgStore :: JournalMsgStore s -> IO ()
  closeMsgStore ms = case queueStore_ ms of
    MQStore st -> do
      readTVarIO (storeLog st) >>= mapM_ closeStoreLog
      closeQueues $ loadedQueues @(JournalQueue s) st
    PQStore st ->
      closeQueues $ loadedQueues @(JournalQueue s) st
    where
      closeQueues qs = readTVarIO qs >>= mapM_ closeMsgQueue

  withActiveMsgQueues :: Monoid a => JournalMsgStore s -> (JournalQueue s -> IO a) -> IO a
  withActiveMsgQueues ms f = case queueStore_ ms of
    MQStore st -> withLoadedQueues st f
    PQStore st -> withLoadedQueues st f

  -- This function is a "foldr" that opens and closes all queues, processes them as defined by action and accumulates the result.
  -- It is used to export storage to a single file and also to expire messages and validate all queues when server is started.
  -- TODO this function requires case-sensitive file system, because it uses queue directory as recipient ID.
  -- It can be made to support case-insensite FS by supporting more than one queue per directory, by getting recipient ID from state file name.
  -- TODO [postgres] this should simply load all known queues and process them
  withAllMsgQueues :: forall a. Monoid a => Bool -> JournalMsgStore s -> (JournalQueue s -> IO a) -> IO a
  withAllMsgQueues tty ms@JournalMsgStore {config} action = ifM (doesDirectoryExist storePath) processStore (pure mempty)
    where
      processStore = do
        (!count, !res) <- foldQueues 0 processQueue (0, mempty) ("", storePath)
        putStrLn $ progress count
        pure res
      JournalStoreConfig {storePath, pathParts} = config
      processQueue :: (Int, a) -> (String, FilePath) -> IO (Int, a)
      processQueue (!i, !r) (queueId, dir) = do
        when (tty && i `mod` 100 == 0) $ putStr (progress i <> "\r") >> IO.hFlush stdout
        r' <- case strDecode $ B.pack queueId of
          Right rId ->
            getQueue ms SRecipient rId >>= \case
              Right q -> unStoreIO (getMsgQueue ms q False) *> action q <* closeMsgQueue q
              Left AUTH -> do
                logWarn $ "STORE: processQueue, queue " <> T.pack queueId <> " was removed, removing " <> T.pack dir
                removeQueueDirectory_ dir
                pure mempty
              Left e -> do
                logError $ "STORE: processQueue, error getting queue " <> T.pack queueId <> ", " <> tshow e
                exitFailure
          Left e -> do
            logError $ "STORE: processQueue, message queue directory " <> T.pack dir <> " is invalid, " <> tshow e
            exitFailure
        pure (i + 1, r <> r')
      progress i = "Processed: " <> show i <> " queues"
      foldQueues depth f acc (queueId, path) = do
        let f' = if depth == pathParts - 1 then f else foldQueues (depth + 1) f
        listDirs >>= foldM f' acc
        where
          listDirs = fmap catMaybes . mapM queuePath =<< listDirectory path
          queuePath dir = do
            let !path' = path </> dir
                !queueId' = queueId <> dir
            ifM
              (doesDirectoryExist path')
              (pure $ Just (queueId', path'))
              (Nothing <$ putStrLn ("Error: path " <> path' <> " is not a directory, skipping"))

  logQueueStates :: JournalMsgStore s -> IO ()
  logQueueStates ms = withActiveMsgQueues ms $ unStoreIO . logQueueState

  logQueueState :: JournalQueue s -> StoreIO s ()
  logQueueState q =
    StoreIO . void $
      readTVarIO (msgQueue' q)
        $>>= \mq -> readTVarIO (handles mq)
        $>>= (\hs -> (readTVarIO (state mq) >>= appendState (stateHandle hs)) $> Just ())

  queueStore = queueStore_
  {-# INLINE queueStore #-}

  mkQueue :: JournalMsgStore s -> RecipientId -> QueueRec -> IO (JournalQueue s)
  mkQueue ms rId qr = do
    queueLock <- atomically $ getMapLock (queueLocks ms) rId
    queueRec' <- newTVarIO $ Just qr
    msgQueue' <- newTVarIO Nothing
    activeAt <- newTVarIO 0
    queueState <- newTVarIO Nothing
    pure $
      JournalQueue
        { recipientId' = rId,
          queueLock,
          queueRec',
          msgQueue',
          activeAt,
          queueState
        }

  getMsgQueue :: JournalMsgStore s -> JournalQueue s -> Bool -> StoreIO s (JournalMsgQueue s)
  getMsgQueue ms@JournalMsgStore {random} q'@JournalQueue {recipientId' = rId, msgQueue'} forWrite =
    StoreIO $ readTVarIO msgQueue' >>= maybe newQ pure
    where
      newQ = do
        let dir = msgQueueDirectory ms rId
            statePath = msgQueueStatePath dir $ B.unpack (strEncode rId)
            queue = JMQueue {queueDirectory = dir, statePath}
        q <- ifM (doesDirectoryExist dir) (openMsgQueue ms queue forWrite) (createQ queue)
        atomically $ writeTVar msgQueue' $ Just q
        st <- readTVarIO $ state q
        atomically $ writeTVar (queueState q') $ Just $! qState st
        pure q
        where
          createQ :: JMQueue -> IO (JournalMsgQueue s)
          createQ queue = do
            -- folder and files are not created here,
            -- to avoid file IO for queues without messages during subscription
            journalId <- newJournalId random
            mkJournalQueue queue (newMsgQueueState journalId) Nothing

  getPeekMsgQueue :: JournalMsgStore s -> JournalQueue s -> StoreIO s (Maybe (JournalMsgQueue s, Message))
  getPeekMsgQueue ms q@JournalQueue {queueState} =
    StoreIO (readTVarIO queueState) >>= \case
      Just QState {hasPending} -> if hasPending then peek else pure Nothing
      Nothing -> do
        -- We only close the queue if we just learnt it's empty.
        -- This is needed to reduce file descriptors and memory usage
        -- after the server just started and many clients subscribe.
        -- In case the queue became non-empty on write and then again empty on read
        -- we won't be closing it, to avoid frequent open/close on active queues.
        r <- peek
        when (isNothing r) $ StoreIO $ closeMsgQueue q
        pure r
    where
      peek = do
        mq <- getMsgQueue ms q False
        (mq,) <$$> tryPeekMsg_ q mq

  -- only runs action if queue is not empty
  withIdleMsgQueue :: Int64 -> JournalMsgStore s -> JournalQueue s -> (JournalMsgQueue s -> StoreIO s a) -> StoreIO s (Maybe a, Int)
  withIdleMsgQueue now ms@JournalMsgStore {config} q@JournalQueue {queueState} action =
    StoreIO $ readTVarIO (msgQueue' q) >>= \case
      Nothing ->
        E.bracket
          getNonEmptyMsgQueue
          (mapM_ $ \_ -> closeMsgQueue q)
          (maybe (pure (Nothing, 0)) (unStoreIO . run))
        where
          run mq = do
            r <- action mq
            sz <- getQueueSize_ mq
            pure (Just r, sz)
      Just mq -> do
        ts <- readTVarIO $ activeAt q
        r <- if now - ts >= idleInterval config
          then Just <$> unStoreIO (action mq) `E.finally` closeMsgQueue q
          else pure Nothing
        sz <- unStoreIO $ getQueueSize_ mq
        pure (r, sz)
    where
      getNonEmptyMsgQueue :: IO (Maybe (JournalMsgQueue s))
      getNonEmptyMsgQueue =
        readTVarIO queueState >>= \case
          Just QState {hasStored}
            | hasStored -> Just <$> unStoreIO (getMsgQueue ms q False)
            | otherwise -> pure Nothing
          Nothing -> do
            mq <- unStoreIO $ getMsgQueue ms q False
            -- queueState was updated in getMsgQueue
            readTVarIO queueState >>= \case
              Just QState {hasStored} | not hasStored -> closeMsgQueue q $> Nothing
              _ -> pure $ Just mq

  deleteQueue :: JournalMsgStore s -> JournalQueue s -> IO (Either ErrorType QueueRec)
  deleteQueue ms q = fst <$$> deleteQueue_ ms q

  deleteQueueSize :: JournalMsgStore s -> JournalQueue s -> IO (Either ErrorType (QueueRec, Int))
  deleteQueueSize ms q =
    deleteQueue_ ms q >>= mapM (traverse getSize)
    -- traverse operates on the second tuple element
    where
      getSize = maybe (pure (-1)) (fmap size . readTVarIO . state)

  getQueueMessages_ :: Bool -> JournalQueue s -> JournalMsgQueue s -> StoreIO s [Message]
  getQueueMessages_ drainMsgs q' q = StoreIO (run [])
    where
      run msgs = readTVarIO (handles q) >>= maybe (pure []) (getMsg msgs)
      getMsg msgs hs = chooseReadJournal q' q drainMsgs hs >>= maybe (pure msgs) readMsg
        where
          readMsg (rs, h) = do
            (msg, len) <- hGetMsgAt h $ bytePos rs
            updateReadPos q' q drainMsgs len hs
            (msg :) <$> run msgs

  writeMsg :: JournalMsgStore s -> JournalQueue s -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  writeMsg ms q' logState msg = isolateQueue q' "writeMsg" $ do
    q <- getMsgQueue ms q' True
    StoreIO $ (`E.finally` updateActiveAt q') $ do
      st@MsgQueueState {canWrite, size} <- readTVarIO (state q)
      let empty = size == 0
      if canWrite || empty
        then do
          let canWrt' = quota > size
          if canWrt'
            then writeToJournal q st canWrt' msg $> Just (msg, empty)
            else writeToJournal q st canWrt' msgQuota $> Nothing
        else pure Nothing
    where
      JournalStoreConfig {quota, maxMsgCount} = config ms
      msgQuota = MessageQuota {msgId = messageId msg, msgTs = messageTs msg}
      writeToJournal q st@MsgQueueState {writeState, readState = rs, size} canWrt' !msg' = do
        let msgStr = strEncode msg' `B.snoc` '\n'
            msgLen = fromIntegral $ B.length msgStr
        hs <- maybe createQueueDir pure =<< readTVarIO handles
        (ws, wh) <- case writeHandle hs of
          Nothing | msgCount writeState >= maxMsgCount -> switchWriteJournal hs
          wh_ -> pure (writeState, fromMaybe (readHandle hs) wh_)
        let msgPos' = msgPos ws + 1
            bytePos' = bytePos ws + msgLen
            ws' = ws {msgPos = msgPos', msgCount = msgPos', bytePos = bytePos', byteCount = bytePos'}
            rs' = if journalId ws == journalId rs then rs {msgCount = msgPos', byteCount = bytePos'} else rs
            !st' = st {writeState = ws', readState = rs', canWrite = canWrt', size = size + 1}
        hAppend wh (bytePos ws) msgStr
        updateQueueState q' q logState hs st' $
          when (size == 0) $ writeTVar (tipMsg q) $ Just (Just (msg, msgLen))
        where
          JournalMsgQueue {queue = JMQueue {queueDirectory, statePath}, handles} = q
          createQueueDir = do
            createDirectoryIfMissing True queueDirectory
            sh <- openFile statePath AppendMode
            rh <- createNewJournal queueDirectory $ journalId rs
            let hs = MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = Nothing}
            atomically $ writeTVar handles $ Just hs
            pure hs
          switchWriteJournal hs = do
            journalId <- newJournalId $ random ms
            wh <- createNewJournal queueDirectory journalId
            atomically $ writeTVar handles $ Just $ hs {writeHandle = Just wh}
            pure (newJournalState journalId, wh)

  -- can ONLY be used while restoring messages, not while server running
  setOverQuota_ :: JournalQueue s -> IO ()
  setOverQuota_ q =
    readTVarIO (msgQueue' q)
      >>= mapM_ (\JournalMsgQueue {state} -> atomically $ modifyTVar' state $ \st -> st {canWrite = False})

  getQueueSize_ :: JournalMsgQueue s -> StoreIO s Int
  getQueueSize_ JournalMsgQueue {state} = StoreIO $ size <$> readTVarIO state

  tryPeekMsg_ :: JournalQueue s -> JournalMsgQueue s -> StoreIO s (Maybe Message)
  tryPeekMsg_ q mq@JournalMsgQueue {tipMsg, handles} =
    StoreIO $ (readTVarIO handles $>>= chooseReadJournal q mq True $>>= peekMsg)
    where
      peekMsg (rs, h) = readTVarIO tipMsg >>= maybe readMsg (pure . fmap fst)
        where
          readMsg = do
            ml@(msg, _) <- hGetMsgAt h $ bytePos rs
            atomically $ writeTVar tipMsg $ Just (Just ml)
            pure $ Just msg

  tryDeleteMsg_ :: JournalQueue s -> JournalMsgQueue s -> Bool -> StoreIO s ()
  tryDeleteMsg_ q mq@JournalMsgQueue {tipMsg, handles} logState = StoreIO $ (`E.finally` when logState (updateActiveAt q)) $
    void $
      readTVarIO tipMsg -- if there is no cached tipMsg, do nothing
        $>>= (pure . fmap snd)
        $>>= \len -> readTVarIO handles
        $>>= \hs -> updateReadPos q mq logState len hs $> Just ()

  isolateQueue :: JournalQueue s -> String -> StoreIO s a -> ExceptT ErrorType IO a
  isolateQueue JournalQueue {recipientId' = rId, queueLock} op a =
    tryStore' op rId $ withLock' queueLock op $ unStoreIO a

updateActiveAt :: JournalQueue s -> IO ()
updateActiveAt q = atomically . writeTVar (activeAt q) . systemSeconds =<< getSystemTime

tryStore' :: String -> RecipientId -> IO a -> ExceptT ErrorType IO a
tryStore' op rId = tryStore op rId . fmap Right

tryStore :: forall a. String -> RecipientId -> IO (Either ErrorType a) -> ExceptT ErrorType IO a
tryStore op rId a = ExceptT $ E.mask_ $ E.try a >>= either storeErr pure
  where
    storeErr :: E.SomeException -> IO (Either ErrorType a)
    storeErr e =
      let e' = intercalate ", " [op, B.unpack $ strEncode rId, show e]
       in logError ("STORE: " <> T.pack e') $> Left (STORE e')

isolateQueueId :: String -> JournalMsgStore s -> RecipientId -> IO (Either ErrorType a) -> ExceptT ErrorType IO a
isolateQueueId op ms rId = tryStore op rId . withLockMap (queueLocks ms) rId op

openMsgQueue :: JournalMsgStore s -> JMQueue -> Bool -> IO (JournalMsgQueue s)
openMsgQueue ms@JournalMsgStore {config} q@JMQueue {queueDirectory = dir, statePath} forWrite = do
  (st_, shouldBackup) <- readQueueState ms statePath
  case st_ of
    Nothing -> do
      st <- newMsgQueueState <$> newJournalId (random ms)
      when shouldBackup $ backupQueueState statePath -- rename invalid state file
      mkJournalQueue q st Nothing
    Just st
      | size st == 0 -> do
          (st', hs_) <- removeJournals st shouldBackup
          mkJournalQueue q st' hs_
      | otherwise -> do
          sh <- openBackupQueueState st shouldBackup
          (st', rh, wh_) <- closeOnException sh $ openJournals ms dir st sh
          let hs = MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = wh_}
          mkJournalQueue q st' (Just hs)
  where
    -- If the queue is empty, journals are deleted.
    -- New journal is created if queue is written to.
    -- canWrite is set to True.
    removeJournals MsgQueueState {readState = rs, writeState = ws} shouldBackup = E.uninterruptibleMask_ $ do
      rjId <- newJournalId $ random ms
      let st = newMsgQueueState rjId
      hs_ <-
        if forWrite
          then Just <$> newJournalHandles st rjId
          else Nothing <$ backupQueueState statePath
      removeJournalIfExists dir rs
      unless (journalId ws == journalId rs) $ removeJournalIfExists dir ws
      pure (st, hs_)
      where
        newJournalHandles st rjId = do
          sh <- openBackupQueueState st shouldBackup
          appendState_ sh st
          rh <- closeOnException sh $ createNewJournal dir rjId
          pure MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = Nothing}
    openBackupQueueState st shouldBackup
      | shouldBackup = do
          -- State backup is made in two steps to mitigate the crash during the backup.
          -- Temporary backup file will be used when it is present.
          let tempBackup = statePath <> ".bak"
          renameFile statePath tempBackup -- 1) temp backup
          sh <- openFile statePath AppendMode
          closeOnException sh $ appendState sh st -- 2) save state to new file
          backupQueueState tempBackup -- 3) timed backup
          pure sh
      | otherwise = openFile statePath AppendMode
    backupQueueState path = do
      ts <- getCurrentTime
      renameFile path $ stateBackupPath statePath ts
      -- remove old backups
      times <- sort . mapMaybe backupPathTime <$> listDirectory dir
      let toDelete = filter (< expireBackupsBefore ms) $ take (length times - keepMinBackups config) times
      mapM_ (safeRemoveFile "removeBackups" . stateBackupPath statePath) toDelete
      where
        backupPathTime :: FilePath -> Maybe UTCTime
        backupPathTime = iso8601ParseM . T.unpack <=< T.stripSuffix ".bak" <=< T.stripPrefix statePathPfx . T.pack
        statePathPfx = T.pack $ takeFileName statePath <> "."

mkJournalQueue :: JMQueue -> MsgQueueState -> Maybe MsgQueueHandles -> IO (JournalMsgQueue s)
mkJournalQueue queue st hs_ = do
  state <- newTVarIO st
  tipMsg <- newTVarIO Nothing
  handles <- newTVarIO hs_
  -- using the same queue lock which is currently locked,
  -- to avoid map lookup on queue operations
  pure JournalMsgQueue {queue, state, tipMsg, handles}

chooseReadJournal :: JournalQueue s -> JournalMsgQueue s -> Bool -> MsgQueueHandles -> IO (Maybe (JournalState 'JTRead, Handle))
chooseReadJournal q' q log' hs = do
  st@MsgQueueState {writeState = ws, readState = rs} <- readTVarIO (state q)
  case writeHandle hs of
    Just wh | msgPos rs >= msgCount rs && journalId rs /= journalId ws -> do
      -- switching to write journal
      atomically $ writeTVar (handles q) $ Just hs {readHandle = wh, writeHandle = Nothing}
      hClose $ readHandle hs
      when log' $ removeJournal (queueDirectory $ queue q) rs
      let !rs' = (newJournalState $ journalId ws) {msgCount = msgCount ws, byteCount = byteCount ws}
          !st' = st {readState = rs'}
      updateQueueState q' q log' hs st' $ pure ()
      pure $ Just (rs', wh)
    _ | msgPos rs >= msgCount rs && journalId rs == journalId ws -> pure Nothing
    _ -> pure $ Just (rs, readHandle hs)

updateQueueState :: JournalQueue s -> JournalMsgQueue s -> Bool -> MsgQueueHandles -> MsgQueueState -> STM () -> IO ()
updateQueueState q' q log' hs st a = do
  unless (validQueueState st) $ E.throwIO $ userError $ "updateQueueState invalid state: " <> show st
  when log' $ appendState (stateHandle hs) st
  atomically $ writeTVar (queueState q') $ Just $! qState st
  atomically $ writeTVar (state q) st >> a

appendState :: Handle -> MsgQueueState -> IO ()
appendState h = E.uninterruptibleMask_ . appendState_ h
{-# INLINE appendState #-}

appendState_ :: Handle -> MsgQueueState -> IO ()
appendState_ h st = B.hPutStr h $ strEncode st `B.snoc` '\n'

updateReadPos :: JournalQueue s -> JournalMsgQueue s -> Bool -> Int64 -> MsgQueueHandles -> IO ()
updateReadPos q' q log' len hs = do
  st@MsgQueueState {readState = rs, size} <- readTVarIO (state q)
  let JournalState {msgPos, bytePos} = rs
  let msgPos' = msgPos + 1
      rs' = rs {msgPos = msgPos', bytePos = bytePos + len}
      st' = st {readState = rs', size = size - 1}
  updateQueueState q' q log' hs st' $ writeTVar (tipMsg q) Nothing

msgQueueDirectory :: JournalMsgStore s -> RecipientId -> FilePath
msgQueueDirectory JournalMsgStore {config = JournalStoreConfig {storePath, pathParts}} rId =
  storePath </> B.unpack (B.intercalate "/" $ splitSegments pathParts $ strEncode rId)
  where
    splitSegments _ "" = []
    splitSegments 1 s = [s]
    splitSegments n s =
      let (seg, s') = B.splitAt 2 s
       in seg : splitSegments (n - 1) s'

msgQueueStatePath :: FilePath -> String -> FilePath
msgQueueStatePath dir queueId = dir </> (queueLogFileName <> "." <> queueId <> logFileExt)

createNewJournal :: FilePath -> ByteString -> IO Handle
createNewJournal dir journalId = do
  let path = journalFilePath dir journalId -- TODO retry if file exists
  h <- openFile path ReadWriteMode
  B.hPutStr h ""
  pure h

newJournalId :: TVar StdGen -> IO ByteString
newJournalId g = strEncode <$> atomically (stateTVar g $ genByteString 12)

openJournals :: JournalMsgStore s -> FilePath -> MsgQueueState -> Handle -> IO (MsgQueueState, Handle, Maybe Handle)
openJournals ms dir st@MsgQueueState {readState = rs, writeState = ws} sh = do
  let rjId = journalId rs
      wjId = journalId ws
  openJournal rs >>= \case
    Left path
      | rjId == wjId -> do
          logError $ "STORE: openJournals, no read/write file - creating new file, " <> T.pack path
          newReadJournal
      | otherwise -> do
          let rs' = (newJournalState wjId) {msgCount = msgCount ws, byteCount = byteCount ws}
              st' = st {readState = rs', size = msgCount ws}
          openJournal rs' >>= \case
            Left path' -> do
              logError $ "STORE: openJournals, no read and write files - creating new file, read: " <> T.pack path <> ", write: " <> T.pack path'
              newReadJournal
            Right rh -> do
              logError $ "STORE: openJournals, no read file - switched to write file, " <> T.pack path
              closeOnException rh $ fixFileSize rh $ bytePos ws
              pure (st', rh, Nothing)
    Right rh
      | rjId == wjId -> do
          closeOnException rh $ fixFileSize rh $ bytePos ws
          pure (st, rh, Nothing)
      | otherwise -> closeOnException rh $ do
          fixFileSize rh $ byteCount rs
          openJournal ws >>= \case
            Left path -> do
              let msgs = msgCount rs
                  bytes = byteCount rs
                  size' = msgs - msgPos rs
                  ws' = (newJournalState rjId) {msgPos = msgs, msgCount = msgs, bytePos = bytes, byteCount = bytes}
                  st' = st {writeState = ws', size = size'} -- we don't amend canWrite to trigger QCONT
              logError $ "STORE: openJournals, no write file, " <> T.pack path
              pure (st', rh, Nothing)
            Right wh -> do
              closeOnException wh $ fixFileSize wh $ bytePos ws
              pure (st, rh, Just wh)
  where
    newReadJournal = do
      rjId' <- newJournalId $ random ms
      rh <- createNewJournal dir rjId'
      let st' = newMsgQueueState rjId'
      closeOnException rh $ appendState sh st'
      pure (st', rh, Nothing)
    openJournal :: JournalState t -> IO (Either FilePath Handle)
    openJournal JournalState {journalId} =
      let path = journalFilePath dir journalId
       in ifM (doesFileExist path) (Right <$> openFile path ReadWriteMode) (pure $ Left path)
    -- do that for all append operations

fixFileSize :: Handle -> Int64 -> IO ()
fixFileSize h pos = do
  let pos' = fromIntegral pos
  size <- IO.hFileSize h
  if
    | size > pos' -> do
        name <- IO.hShow h
        logWarn $ "STORE: fixFileSize, size " <> tshow size <> " > pos " <> tshow pos <> " - truncating, " <> T.pack name
        IO.hSetFileSize h pos'
    | size < pos' -> do
        -- From code logic this can't happen.
        name <- IO.hShow h
        E.throwIO $ userError $ "fixFileSize size " <> show size <> " < pos " <> show pos <> " - aborting: " <> name
    | otherwise -> pure ()

removeJournal :: FilePath -> JournalState t -> IO ()
removeJournal dir JournalState {journalId} =
  safeRemoveFile "removeJournal" $ journalFilePath dir journalId

removeJournalIfExists :: FilePath -> JournalState t -> IO ()
removeJournalIfExists dir JournalState {journalId} = do
  let path = journalFilePath dir journalId
  handleError "removeJournalIfExists" path $
    whenM (doesFileExist path) $ removeFile path

safeRemoveFile :: Text -> FilePath -> IO ()
safeRemoveFile cxt path = handleError cxt path $ removeFile path

handleError :: Text -> FilePath -> IO () -> IO ()
handleError cxt path a =
  a `catchAny` \e -> logError $ "STORE: " <> cxt <> ", " <> T.pack path <> ", " <> tshow e

-- This function is supposed to be resilient to crashes while updating state files,
-- and also resilient to crashes during its execution.
readQueueState :: JournalMsgStore s -> FilePath -> IO (Maybe MsgQueueState, Bool)
readQueueState JournalMsgStore {config} statePath =
  ifM
    (doesFileExist tempBackup)
    (renameFile tempBackup statePath >> readState)
    (ifM (doesFileExist statePath) readState $ pure (Nothing, False))
  where
    tempBackup = statePath <> ".bak"
    readState = do
      ls <- B.lines <$> readFileTail
      case ls of
        [] -> do
          logWarn $ "STORE: readWriteQueueState, empty queue state, " <> T.pack statePath
          pure (Nothing, False)
        _ -> do
          r <- useLastLine (length ls) True ls
          forM_ (fst r) $ \st ->
            unless (validQueueState st) $ E.throwIO $ userError $ "readWriteQueueState inconsistent state: " <> show st
          pure r
    useLastLine len isLastLine ls = case strDecode $ last ls of
      Right st ->
        -- when state file has fewer than maxStateLines, we don't compact it
        let shouldBackup = len > maxStateLines config || not isLastLine
         in pure (Just st, shouldBackup)
      Left e -- if the last line failed to parse
        | isLastLine -> case init ls of -- or use the previous line
            [] -> do
              logWarn $ "STORE: readWriteQueueState, invalid 1-line queue state - initialized, " <> T.pack statePath
              pure (Nothing, True) -- backup state file, because last line was invalid
            ls' -> do
              logWarn $ "STORE: readWriteQueueState, invalid last line in queue state - using the previous line, " <> T.pack statePath
              useLastLine len False ls'
        | otherwise -> E.throwIO $ userError $ "readWriteQueueState invalid state " <> statePath <> ": " <> show e
    readFileTail =
      IO.withFile statePath ReadMode $ \h -> do
        size <- IO.hFileSize h
        let sz = stateTailSize config
            sz' = fromIntegral sz
        if size > sz'
          then IO.hSeek h AbsoluteSeek (size - sz') >> B.hGet h sz
          else B.hGet h (fromIntegral size)

stateBackupPath :: FilePath -> UTCTime -> FilePath
stateBackupPath statePath ts = statePath <> "." <> iso8601Show ts <> ".bak"

validQueueState :: MsgQueueState -> Bool
validQueueState MsgQueueState {readState = rs, writeState = ws, size}
  | journalId rs == journalId ws =
      alwaysValid
        && msgPos rs <= msgPos ws
        && msgCount rs == msgCount ws
        && bytePos rs <= bytePos ws
        && byteCount rs == byteCount ws
        && size == msgCount rs - msgPos rs
  | otherwise =
      alwaysValid
        && size == msgCount ws + msgCount rs - msgPos rs
  where
    alwaysValid =
      msgPos rs <= msgCount rs
        && bytePos rs <= byteCount rs
        && msgPos ws == msgCount ws
        && bytePos ws == byteCount ws

-- TODO [postgres] possibly, we need to remove the lock from map
deleteQueue_ :: JournalMsgStore s -> JournalQueue s -> IO (Either ErrorType (QueueRec, Maybe (JournalMsgQueue s)))
deleteQueue_ ms q =
  runExceptT $ isolateQueueId "deleteQueue_" ms rId $
    deleteStoreQueue (queueStore_ ms) q >>= mapM remove
  where
    rId = recipientId q
    remove r@(_, mq_) = do
      mapM_ closeMsgQueueHandles mq_
      removeQueueDirectory ms rId
      pure r

closeMsgQueue :: JournalQueue s -> IO ()
closeMsgQueue JournalQueue {msgQueue'} = atomically (swapTVar msgQueue' Nothing) >>= mapM_ closeMsgQueueHandles

closeMsgQueueHandles :: JournalMsgQueue s -> IO ()
closeMsgQueueHandles q = readTVarIO (handles q) >>= mapM_ closeHandles
  where
    closeHandles (MsgQueueHandles sh rh wh_) = do
      hClose sh
      hClose rh
      mapM_ hClose wh_

removeQueueDirectory :: JournalMsgStore s -> RecipientId -> IO ()
removeQueueDirectory st = removeQueueDirectory_ . msgQueueDirectory st

removeQueueDirectory_ :: FilePath -> IO ()
removeQueueDirectory_ dir =
  handleError "removeQueueDirectory" dir $ removePathForcibly dir

hAppend :: Handle -> Int64 -> ByteString -> IO ()
hAppend h pos s = do
  fixFileSize h pos
  IO.hSeek h SeekFromEnd 0
  B.hPutStr h s

hGetMsgAt :: Handle -> Int64 -> IO (Message, Int64)
hGetMsgAt h pos = do
  IO.hSeek h AbsoluteSeek $ fromIntegral pos
  s <- B.hGetLine h
  case strDecode s of
    Right !msg ->
      let !len = fromIntegral (B.length s) + 1
       in pure (msg, len)
    Left e -> E.throwIO $ userError $ "hGetMsgAt invalid message: " <> e

openFile :: FilePath -> IOMode -> IO Handle
openFile f mode = do
  h <- IO.openFile f mode
  IO.hSetBuffering h LineBuffering
  pure h

hClose :: Handle -> IO ()
hClose h =
  IO.hClose h `catchAny` \e -> do
    name <- IO.hShow h
    logError $ "STORE: hClose, " <> T.pack name <> ", " <> tshow e

closeOnException :: Handle -> IO a -> IO a
closeOnException h a = a `E.onException` hClose h
