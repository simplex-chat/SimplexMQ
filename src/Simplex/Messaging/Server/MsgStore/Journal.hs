{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Server.MsgStore.Journal
  ( JournalMsgStore (queueStore, random),
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
    readWriteQueueState,
    newMsgQueueState,
    newJournalId,
    appendState,
    queueLogFileName,
    journalFilePath,
    logFileExt,
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
import Data.List (intercalate)
import Data.Maybe (catMaybes, fromMaybe, isNothing)
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.System (SystemTime (..), getSystemTime)
import Data.Time.Format.ISO8601 (iso8601Show)
import GHC.IO (catchAny)
import Simplex.Messaging.Agent.Client (getMapLock, withLockMap)
import Simplex.Messaging.Agent.Lock
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.STM
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Util (ifM, tshow, whenM, ($>>=), (<$$>))
import System.Directory
import System.Exit
import System.FilePath ((</>))
import System.IO (BufferMode (..), Handle, IOMode (..), SeekMode (..), stdout)
import qualified System.IO as IO
import System.Random (StdGen, genByteString, newStdGen)

data JournalMsgStore = JournalMsgStore
  { config :: JournalStoreConfig,
    random :: TVar StdGen,
    queueLocks :: TMap RecipientId Lock,
    queueStore :: STMQueueStore JournalQueue
  }

data JournalStoreConfig = JournalStoreConfig
  { storePath :: FilePath,
    pathParts :: Int,
    quota :: Int,
    -- Max number of messages per journal file - ignored in STM store.
    -- When this limit is reached, the file will be changed.
    -- This number should be set bigger than queue quota.
    maxMsgCount :: Int,
    maxStateLines :: Int,
    stateTailSize :: Int,
    -- time in seconds after which the queue will be closed after message expiration
    idleInterval :: Int64
  }

data JournalQueue = JournalQueue
  { recipientId :: RecipientId,
    queueLock :: Lock,
    -- To avoid race conditions and errors when restoring queues,
    -- Nothing is written to TVar when queue is deleted.
    queueRec :: TVar (Maybe QueueRec),
    msgQueue_ :: TVar (Maybe JournalMsgQueue),
    -- system time in seconds since epoch
    activeAt :: TVar Int64,
    -- Just True - empty, Just False - non-empty, Nothing - unknown
    isEmpty :: TVar (Maybe Bool)
  }

data JMQueue = JMQueue
  { queueDirectory :: FilePath,
    statePath :: FilePath
  }

data JournalMsgQueue = JournalMsgQueue
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

newtype StoreIO a = StoreIO {unStoreIO :: IO a}
  deriving newtype (Functor, Applicative, Monad)

instance STMStoreClass JournalMsgStore where
  stmQueueStore JournalMsgStore {queueStore} = queueStore
  mkQueue st rId qr = do
    lock <- getMapLock (queueLocks st) rId
    q <- newTVar $ Just qr
    mq <- newTVar Nothing
    activeAt <- newTVar 0
    isEmpty <- newTVar Nothing
    pure $ JournalQueue rId lock q mq activeAt isEmpty
  msgQueue_' = msgQueue_

instance MsgStoreClass JournalMsgStore where
  type StoreMonad JournalMsgStore = StoreIO
  type StoreQueue JournalMsgStore = JournalQueue
  type MsgQueue JournalMsgStore = JournalMsgQueue
  type MsgStoreConfig JournalMsgStore = JournalStoreConfig

  newMsgStore :: JournalStoreConfig -> IO JournalMsgStore
  newMsgStore config = do
    random <- newTVarIO =<< newStdGen
    queueLocks <- TM.emptyIO
    queueStore <- newQueueStore
    pure JournalMsgStore {config, random, queueLocks, queueStore}

  setStoreLog :: JournalMsgStore -> StoreLog 'WriteMode -> IO ()
  setStoreLog st sl = atomically $ writeTVar (storeLog $ queueStore st) (Just sl)

  closeMsgStore JournalMsgStore {queueStore = st} = do
    readTVarIO (storeLog st) >>= mapM_ closeStoreLog
    readTVarIO (queues st) >>= mapM_ closeMsgQueue

  -- This function is a "foldr" that opens and closes all queues, processes them as defined by action and accumulates the result.
  -- It is used to export storage to a single file and also to expire messages and validate all queues when server is started.
  -- TODO this function requires case-sensitive file system, because it uses queue directory as recipient ID.
  -- It can be made to support case-insensite FS by supporting more than one queue per directory, by getting recipient ID from state file name.
  withAllMsgQueues :: forall a. Monoid a => Bool -> JournalMsgStore -> (JournalQueue -> IO a) -> IO a
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
              Right q -> unStoreIO (getMsgQueue ms q) *> action q <* closeMsgQueue q
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

  logQueueStates :: JournalMsgStore -> IO ()
  logQueueStates ms = withActiveMsgQueues ms $ unStoreIO . logQueueState

  logQueueState :: JournalQueue -> StoreIO ()
  logQueueState q =
    StoreIO . void $
      readTVarIO (msgQueue_ q)
        $>>= \mq -> readTVarIO (handles mq)
        $>>= (\hs -> (readTVarIO (state mq) >>= appendState (stateHandle hs)) $> Just ())

  recipientId' = recipientId
  {-# INLINE recipientId' #-}

  queueRec' = queueRec
  {-# INLINE queueRec' #-}

  getMsgQueue :: JournalMsgStore -> JournalQueue -> StoreIO JournalMsgQueue
  getMsgQueue ms@JournalMsgStore {random} JournalQueue {recipientId = rId, msgQueue_} =
    StoreIO $ readTVarIO msgQueue_ >>= maybe newQ pure
    where
      newQ = do
        let dir = msgQueueDirectory ms rId
            statePath = msgQueueStatePath dir $ B.unpack (strEncode rId)
            queue = JMQueue {queueDirectory = dir, statePath}
        q <- ifM (doesDirectoryExist dir) (openMsgQueue ms queue) (createQ queue)
        atomically $ writeTVar msgQueue_ $ Just q
        pure q
        where
          createQ :: JMQueue -> IO JournalMsgQueue
          createQ queue = do
            -- folder and files are not created here,
            -- to avoid file IO for queues without messages during subscription
            journalId <- newJournalId random
            mkJournalQueue queue (newMsgQueueState journalId) Nothing

  getPeekMsgQueue :: JournalMsgStore -> JournalQueue -> StoreIO (Maybe (JournalMsgQueue, Message))
  getPeekMsgQueue ms q@JournalQueue {isEmpty} =
    StoreIO (readTVarIO isEmpty) >>= \case
      Just True -> pure Nothing
      Just False -> peek
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
        mq <- getMsgQueue ms q
        (mq,) <$$> tryPeekMsg_ q mq

  -- only runs action if queue is not empty
  withIdleMsgQueue :: Int64 -> JournalMsgStore -> JournalQueue -> (JournalMsgQueue -> StoreIO a) -> StoreIO (Maybe a, Int)
  withIdleMsgQueue now ms@JournalMsgStore {config} q action =
    StoreIO $ readTVarIO (msgQueue_ q) >>= \case
      Nothing ->
        E.bracket
          (unStoreIO $ getPeekMsgQueue ms q)
          (mapM_ $ \_ -> closeMsgQueue q)
          (maybe (pure (Nothing, 0)) (unStoreIO . run))
        where
          run (mq, _) = do
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

  deleteQueue :: JournalMsgStore -> JournalQueue -> IO (Either ErrorType QueueRec)
  deleteQueue ms q = fst <$$> deleteQueue_ ms q

  deleteQueueSize :: JournalMsgStore -> JournalQueue -> IO (Either ErrorType (QueueRec, Int))
  deleteQueueSize ms q =
    deleteQueue_ ms q >>= mapM (traverse getSize)
    -- traverse operates on the second tuple element
    where
      getSize = maybe (pure (-1)) (fmap size . readTVarIO . state)

  getQueueMessages_ :: Bool -> JournalMsgQueue -> StoreIO [Message]
  getQueueMessages_ drainMsgs q = StoreIO (run [])
    where
      run msgs = readTVarIO (handles q) >>= maybe (pure []) (getMsg msgs)
      getMsg msgs hs = chooseReadJournal q drainMsgs hs >>= maybe (pure msgs) readMsg
        where
          readMsg (rs, h) = do
            (msg, len) <- hGetMsgAt h $ bytePos rs
            updateReadPos q drainMsgs len hs
            (msg :) <$> run msgs

  writeMsg :: JournalMsgStore -> JournalQueue -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  writeMsg ms q' logState msg = isolateQueue q' "writeMsg" $ do
    q <- getMsgQueue ms q'
    StoreIO $ (`E.finally` updateActiveAt q') $ do
      st@MsgQueueState {canWrite, size} <- readTVarIO (state q)
      let empty = size == 0
      if canWrite || empty
        then do
          atomically $ writeTVar (isEmpty q') (Just False)
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
        updateQueueState q logState hs st' $
          when (size == 0) $ writeTVar (tipMsg q) $ Just (Just (msg, msgLen))
        where
          JournalMsgQueue {queue = JMQueue {queueDirectory, statePath}, handles} = q
          createQueueDir = do
            createDirectoryIfMissing True queueDirectory
            sh <- openFile statePath AppendMode
            B.hPutStr sh ""
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
  setOverQuota_ :: JournalQueue -> IO ()
  setOverQuota_ q =
    readTVarIO (msgQueue_ q)
      >>= mapM_ (\JournalMsgQueue {state} -> atomically $ modifyTVar' state $ \st -> st {canWrite = False})

  getQueueSize_ :: JournalMsgQueue -> StoreIO Int
  getQueueSize_ JournalMsgQueue {state} = StoreIO $ size <$> readTVarIO state

  tryPeekMsg_ :: JournalQueue -> JournalMsgQueue -> StoreIO (Maybe Message)
  tryPeekMsg_ q mq@JournalMsgQueue {tipMsg, handles} =
    StoreIO $ (readTVarIO handles $>>= chooseReadJournal mq True $>>= peekMsg) >>= setEmpty
    where
      peekMsg (rs, h) = readTVarIO tipMsg >>= maybe readMsg (pure . fmap fst)
        where
          readMsg = do
            ml@(msg, _) <- hGetMsgAt h $ bytePos rs
            atomically $ writeTVar tipMsg $ Just (Just ml)
            pure $ Just msg
      setEmpty msg = do
        atomically $ writeTVar (isEmpty q) (Just $ isNothing msg)
        pure msg

  tryDeleteMsg_ :: JournalQueue -> JournalMsgQueue -> Bool -> StoreIO ()
  tryDeleteMsg_ q mq@JournalMsgQueue {tipMsg, handles} logState = StoreIO $ (`E.finally` when logState (updateActiveAt q)) $
    void $
      readTVarIO tipMsg -- if there is no cached tipMsg, do nothing
        $>>= (pure . fmap snd)
        $>>= \len -> readTVarIO handles
        $>>= \hs -> updateReadPos mq logState len hs $> Just ()

  isolateQueue :: JournalQueue -> String -> StoreIO a -> ExceptT ErrorType IO a
  isolateQueue JournalQueue {recipientId, queueLock} op =
    tryStore' op recipientId . withLock' queueLock op . unStoreIO

updateActiveAt :: JournalQueue -> IO ()
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

isolateQueueId :: String -> JournalMsgStore -> RecipientId -> IO (Either ErrorType a) -> ExceptT ErrorType IO a
isolateQueueId op ms rId = tryStore op rId . withLockMap (queueLocks ms) rId op

openMsgQueue :: JournalMsgStore -> JMQueue -> IO JournalMsgQueue
openMsgQueue ms q@JMQueue {queueDirectory = dir, statePath} = do
  (st, sh) <- readWriteQueueState ms statePath
  let MsgQueueState {readState = rs, writeState = ws, size} = st
  if size == 0
    then E.uninterruptibleMask_ $ do
      -- If the queue is empty, journals are deleted and state file is closed
      -- Journal will be created again if queue is written to.
      -- canWrite is set to True.
      st' <- newMsgQueueState <$> newJournalId (random ms)
      unsafeAppendState sh st'
      hClose sh
      removeJournalIfExists dir rs
      unless (journalId ws == journalId rs) $ removeJournalIfExists dir ws
      mkJournalQueue q st' Nothing
    else do
      (st', rh, wh_) <- closeOnException sh $ openJournals ms dir st sh
      let hs = MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = wh_}
      mkJournalQueue q st' (Just hs)

mkJournalQueue :: JMQueue -> MsgQueueState -> Maybe MsgQueueHandles -> IO JournalMsgQueue
mkJournalQueue queue st hs_ = do
  state <- newTVarIO st
  tipMsg <- newTVarIO Nothing
  handles <- newTVarIO hs_
  -- using the same queue lock which is currently locked,
  -- to avoid map lookup on queue operations
  pure JournalMsgQueue {queue, state, tipMsg, handles}

chooseReadJournal :: JournalMsgQueue -> Bool -> MsgQueueHandles -> IO (Maybe (JournalState 'JTRead, Handle))
chooseReadJournal q log' hs = do
  st@MsgQueueState {writeState = ws, readState = rs} <- readTVarIO (state q)
  case writeHandle hs of
    Just wh | msgPos rs >= msgCount rs && journalId rs /= journalId ws -> do
      -- switching to write journal
      atomically $ writeTVar (handles q) $ Just hs {readHandle = wh, writeHandle = Nothing}
      hClose $ readHandle hs
      when log' $ removeJournal (queueDirectory $ queue q) rs
      let !rs' = (newJournalState $ journalId ws) {msgCount = msgCount ws, byteCount = byteCount ws}
          !st' = st {readState = rs'}
      updateQueueState q log' hs st' $ pure ()
      pure $ Just (rs', wh)
    _ | msgPos rs >= msgCount rs && journalId rs == journalId ws -> pure Nothing
    _ -> pure $ Just (rs, readHandle hs)

updateQueueState :: JournalMsgQueue -> Bool -> MsgQueueHandles -> MsgQueueState -> STM () -> IO ()
updateQueueState q log' hs st a = do
  unless (validQueueState st) $ E.throwIO $ userError $ "updateQueueState invalid state: " <> show st
  when log' $ appendState (stateHandle hs) st
  atomically $ writeTVar (state q) st >> a

appendState :: Handle -> MsgQueueState -> IO ()
appendState h = E.uninterruptibleMask_ . unsafeAppendState h
{-# INLINE appendState #-}

unsafeAppendState :: Handle -> MsgQueueState -> IO ()
unsafeAppendState h st = B.hPutStr h $ strEncode st `B.snoc` '\n'

updateReadPos :: JournalMsgQueue -> Bool -> Int64 -> MsgQueueHandles -> IO ()
updateReadPos q log' len hs = do
  st@MsgQueueState {readState = rs, size} <- readTVarIO (state q)
  let JournalState {msgPos, bytePos} = rs
  let msgPos' = msgPos + 1
      rs' = rs {msgPos = msgPos', bytePos = bytePos + len}
      st' = st {readState = rs', size = size - 1}
  updateQueueState q log' hs st' $ writeTVar (tipMsg q) Nothing

msgQueueDirectory :: JournalMsgStore -> RecipientId -> FilePath
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

openJournals :: JournalMsgStore -> FilePath -> MsgQueueState -> Handle -> IO (MsgQueueState, Handle, Maybe Handle)
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
removeJournal dir JournalState {journalId} = safeRemoveFile $ journalFilePath dir journalId

removeJournalIfExists :: FilePath -> JournalState t -> IO ()
removeJournalIfExists dir JournalState {journalId} = do
  let path = journalFilePath dir journalId
  whenM (doesFileExist path) $ safeRemoveFile path

safeRemoveFile :: FilePath -> IO ()
safeRemoveFile path =
  removeFile path `catchAny` (\e -> logError $ "STORE: removeJournal, " <> T.pack path <> ", " <> tshow e)

-- This function is supposed to be resilient to crashes while updating state files,
-- and also resilient to crashes during its execution.
-- TODO expire old state files
readWriteQueueState :: JournalMsgStore -> FilePath -> IO (MsgQueueState, Handle)
readWriteQueueState JournalMsgStore {random, config} statePath =
  ifM
    (doesFileExist tempBackup)
    (renameFile tempBackup statePath >> readQueueState)
    (ifM (doesFileExist statePath) readQueueState writeNewQueueState)
  where
    tempBackup = statePath <> ".bak"
    readQueueState = do
      ls <- B.lines <$> readFileTail
      case ls of
        [] -> writeNewQueueState
        _ -> do
          r@(st, _) <- useLastLine (length ls) True ls
          unless (validQueueState st) $ E.throwIO $ userError $ "readWriteQueueState inconsistent state: " <> show st
          pure r
    writeNewQueueState = do
      logWarn $ "STORE: readWriteQueueState, empty queue state - initialized, " <> T.pack statePath
      st <- newMsgQueueState <$> newJournalId random
      writeQueueState st
    useLastLine len isLastLine ls = case strDecode $ last ls of
      Right st
        | len > maxStateLines config || not isLastLine ->
            backupWriteQueueState st
        | otherwise -> do
            -- when state file has fewer than maxStateLines, we don't compact it
            sh <- openFile statePath AppendMode
            pure (st, sh)
      Left e -- if the last line failed to parse
        | isLastLine -> case init ls of -- or use the previous line
            [] -> do
              logWarn $ "STORE: readWriteQueueState, invalid 1-line queue state - initialized, " <> T.pack statePath
              st <- newMsgQueueState <$> newJournalId random
              backupWriteQueueState st
            ls' -> do
              logWarn $ "STORE: readWriteQueueState, invalid last line in queue state - using the previous line, " <> T.pack statePath
              useLastLine len False ls'
        | otherwise -> E.throwIO $ userError $ "readWriteQueueState invalid state " <> statePath <> ": " <> show e
    backupWriteQueueState st = do
      -- State backup is made in two steps to mitigate the crash during the backup.
      -- Temporary backup file will be used when it is present.
      renameFile statePath tempBackup -- 1) temp backup
      r <- writeQueueState st -- 2) save state
      ts <- getCurrentTime
      renameFile tempBackup (statePath <> "." <> iso8601Show ts <> ".bak") -- 3) timed backup
      pure r
    writeQueueState st = do
      sh <- openFile statePath AppendMode
      closeOnException sh $ appendState sh st
      pure (st, sh)
    readFileTail =
      IO.withFile statePath ReadMode $ \h -> do
        size <- IO.hFileSize h
        let sz = stateTailSize config
            sz' = fromIntegral sz
        if size > sz'
          then IO.hSeek h AbsoluteSeek (size - sz') >> B.hGet h sz
          else B.hGet h (fromIntegral size)

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

deleteQueue_ :: JournalMsgStore -> JournalQueue -> IO (Either ErrorType (QueueRec, Maybe JournalMsgQueue))
deleteQueue_ ms q =
  runExceptT $ isolateQueueId "deleteQueue_" ms rId $
    deleteQueue' ms q >>= mapM remove
  where
    rId = recipientId q
    remove r@(_, mq_) = do
      mapM_ closeMsgQueueHandles mq_
      removeQueueDirectory ms rId
      pure r

closeMsgQueue :: JournalQueue -> IO ()
closeMsgQueue JournalQueue {msgQueue_} = atomically (swapTVar msgQueue_ Nothing) >>= mapM_ closeMsgQueueHandles

closeMsgQueueHandles :: JournalMsgQueue -> IO ()
closeMsgQueueHandles q = readTVarIO (handles q) >>= mapM_ closeHandles
  where
    closeHandles (MsgQueueHandles sh rh wh_) = do
      hClose sh
      hClose rh
      mapM_ hClose wh_

removeQueueDirectory :: JournalMsgStore -> RecipientId -> IO ()
removeQueueDirectory st = removeQueueDirectory_ . msgQueueDirectory st

removeQueueDirectory_ :: FilePath -> IO ()
removeQueueDirectory_ dir =
  removePathForcibly dir `catchAny` \e ->
    logError $ "STORE: removeQueueDirectory, " <> T.pack dir <> ", " <> tshow e

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
