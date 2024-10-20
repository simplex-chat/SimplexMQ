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
{-# LANGUAGE TypeFamilies #-}

module Simplex.Messaging.Server.MsgStore.Journal
  ( JournalMsgStore (msgQueues, random),
    JournalMsgQueue,
    JournalStoreConfig (..),
    getQueueMessages,
    -- below are exported for tests
    MsgQueueState (..),
    JournalState (..),
    msgQueueDirectory,
    readWriteQueueState,
    newMsgQueueState,
    newJournalId,
    logQueueState,
    queueLogFileName,
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
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Functor (($>))
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format.ISO8601 (iso8601Show)
import GHC.IO (catchAny)
import Simplex.Messaging.Agent.Client (getMapLock, withLockMap)
import Simplex.Messaging.Agent.Lock
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (ErrorType (..), Message (..), RecipientId)
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (ifM, tshow, ($>>=))
import System.Directory
import System.FilePath ((</>))
import System.IO (BufferMode (..), Handle, IOMode (..), SeekMode (..), stdout)
import qualified System.IO as IO
import System.Random (StdGen, genByteString, newStdGen)

data JournalMsgStore = JournalMsgStore
  { config :: JournalStoreConfig,
    random :: TVar StdGen,
    queueLocks :: TMap RecipientId Lock,
    msgQueues :: TMap RecipientId JournalMsgQueue
  }

data JournalStoreConfig = JournalStoreConfig
  { storePath :: FilePath,
    pathParts :: Int,
    quota :: Int,
    -- Max number of messages per journal file - ignored in STM store.
    -- When this limit is reached, the file will be changed.
    -- This number should be set bigger than queue quota.
    maxMsgCount :: Int,
    maxStateLines :: Int
  }

data JournalMsgQueue = JournalMsgQueue
  { config :: JournalStoreConfig,
    queueDirectory :: FilePath,
    queueLock :: Lock,
    state :: TVar MsgQueueState,
    -- Last message and length incl. newline
    -- Nothing - unknown, Just Nothing - empty queue.
    -- This optimization  prevents reading each message at least twice,
    -- or reading it after it was just written.
    tipMsg :: TVar (Maybe (Maybe (Message, Int))),
    handles :: TVar (Maybe MsgQueueHandles),
    random :: TVar StdGen
  }

data MsgQueueState = MsgQueueState
  { writeState :: JournalState,
    readState :: JournalState,
    canWrite :: Bool,
    size :: Int
  }
  deriving (Show)

data MsgQueueHandles = MsgQueueHandles
  { stateHandle :: Handle, -- handle to queue state log file, rotates and removes old backups when server is restarted
    readHandle :: Handle,
    writeHandle :: Maybe Handle -- optional, used when write file is different from read file
  }

data JournalState = JournalState
  { journalId :: ByteString,
    msgPos :: Int,
    msgCount :: Int,
    bytePos :: Int
  }
  deriving (Show)

newMsgQueueState :: ByteString -> MsgQueueState
newMsgQueueState journalId =
  let st = newJournalState journalId
   in MsgQueueState {writeState = st, readState = st, canWrite = True, size = 0}

newJournalState :: ByteString -> JournalState
newJournalState journalId = JournalState {journalId, msgPos = 0, msgCount = 0, bytePos = 0}

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

instance StrEncoding JournalState where
  strEncode JournalState {journalId, msgPos, msgCount, bytePos} =
    B.intercalate "," [journalId, strEncode msgPos, strEncode msgCount, strEncode bytePos]
  strP = do
    journalId <- A.takeTill (== ',')
    msgPos <- A.char ',' *> strP
    msgCount <- A.char ',' *> strP
    bytePos <- A.char ',' *> strP
    pure JournalState {journalId, msgPos, msgCount, bytePos}

queueLogFileName :: String
queueLogFileName = "queue_state"

msgLogFileName :: String
msgLogFileName = "messages"

logFileExt :: String
logFileExt = ".log"

newtype NonAtomicIO a = NonAtomicIO (IO a)
  deriving newtype (Functor, Applicative, Monad)

instance MsgStoreClass JournalMsgStore where
  type StoreMonad JournalMsgStore = NonAtomicIO
  type MsgQueue JournalMsgStore = JournalMsgQueue
  type MsgStoreConfig JournalMsgStore = JournalStoreConfig

  newMsgStore :: JournalStoreConfig -> IO JournalMsgStore
  newMsgStore config = do
    random <- newTVarIO =<< newStdGen
    queueLocks <- TM.emptyIO
    msgQueues <- TM.emptyIO
    pure JournalMsgStore {config, random, queueLocks, msgQueues}

  closeMsgStore st = readTVarIO (msgQueues st) >>= mapM_ closeMsgQueue_

  activeMsgQueues = msgQueues
  {-# INLINE activeMsgQueues #-}

  -- This function opens and closes all queues.
  -- It is used to export storage to a single file, not during normal server execution.
  withAllMsgQueues :: JournalMsgStore -> (RecipientId -> JournalMsgQueue -> IO Int) -> IO Int
  withAllMsgQueues st@JournalMsgStore {config} action = do
    closeMsgStore st
    lock <- createLockIO -- the same lock is used for all queues
    dirs <- zip [0..] <$> listQueueDirs 0 ("", storePath)
    let count = length dirs
    total <- foldM (processQueue lock count) 0 dirs
    progress count count
    putStrLn ""
    pure total
    where
      JournalStoreConfig {storePath, pathParts} = config
      processQueue lock count !total (i :: Int, (queueId, dir)) = do
        when (i `mod` 100 == 0) $ progress i count
        q <- openMsgQueue st dir lock
        total' <- case strDecode $ B.pack queueId of
          Right rId -> (total +) <$> action rId q
          Left e -> total <$ putStrLn ("Error: message queue directory " <> dir <> " is invalid: " <> e)
        closeMsgQueue_ q
        pure total'
      progress i count = do
        putStr $ "Processed: " <> show i <> "/" <> show count <> " queues\r"
        IO.hFlush stdout
      listQueueDirs depth (queueId, path)
        | depth == pathParts - 1 = listDirs
        | otherwise = fmap concat . mapM (listQueueDirs (depth + 1)) =<< listDirs
        where
          listDirs = fmap catMaybes . mapM queuePath =<< listDirectory path
          queuePath dir = do
            let path' = path </> dir
            ifM
              (doesDirectoryExist path')
              (pure $ Just (queueId <> dir, path'))
              (Nothing <$ putStrLn ("Error: path " <> path' <> " is not a directory, skipping"))

  logQueueStates :: JournalMsgStore -> IO ()
  logQueueStates st =
    void $ withActiveMsgQueues st $ \_ q ->
      readTVarIO (handles q)
        >>= maybe (pure ()) (\hs -> readTVarIO (state q) >>= logQueueState (stateHandle hs))
        >> pure 0

  getMsgQueue :: JournalMsgStore -> RecipientId -> ExceptT ErrorType IO JournalMsgQueue
  getMsgQueue store@JournalMsgStore {queueLocks, msgQueues, random} rId =
    tryStore "getMsgQueue" $ withLockMap queueLocks rId "getMsgQueue" $
      TM.lookupIO rId msgQueues >>= maybe newQ pure
    where
      newQ = do
        let dir = msgQueueDirectory store rId
        queueLock <- atomically $ getMapLock queueLocks rId
        q <- ifM (doesDirectoryExist dir) (openMsgQueue store dir queueLock) (createQ dir queueLock)
        atomically $ TM.insert rId q msgQueues
        pure q
        where
          createQ :: FilePath -> Lock -> IO JournalMsgQueue
          createQ dir queueLock = do
            -- folder and files are not created here,
            -- to avoid file IO for queues without messages during subscription
            journalId <- newJournalId random
            mkJournalQueue store dir queueLock (newMsgQueueState journalId, Nothing)

  delMsgQueue :: JournalMsgStore -> RecipientId -> IO ()
  delMsgQueue st rId = withLockMap (queueLocks st) rId "delMsgQueue" $ do
    void $ closeMsgQueue st rId
    removeQueueDirectory st rId

  delMsgQueueSize :: JournalMsgStore -> RecipientId -> IO Int
  delMsgQueueSize st rId = withLockMap (queueLocks st) rId "delMsgQueue" $ do
    state_ <- closeMsgQueue st rId
    sz <- maybe (pure $ -1) (fmap size . readTVarIO) state_
    removeQueueDirectory st rId
    pure sz

  getQueueMessages :: Bool -> JournalMsgQueue -> IO [Message]
  getQueueMessages drainMsgs q = readTVarIO (handles q) >>= maybe (pure []) (getMsg [])
    where
      getMsg ms hs = chooseReadJournal q drainMsgs hs >>= maybe (pure ms) readMsg
        where
          readMsg (rs, h) = do
            -- TODO handle errors
            s <- hGetLineAt h $ bytePos rs
            -- TODO handle errors
            Right msg <- pure $ strDecode s
            updateReadPos q drainMsgs (B.length s + 1) hs -- 1 is to account for new line
            (msg :) <$> getMsg ms hs

  writeMsg :: JournalMsgQueue -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  writeMsg q@JournalMsgQueue {queueDirectory, handles, config, random} logState !msg =
    tryStore "writeMsg" $ withLock' (queueLock q) "writeMsg" $ do
      st@MsgQueueState {canWrite, size} <- readTVarIO (state q)
      let empty = size == 0
      if canWrite || empty
        then do
          let canWrt' = quota > size
          if canWrt'
            then writeToJournal st canWrt' msg $> Just (msg, empty)
            else writeToJournal st canWrt' msgQuota $> Nothing
        else pure Nothing
    where
      JournalStoreConfig {quota, maxMsgCount} = config
      msgQuota = MessageQuota {msgId = msgId msg, msgTs = msgTs msg}
      writeToJournal st@MsgQueueState {writeState, readState = rs, size} canWrt' msg' = do
        let msgStr = strEncode msg' `B.snoc` '\n'
            msgLen = B.length msgStr
        hs <- maybe createQueueDir pure =<< readTVarIO handles
        (ws, wh) <- case writeHandle hs of
          Nothing | msgCount writeState >= maxMsgCount -> switchWriteJournal hs
          wh_ -> pure (writeState, fromMaybe (readHandle hs) wh_)
        let msgCount' = msgCount ws + 1
            ws' = ws {msgPos = msgPos ws + 1, msgCount = msgCount', bytePos = bytePos ws + msgLen}
            rs' = if journalId ws == journalId rs then rs {msgCount = msgCount'} else rs
            !st' = st {writeState = ws', readState = rs', canWrite = canWrt', size = size + 1}
        when (size == 0) $ atomically $ writeTVar (tipMsg q) $ Just (Just (msg, msgLen))
        hAppend wh msgStr
        updateQueueState q logState hs st'
        where
          createQueueDir = do
            createDirectoryIfMissing True queueDirectory
            let statePath = queueDirectory </> (queueLogFileName <> logFileExt)
            sh <- openFile statePath AppendMode
            B.hPutStr sh ""
            rh <- createNewJournal queueDirectory $ journalId rs
            let hs = MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = Nothing}
            atomically $ writeTVar handles $ Just hs
            pure hs
          switchWriteJournal hs = do
            journalId <- newJournalId random
            wh <- createNewJournal queueDirectory journalId
            atomically $ writeTVar handles $ Just $ hs {writeHandle = Just wh}
            pure (newJournalState journalId, wh)

  getQueueSize :: JournalMsgQueue -> IO Int
  getQueueSize JournalMsgQueue {state} = size <$> readTVarIO state

  tryPeekMsg_ :: JournalMsgQueue -> NonAtomicIO (Maybe Message)
  tryPeekMsg_ q@JournalMsgQueue {tipMsg, handles} =
    NonAtomicIO $ readTVarIO handles $>>= chooseReadJournal q True $>>= peekMsg
    where
      peekMsg (rs, h) = readTVarIO tipMsg >>= maybe readMsg (pure . fmap fst)
        where
          readMsg = do
            -- TODO handle errors
            s <- hGetLineAt h $ bytePos rs
            -- TODO handle errors
            Right msg <- pure $ strDecode s
            atomically $ writeTVar tipMsg $ Just (Just (msg, B.length s + 1)) -- 1 is to account for new line
            pure $ Just msg

  tryDeleteMsg_ :: JournalMsgQueue -> NonAtomicIO ()
  tryDeleteMsg_ q@JournalMsgQueue {tipMsg, handles} = NonAtomicIO $
    void $
      readTVarIO tipMsg -- if there is no cached tipMsg, do nothing
        $>>= (pure . fmap snd)
        $>>= \len -> readTVarIO handles
        $>>= \hs -> updateReadPos q True len hs $> Just ()

  atomicQueue :: JournalMsgQueue -> String -> NonAtomicIO a -> ExceptT ErrorType IO a
  atomicQueue mq op (NonAtomicIO a) = tryStore op $ withLock' (queueLock mq) op $ a

tryStore :: String -> IO a -> ExceptT ErrorType IO a
tryStore op a =
  ExceptT $
    (Right <$> a) `catchAny` \e ->
      let e' = op <> " " <> show e
       in logError ("STORE ERROR " <> T.pack e') $> Left (STORE e')

openMsgQueue :: JournalMsgStore -> FilePath -> Lock -> IO JournalMsgQueue
openMsgQueue store dir queueLock = do
  let statePath = dir </> (queueLogFileName <> logFileExt)
  (st, sh) <- readWriteQueueState store statePath
  (st', rh, wh_) <- openJournals dir st
  let hs = MsgQueueHandles {stateHandle = sh, readHandle = rh, writeHandle = wh_}
  mkJournalQueue store dir queueLock (st', Just hs)

mkJournalQueue :: JournalMsgStore -> FilePath -> Lock -> (MsgQueueState, Maybe MsgQueueHandles) -> IO JournalMsgQueue
mkJournalQueue JournalMsgStore {random, config} dir queueLock (st, hs_) = do
  state <- newTVarIO st
  tipMsg <- newTVarIO Nothing
  handles <- newTVarIO hs_
  -- using the same queue lock which is currently locked,
  -- to avoid map lookup on queue operations
  pure
    JournalMsgQueue
      { config,
        queueDirectory = dir,
        queueLock,
        state,
        tipMsg,
        handles,
        random
      }

chooseReadJournal :: JournalMsgQueue -> Bool -> MsgQueueHandles -> IO (Maybe (JournalState, Handle))
chooseReadJournal q log' hs = do
  st@MsgQueueState {writeState = ws, readState = rs} <- readTVarIO (state q)
  case writeHandle hs of
    Just wh | msgPos rs >= msgCount rs && journalId rs /= journalId ws -> do
      -- switching to write journal
      atomically $ writeTVar (handles q) $ Just hs {readHandle = wh, writeHandle = Nothing}
      hClose $ readHandle hs
      removeJournal (queueDirectory q) rs
      let !rs' = (newJournalState $ journalId ws) {msgCount = msgCount ws}
          !st' = st {readState = rs'}
      updateQueueState q log' hs st'
      pure $ Just (rs', wh)
    _ | msgPos rs >= msgCount rs && journalId rs == journalId ws -> pure Nothing
    _ -> pure $ Just (rs, readHandle hs)

updateQueueState :: JournalMsgQueue -> Bool -> MsgQueueHandles -> MsgQueueState -> IO ()
updateQueueState q log' hs st = do
  atomically $ writeTVar (state q) st
  when log' $ logQueueState (stateHandle hs) st

logQueueState :: Handle -> MsgQueueState -> IO ()
logQueueState h st = B.hPutStr h $ strEncode st `B.snoc` '\n'

updateReadPos :: JournalMsgQueue -> Bool -> Int -> MsgQueueHandles -> IO ()
updateReadPos q log' len hs = do
  st@MsgQueueState {readState = rs, size} <- readTVarIO (state q)
  let JournalState {msgPos, bytePos} = rs
  let msgPos' = msgPos + 1
      rs' = rs {msgPos = msgPos', bytePos = bytePos + len}
      st' = st {readState = rs', size = size - 1}
  updateQueueState q log' hs st'              
  atomically $ writeTVar (tipMsg q) Nothing

msgQueueDirectory :: JournalMsgStore -> RecipientId -> FilePath
msgQueueDirectory JournalMsgStore {config = JournalStoreConfig {storePath, pathParts}} rId =
  storePath </> B.unpack (B.intercalate "/" $ splitSegments pathParts $ strEncode rId)
  where
    splitSegments _ "" = []
    splitSegments 1 s = [s]
    splitSegments n s =
      let (seg, s') = B.splitAt 2 s
       in seg : splitSegments (n - 1) s'

createNewJournal :: FilePath -> ByteString -> IO Handle
createNewJournal dir journalId = do
  let path = journalFilePath dir journalId -- TODO retry if file exists
  h <- openFile path ReadWriteMode
  B.hPutStr h ""
  pure h

newJournalId :: TVar StdGen -> IO ByteString
newJournalId g = strEncode <$> atomically (stateTVar g $ genByteString 12)

openJournals :: FilePath -> MsgQueueState -> IO (MsgQueueState, Handle, Maybe Handle)
openJournals dir st@MsgQueueState {readState = rs, writeState = ws} = do
  -- TODO verify that file exists, what to do if it's not, or if its state diverges
  -- TODO check current position matches state, fix if not
  let rjId = journalId rs
      wjId = journalId ws
  openJournal rs >>= \case
    Left path -> do
      logError $ "STORE ERROR no read file " <> T.pack path <> ", creating new file"
      rh <- createNewJournal dir rjId
      let st' = newMsgQueueState rjId
      pure (st', rh, Nothing)
    Right rh
      | rjId == wjId -> do
          st' <- fixWriteFileSize rh
          pure (st', rh, Nothing)
      | otherwise ->
          openJournal ws >>= \case
            Left path -> do
              logError $ "STORE ERROR no write file " <> T.pack path <> ", creating new file"
              wh <- createNewJournal dir wjId
              let size' = msgCount rs + msgPos rs
                  st' = st {writeState = newJournalState wjId, size = size'} -- we don't amend canWrite to trigger QCONT
              pure (st', rh, Just wh)
            Right wh -> do
              st' <- fixWriteFileSize wh
              pure (st', rh, Just wh)
  where
    openJournal JournalState {journalId} =
      let path = journalFilePath dir journalId
       in ifM (doesFileExist path) (Right <$> openFile path ReadWriteMode) (pure $ Left path)
    fixWriteFileSize h = do
      let sz = fromIntegral $ bytePos ws
      sz' <- IO.hFileSize h
      if
        | sz' > sz -> logWarn "STORE WARNING" >> IO.hSetFileSize h sz $> st
        | sz' == sz -> pure st
        | otherwise -> pure st -- TODO re-read file to recover what is possible ???

removeJournal :: FilePath -> JournalState -> IO ()
removeJournal dir JournalState {journalId} = do
  let path = journalFilePath dir journalId
  removeFile path `catchAny` (\e -> logError $ "STORE ERROR removing file " <> T.pack path <> ": " <> tshow e)

-- This function is supposed to be resilient to crashes while updating state files,
-- and also resilient to crashes during its execution.
readWriteQueueState :: JournalMsgStore -> FilePath -> IO (MsgQueueState, Handle)
readWriteQueueState JournalMsgStore {random, config} statePath =
  ifM
    (doesFileExist tempBackup)
    (renameFile tempBackup statePath >> readQueueState)
    (ifM (doesFileExist statePath) readQueueState writeNewQueueState)
  where
    tempBackup = statePath <> ".bak"
    readQueueState = do
      ls <- LB.lines <$> LB.readFile statePath
      case ls of
        [] -> writeNewQueueState
        _ -> useLastLine (length ls) True ls
    writeNewQueueState = do
      logWarn $ "STORE WARNING: empty queue state in " <> T.pack statePath <> ", initialized"
      st <- newMsgQueueState <$> newJournalId random
      writeQueueState st
    useLastLine len isLastLine ls = case strDecode $ LB.toStrict $ last ls of
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
              logWarn $ "STORE WARNING: invalid 1-line queue state " <> T.pack statePath <> ", initialized"
              st <- newMsgQueueState <$> newJournalId random
              backupWriteQueueState st
            ls' -> do
              logWarn $ "STORE WARNING: invalid last line in queue state " <> T.pack statePath <> ", using the previous line"
              useLastLine len False ls'
        | otherwise -> do
            logError $ "STORE ERROR invalid queue state in " <> T.pack statePath <> ": " <> tshow e
            E.throwIO $ userError $ "Error reading queue state " <> statePath <> ": " <> show e
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
      logQueueState sh st
      pure (st, sh)

closeMsgQueue :: JournalMsgStore -> RecipientId -> IO (Maybe (TVar MsgQueueState))
closeMsgQueue st rId =
  atomically (TM.lookupDelete rId (msgQueues st))
    >>= mapM (\q -> closeMsgQueue_ q $> state q)

closeMsgQueue_ :: JournalMsgQueue -> IO ()
closeMsgQueue_ q = readTVarIO (handles q) >>= mapM_ closeHandles
  where
    closeHandles (MsgQueueHandles sh rh wh_) = do
      hClose sh
      hClose rh
      mapM_ hClose wh_

removeQueueDirectory :: JournalMsgStore -> RecipientId -> IO ()
removeQueueDirectory st rId =
  let dir = msgQueueDirectory st rId
   in removePathForcibly dir `catchAny` (\e -> logError $ "STORE ERROR removeQueueDirectory " <> T.pack dir <> ": " <> tshow e)

hAppend :: Handle -> ByteString -> IO ()
hAppend h s = IO.hSeek h SeekFromEnd 0 >> B.hPutStr h s

hGetLineAt :: Handle -> Int -> IO ByteString
hGetLineAt h pos = IO.hSeek h AbsoluteSeek (fromIntegral pos) >> B.hGetLine h

openFile :: FilePath -> IOMode -> IO Handle
openFile f mode = do
  h <- IO.openFile f mode
  IO.hSetBuffering h LineBuffering
  pure h

hClose :: Handle -> IO ()
hClose h = IO.hClose h `catchAny` (\e -> logError $ "Error closing file" <> tshow e)
