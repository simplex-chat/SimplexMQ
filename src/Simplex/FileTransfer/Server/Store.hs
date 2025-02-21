{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Simplex.FileTransfer.Server.Store
  ( FileStore (..),
    FileRec (..),
    FileRecipient (..),
    newFileStore,
    addFile,
    setFilePath,
    addRecipient,
    deleteFile,
    blockFile,
    deleteRecipient,
    expiredFilePath,
    getFile,
    ackFile,
    fileTimePrecision,
  )
where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Int (Int64)
import Data.Set (Set)
import qualified Data.Set as S
import Simplex.FileTransfer.Protocol (FileInfo (..), SFileParty (..), XFTPFileId)
import Simplex.FileTransfer.Transport (XFTPErrorType (..))
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (BlockingInfo, RcvPublicAuthKey, RecipientId, SenderId)
import Simplex.Messaging.Server.QueueStore (RoundedSystemTime (..), ServerEntityStatus (..))
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (ifM, ($>>=))

data FileStore = FileStore
  { files :: TMap SenderId FileRec,
    recipients :: TMap RecipientId (SenderId, RcvPublicAuthKey),
    usedStorage :: TVar Int64
  }

data FileRec = FileRec
  { senderId :: SenderId,
    fileInfo :: FileInfo,
    filePath :: TVar (Maybe FilePath),
    recipientIds :: TVar (Set RecipientId),
    createdAt :: RoundedSystemTime,
    fileStatus :: TVar ServerEntityStatus
  }

fileTimePrecision :: Int64
fileTimePrecision = 3600 -- truncate creation time to 1 hour

data FileRecipient = FileRecipient RecipientId RcvPublicAuthKey
  deriving (Show)

instance StrEncoding FileRecipient where
  strEncode (FileRecipient rId rKey) = strEncode rId <> ":" <> strEncode rKey
  strP = FileRecipient <$> strP <* A.char ':' <*> strP

newFileStore :: IO FileStore
newFileStore = do
  files <- TM.emptyIO
  recipients <- TM.emptyIO
  usedStorage <- newTVarIO 0
  pure FileStore {files, recipients, usedStorage}

addFile :: FileStore -> SenderId -> FileInfo -> RoundedSystemTime -> ServerEntityStatus -> STM (Either XFTPErrorType ())
addFile FileStore {files} sId fileInfo createdAt status =
  ifM (TM.member sId files) (pure $ Left DUPLICATE_) $ do
    f <- newFileRec sId fileInfo createdAt status
    TM.insert sId f files
    pure $ Right ()

newFileRec :: SenderId -> FileInfo -> RoundedSystemTime -> ServerEntityStatus -> STM FileRec
newFileRec senderId fileInfo createdAt status = do
  recipientIds <- newTVar S.empty
  filePath <- newTVar Nothing
  fileStatus <- newTVar status
  pure FileRec {senderId, fileInfo, filePath, recipientIds, createdAt, fileStatus}

setFilePath :: FileStore -> SenderId -> FilePath -> STM (Either XFTPErrorType ())
setFilePath st sId fPath =
  withFile st sId $ \FileRec {fileInfo, filePath} -> do
    writeTVar filePath (Just fPath)
    modifyTVar' (usedStorage st) (+ fromIntegral (size fileInfo))
    pure $ Right ()

addRecipient :: FileStore -> SenderId -> FileRecipient -> STM (Either XFTPErrorType ())
addRecipient st@FileStore {recipients} senderId (FileRecipient rId rKey) =
  withFile st senderId $ \FileRec {recipientIds} -> do
    rIds <- readTVar recipientIds
    mem <- TM.member rId recipients
    if rId `S.member` rIds || mem
      then pure $ Left DUPLICATE_
      else do
        writeTVar recipientIds $! S.insert rId rIds
        TM.insert rId (senderId, rKey) recipients
        pure $ Right ()

-- this function must be called after the file is deleted from the file system
deleteFile :: FileStore -> SenderId -> STM (Either XFTPErrorType ())
deleteFile FileStore {files, recipients, usedStorage} senderId = do
  TM.lookupDelete senderId files >>= \case
    Just FileRec {fileInfo, recipientIds} -> do
      readTVar recipientIds >>= mapM_ (`TM.delete` recipients)
      modifyTVar' usedStorage $ subtract (fromIntegral $ size fileInfo)
      pure $ Right ()
    _ -> pure $ Left AUTH

-- this function must be called after the file is deleted from the file system
blockFile :: FileStore -> SenderId -> BlockingInfo -> Bool -> STM (Either XFTPErrorType ())
blockFile st@FileStore {usedStorage} senderId info deleted =
  withFile st senderId $ \FileRec {fileInfo, fileStatus} -> do
    when deleted $ modifyTVar' usedStorage $ subtract (fromIntegral $ size fileInfo)
    writeTVar fileStatus $! EntityBlocked info
    pure $ Right ()

deleteRecipient :: FileStore -> RecipientId -> FileRec -> STM ()
deleteRecipient FileStore {recipients} rId FileRec {recipientIds} = do
  TM.delete rId recipients
  modifyTVar' recipientIds $ S.delete rId

getFile :: FileStore -> SFileParty p -> XFTPFileId -> STM (Either XFTPErrorType (FileRec, C.APublicAuthKey))
getFile st party fId = case party of
  SFSender -> withFile st fId $ pure . Right . (\f -> (f, sndKey $ fileInfo f))
  SFRecipient ->
    TM.lookup fId (recipients st) >>= \case
      Just (sId, rKey) -> withFile st sId $ pure . Right . (,rKey)
      _ -> pure $ Left AUTH

expiredFilePath :: FileStore -> XFTPFileId -> Int64 -> STM (Maybe (Maybe FilePath))
expiredFilePath FileStore {files} sId old =
  TM.lookup sId files
    $>>= \FileRec {filePath, createdAt = RoundedSystemTime createdAt} ->
      if createdAt + fileTimePrecision < old
        then Just <$> readTVar filePath
        else pure Nothing

ackFile :: FileStore -> RecipientId -> STM (Either XFTPErrorType ())
ackFile st@FileStore {recipients} recipientId = do
  TM.lookupDelete recipientId recipients >>= \case
    Just (sId, _) ->
      withFile st sId $ \FileRec {recipientIds} -> do
        modifyTVar' recipientIds $ S.delete recipientId
        pure $ Right ()
    _ -> pure $ Left AUTH

withFile :: FileStore -> SenderId -> (FileRec -> STM (Either XFTPErrorType a)) -> STM (Either XFTPErrorType a)
withFile FileStore {files} sId a =
  TM.lookup sId files >>= \case
    Just f -> a f
    _ -> pure $ Left AUTH
