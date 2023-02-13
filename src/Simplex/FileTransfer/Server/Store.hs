{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TupleSections #-}

module Simplex.FileTransfer.Server.Store
  ( FileStore (..),
    FileRec (..),
    newFileStore,
    addFile,
    setFilePath,
    addRecipient,
    deleteFile,
    getFile,
    ackFile,
  )
where

import Control.Concurrent.STM
import Data.Functor (($>))
import Data.Set (Set)
import qualified Data.Set as S
import Simplex.FileTransfer.Protocol (FileInfo (..), SFileParty (..), XFTPFileId)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol hiding (SParty, SRecipient, SSender)
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (ifM)

data FileStore = FileStore
  { files :: TMap SenderId FileRec,
    recipients :: TMap RecipientId (SenderId, RcvPublicVerifyKey)
  }

data FileRec = FileRec
  { senderId :: SenderId,
    fileInfo :: FileInfo,
    filePath :: TVar (Maybe FilePath),
    recipientIds :: TVar (Set RecipientId)
  }
  deriving (Eq)

newFileStore :: STM FileStore
newFileStore = do
  files <- TM.empty
  recipients <- TM.empty
  pure FileStore {files, recipients}

addFile :: FileStore -> SenderId -> FileInfo -> STM (Either ErrorType ())
addFile FileStore {files} sId fileInfo =
  ifM (TM.member sId files) (pure $ Left DUPLICATE_) $ do
    f <- newFileRec sId fileInfo
    TM.insert sId f files
    pure $ Right ()

newFileRec :: SenderId -> FileInfo -> STM FileRec
newFileRec senderId fileInfo = do
  recipientIds <- newTVar S.empty
  filePath <- newTVar Nothing
  pure FileRec {senderId, fileInfo, filePath, recipientIds}

setFilePath :: FileStore -> SenderId -> FilePath -> STM (Either ErrorType ())
setFilePath st sId fPath =
  withFile st sId $ \FileRec {filePath} ->
    writeTVar filePath (Just fPath) $> Right ()

addRecipient :: FileStore -> SenderId -> (RecipientId, RcvPublicVerifyKey) -> STM (Either ErrorType ())
addRecipient st@FileStore {recipients} senderId (rId, rKey) =
  withFile st senderId $ \FileRec {recipientIds} -> do
    rIds <- readTVar recipientIds
    mem <- TM.member rId recipients
    if rId `S.member` rIds || mem
      then pure $ Left DUPLICATE_
      else do
        writeTVar recipientIds $! S.insert rId rIds
        TM.insert rId (senderId, rKey) recipients
        pure $ Right ()

deleteFile :: FileStore -> SenderId -> STM (Either ErrorType ())
deleteFile FileStore {files, recipients} senderId = do
  TM.lookupDelete senderId files >>= \case
    Just FileRec {recipientIds} -> do
      readTVar recipientIds >>= mapM_ (`TM.delete` recipients)
      pure $ Right ()
    _ -> pure $ Left AUTH

getFile :: FileStore -> SFileParty p -> XFTPFileId -> STM (Either ErrorType (FileRec, C.APublicVerifyKey))
getFile st party fId = case party of
  SSender -> withFile st fId $ pure . Right . (\f -> (f, sndKey $ fileInfo f))
  SRecipient ->
    TM.lookup fId (recipients st) >>= \case
      Just (sId, rKey) -> withFile st sId $ pure . Right . (,rKey)
      _ -> pure $ Left AUTH

ackFile :: FileStore -> RecipientId -> STM (Either ErrorType ())
ackFile st@FileStore {recipients} recipientId = do
  TM.lookupDelete recipientId recipients >>= \case
    Just (sId, _) ->
      withFile st sId $ \FileRec {recipientIds} -> do
        modifyTVar' recipientIds $ S.delete recipientId
        pure $ Right ()
    _ -> pure $ Left AUTH

withFile :: FileStore -> SenderId -> (FileRec -> STM (Either ErrorType a)) -> STM (Either ErrorType a)
withFile FileStore {files} sId a =
  TM.lookup sId files >>= \case
    Just f -> a f
    _ -> pure $ Left AUTH
