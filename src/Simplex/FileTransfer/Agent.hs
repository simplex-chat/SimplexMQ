{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.FileTransfer.Agent
  ( receiveFile,
  )
where

import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Bifunctor (first)
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Int (Int64)
import Simplex.FileTransfer.Description
import Simplex.FileTransfer.Protocol (FileParty (..))
import Simplex.FileTransfer.Transport (XFTPRcvChunkSpec (..))
import Simplex.FileTransfer.Types
import Simplex.FileTransfer.Util (uniqueCombine)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol (ACommand (FRCVD), AParty (..), AgentErrorType (INTERNAL))
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite
import qualified Simplex.Messaging.Crypto.Lazy as LC
import Simplex.Messaging.Encoding
import Simplex.Messaging.Protocol (XFTPServer)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (whenM)
import UnliftIO
import UnliftIO.Directory
import qualified UnliftIO.Exception as E

receiveFile :: AgentMonad m => AgentClient -> UserId -> ValidFileDescription 'FPRecipient -> FilePath -> m Int64
receiveFile c userId (ValidFileDescription fd@FileDescription {chunks}) xftpPath = do
  encPath <- uniqueCombine xftpPath "xftp.encrypted"
  createDirectory encPath
  fId <- withStore' c $ \db -> createRcvFile db userId fd xftpPath encPath
  forM_ chunks downloadChunk
  pure fId
  where
    downloadChunk :: AgentMonad m => FileChunk -> m ()
    downloadChunk FileChunk {replicas = (FileChunkReplica {server} : _)} = do
      addWorker c (Just server)
    downloadChunk _ = throwError $ INTERNAL "no replicas"

addWorker :: AgentMonad m => AgentClient -> Maybe XFTPServer -> m ()
addWorker c srv_ = do
  ws <- asks $ xftpWorkers . xftpAgent
  atomically (TM.lookup srv_ ws) >>= \case
    Nothing -> do
      doWork <- newTMVarIO ()
      let runWorker = case srv_ of
            Just srv -> runXFTPWorker c srv doWork
            Nothing -> runXFTPLocalWorker c doWork
      worker <- async $ runWorker `E.finally` atomically (TM.delete srv_ ws)
      atomically $ TM.insert srv_ (doWork, worker) ws
    Just (doWork, _) ->
      void . atomically $ tryPutTMVar doWork ()

runXFTPWorker :: forall m. AgentMonad m => AgentClient -> XFTPServer -> TMVar () -> m ()
runXFTPWorker c srv doWork = do
  forever $ do
    void . atomically $ readTMVar doWork
    agentOperationBracket c AORcvNetwork throwWhenInactive runXftpOperation
  where
    runXftpOperation :: m ()
    runXftpOperation = do
      nextChunk <- withStore' c (`getNextRcvChunkToDownload` srv)
      case nextChunk of
        Nothing -> noWorkToDo
        Just fc@RcvFileChunk {nextDelay} -> do
          ri <- asks $ reconnectInterval . config
          let ri' = maybe ri (\d -> ri {initialInterval = d}) nextDelay
          withRetryInterval ri' $ \loop ->
            downloadFileChunk fc
              `catchError` \e -> do
                liftIO $ print e
                -- TODO don't loop on permanent errors
                -- TODO increase replica retries count
                -- TODO update nextDelay (modify withRetryInterval to expose current delay)
                loop
    noWorkToDo = void . atomically $ tryTakeTMVar doWork
    downloadFileChunk :: RcvFileChunk -> m ()
    downloadFileChunk RcvFileChunk {userId, rcvFileId, rcvChunkId, chunkNo, chunkSize, digest, fileTmpPath, replicas = replica : _} = do
      chunkPath <- uniqueCombine fileTmpPath $ show chunkNo
      let chunkSpec = XFTPRcvChunkSpec chunkPath (unFileSize chunkSize) (unFileDigest digest)
      agentXFTPDownloadChunk c userId replica chunkSpec
      fileReceived <- withStore c $ \db -> runExceptT $ do
        -- both actions can be done in a single store method
        fd <- ExceptT $ updateRcvFileChunkReceived db (rcvChunkReplicaId replica) rcvChunkId rcvFileId chunkPath
        let fileReceived = allChunksReceived fd
        when fileReceived $
          liftIO $ updateRcvFileStatus db rcvFileId RFSReceived
        pure fileReceived
      -- check if chunk is downloaded and not acknowledged via flag acknowledged?
      -- or just catch and ignore error on acknowledgement? (and remove flag)
      -- agentXFTPAckChunk c replicaKey (unChunkReplicaId replicaId) `catchError` \_ -> pure ()
      when fileReceived $ addWorker c Nothing
      where
        allChunksReceived :: RcvFile -> Bool
        allChunksReceived RcvFile {chunks} =
          all (\RcvFileChunk {replicas} -> any received replicas) chunks
    downloadFileChunk _ = throwError $ INTERNAL "no replica"

runXFTPLocalWorker :: forall m. AgentMonad m => AgentClient -> TMVar () -> m ()
runXFTPLocalWorker c@AgentClient {subQ} doWork = do
  forever $ do
    void . atomically $ readTMVar doWork
    runXftpOperation
  where
    runXftpOperation :: m ()
    runXftpOperation = do
      nextFile <- withStore' c getNextRcvFileToDecrypt
      case nextFile of
        Nothing -> noWorkToDo
        Just fd -> do
          ri <- asks $ reconnectInterval . config
          withRetryInterval ri $ \loop ->
            decryptFile fd
              `catchError` \e -> do
                liftIO $ print e
                -- TODO don't loop on permanent errors
                -- TODO fixed number of retries instead of exponential backoff?
                loop
    noWorkToDo = void . atomically $ tryTakeTMVar doWork
    decryptFile :: RcvFile -> m ()
    decryptFile RcvFile {rcvFileId, key, nonce, tmpPath, saveDir, chunks} = do
      -- TODO remove tmpPath if exists
      withStore' c $ \db -> updateRcvFileStatus db rcvFileId RFSDecrypting
      chunkPaths <- getChunkPaths chunks
      encSize <- liftIO $ foldM (\s path -> (s +) . fromIntegral <$> getFileSize path) 0 chunkPaths
      path <- decrypt encSize chunkPaths
      whenM (doesPathExist tmpPath) $ removeDirectoryRecursive tmpPath
      withStore' c $ \db -> updateRcvFileComplete db rcvFileId path
      notify $ FRCVD rcvFileId path
      where
        notify :: ACommand 'Agent -> m ()
        notify cmd = atomically $ writeTBQueue subQ ("", "", cmd)
        getChunkPaths :: [RcvFileChunk] -> m [FilePath]
        getChunkPaths [] = pure []
        getChunkPaths (RcvFileChunk {chunkTmpPath = Just path} : cs) = do
          ps <- getChunkPaths cs
          pure $ path : ps
        getChunkPaths (RcvFileChunk {chunkTmpPath = Nothing} : _cs) =
          throwError $ INTERNAL "no chunk path"
        decrypt :: Int64 -> [FilePath] -> m FilePath
        decrypt encSize chunkPaths = do
          lazyChunks <- readChunks chunkPaths
          (authOk, f) <- liftEither . first cryptoError $ LC.sbDecryptTailTag key nonce (encSize - authTagSize) lazyChunks
          let (fileHdr, f') = LB.splitAt 1024 f
          -- withFile encPath ReadMode $ \r -> do
          --   fileHdr <- liftIO $ B.hGet r 1024
          case A.parse smpP $ LB.toStrict fileHdr of
            -- TODO XFTP errors
            A.Fail _ _ e -> throwError $ INTERNAL $ "Invalid file header: " <> e
            A.Partial _ -> throwError $ INTERNAL "Invalid file header"
            A.Done rest FileHeader {fileName} -> do
              -- TODO touch file in agent bracket
              path <- uniqueCombine saveDir fileName
              liftIO $ LB.writeFile path $ LB.fromStrict rest <> f'
              unless authOk $ do
                removeFile path
                throwError $ INTERNAL "Error decrypting file: incorrect auth tag"
              pure path
        readChunks :: [FilePath] -> m LB.ByteString
        readChunks =
          foldM
            ( \s path -> do
                chunk <- liftIO $ LB.readFile path
                pure $ s <> chunk
            )
            LB.empty
