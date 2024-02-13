{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}

module Simplex.FileTransfer.Agent
  ( startXFTPWorkers,
    closeXFTPAgent,
    toFSFilePath,
    -- Receiving files
    xftpReceiveFile',
    xftpDeleteRcvFile',
    -- Sending files
    xftpSendFile',
    xftpSendDescription',
    deleteSndFileInternal,
    deleteSndFileRemote,
  )
where

import Control.Logger.Simple (logError)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Composition ((.:))
import Data.Int (Int64)
import Data.List (foldl', sortOn)
import qualified Data.List.NonEmpty as L
import Data.Map (Map)
import qualified Data.Map.Strict as M
import Data.Text (Text)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Simplex.FileTransfer.Client (XFTPChunkSpec (..))
import Simplex.FileTransfer.Client.Main
import Simplex.FileTransfer.Crypto
import Simplex.FileTransfer.Description
import Simplex.FileTransfer.Protocol (FileParty (..), SFileParty (..))
import qualified Simplex.FileTransfer.Protocol as XFTP
import Simplex.FileTransfer.Transport (XFTPRcvChunkSpec (..))
import Simplex.FileTransfer.Types
import Simplex.FileTransfer.Util (removePath, uniqueCombine)
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store.SQLite
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.File (CryptoFile (..), CryptoFileArgs)
import qualified Simplex.Messaging.Crypto.File as CF
import qualified Simplex.Messaging.Crypto.Lazy as LC
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String (strDecode, strEncode)
import Simplex.Messaging.Protocol (EntityId, XFTPServer)
import Simplex.Messaging.Util (liftError, tshow, unlessM, whenM)
import System.FilePath (takeFileName, (</>))
import UnliftIO
import UnliftIO.Directory

startXFTPWorkers :: AgentMonad m => AgentClient -> Maybe FilePath -> m ()
startXFTPWorkers c workDir = do
  wd <- asks $ xftpWorkDir . xftpAgent
  atomically $ writeTVar wd workDir
  cfg <- asks config
  startRcvFiles cfg
  startSndFiles cfg
  startDelFiles cfg
  where
    startRcvFiles AgentConfig {rcvFilesTTL} = do
      pendingRcvServers <- withStore' c (`getPendingRcvFilesServers` rcvFilesTTL)
      forM_ pendingRcvServers $ \s -> resumeXFTPRcvWork c (Just s)
      -- start local worker for files pending decryption,
      -- no need to make an extra query for the check
      -- as the worker will check the store anyway
      resumeXFTPRcvWork c Nothing
    startSndFiles AgentConfig {sndFilesTTL} = do
      -- start worker for files pending encryption/creation
      resumeXFTPSndWork c Nothing
      pendingSndServers <- withStore' c (`getPendingSndFilesServers` sndFilesTTL)
      forM_ pendingSndServers $ \s -> resumeXFTPSndWork c (Just s)
    startDelFiles AgentConfig {rcvFilesTTL} = do
      pendingDelServers <- withStore' c (`getPendingDelFilesServers` rcvFilesTTL)
      forM_ pendingDelServers $ resumeXFTPDelWork c

closeXFTPAgent :: MonadUnliftIO m => XFTPAgent -> m ()
closeXFTPAgent a = do
  stopWorkers $ xftpRcvWorkers a
  stopWorkers $ xftpSndWorkers a
  stopWorkers $ xftpDelWorkers a
  where
    stopWorkers workers = atomically (swapTVar workers M.empty) >>= mapM_ (liftIO . cancelWorker)

xftpReceiveFile' :: AgentMonad m => AgentClient -> UserId -> ValidFileDescription 'FRecipient -> Maybe CryptoFileArgs -> m RcvFileId
xftpReceiveFile' c userId (ValidFileDescription fd@FileDescription {chunks, redirect}) cfArgs = do
  g <- asks random
  prefixPath <- getPrefixPath "rcv.xftp"
  createDirectory prefixPath
  let relPrefixPath = takeFileName prefixPath
      relTmpPath = relPrefixPath </> "xftp.encrypted"
      relSavePath = relPrefixPath </> "xftp.decrypted"
  createDirectory =<< toFSFilePath relTmpPath
  createEmptyFile =<< toFSFilePath relSavePath
  let saveFile = CryptoFile relSavePath cfArgs
  fId <- case redirect of
    Nothing -> withStore c $ \db -> createRcvFile db g userId fd relPrefixPath relTmpPath saveFile
    Just _ -> do
      -- prepare description paths
      let relTmpPathRedirect = relPrefixPath </> "xftp.redirect-encrypted"
          relSavePathRedirect = relPrefixPath </> "xftp.redirect-decrypted"
      createDirectory =<< toFSFilePath relTmpPathRedirect
      createEmptyFile =<< toFSFilePath relSavePathRedirect
      cfArgsRedirect <- atomically $ CF.randomArgs g
      let saveFileRedirect = CryptoFile relSavePathRedirect $ Just cfArgsRedirect
      -- create download tasks
      withStore c $ \db -> createRcvFileRedirect db g userId fd relPrefixPath relTmpPathRedirect saveFileRedirect relTmpPath saveFile
  forM_ chunks (downloadChunk c)
  pure fId

downloadChunk :: AgentMonad m => AgentClient -> FileChunk -> m ()
downloadChunk c FileChunk {replicas = (FileChunkReplica {server} : _)} = do
  void $ getXFTPRcvWorker True c (Just server)
downloadChunk _ _ = throwError $ INTERNAL "no replicas"

getPrefixPath :: AgentMonad m => String -> m FilePath
getPrefixPath suffix = do
  workPath <- getXFTPWorkPath
  ts <- liftIO getCurrentTime
  let isoTime = formatTime defaultTimeLocale "%Y%m%d_%H%M%S_%6q" ts
  uniqueCombine workPath (isoTime <> "_" <> suffix)

toFSFilePath :: AgentMonad m => FilePath -> m FilePath
toFSFilePath f = (</> f) <$> getXFTPWorkPath

createEmptyFile :: AgentMonad m => FilePath -> m ()
createEmptyFile fPath = liftIO $ B.writeFile fPath ""

resumeXFTPRcvWork :: AgentMonad' m => AgentClient -> Maybe XFTPServer -> m ()
resumeXFTPRcvWork = void .: getXFTPRcvWorker False

getXFTPRcvWorker :: AgentMonad' m => Bool -> AgentClient -> Maybe XFTPServer -> m Worker
getXFTPRcvWorker hasWork c server = do
  ws <- asks $ xftpRcvWorkers . xftpAgent
  getAgentWorker "xftp_rcv" hasWork c server ws $
    maybe (runXFTPRcvLocalWorker c) (runXFTPRcvWorker c) server

runXFTPRcvWorker :: forall m. AgentMonad m => AgentClient -> XFTPServer -> Worker -> m ()
runXFTPRcvWorker c srv Worker {doWork} = do
  cfg <- asks config
  forever $ do
    waitForWork doWork
    atomically $ assertAgentForeground c
    runXFTPOperation cfg
  where
    runXFTPOperation :: AgentConfig -> m ()
    runXFTPOperation AgentConfig {rcvFilesTTL, reconnectInterval = ri, xftpNotifyErrsOnRetry = notifyOnRetry, xftpConsecutiveRetries} =
      withWork c doWork (\db -> getNextRcvChunkToDownload db srv rcvFilesTTL) $ \case
        RcvFileChunk {rcvFileId, rcvFileEntityId, fileTmpPath, replicas = []} -> rcvWorkerInternalError c rcvFileId rcvFileEntityId (Just fileTmpPath) "chunk has no replicas"
        fc@RcvFileChunk {userId, rcvFileId, rcvFileEntityId, digest, fileTmpPath, replicas = replica@RcvFileChunkReplica {rcvChunkReplicaId, server, delay} : _} -> do
          let ri' = maybe ri (\d -> ri {initialInterval = d, increaseAfter = 0}) delay
          withRetryIntervalLimit xftpConsecutiveRetries ri' $ \delay' loop ->
            downloadFileChunk fc replica
              `catchAgentError` \e -> retryOnError "XFTP rcv worker" (retryLoop loop e delay') (retryDone e) e
          where
            retryLoop loop e replicaDelay = do
              flip catchAgentError (\_ -> pure ()) $ do
                when notifyOnRetry $ notify c rcvFileEntityId $ RFERR e
                closeXFTPServerClient c userId server digest
                withStore' c $ \db -> updateRcvChunkReplicaDelay db rcvChunkReplicaId replicaDelay
              atomically $ assertAgentForeground c
              loop
            retryDone e = rcvWorkerInternalError c rcvFileId rcvFileEntityId (Just fileTmpPath) (show e)
    downloadFileChunk :: RcvFileChunk -> RcvFileChunkReplica -> m ()
    downloadFileChunk RcvFileChunk {userId, rcvFileId, rcvFileEntityId, rcvChunkId, chunkNo, chunkSize, digest, fileTmpPath} replica = do
      fsFileTmpPath <- toFSFilePath fileTmpPath
      chunkPath <- uniqueCombine fsFileTmpPath $ show chunkNo
      let chunkSpec = XFTPRcvChunkSpec chunkPath (unFileSize chunkSize) (unFileDigest digest)
          relChunkPath = fileTmpPath </> takeFileName chunkPath
      agentXFTPDownloadChunk c userId digest replica chunkSpec
      atomically $ waitUntilForeground c
      (entityId, complete, progress) <- withStore c $ \db -> runExceptT $ do
        liftIO $ updateRcvFileChunkReceived db (rcvChunkReplicaId replica) rcvChunkId relChunkPath
        RcvFile {size = FileSize currentSize, chunks, redirect} <- ExceptT $ getRcvFile db rcvFileId
        let rcvd = receivedSize chunks
            complete = all chunkReceived chunks
            (entityId, total) = case redirect of
              Nothing -> (rcvFileEntityId, currentSize)
              Just RcvFileRedirect {redirectFileInfo = RedirectFileInfo {size = FileSize finalSize}, redirectEntityId} -> (redirectEntityId, finalSize)
        liftIO . when complete $ updateRcvFileStatus db rcvFileId RFSReceived
        pure (entityId, complete, RFPROG rcvd total)
      notify c entityId progress
      when complete . void $
        getXFTPRcvWorker True c Nothing
      where
        receivedSize :: [RcvFileChunk] -> Int64
        receivedSize = foldl' (\sz ch -> sz + receivedChunkSize ch) 0
        receivedChunkSize ch@RcvFileChunk {chunkSize = s}
          | chunkReceived ch = fromIntegral (unFileSize s)
          | otherwise = 0
        chunkReceived RcvFileChunk {replicas} = any received replicas

-- The first call of action has n == 0, maxN is max number of retries
withRetryIntervalLimit :: forall m. MonadIO m => Int -> RetryInterval -> (Int64 -> m () -> m ()) -> m ()
withRetryIntervalLimit maxN ri action =
  withRetryIntervalCount ri $ \n delay loop ->
    when (n < maxN) $ action delay loop

retryOnError :: AgentMonad m => Text -> m a -> m a -> AgentErrorType -> m a
retryOnError name loop done e = do
  logError $ name <> " error: " <> tshow e
  if temporaryAgentError e
    then loop
    else done

rcvWorkerInternalError :: AgentMonad m => AgentClient -> DBRcvFileId -> RcvFileId -> Maybe FilePath -> String -> m ()
rcvWorkerInternalError c rcvFileId rcvFileEntityId tmpPath internalErrStr = do
  forM_ tmpPath (removePath <=< toFSFilePath)
  withStore' c $ \db -> updateRcvFileError db rcvFileId internalErrStr
  notify c rcvFileEntityId $ RFERR $ INTERNAL internalErrStr

runXFTPRcvLocalWorker :: forall m. AgentMonad m => AgentClient -> Worker -> m ()
runXFTPRcvLocalWorker c Worker {doWork} = do
  cfg <- asks config
  forever $ do
    waitForWork doWork
    atomically $ assertAgentForeground c
    runXFTPOperation cfg
  where
    runXFTPOperation :: AgentConfig -> m ()
    runXFTPOperation AgentConfig {rcvFilesTTL} =
      withWork c doWork (`getNextRcvFileToDecrypt` rcvFilesTTL) $
        \f@RcvFile {rcvFileId, rcvFileEntityId, tmpPath} ->
          decryptFile f `catchAgentError` (rcvWorkerInternalError c rcvFileId rcvFileEntityId tmpPath . show)
    decryptFile :: RcvFile -> m ()
    decryptFile RcvFile {rcvFileId, rcvFileEntityId, size, digest, key, nonce, tmpPath, saveFile, status, chunks, redirect} = do
      let CryptoFile savePath cfArgs = saveFile
      fsSavePath <- toFSFilePath savePath
      when (status == RFSDecrypting) $
        whenM (doesFileExist fsSavePath) (removeFile fsSavePath >> createEmptyFile fsSavePath)
      withStore' c $ \db -> updateRcvFileStatus db rcvFileId RFSDecrypting
      chunkPaths <- getChunkPaths chunks
      encSize <- liftIO $ foldM (\s path -> (s +) . fromIntegral <$> getFileSize path) 0 chunkPaths
      when (FileSize encSize /= size) $ throwError $ XFTP XFTP.SIZE
      encDigest <- liftIO $ LC.sha512Hash <$> readChunks chunkPaths
      when (FileDigest encDigest /= digest) $ throwError $ XFTP XFTP.DIGEST
      let destFile = CryptoFile fsSavePath cfArgs
      void $ liftError (INTERNAL . show) $ decryptChunks encSize chunkPaths key nonce $ \_ -> pure destFile
      case redirect of
        Nothing -> do
          notify c rcvFileEntityId $ RFDONE fsSavePath
          forM_ tmpPath (removePath <=< toFSFilePath)
          atomically $ waitUntilForeground c
          withStore' c (`updateRcvFileComplete` rcvFileId)
        Just RcvFileRedirect {redirectFileInfo, redirectDbId} -> do
          let RedirectFileInfo {size = redirectSize, digest = redirectDigest} = redirectFileInfo
          forM_ tmpPath (removePath <=< toFSFilePath)
          atomically $ waitUntilForeground c
          withStore' c (`updateRcvFileComplete` rcvFileId)
          -- proceed with redirect
          yaml <- liftError (INTERNAL . show) (CF.readFile $ CryptoFile fsSavePath cfArgs) `finally` (toFSFilePath fsSavePath >>= removePath)
          next@FileDescription {chunks = nextChunks} <- case strDecode (LB.toStrict yaml) of
            Left _ -> throwError . XFTP $ XFTP.REDIRECT "decode error"
            Right (ValidFileDescription fd@FileDescription {size = dstSize, digest = dstDigest})
              | dstSize /= redirectSize -> throwError . XFTP $ XFTP.REDIRECT "size mismatch"
              | dstDigest /= redirectDigest -> throwError . XFTP $ XFTP.REDIRECT "digest mismatch"
              | otherwise -> pure fd
          -- register and download chunks from the actual file
          withStore c $ \db -> updateRcvFileRedirect db redirectDbId next
          forM_ nextChunks (downloadChunk c)
      where
        getChunkPaths :: [RcvFileChunk] -> m [FilePath]
        getChunkPaths [] = pure []
        getChunkPaths (RcvFileChunk {chunkTmpPath = Just path} : cs) = do
          ps <- getChunkPaths cs
          fsPath <- toFSFilePath path
          pure $ fsPath : ps
        getChunkPaths (RcvFileChunk {chunkTmpPath = Nothing} : _cs) =
          throwError $ INTERNAL "no chunk path"

xftpDeleteRcvFile' :: AgentMonad m => AgentClient -> RcvFileId -> m ()
xftpDeleteRcvFile' c rcvFileEntityId = do
  RcvFile {rcvFileId, prefixPath, status} <- withStore c $ \db -> getRcvFileByEntityId db rcvFileEntityId
  if status == RFSComplete || status == RFSError
    then do
      removePath prefixPath
      withStore' c (`deleteRcvFile'` rcvFileId)
    else withStore' c (`updateRcvFileDeleted` rcvFileId)

notify :: forall m e. (MonadUnliftIO m, AEntityI e) => AgentClient -> EntityId -> ACommand 'Agent e -> m ()
notify c entId cmd = atomically $ writeTBQueue (subQ c) ("", entId, APC (sAEntity @e) cmd)

xftpSendFile' :: AgentMonad m => AgentClient -> UserId -> CryptoFile -> Int -> m SndFileId
xftpSendFile' c userId file numRecipients = do
  g <- asks random
  prefixPath <- getPrefixPath "snd.xftp"
  createDirectory prefixPath
  let relPrefixPath = takeFileName prefixPath
  key <- atomically $ C.randomSbKey g
  nonce <- atomically $ C.randomCbNonce g
  -- saving absolute filePath will not allow to restore file encryption after app update, but it's a short window
  fId <- withStore c $ \db -> createSndFile db g userId file numRecipients relPrefixPath key nonce Nothing
  void $ getXFTPSndWorker True c Nothing
  pure fId

xftpSendDescription' :: forall m. AgentMonad m => AgentClient -> UserId -> ValidFileDescription 'FRecipient -> m SndFileId
xftpSendDescription' c userId (ValidFileDescription fdDirect@FileDescription {size, digest}) = do
  g <- asks random
  prefixPath <- getPrefixPath "snd.xftp"
  createDirectory prefixPath
  let relPrefixPath = takeFileName prefixPath
  let directYaml = prefixPath </> "direct.yaml"
  cfArgs <- atomically $ CF.randomArgs g
  let file = CryptoFile directYaml (Just cfArgs)
  liftError (INTERNAL . show) $ CF.writeFile file (LB.fromStrict $ strEncode fdDirect)
  key <- atomically $ C.randomSbKey g
  nonce <- atomically $ C.randomCbNonce g
  fId <- withStore c $ \db -> createSndFile db g userId file 1 relPrefixPath key nonce $ Just RedirectFileInfo {size, digest}
  void $ getXFTPSndWorker True c Nothing
  pure fId

resumeXFTPSndWork :: AgentMonad' m => AgentClient -> Maybe XFTPServer -> m ()
resumeXFTPSndWork = void .: getXFTPSndWorker False

getXFTPSndWorker :: AgentMonad' m => Bool -> AgentClient -> Maybe XFTPServer -> m Worker
getXFTPSndWorker hasWork c server = do
  ws <- asks $ xftpSndWorkers . xftpAgent
  getAgentWorker "xftp_snd" hasWork c server ws $
    maybe (runXFTPSndPrepareWorker c) (runXFTPSndWorker c) server

runXFTPSndPrepareWorker :: forall m. AgentMonad m => AgentClient -> Worker -> m ()
runXFTPSndPrepareWorker c Worker {doWork} = do
  cfg <- asks config
  forever $ do
    waitForWork doWork
    atomically $ assertAgentForeground c
    runXFTPOperation cfg
  where
    runXFTPOperation :: AgentConfig -> m ()
    runXFTPOperation cfg@AgentConfig {sndFilesTTL} =
      withWork c doWork (`getNextSndFileToPrepare` sndFilesTTL) $
        \f@SndFile {sndFileId, sndFileEntityId, prefixPath} ->
          prepareFile cfg f `catchAgentError` (sndWorkerInternalError c sndFileId sndFileEntityId prefixPath . show)
    prepareFile :: AgentConfig -> SndFile -> m ()
    prepareFile _ SndFile {prefixPath = Nothing} =
      throwError $ INTERNAL "no prefix path"
    prepareFile cfg sndFile@SndFile {sndFileId, userId, prefixPath = Just ppath, status} = do
      SndFile {numRecipients, chunks} <-
        if status /= SFSEncrypted -- status is SFSNew or SFSEncrypting
          then do
            fsEncPath <- toFSFilePath $ sndFileEncPath ppath
            when (status == SFSEncrypting) . whenM (doesFileExist fsEncPath) $
              removeFile fsEncPath
            withStore' c $ \db -> updateSndFileStatus db sndFileId SFSEncrypting
            (digest, chunkSpecsDigests) <- encryptFileForUpload sndFile fsEncPath
            withStore c $ \db -> do
              updateSndFileEncrypted db sndFileId digest chunkSpecsDigests
              getSndFile db sndFileId
          else pure sndFile
      let numRecipients' = min numRecipients maxRecipients
      -- concurrently?
      -- separate worker to create chunks? record retries and delay on snd_file_chunks?
      forM_ (filter (not . chunkCreated) chunks) $ createChunk numRecipients'
      withStore' c $ \db -> updateSndFileStatus db sndFileId SFSUploading
      where
        AgentConfig {xftpMaxRecipientsPerRequest = maxRecipients, messageRetryInterval = ri} = cfg
        encryptFileForUpload :: SndFile -> FilePath -> m (FileDigest, [(XFTPChunkSpec, FileDigest)])
        encryptFileForUpload SndFile {key, nonce, srcFile} fsEncPath = do
          let CryptoFile {filePath} = srcFile
              fileName = takeFileName filePath
          fileSize <- liftIO $ fromInteger <$> CF.getFileContentsSize srcFile
          when (fileSize > maxFileSize) $ throwError $ INTERNAL "max file size exceeded"
          let fileHdr = smpEncode FileHeader {fileName, fileExtra = Nothing}
              fileSize' = fromIntegral (B.length fileHdr) + fileSize
              chunkSizes = prepareChunkSizes $ fileSize' + fileSizeLen + authTagSize
              chunkSizes' = map fromIntegral chunkSizes
              encSize = sum chunkSizes'
          void $ liftError (INTERNAL . show) $ encryptFile srcFile fileHdr key nonce fileSize' encSize fsEncPath
          digest <- liftIO $ LC.sha512Hash <$> LB.readFile fsEncPath
          let chunkSpecs = prepareChunkSpecs fsEncPath chunkSizes
          chunkDigests <- map FileDigest <$> mapM (liftIO . getChunkDigest) chunkSpecs
          pure (FileDigest digest, zip chunkSpecs chunkDigests)
        chunkCreated :: SndFileChunk -> Bool
        chunkCreated SndFileChunk {replicas} =
          any (\SndFileChunkReplica {replicaStatus} -> replicaStatus == SFRSCreated) replicas
        createChunk :: Int -> SndFileChunk -> m ()
        createChunk numRecipients' ch = do
          atomically $ assertAgentForeground c
          (replica, ProtoServerWithAuth srv _) <- tryCreate
          withStore' c $ \db -> createSndFileReplica db ch replica
          void $ getXFTPSndWorker True c (Just srv)
          where
            tryCreate = do
              usedSrvs <- newTVarIO ([] :: [XFTPServer])
              withRetryInterval (riFast ri) $ \_ loop ->
                createWithNextSrv usedSrvs
                  `catchAgentError` \e -> retryOnError "XFTP prepare worker" (retryLoop loop) (throwError e) e
              where
                retryLoop loop = atomically (assertAgentForeground c) >> loop
            createWithNextSrv usedSrvs = do
              deleted <- withStore' c $ \db -> getSndFileDeleted db sndFileId
              when deleted $ throwError $ INTERNAL "file deleted, aborting chunk creation"
              withNextSrv c userId usedSrvs [] $ \srvAuth -> do
                replica <- agentXFTPNewChunk c ch numRecipients' srvAuth
                pure (replica, srvAuth)

sndWorkerInternalError :: AgentMonad m => AgentClient -> DBSndFileId -> SndFileId -> Maybe FilePath -> String -> m ()
sndWorkerInternalError c sndFileId sndFileEntityId prefixPath internalErrStr = do
  forM_ prefixPath $ removePath <=< toFSFilePath
  withStore' c $ \db -> updateSndFileError db sndFileId internalErrStr
  notify c sndFileEntityId $ SFERR $ INTERNAL internalErrStr

runXFTPSndWorker :: forall m. AgentMonad m => AgentClient -> XFTPServer -> Worker -> m ()
runXFTPSndWorker c srv Worker {doWork} = do
  cfg <- asks config
  forever $ do
    waitForWork doWork
    atomically $ assertAgentForeground c
    runXFTPOperation cfg
  where
    runXFTPOperation :: AgentConfig -> m ()
    runXFTPOperation cfg@AgentConfig {sndFilesTTL, reconnectInterval = ri, xftpNotifyErrsOnRetry = notifyOnRetry, xftpConsecutiveRetries} = do
      withWork c doWork (\db -> getNextSndChunkToUpload db srv sndFilesTTL) $ \case
        SndFileChunk {sndFileId, sndFileEntityId, filePrefixPath, replicas = []} -> sndWorkerInternalError c sndFileId sndFileEntityId (Just filePrefixPath) "chunk has no replicas"
        fc@SndFileChunk {userId, sndFileId, sndFileEntityId, filePrefixPath, digest, replicas = replica@SndFileChunkReplica {sndChunkReplicaId, server, delay} : _} -> do
          let ri' = maybe ri (\d -> ri {initialInterval = d, increaseAfter = 0}) delay
          withRetryIntervalLimit xftpConsecutiveRetries ri' $ \delay' loop ->
            uploadFileChunk cfg fc replica
              `catchAgentError` \e -> retryOnError "XFTP snd worker" (retryLoop loop e delay') (retryDone e) e
          where
            retryLoop loop e replicaDelay = do
              flip catchAgentError (\_ -> pure ()) $ do
                when notifyOnRetry $ notify c sndFileEntityId $ SFERR e
                closeXFTPServerClient c userId server digest
                withStore' c $ \db -> updateSndChunkReplicaDelay db sndChunkReplicaId replicaDelay
              atomically $ assertAgentForeground c
              loop
            retryDone e = sndWorkerInternalError c sndFileId sndFileEntityId (Just filePrefixPath) (show e)
    uploadFileChunk :: AgentConfig -> SndFileChunk -> SndFileChunkReplica -> m ()
    uploadFileChunk AgentConfig {xftpMaxRecipientsPerRequest = maxRecipients} sndFileChunk@SndFileChunk {sndFileId, userId, chunkSpec = chunkSpec@XFTPChunkSpec {filePath}, digest = chunkDigest} replica = do
      replica'@SndFileChunkReplica {sndChunkReplicaId} <- addRecipients sndFileChunk replica
      fsFilePath <- toFSFilePath filePath
      unlessM (doesFileExist fsFilePath) $ throwError $ INTERNAL "encrypted file doesn't exist on upload"
      let chunkSpec' = chunkSpec {filePath = fsFilePath} :: XFTPChunkSpec
      atomically $ assertAgentForeground c
      agentXFTPUploadChunk c userId chunkDigest replica' chunkSpec'
      atomically $ waitUntilForeground c
      sf@SndFile {sndFileEntityId, prefixPath, chunks} <- withStore c $ \db -> do
        updateSndChunkReplicaStatus db sndChunkReplicaId SFRSUploaded
        getSndFile db sndFileId
      let uploaded = uploadedSize chunks
          total = totalSize chunks
          complete = all chunkUploaded chunks
      notify c sndFileEntityId $ SFPROG uploaded total
      when complete $ do
        (sndDescr, rcvDescrs) <- sndFileToDescrs sf
        notify c sndFileEntityId $ SFDONE sndDescr rcvDescrs
        forM_ prefixPath $ removePath <=< toFSFilePath
        withStore' c $ \db -> updateSndFileComplete db sndFileId
      where
        addRecipients :: SndFileChunk -> SndFileChunkReplica -> m SndFileChunkReplica
        addRecipients ch@SndFileChunk {numRecipients} cr@SndFileChunkReplica {rcvIdsKeys}
          | length rcvIdsKeys > numRecipients = throwError $ INTERNAL "too many recipients"
          | length rcvIdsKeys == numRecipients = pure cr
          | otherwise = do
              let numRecipients' = min (numRecipients - length rcvIdsKeys) maxRecipients
              rcvIdsKeys' <- agentXFTPAddRecipients c userId chunkDigest cr numRecipients'
              cr' <- withStore' c $ \db -> addSndChunkReplicaRecipients db cr $ L.toList rcvIdsKeys'
              addRecipients ch cr'
        sndFileToDescrs :: SndFile -> m (ValidFileDescription 'FSender, [ValidFileDescription 'FRecipient])
        sndFileToDescrs SndFile {digest = Nothing} = throwError $ INTERNAL "snd file has no digest"
        sndFileToDescrs SndFile {chunks = []} = throwError $ INTERNAL "snd file has no chunks"
        sndFileToDescrs SndFile {digest = Just digest, key, nonce, chunks = chunks@(fstChunk : _), redirect} = do
          let chunkSize = FileSize $ sndChunkSize fstChunk
              size = FileSize $ sum $ map (fromIntegral . sndChunkSize) chunks
          -- snd description
          sndDescrChunks <- mapM toSndDescrChunk chunks
          let fdSnd = FileDescription {party = SFSender, size, digest, key, nonce, chunkSize, chunks = sndDescrChunks, redirect = Nothing}
          validFdSnd <- either (throwError . INTERNAL) pure $ validateFileDescription fdSnd
          -- rcv descriptions
          let fdRcv = FileDescription {party = SFRecipient, size, digest, key, nonce, chunkSize, chunks = [], redirect}
              fdRcvs = createRcvFileDescriptions fdRcv chunks
          validFdRcvs <- either (throwError . INTERNAL) pure $ mapM validateFileDescription fdRcvs
          pure (validFdSnd, validFdRcvs)
        toSndDescrChunk :: SndFileChunk -> m FileChunk
        toSndDescrChunk SndFileChunk {replicas = []} = throwError $ INTERNAL "snd file chunk has no replicas"
        toSndDescrChunk ch@SndFileChunk {chunkNo, digest = chDigest, replicas = (SndFileChunkReplica {server, replicaId, replicaKey} : _)} = do
          let chunkSize = FileSize $ sndChunkSize ch
              replicas = [FileChunkReplica {server, replicaId, replicaKey}]
          pure FileChunk {chunkNo, digest = chDigest, chunkSize, replicas}
        createRcvFileDescriptions :: FileDescription 'FRecipient -> [SndFileChunk] -> [FileDescription 'FRecipient]
        createRcvFileDescriptions fd sndChunks = map (\chunks -> (fd :: (FileDescription 'FRecipient)) {chunks}) rcvChunks
          where
            rcvReplicas :: [SentRecipientReplica]
            rcvReplicas = concatMap toSentRecipientReplicas sndChunks
            toSentRecipientReplicas :: SndFileChunk -> [SentRecipientReplica]
            toSentRecipientReplicas ch@SndFileChunk {chunkNo, digest, replicas} =
              let chunkSize = FileSize $ sndChunkSize ch
               in concatMap
                    ( \SndFileChunkReplica {server, rcvIdsKeys} ->
                        zipWith
                          (\rcvNo (replicaId, replicaKey) -> SentRecipientReplica {chunkNo, server, rcvNo, replicaId, replicaKey, digest, chunkSize})
                          [1 ..]
                          rcvIdsKeys
                    )
                    replicas
            rcvChunks :: [[FileChunk]]
            rcvChunks = map (sortChunks . M.elems) $ M.elems $ foldl' addRcvChunk M.empty rcvReplicas
            sortChunks :: [FileChunk] -> [FileChunk]
            sortChunks = map reverseReplicas . sortOn (\FileChunk {chunkNo} -> chunkNo)
            reverseReplicas ch@FileChunk {replicas} = (ch :: FileChunk) {replicas = reverse replicas}
            addRcvChunk :: Map Int (Map Int FileChunk) -> SentRecipientReplica -> Map Int (Map Int FileChunk)
            addRcvChunk m SentRecipientReplica {chunkNo, server, rcvNo, replicaId, replicaKey, digest, chunkSize} =
              M.alter (Just . addOrChangeRecipient) rcvNo m
              where
                addOrChangeRecipient :: Maybe (Map Int FileChunk) -> Map Int FileChunk
                addOrChangeRecipient = \case
                  Just m' -> M.alter (Just . addOrChangeChunk) chunkNo m'
                  _ -> M.singleton chunkNo $ FileChunk {chunkNo, digest, chunkSize, replicas = [replica']}
                addOrChangeChunk :: Maybe FileChunk -> FileChunk
                addOrChangeChunk = \case
                  Just ch@FileChunk {replicas} -> ch {replicas = replica' : replicas}
                  _ -> FileChunk {chunkNo, digest, chunkSize, replicas = [replica']}
                replica' = FileChunkReplica {server, replicaId, replicaKey}
        uploadedSize :: [SndFileChunk] -> Int64
        uploadedSize = foldl' (\sz ch -> sz + uploadedChunkSize ch) 0
        uploadedChunkSize ch
          | chunkUploaded ch = fromIntegral (sndChunkSize ch)
          | otherwise = 0
        totalSize :: [SndFileChunk] -> Int64
        totalSize = foldl' (\sz ch -> sz + fromIntegral (sndChunkSize ch)) 0
        chunkUploaded :: SndFileChunk -> Bool
        chunkUploaded SndFileChunk {replicas} =
          any (\SndFileChunkReplica {replicaStatus} -> replicaStatus == SFRSUploaded) replicas

deleteSndFileInternal :: AgentMonad m => AgentClient -> SndFileId -> m ()
deleteSndFileInternal c sndFileEntityId = do
  SndFile {sndFileId, prefixPath, status} <- withStore c $ \db -> getSndFileByEntityId db sndFileEntityId
  if status == SFSComplete || status == SFSError
    then do
      forM_ prefixPath $ removePath <=< toFSFilePath
      withStore' c (`deleteSndFile'` sndFileId)
    else withStore' c (`updateSndFileDeleted` sndFileId)

deleteSndFileRemote :: forall m. AgentMonad m => AgentClient -> UserId -> SndFileId -> ValidFileDescription 'FSender -> m ()
deleteSndFileRemote c userId sndFileEntityId (ValidFileDescription FileDescription {chunks}) = do
  deleteSndFileInternal c sndFileEntityId `catchAgentError` (notify c sndFileEntityId . SFERR)
  forM_ chunks $ \ch -> deleteFileChunk ch `catchAgentError` (notify c sndFileEntityId . SFERR)
  where
    deleteFileChunk :: FileChunk -> m ()
    deleteFileChunk FileChunk {digest, replicas = replica@FileChunkReplica {server} : _} = do
      withStore' c $ \db -> createDeletedSndChunkReplica db userId replica digest
      void $ getXFTPDelWorker True c server
    deleteFileChunk _ = pure ()

resumeXFTPDelWork :: AgentMonad' m => AgentClient -> XFTPServer -> m ()
resumeXFTPDelWork = void .: getXFTPDelWorker False

getXFTPDelWorker :: AgentMonad' m => Bool -> AgentClient -> XFTPServer -> m Worker
getXFTPDelWorker hasWork c server = do
  ws <- asks $ xftpDelWorkers . xftpAgent
  getAgentWorker "xftp_del" hasWork c server ws $ runXFTPDelWorker c server

runXFTPDelWorker :: forall m. AgentMonad m => AgentClient -> XFTPServer -> Worker -> m ()
runXFTPDelWorker c srv Worker {doWork} = do
  cfg <- asks config
  forever $ do
    waitForWork doWork
    atomically $ assertAgentForeground c
    runXFTPOperation cfg
  where
    runXFTPOperation :: AgentConfig -> m ()
    runXFTPOperation AgentConfig {rcvFilesTTL, reconnectInterval = ri, xftpNotifyErrsOnRetry = notifyOnRetry, xftpConsecutiveRetries} = do
      -- no point in deleting files older than rcv ttl, as they will be expired on server
      withWork c doWork (\db -> getNextDeletedSndChunkReplica db srv rcvFilesTTL) processDeletedReplica
      where
        processDeletedReplica replica@DeletedSndChunkReplica {deletedSndChunkReplicaId, userId, server, chunkDigest, delay} = do
          let ri' = maybe ri (\d -> ri {initialInterval = d, increaseAfter = 0}) delay
          withRetryIntervalLimit xftpConsecutiveRetries ri' $ \delay' loop ->
            deleteChunkReplica
              `catchAgentError` \e -> retryOnError "XFTP del worker" (retryLoop loop e delay') (retryDone e) e
          where
            retryLoop loop e replicaDelay = do
              flip catchAgentError (\_ -> pure ()) $ do
                when notifyOnRetry $ notify c "" $ SFERR e
                closeXFTPServerClient c userId server chunkDigest
                withStore' c $ \db -> updateDeletedSndChunkReplicaDelay db deletedSndChunkReplicaId replicaDelay
              atomically $ assertAgentForeground c
              loop
            retryDone = delWorkerInternalError c deletedSndChunkReplicaId
            deleteChunkReplica = do
              agentXFTPDeleteChunk c userId replica
              withStore' c $ \db -> deleteDeletedSndChunkReplica db deletedSndChunkReplicaId

delWorkerInternalError :: AgentMonad m => AgentClient -> Int64 -> AgentErrorType -> m ()
delWorkerInternalError c deletedSndChunkReplicaId e = do
  withStore' c $ \db -> deleteDeletedSndChunkReplica db deletedSndChunkReplicaId
  notify c "" $ SFERR e

assertAgentForeground :: AgentClient -> STM ()
assertAgentForeground c = do
  throwWhenInactive c
  waitUntilForeground c
