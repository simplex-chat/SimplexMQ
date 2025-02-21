{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Simplex.FileTransfer.Server where

import Control.Logger.Simple
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Trans.Except
import Data.Bifunctor (first)
import qualified Data.ByteString.Base64.URL as B64
import Data.ByteString.Builder (Builder, byteString)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Int (Int64)
import Data.List (intercalate)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as L
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, isJust)
import qualified Data.Text as T
import Data.Time.Clock (UTCTime (..), diffTimeToPicoseconds, getCurrentTime)
import Data.Time.Format.ISO8601 (iso8601Show)
import Data.Word (Word32)
import qualified Data.X509 as X
import GHC.IO.Handle (hSetNewlineMode)
import GHC.IORef (atomicSwapIORef)
import GHC.Stats (getRTSStats)
import qualified Network.HTTP.Types as N
import qualified Network.HTTP2.Server as H
import Network.Socket
import Simplex.FileTransfer.Protocol
import Simplex.FileTransfer.Server.Control
import Simplex.FileTransfer.Server.Env
import Simplex.FileTransfer.Server.Stats
import Simplex.FileTransfer.Server.Store
import Simplex.FileTransfer.Server.StoreLog
import Simplex.FileTransfer.Transport
import qualified Simplex.Messaging.Crypto as C
import qualified Simplex.Messaging.Crypto.Lazy as LC
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (CorrId (..), BlockingInfo, EntityId (..), RcvPublicAuthKey, RcvPublicDhKey, RecipientId, TransmissionAuth, pattern NoEntity)
import Simplex.Messaging.Server (dummyVerifyCmd, verifyCmdAuthorization)
import Simplex.Messaging.Server.Control (CPClientRole (..))
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Server.QueueStore (RoundedSystemTime, ServerEntityStatus (..), getRoundedSystemTime)
import Simplex.Messaging.Server.Stats
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport (ALPN, SessionId, THandleAuth (..), THandleParams (..), TransportPeer (..), defaultSupportedParams)
import Simplex.Messaging.Transport.Buffer (trimCR)
import Simplex.Messaging.Transport.HTTP2
import Simplex.Messaging.Transport.HTTP2.File (fileBlockSize)
import Simplex.Messaging.Transport.HTTP2.Server
import Simplex.Messaging.Transport.Server (runLocalTCPServer)
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import System.Exit (exitFailure)
import System.FilePath ((</>))
import System.IO (hPrint, hPutStrLn, universalNewlineMode)
#ifdef slow_servers
import System.Random (getStdRandom, randomR)
#endif
import UnliftIO
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Directory (doesFileExist, removeFile, renameFile)
import qualified UnliftIO.Exception as E

type M a = ReaderT XFTPEnv IO a

data XFTPTransportRequest = XFTPTransportRequest
  { thParams :: THandleParamsXFTP 'TServer,
    reqBody :: HTTP2Body,
    request :: H.Request,
    sendResponse :: H.Response -> IO ()
  }

runXFTPServer :: XFTPServerConfig -> IO ()
runXFTPServer cfg = do
  started <- newEmptyTMVarIO
  runXFTPServerBlocking started cfg $ Just supportedXFTPhandshakes

runXFTPServerBlocking :: TMVar Bool -> XFTPServerConfig -> Maybe [ALPN] -> IO ()
runXFTPServerBlocking started cfg alpn_ = newXFTPServerEnv cfg >>= runReaderT (xftpServer cfg started alpn_)

data Handshake
  = HandshakeSent C.PrivateKeyX25519
  | HandshakeAccepted (THandleParams XFTPVersion 'TServer)

xftpServer :: XFTPServerConfig -> TMVar Bool -> Maybe [ALPN] -> M ()
xftpServer cfg@XFTPServerConfig {xftpPort, transportConfig, inactiveClientExpiration, fileExpiration, xftpServerVRange} started alpn_ = do
  mapM_ (expireServerFiles Nothing) fileExpiration
  restoreServerStats
  raceAny_ (runServer : expireFilesThread_ cfg <> serverStatsThread_ cfg <> controlPortThread_ cfg) `finally` stopServer
  where
    runServer :: M ()
    runServer = do
      srvCreds@(chain, pk) <- asks tlsServerCreds
      signKey <- liftIO $ case C.x509ToPrivate (pk, []) >>= C.privKey of
        Right pk' -> pure pk'
        Left e -> putStrLn ("servers has no valid key: " <> show e) >> exitFailure
      env <- ask
      sessions <- liftIO TM.emptyIO
      let cleanup sessionId = atomically $ TM.delete sessionId sessions
      liftIO . runHTTP2Server started xftpPort defaultHTTP2BufferSize defaultSupportedParams srvCreds alpn_ transportConfig inactiveClientExpiration cleanup $ \sessionId sessionALPN r sendResponse -> do
        reqBody <- getHTTP2Body r xftpBlockSize
        let v = VersionXFTP 1
            thServerVRange = versionToRange v
            thParams0 = THandleParams {sessionId, blockSize = xftpBlockSize, thVersion = v, thServerVRange, thAuth = Nothing, implySessId = False, encryptBlock = Nothing, batch = True}
            req0 = XFTPTransportRequest {thParams = thParams0, request = r, reqBody, sendResponse}
        flip runReaderT env $ case sessionALPN of
          Nothing -> processRequest req0
          Just "xftp/1" ->
            xftpServerHandshakeV1 chain signKey sessions req0 >>= \case
              Nothing -> pure () -- handshake response sent
              Just thParams -> processRequest req0 {thParams} -- proceed with new version (XXX: may as well switch the request handler here)
          _ -> liftIO . sendResponse $ H.responseNoBody N.ok200 [] -- shouldn't happen: means server picked handshake protocol it doesn't know about
    xftpServerHandshakeV1 :: X.CertificateChain -> C.APrivateSignKey -> TMap SessionId Handshake -> XFTPTransportRequest -> M (Maybe (THandleParams XFTPVersion 'TServer))
    xftpServerHandshakeV1 chain serverSignKey sessions XFTPTransportRequest {thParams = thParams0@THandleParams {sessionId}, reqBody = HTTP2Body {bodyHead}, sendResponse} = do
      s <- atomically $ TM.lookup sessionId sessions
      r <- runExceptT $ case s of
        Nothing -> processHello
        Just (HandshakeSent pk) -> processClientHandshake pk
        Just (HandshakeAccepted thParams) -> pure $ Just thParams
      either sendError pure r
      where
        processHello = do
          unless (B.null bodyHead) $ throwE HANDSHAKE
          (k, pk) <- atomically . C.generateKeyPair =<< asks random
          atomically $ TM.insert sessionId (HandshakeSent pk) sessions
          let authPubKey = (chain, C.signX509 serverSignKey $ C.publicToX509 k)
          let hs = XFTPServerHandshake {xftpVersionRange = xftpServerVRange, sessionId, authPubKey}
          shs <- encodeXftp hs
#ifdef slow_servers
          lift randomDelay
#endif
          liftIO . sendResponse $ H.responseBuilder N.ok200 [] shs
          pure Nothing
        processClientHandshake pk = do
          unless (B.length bodyHead == xftpBlockSize) $ throwE HANDSHAKE
          body <- liftHS $ C.unPad bodyHead
          XFTPClientHandshake {xftpVersion = v, keyHash} <- liftHS $ smpDecode body
          kh <- asks serverIdentity
          unless (keyHash == kh) $ throwE HANDSHAKE
          case compatibleVRange' xftpServerVRange v of
            Just (Compatible vr) -> do
              let auth = THAuthServer {serverPrivKey = pk, sessSecret' = Nothing}
                  thParams = thParams0 {thAuth = Just auth, thVersion = v, thServerVRange = vr}
              atomically $ TM.insert sessionId (HandshakeAccepted thParams) sessions
#ifdef slow_servers
              lift randomDelay
#endif
              liftIO . sendResponse $ H.responseNoBody N.ok200 []
              pure Nothing
            Nothing -> throwE HANDSHAKE
        sendError :: XFTPErrorType -> M (Maybe (THandleParams XFTPVersion 'TServer))
        sendError err = do
          runExceptT (encodeXftp err) >>= \case
            Right bs -> liftIO . sendResponse $ H.responseBuilder N.ok200 [] bs
            Left _ -> logError $ "Error encoding handshake error: " <> tshow err
          pure Nothing
        encodeXftp :: Encoding a => a -> ExceptT XFTPErrorType (ReaderT XFTPEnv IO) Builder
        encodeXftp a = byteString <$> liftHS (C.pad (smpEncode a) xftpBlockSize)
        liftHS = liftEitherWith (const HANDSHAKE)

    stopServer :: M ()
    stopServer = do
      withFileLog closeStoreLog
      saveServerStats
      logInfo "Server stopped"

    expireFilesThread_ :: XFTPServerConfig -> [M ()]
    expireFilesThread_ XFTPServerConfig {fileExpiration = Just fileExp} = [expireFiles fileExp]
    expireFilesThread_ _ = []

    expireFiles :: ExpirationConfig -> M ()
    expireFiles expCfg = do
      let interval = checkInterval expCfg * 1000000
      forever $ do
        liftIO $ threadDelay' interval
        expireServerFiles (Just 100000) expCfg

    serverStatsThread_ :: XFTPServerConfig -> [M ()]
    serverStatsThread_ XFTPServerConfig {logStatsInterval = Just interval, logStatsStartTime, serverStatsLogFile} =
      [logServerStats logStatsStartTime interval serverStatsLogFile]
    serverStatsThread_ _ = []

    logServerStats :: Int64 -> Int64 -> FilePath -> M ()
    logServerStats startAt logInterval statsFilePath = do
      initialDelay <- (startAt -) . fromIntegral . (`div` 1000000_000000) . diffTimeToPicoseconds . utctDayTime <$> liftIO getCurrentTime
      liftIO $ putStrLn $ "server stats log enabled: " <> statsFilePath
      liftIO $ threadDelay' $ 1_000_000 * (initialDelay + if initialDelay < 0 then 86_400 else 0)
      FileServerStats {fromTime, filesCreated, fileRecipients, filesUploaded, filesExpired, filesDeleted, filesDownloaded, fileDownloads, fileDownloadAcks, filesCount, filesSize} <- asks serverStats
      let interval = 1_000_000 * logInterval
      forever $ do
        withFile statsFilePath AppendMode $ \h -> liftIO $ do
          hSetBuffering h LineBuffering
          ts <- getCurrentTime
          fromTime' <- atomicSwapIORef fromTime ts
          filesCreated' <- atomicSwapIORef filesCreated 0
          fileRecipients' <- atomicSwapIORef fileRecipients 0
          filesUploaded' <- atomicSwapIORef filesUploaded 0
          filesExpired' <- atomicSwapIORef filesExpired 0
          filesDeleted' <- atomicSwapIORef filesDeleted 0
          files <- liftIO $ periodStatCounts filesDownloaded ts
          fileDownloads' <- atomicSwapIORef fileDownloads 0
          fileDownloadAcks' <- atomicSwapIORef fileDownloadAcks 0
          filesCount' <- readIORef filesCount
          filesSize' <- readIORef filesSize
          hPutStrLn h $
            intercalate
              ","
              [ iso8601Show $ utctDay fromTime',
                show filesCreated',
                show fileRecipients',
                show filesUploaded',
                show filesDeleted',
                dayCount files,
                weekCount files,
                monthCount files,
                show fileDownloads',
                show fileDownloadAcks',
                show filesCount',
                show filesSize',
                show filesExpired'
              ]
        liftIO $ threadDelay' interval

    controlPortThread_ :: XFTPServerConfig -> [M ()]
    controlPortThread_ XFTPServerConfig {controlPort = Just port} = [runCPServer port]
    controlPortThread_ _ = []

    runCPServer :: ServiceName -> M ()
    runCPServer port = do
      cpStarted <- newEmptyTMVarIO
      u <- askUnliftIO
      liftIO $ do
        labelMyThread "control port server"
        runLocalTCPServer cpStarted port $ runCPClient u
      where
        runCPClient :: UnliftIO (ReaderT XFTPEnv IO) -> Socket -> IO ()
        runCPClient u sock = do
          labelMyThread "control port client"
          h <- socketToHandle sock ReadWriteMode
          hSetBuffering h LineBuffering
          hSetNewlineMode h universalNewlineMode
          hPutStrLn h "XFTP server control port\n'help' for supported commands"
          role <- newTVarIO CPRNone
          cpLoop h role
          where
            cpLoop h role = do
              s <- trimCR <$> B.hGetLine h
              case strDecode s of
                Right CPQuit -> hClose h
                Right cmd -> logCmd s cmd >> processCP h role cmd >> cpLoop h role
                Left err -> hPutStrLn h ("error: " <> err) >> cpLoop h role
            logCmd s cmd = when shouldLog $ logWarn $ "ControlPort: " <> tshow s
              where
                shouldLog = case cmd of
                  CPAuth _ -> False
                  CPHelp -> False
                  CPQuit -> False
                  CPSkip -> False
                  _ -> True
            processCP h role = \case
              CPAuth auth -> atomically $ writeTVar role $! newRole cfg
                where
                  newRole XFTPServerConfig {controlPortUserAuth = user, controlPortAdminAuth = admin}
                    | Just auth == admin = CPRAdmin
                    | Just auth == user = CPRUser
                    | otherwise = CPRNone
              CPStatsRTS -> E.tryAny getRTSStats >>= either (hPrint h) (hPrint h)
              CPDelete fileId -> withUserRole $ unliftIO u $ do
                fs <- asks store
                r <- runExceptT $ do
                  (fr, _) <- ExceptT $ atomically $ getFile fs SFRecipient fileId
                  ExceptT $ deleteServerFile_ fr
                liftIO . hPutStrLn h $ either (\e -> "error: " <> show e) (\() -> "ok") r
              CPBlock fileId info -> withUserRole $ unliftIO u $ do
                fs <- asks store
                r <- runExceptT $ do
                  (fr, _) <- ExceptT $ atomically $ getFile fs SFRecipient fileId
                  ExceptT $ blockServerFile fr info
                liftIO . hPutStrLn h $ either (\e -> "error: " <> show e) (\() -> "ok") r
              CPHelp -> hPutStrLn h "commands: stats-rts, delete, help, quit"
              CPQuit -> pure ()
              CPSkip -> pure ()
              where
                withUserRole action =
                  readTVarIO role >>= \case
                    CPRAdmin -> action
                    CPRUser -> action
                    _ -> do
                      logError "Unauthorized control port command"
                      hPutStrLn h "AUTH"

data ServerFile = ServerFile
  { filePath :: FilePath,
    fileSize :: Word32,
    sbState :: LC.SbState
  }

processRequest :: XFTPTransportRequest -> M ()
processRequest XFTPTransportRequest {thParams, reqBody = body@HTTP2Body {bodyHead}, sendResponse}
  | B.length bodyHead /= xftpBlockSize = sendXFTPResponse ("", NoEntity, FRErr BLOCK) Nothing
  | otherwise = do
      case xftpDecodeTransmission thParams bodyHead of
        Right (sig_, signed, (corrId, fId, cmdOrErr)) ->
          case cmdOrErr of
            Right cmd -> do
              let THandleParams {thAuth} = thParams
              verifyXFTPTransmission ((,C.cbNonce (bs corrId)) <$> thAuth) sig_ signed fId cmd >>= \case
                VRVerified req -> uncurry send =<< processXFTPRequest body req
                VRFailed e -> send (FRErr e) Nothing
            Left e -> send (FRErr e) Nothing
          where
            send resp = sendXFTPResponse (corrId, fId, resp)
        Left e -> sendXFTPResponse ("", NoEntity, FRErr e) Nothing
  where
    sendXFTPResponse (corrId, fId, resp) serverFile_ = do
      let t_ = xftpEncodeTransmission thParams (corrId, fId, resp)
#ifdef slow_servers
      randomDelay
#endif
      liftIO $ sendResponse $ H.responseStreaming N.ok200 [] $ streamBody t_
      where
        streamBody t_ send done = do
          case t_ of
            Left _ -> do
              send "padding error" -- TODO respond with BLOCK error?
              done
            Right t -> do
              send $ byteString t
              -- timeout sending file in the same way as receiving
              forM_ serverFile_ $ \ServerFile {filePath, fileSize, sbState} -> do
                withFile filePath ReadMode $ \h -> sendEncFile h send sbState fileSize
          done

#ifdef slow_servers
randomDelay :: M ()
randomDelay = do
  d <- asks $ responseDelay . config
  when (d > 0) $ do
    pc <- getStdRandom (randomR (-200, 200))
    threadDelay $ (d * (1000 + pc)) `div` 1000
#endif

data VerificationResult = VRVerified XFTPRequest | VRFailed XFTPErrorType

verifyXFTPTransmission :: Maybe (THandleAuth 'TServer, C.CbNonce) -> Maybe TransmissionAuth -> ByteString -> XFTPFileId -> FileCmd -> M VerificationResult
verifyXFTPTransmission auth_ tAuth authorized fId cmd =
  case cmd of
    FileCmd SFSender (FNEW file rcps auth') -> pure $ XFTPReqNew file rcps auth' `verifyWith` sndKey file
    FileCmd SFRecipient PING -> pure $ VRVerified XFTPReqPing
    FileCmd party _ -> verifyCmd party
  where
    verifyCmd :: SFileParty p -> M VerificationResult
    verifyCmd party = do
      st <- asks store
      atomically $ verify =<< getFile st party fId
      where
        verify = \case
          Right (fr, k) -> result <$> readTVar (fileStatus fr)
            where
              result = \case
                EntityActive -> XFTPReqCmd fId fr cmd `verifyWith` k
                EntityBlocked info -> VRFailed $ BLOCKED info
                EntityOff -> noFileAuth
          Left _ -> pure noFileAuth
        noFileAuth = maybe False (dummyVerifyCmd Nothing authorized) tAuth `seq` VRFailed AUTH
    -- TODO verify with DH authorization
    req `verifyWith` k = if verifyCmdAuthorization auth_ tAuth authorized k then VRVerified req else VRFailed AUTH

processXFTPRequest :: HTTP2Body -> XFTPRequest -> M (FileResponse, Maybe ServerFile)
processXFTPRequest HTTP2Body {bodyPart} = \case
  XFTPReqNew file rks auth -> noFile =<< ifM allowNew (createFile file rks) (pure $ FRErr AUTH)
    where
      allowNew = do
        XFTPServerConfig {allowNewFiles, newFileBasicAuth} <- asks config
        pure $ allowNewFiles && maybe True ((== auth) . Just) newFileBasicAuth
  XFTPReqCmd fId fr (FileCmd _ cmd) -> case cmd of
    FADD rks -> noFile =<< addRecipients fId rks
    FPUT -> noFile =<< receiveServerFile fr
    FDEL -> noFile =<< deleteServerFile fr
    FGET rDhKey -> sendServerFile fr rDhKey
    FACK -> noFile =<< ackFileReception fId fr
    -- it should never get to the commands below, they are passed in other constructors of XFTPRequest
    FNEW {} -> noFile $ FRErr INTERNAL
    PING -> noFile $ FRErr INTERNAL
  XFTPReqPing -> noFile FRPong
  where
    noFile resp = pure (resp, Nothing)
    createFile :: FileInfo -> NonEmpty RcvPublicAuthKey -> M FileResponse
    createFile file rks = do
      st <- asks store
      r <- runExceptT $ do
        sizes <- asks $ allowedChunkSizes . config
        unless (size file `elem` sizes) $ throwE SIZE
        ts <- liftIO getFileTime
        -- TODO validate body empty
        sId <- ExceptT $ addFileRetry st file 3 ts
        rcps <- mapM (ExceptT . addRecipientRetry st 3 sId) rks
        lift $ withFileLog $ \sl -> do
          logAddFile sl sId file ts EntityActive
          logAddRecipients sl sId rcps
        stats <- asks serverStats
        lift $ incFileStat filesCreated
        liftIO $ atomicModifyIORef'_ (fileRecipients stats) (+ length rks)
        let rIds = L.map (\(FileRecipient rId _) -> rId) rcps
        pure $ FRSndIds sId rIds
      pure $ either FRErr id r
    addFileRetry :: FileStore -> FileInfo -> Int -> RoundedSystemTime -> M (Either XFTPErrorType XFTPFileId)
    addFileRetry st file n ts =
      retryAdd n $ \sId -> runExceptT $ do
        ExceptT $ addFile st sId file ts EntityActive
        pure sId
    addRecipientRetry :: FileStore -> Int -> XFTPFileId -> RcvPublicAuthKey -> M (Either XFTPErrorType FileRecipient)
    addRecipientRetry st n sId rpk =
      retryAdd n $ \rId -> runExceptT $ do
        let rcp = FileRecipient rId rpk
        ExceptT $ addRecipient st sId rcp
        pure rcp
    retryAdd :: Int -> (XFTPFileId -> STM (Either XFTPErrorType a)) -> M (Either XFTPErrorType a)
    retryAdd 0 _ = pure $ Left INTERNAL
    retryAdd n add = do
      fId <- getFileId
      atomically (add fId) >>= \case
        Left DUPLICATE_ -> retryAdd (n - 1) add
        r -> pure r
    addRecipients :: XFTPFileId -> NonEmpty RcvPublicAuthKey -> M FileResponse
    addRecipients sId rks = do
      st <- asks store
      r <- runExceptT $ do
        rcps <- mapM (ExceptT . addRecipientRetry st 3 sId) rks
        lift $ withFileLog $ \sl -> logAddRecipients sl sId rcps
        stats <- asks serverStats
        liftIO $ atomicModifyIORef'_ (fileRecipients stats) (+ length rks)
        let rIds = L.map (\(FileRecipient rId _) -> rId) rcps
        pure $ FRRcvIds rIds
      pure $ either FRErr id r
    receiveServerFile :: FileRec -> M FileResponse
    receiveServerFile FileRec {senderId, fileInfo = FileInfo {size, digest}, filePath} = case bodyPart of
      Nothing -> pure $ FRErr SIZE
      -- TODO validate body size from request before downloading, once it's populated
      Just getBody -> skipCommitted $ ifM reserve receive (pure $ FRErr QUOTA)
        where
          -- having a filePath means the file is already uploaded and committed, must not change anything
          skipCommitted = ifM (isJust <$> readTVarIO filePath) (liftIO $ drain $ fromIntegral size)
            where
              -- can't send FROk without reading the request body or a client will block on sending it
              -- can't send any old error as the client would fail or restart indefinitely
              drain s = do
                bs <- B.length <$> getBody fileBlockSize
                if
                  | bs == s -> pure FROk
                  | bs == 0 || bs > s -> pure $ FRErr SIZE
                  | otherwise -> drain (s - bs)
          reserve = do
            us <- asks $ usedStorage . store
            quota <- asks $ fromMaybe maxBound . fileSizeQuota . config
            atomically . stateTVar us $
              \used -> let used' = used + fromIntegral size in if used' <= quota then (True, used') else (False, used)
          receive = do
            path <- asks $ filesPath . config
            let fPath = path </> B.unpack (B64.encode $ unEntityId senderId)
            receiveChunk (XFTPRcvChunkSpec fPath size digest) >>= \case
              Right () -> do
                stats <- asks serverStats
                withFileLog $ \sl -> logPutFile sl senderId fPath
                atomically $ writeTVar filePath (Just fPath)
                incFileStat filesUploaded
                incFileStat filesCount
                liftIO $ atomicModifyIORef'_ (filesSize stats) (+ fromIntegral size)
                pure FROk
              Left e -> do
                us <- asks $ usedStorage . store
                atomically $ modifyTVar' us $ subtract (fromIntegral size)
                liftIO $ whenM (doesFileExist fPath) (removeFile fPath) `catch` logFileError
                pure $ FRErr e
          receiveChunk spec = do
            t <- asks $ fileTimeout . config
            liftIO $ fromMaybe (Left TIMEOUT) <$> timeout t (runExceptT $ receiveFile getBody spec)
    sendServerFile :: FileRec -> RcvPublicDhKey -> M (FileResponse, Maybe ServerFile)
    sendServerFile FileRec {senderId, filePath, fileInfo = FileInfo {size}} rDhKey = do
      readTVarIO filePath >>= \case
        Just path -> ifM (doesFileExist path) sendFile (pure (FRErr AUTH, Nothing))
          where
            sendFile = do
              g <- asks random
              (sDhKey, spDhKey) <- atomically $ C.generateKeyPair g
              let dhSecret = C.dh' rDhKey spDhKey
              cbNonce <- atomically $ C.randomCbNonce g
              case LC.cbInit dhSecret cbNonce of
                Right sbState -> do
                  stats <- asks serverStats
                  incFileStat fileDownloads
                  liftIO $ updatePeriodStats (filesDownloaded stats) senderId
                  pure (FRFile sDhKey cbNonce, Just ServerFile {filePath = path, fileSize = size, sbState})
                _ -> pure (FRErr INTERNAL, Nothing)
        _ -> pure (FRErr NO_FILE, Nothing)

    deleteServerFile :: FileRec -> M FileResponse
    deleteServerFile fr = either FRErr (\() -> FROk) <$> deleteServerFile_ fr

    logFileError :: SomeException -> IO ()
    logFileError e = logError $ "Error deleting file: " <> tshow e

    ackFileReception :: RecipientId -> FileRec -> M FileResponse
    ackFileReception rId fr = do
      withFileLog (`logAckFile` rId)
      st <- asks store
      atomically $ deleteRecipient st rId fr
      incFileStat fileDownloadAcks
      pure FROk

deleteServerFile_ :: FileRec -> M (Either XFTPErrorType ())
deleteServerFile_ fr@FileRec {senderId} = do
  withFileLog (`logDeleteFile` senderId)
  deleteOrBlockServerFile_ fr filesDeleted (`deleteFile` senderId)

-- this also deletes the file from storage, but doesn't include it in delete statistics
blockServerFile :: FileRec -> BlockingInfo -> M (Either XFTPErrorType ())
blockServerFile fr@FileRec {senderId} info = do
  withFileLog $ \sl -> logBlockFile sl senderId info
  deleteOrBlockServerFile_ fr filesBlocked $ \st -> blockFile st senderId info True

deleteOrBlockServerFile_ :: FileRec -> (FileServerStats -> IORef Int) -> (FileStore -> STM (Either XFTPErrorType ())) -> M (Either XFTPErrorType ())
deleteOrBlockServerFile_ FileRec {filePath, fileInfo} stat storeAction = runExceptT $ do
  path <- readTVarIO filePath
  stats <- asks serverStats
  ExceptT $ first (\(_ :: SomeException) -> FILE_IO) <$> try (forM_ path $ \p -> whenM (doesFileExist p) (removeFile p >> deletedStats stats))
  st <- asks store
  void $ atomically $ storeAction st
  lift $ incFileStat stat
  where
    deletedStats stats = do
      liftIO $ atomicModifyIORef'_ (filesCount stats) (subtract 1)
      liftIO $ atomicModifyIORef'_ (filesSize stats) (subtract $ fromIntegral $ size fileInfo)

getFileTime :: IO RoundedSystemTime
getFileTime = getRoundedSystemTime fileTimePrecision

expireServerFiles :: Maybe Int -> ExpirationConfig -> M ()
expireServerFiles itemDelay expCfg = do
  st <- asks store
  usedStart <- readTVarIO $ usedStorage st
  old <- liftIO $ expireBeforeEpoch expCfg
  files' <- readTVarIO (files st)
  logInfo $ "Expiration check: " <> tshow (M.size files') <> " files"
  forM_ (M.keys files') $ \sId -> do
    mapM_ threadDelay itemDelay
    atomically (expiredFilePath st sId old)
      >>= mapM_ (maybeRemove $ delete st sId)
  usedEnd <- readTVarIO $ usedStorage st
  logInfo $ "Used " <> mbs usedStart <> " -> " <> mbs usedEnd <> ", " <> mbs (usedStart - usedEnd) <> " reclaimed."
  where
    mbs bs = tshow (bs `div` 1048576) <> "mb"
    maybeRemove del = maybe del (remove del)
    remove del filePath =
      ifM
        (doesFileExist filePath)
        ((removeFile filePath >> del) `catch` \(e :: SomeException) -> logError $ "failed to remove expired file " <> tshow filePath <> ": " <> tshow e)
        del
    delete st sId = do
      withFileLog (`logDeleteFile` sId)
      void . atomically $ deleteFile st sId -- will not update usedStorage if sId isn't in store
      incFileStat filesExpired

randomId :: Int -> M ByteString
randomId n = atomically . C.randomBytes n =<< asks random

getFileId :: M XFTPFileId
getFileId = fmap EntityId . randomId =<< asks (fileIdSize . config)

withFileLog :: (StoreLog 'WriteMode -> IO a) -> M ()
withFileLog action = liftIO . mapM_ action =<< asks storeLog

incFileStat :: (FileServerStats -> IORef Int) -> M ()
incFileStat statSel = do
  stats <- asks serverStats
  liftIO $ atomicModifyIORef'_ (statSel stats) (+ 1)

saveServerStats :: M ()
saveServerStats =
  asks (serverStatsBackupFile . config)
    >>= mapM_ (\f -> asks serverStats >>= liftIO . getFileServerStatsData >>= liftIO . saveStats f)
  where
    saveStats f stats = do
      logInfo $ "saving server stats to file " <> T.pack f
      B.writeFile f $ strEncode stats
      logInfo "server stats saved"

restoreServerStats :: M ()
restoreServerStats = asks (serverStatsBackupFile . config) >>= mapM_ restoreStats
  where
    restoreStats f = whenM (doesFileExist f) $ do
      logInfo $ "restoring server stats from file " <> T.pack f
      liftIO (strDecode <$> B.readFile f) >>= \case
        Right d@FileServerStatsData {_filesCount = statsFilesCount, _filesSize = statsFilesSize} -> do
          s <- asks serverStats
          FileStore {files, usedStorage} <- asks store
          _filesCount <- M.size <$> readTVarIO files
          _filesSize <- readTVarIO usedStorage
          liftIO $ setFileServerStats s d {_filesCount, _filesSize}
          renameFile f $ f <> ".bak"
          logInfo "server stats restored"
          when (statsFilesCount /= _filesCount) $ logWarn $ "Files count differs: stats: " <> tshow statsFilesCount <> ", store: " <> tshow _filesCount
          when (statsFilesSize /= _filesSize) $ logWarn $ "Files size differs: stats: " <> tshow statsFilesSize <> ", store: " <> tshow _filesSize
          logInfo $ "Restored " <> tshow (_filesSize `div` 1048576) <> " MBs in " <> tshow _filesCount <> " files"
        Left e -> do
          logInfo $ "error restoring server stats: " <> T.pack e
          liftIO exitFailure
