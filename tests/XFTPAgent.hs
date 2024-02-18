{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}

module XFTPAgent where

import AgentTests.FunctionalAPITests (get, getSMPAgentClient', rfGet, runRight, runRight_, sfGet)

import Control.Logger.Simple
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as LB
import Data.Int (Int64)
import Data.List (find, isSuffixOf)
import Data.Maybe (fromJust)
import SMPAgentClient (agentCfg, initAgentServers, testDB, testDB2, testDB3)
import Simplex.FileTransfer.Description (FileDescription (..), FileDescriptionURI (..), ValidFileDescription, fileDescriptionURI, mb, qrSizeLimit, pattern ValidFileDescription)
import Simplex.FileTransfer.Protocol (FileParty (..), XFTPErrorType (AUTH))
import Simplex.FileTransfer.Server.Env (XFTPServerConfig (..))
import Simplex.Messaging.Agent (AgentClient, disconnectAgentClient, testProtocolServer, xftpDeleteRcvFile, xftpDeleteSndFileInternal, xftpDeleteSndFileRemote, xftpReceiveFile, xftpSendDescription, xftpSendFile, xftpStartWorkers)
import Simplex.Messaging.Agent.Client (ProtocolTestFailure (..), ProtocolTestStep (..))
import Simplex.Messaging.Agent.Protocol (ACommand (..), AgentErrorType (..), BrokerErrorType (..), RcvFileId, SndFileId, noAuthSrv)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.File (CryptoFile (..), CryptoFileArgs)
import qualified Simplex.Messaging.Crypto.File as CF
import Simplex.Messaging.Encoding.String (StrEncoding (..))
import Simplex.Messaging.Protocol (BasicAuth, ProtoServerWithAuth (..), ProtocolServer (..), XFTPServerWithAuth)
import Simplex.Messaging.Server.Expiration (ExpirationConfig (..))
import Simplex.Messaging.Util (tshow)
import System.Directory (doesDirectoryExist, doesFileExist, getFileSize, listDirectory, removeFile)
import System.FilePath ((</>))
import Test.Hspec
import UnliftIO
import UnliftIO.Concurrent
import XFTPCLI
import XFTPClient

xftpAgentTests :: Spec
xftpAgentTests = around_ testBracket . describe "agent XFTP API" $ do
  it "should send and receive file" testXFTPAgentSendReceive
  it "should send and receive with encrypted local files" testXFTPAgentSendReceiveEncrypted
  it "should send and receive large file with a redirect" testXFTPAgentSendReceiveRedirect
  it "should send and receive small file without a redirect" testXFTPAgentSendReceiveNoRedirect
  it "should resume receiving file after restart" testXFTPAgentReceiveRestore
  it "should cleanup rcv tmp path after permanent error" testXFTPAgentReceiveCleanup
  it "should resume sending file after restart" testXFTPAgentSendRestore
  it "should cleanup snd prefix path after permanent error" testXFTPAgentSendCleanup
  it "should delete sent file on server" testXFTPAgentDelete
  it "should resume deleting file after restart" testXFTPAgentDeleteRestore
  -- TODO when server is fixed to correctly send AUTH error, this test has to be modified to expect AUTH error
  it "if file is deleted on server, should limit retries and continue receiving next file" testXFTPAgentDeleteOnServer
  it "if file is expired on server, should report error and continue receiving next file" testXFTPAgentExpiredOnServer
  it "should request additional recipient IDs when number of recipients exceeds maximum per request" testXFTPAgentRequestAdditionalRecipientIDs
  describe "XFTP server test via agent API" $ do
    it "should pass without basic auth" $ testXFTPServerTest Nothing (noAuthSrv testXFTPServer2) `shouldReturn` Nothing
    let srv1 = testXFTPServer2 {keyHash = "1234"}
    it "should fail with incorrect fingerprint" $ do
      testXFTPServerTest Nothing (noAuthSrv srv1) `shouldReturn` Just (ProtocolTestFailure TSConnect $ BROKER (B.unpack $ strEncode srv1) NETWORK)
    describe "server with password" $ do
      let auth = Just "abcd"
          srv = ProtoServerWithAuth testXFTPServer2
          authErr = Just (ProtocolTestFailure TSCreateFile $ XFTP AUTH)
      it "should pass with correct password" $ testXFTPServerTest auth (srv auth) `shouldReturn` Nothing
      it "should fail without password" $ testXFTPServerTest auth (srv Nothing) `shouldReturn` authErr
      it "should fail with incorrect password" $ testXFTPServerTest auth (srv $ Just "wrong") `shouldReturn` authErr

rfProgress :: forall m. (HasCallStack, MonadIO m, MonadFail m) => AgentClient -> Int64 -> m ()
rfProgress c expected = loop 0
  where
    loop :: HasCallStack => Int64 -> m ()
    loop prev = do
      (_, _, RFPROG rcvd total) <- rfGet c
      checkProgress (prev, expected) (rcvd, total) loop

sfProgress :: forall m. (HasCallStack, MonadIO m, MonadFail m) => AgentClient -> Int64 -> m ()
sfProgress c expected = loop 0
  where
    loop :: HasCallStack => Int64 -> m ()
    loop prev = do
      (_, _, SFPROG sent total) <- sfGet c
      checkProgress (prev, expected) (sent, total) loop

-- checks that progress increases till it reaches total
checkProgress :: (HasCallStack, MonadIO m) => (Int64, Int64) -> (Int64, Int64) -> (Int64 -> m ()) -> m ()
checkProgress (prev, expected) (progress, total) loop
  | total /= expected = error "total /= expected"
  | progress <= prev = error "progress <= prev"
  | progress > total = error "progress > total"
  | progress < total = loop progress
  | otherwise = pure ()

testXFTPAgentSendReceive :: HasCallStack => IO ()
testXFTPAgentSendReceive = withXFTPServer $ do
  filePath <- createRandomFile
  -- send file, delete snd file internally
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  (rfd1, rfd2) <- runRight $ do
    (sfId, _, rfd1, rfd2) <- testSend sndr filePath
    xftpDeleteSndFileInternal sndr sfId
    pure (rfd1, rfd2)

  -- receive file, delete rcv file
  testReceiveDelete 2 rfd1 filePath
  testReceiveDelete 3 rfd2 filePath
  where
    testReceiveDelete clientId rfd originalFilePath = do
      rcp <- getSMPAgentClient' clientId agentCfg initAgentServers testDB2
      runRight_ $ do
        rfId <- testReceive rcp rfd originalFilePath
        xftpDeleteRcvFile rcp rfId
      disconnectAgentClient rcp

testXFTPAgentSendReceiveEncrypted :: HasCallStack => IO ()
testXFTPAgentSendReceiveEncrypted = withXFTPServer $ do
  g <- C.newRandom
  filePath <- createRandomFile
  s <- LB.readFile filePath
  file <- atomically $ CryptoFile (senderFiles </> "encrypted_testfile") . Just <$> CF.randomArgs g
  runRight_ $ CF.writeFile file s
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  (rfd1, rfd2) <- runRight $ do
    (sfId, _, rfd1, rfd2) <- testSendCF sndr file
    xftpDeleteSndFileInternal sndr sfId
    pure (rfd1, rfd2)
  -- receive file, delete rcv file
  testReceiveDelete 2 rfd1 filePath g
  testReceiveDelete 3 rfd2 filePath g
  where
    testReceiveDelete clientId rfd originalFilePath g = do
      rcp <- getSMPAgentClient' clientId agentCfg initAgentServers testDB2
      cfArgs <- atomically $ Just <$> CF.randomArgs g
      runRight_ $ do
        rfId <- testReceiveCF rcp rfd cfArgs originalFilePath
        xftpDeleteRcvFile rcp rfId
      disconnectAgentClient rcp

testXFTPAgentSendReceiveRedirect :: HasCallStack => IO ()
testXFTPAgentSendReceiveRedirect = withXFTPServer $ do
  --- sender
  filePathIn <- createRandomFile
  let fileSize = mb 17
      totalSize = fileSize + mb 1
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  directFileId <- runRight $ xftpSendFile sndr 1 (CryptoFile filePathIn Nothing) 1
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 4194304 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 8388608 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 12582912 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 16777216 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 17825792 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG totalSize totalSize)
  vfdDirect <-
    sfGet sndr >>= \case
      (_, _, SFDONE _snd (vfd : _)) -> pure vfd
      r -> error $ "Expected SFDONE, got " <> show r
  redirectFileId <- runRight $ xftpSendDescription sndr 1 vfdDirect 1
  logInfo $ "File sent, sending redirect: " <> tshow redirectFileId
  sfGet sndr `shouldReturn` ("", redirectFileId, SFPROG 65536 65536)
  vfdRedirect@(ValidFileDescription fdRedirect) <-
    sfGet sndr >>= \case
      (_, _, SFDONE _snd (vfd : _)) -> pure vfd
      r -> error $ "Expected SFDONE, got " <> show r
  case fdRedirect of
    FileDescription {redirect = Just _} -> pure ()
    _ -> error "missing RedirectFileInfo"
  let uri = strEncode $ fileDescriptionURI vfdRedirect
  case strDecode uri of
    Left err -> fail err
    Right ok -> ok `shouldBe` fileDescriptionURI vfdRedirect
  disconnectAgentClient sndr
  --- recipient
  rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  FileDescriptionURI {description} <- either fail pure $ strDecode uri

  rcvFileId <- runRight $ xftpReceiveFile rcp 1 description Nothing
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 65536 totalSize) -- extra RFPROG before switching to real file
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 4194304 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 8388608 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 12582912 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 16777216 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 17825792 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG totalSize totalSize)
  out <-
    rfGet rcp >>= \case
      (_, _, RFDONE out) -> pure out
      r -> error $ "Expected RFDONE, got " <> show r
  disconnectAgentClient rcp

  inBytes <- B.readFile filePathIn
  B.readFile out `shouldReturn` inBytes

testXFTPAgentSendReceiveNoRedirect :: HasCallStack => IO ()
testXFTPAgentSendReceiveNoRedirect = withXFTPServer $ do
  --- sender
  let fileSize = mb 5
  filePathIn <- createRandomFile_ fileSize "testfile"
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  directFileId <- runRight $ xftpSendFile sndr 1 (CryptoFile filePathIn Nothing) 1
  let totalSize = fileSize + mb 1
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 4194304 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG 5242880 totalSize)
  sfGet sndr `shouldReturn` ("", directFileId, SFPROG totalSize totalSize)
  vfdDirect <-
    sfGet sndr >>= \case
      (_, _, SFDONE _snd (vfd : _)) -> pure vfd
      r -> error $ "Expected SFDONE, got " <> show r
  let uri = strEncode $ fileDescriptionURI vfdDirect
  B.length uri `shouldSatisfy` (< qrSizeLimit)
  case strDecode uri of
    Left err -> fail err
    Right ok -> ok `shouldBe` fileDescriptionURI vfdDirect
  disconnectAgentClient sndr
  --- recipient
  rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  FileDescriptionURI {description} <- either fail pure $ strDecode uri
  let ValidFileDescription FileDescription {redirect} = description
  redirect `shouldBe` Nothing
  rcvFileId <- runRight $ xftpReceiveFile rcp 1 description Nothing
  -- NO extra "RFPROG 65k 65k" before switching to real file
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 4194304 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG 5242880 totalSize)
  rfGet rcp `shouldReturn` ("", rcvFileId, RFPROG totalSize totalSize)
  out <-
    rfGet rcp >>= \case
      (_, _, RFDONE out) -> pure out
      r -> error $ "Expected RFDONE, got " <> show r
  disconnectAgentClient rcp

  inBytes <- B.readFile filePathIn
  B.readFile out `shouldReturn` inBytes

createRandomFile :: HasCallStack => IO FilePath
createRandomFile = createRandomFile' "testfile"

createRandomFile' :: HasCallStack => FilePath -> IO FilePath
createRandomFile' = createRandomFile_ (mb 17 :: Integer)

createRandomFile_ :: (HasCallStack, Integral s, Show s) => s -> FilePath -> IO FilePath
createRandomFile_ size fileName = do
  let filePath = senderFiles </> fileName
  xftpCLI ["rand", filePath, show size] `shouldReturn` ["File created: " <> filePath]
  getFileSize filePath `shouldReturn` toInteger size
  pure filePath

testSend :: HasCallStack => AgentClient -> FilePath -> ExceptT AgentErrorType IO (SndFileId, ValidFileDescription 'FSender, ValidFileDescription 'FRecipient, ValidFileDescription 'FRecipient)
testSend sndr = testSendCF sndr . CF.plain

testSendCF :: HasCallStack => AgentClient -> CryptoFile -> ExceptT AgentErrorType IO (SndFileId, ValidFileDescription 'FSender, ValidFileDescription 'FRecipient, ValidFileDescription 'FRecipient)
testSendCF sndr file = do
  xftpStartWorkers sndr (Just senderFiles)
  sfId <- xftpSendFile sndr 1 file 2
  sfProgress sndr $ mb 18
  ("", sfId', SFDONE sndDescr [rfd1, rfd2]) <- sfGet sndr
  liftIO $ sfId' `shouldBe` sfId
  pure (sfId, sndDescr, rfd1, rfd2)

testReceive :: HasCallStack => AgentClient -> ValidFileDescription 'FRecipient -> FilePath -> ExceptT AgentErrorType IO RcvFileId
testReceive rcp rfd = testReceiveCF rcp rfd Nothing

testReceiveCF :: HasCallStack => AgentClient -> ValidFileDescription 'FRecipient -> Maybe CryptoFileArgs -> FilePath -> ExceptT AgentErrorType IO RcvFileId
testReceiveCF rcp rfd cfArgs originalFilePath = do
  xftpStartWorkers rcp (Just recipientFiles)
  testReceiveCF' rcp rfd cfArgs originalFilePath

testReceive' :: HasCallStack => AgentClient -> ValidFileDescription 'FRecipient -> FilePath -> ExceptT AgentErrorType IO RcvFileId
testReceive' rcp rfd = testReceiveCF' rcp rfd Nothing

testReceiveCF' :: HasCallStack => AgentClient -> ValidFileDescription 'FRecipient -> Maybe CryptoFileArgs -> FilePath -> ExceptT AgentErrorType IO RcvFileId
testReceiveCF' rcp rfd cfArgs originalFilePath = do
  rfId <- xftpReceiveFile rcp 1 rfd cfArgs
  rfProgress rcp $ mb 18
  ("", rfId', RFDONE path) <- rfGet rcp
  liftIO $ do
    rfId' `shouldBe` rfId
    sentFile <- LB.readFile originalFilePath
    runExceptT (CF.readFile $ CryptoFile path cfArgs) `shouldReturn` Right sentFile
  pure rfId

logCfgNoLogs :: LogConfig
logCfgNoLogs = LogConfig {lc_file = Nothing, lc_stderr = False}

testXFTPAgentReceiveRestore :: HasCallStack => IO ()
testXFTPAgentReceiveRestore = withGlobalLogging logCfgNoLogs $ do
  filePath <- createRandomFile

  rfd <- withXFTPServerStoreLogOn $ \_ -> do
    -- send file
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    runRight $ do
      (_, _, rfd, _) <- testSend sndr filePath
      pure rfd

  -- receive file - should not succeed with server down
  rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  rfId <- runRight $ do
    xftpStartWorkers rcp (Just recipientFiles)
    rfId <- xftpReceiveFile rcp 1 rfd Nothing
    liftIO $ timeout 300000 (get rcp) `shouldReturn` Nothing -- wait for worker attempt
    pure rfId
  disconnectAgentClient rcp

  [prefixDir] <- listDirectory recipientFiles
  let tmpPath = recipientFiles </> prefixDir </> "xftp.encrypted"
  doesDirectoryExist tmpPath `shouldReturn` True

  withXFTPServerStoreLogOn $ \_ -> do
    -- receive file - should start downloading with server up
    rcp' <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    runRight_ $ xftpStartWorkers rcp' (Just recipientFiles)
    ("", rfId', RFPROG _ _) <- rfGet rcp'
    liftIO $ rfId' `shouldBe` rfId
    disconnectAgentClient rcp'

  threadDelay 100000

  withXFTPServerStoreLogOn $ \_ -> do
    -- receive file - should continue downloading with server up
    rcp' <- getSMPAgentClient' 4 agentCfg initAgentServers testDB2
    runRight_ $ xftpStartWorkers rcp' (Just recipientFiles)
    rfProgress rcp' $ mb 18
    ("", rfId', RFDONE path) <- rfGet rcp'
    liftIO $ do
      rfId' `shouldBe` rfId
      file <- B.readFile filePath
      B.readFile path `shouldReturn` file

  -- tmp path should be removed after receiving file
  doesDirectoryExist tmpPath `shouldReturn` False

testXFTPAgentReceiveCleanup :: HasCallStack => IO ()
testXFTPAgentReceiveCleanup = withGlobalLogging logCfgNoLogs $ do
  filePath <- createRandomFile

  rfd <- withXFTPServerStoreLogOn $ \_ -> do
    -- send file
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    runRight $ do
      (_, _, rfd, _) <- testSend sndr filePath
      pure rfd

  -- receive file - should not succeed with server down
  rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  rfId <- runRight $ do
    xftpStartWorkers rcp (Just recipientFiles)
    rfId <- xftpReceiveFile rcp 1 rfd Nothing
    liftIO $ timeout 300000 (get rcp) `shouldReturn` Nothing -- wait for worker attempt
    pure rfId
  disconnectAgentClient rcp

  [prefixDir] <- listDirectory recipientFiles
  let tmpPath = recipientFiles </> prefixDir </> "xftp.encrypted"
  doesDirectoryExist tmpPath `shouldReturn` True

  withXFTPServerThreadOn $ \_ -> do
    -- receive file - should fail with AUTH error
    rcp' <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    runRight_ $ xftpStartWorkers rcp' (Just recipientFiles)
    ("", rfId', RFERR (INTERNAL "XFTP {xftpErr = AUTH}")) <- rfGet rcp'
    rfId' `shouldBe` rfId

  -- tmp path should be removed after permanent error
  doesDirectoryExist tmpPath `shouldReturn` False

testXFTPAgentSendRestore :: HasCallStack => IO ()
testXFTPAgentSendRestore = withGlobalLogging logCfgNoLogs $ do
  filePath <- createRandomFile

  -- send file - should not succeed with server down
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  sfId <- runRight $ do
    xftpStartWorkers sndr (Just senderFiles)
    sfId <- xftpSendFile sndr 1 (CF.plain filePath) 2
    liftIO $ timeout 1000000 (get sndr) `shouldReturn` Nothing -- wait for worker to encrypt and attempt to create file
    pure sfId
  disconnectAgentClient sndr

  dirEntries <- listDirectory senderFiles
  let prefixDir = fromJust $ find (isSuffixOf "_snd.xftp") dirEntries
      prefixPath = senderFiles </> prefixDir
      encPath = prefixPath </> "xftp.encrypted"
  doesDirectoryExist prefixPath `shouldReturn` True
  doesFileExist encPath `shouldReturn` True

  withXFTPServerStoreLogOn $ \_ -> do
    -- send file - should start uploading with server up
    sndr' <- getSMPAgentClient' 2 agentCfg initAgentServers testDB
    runRight_ $ xftpStartWorkers sndr' (Just senderFiles)
    ("", sfId', SFPROG _ _) <- sfGet sndr'
    liftIO $ sfId' `shouldBe` sfId
    disconnectAgentClient sndr'

  threadDelay 100000

  withXFTPServerStoreLogOn $ \_ -> do
    -- send file - should continue uploading with server up
    sndr' <- getSMPAgentClient' 3 agentCfg initAgentServers testDB
    runRight_ $ xftpStartWorkers sndr' (Just senderFiles)
    sfProgress sndr' $ mb 18
    ("", sfId', SFDONE _sndDescr [rfd1, _rfd2]) <- sfGet sndr'
    liftIO $ sfId' `shouldBe` sfId

    -- prefix path should be removed after sending file
    threadDelay 100000
    doesDirectoryExist prefixPath `shouldReturn` False
    doesFileExist encPath `shouldReturn` False

    -- receive file
    rcp <- getSMPAgentClient' 4 agentCfg initAgentServers testDB2
    runRight_ . void $
      testReceive rcp rfd1 filePath

testXFTPAgentSendCleanup :: HasCallStack => IO ()
testXFTPAgentSendCleanup = withGlobalLogging logCfgNoLogs $ do
  filePath <- createRandomFile

  sfId <- withXFTPServerStoreLogOn $ \_ -> do
    -- send file
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    sfId <- runRight $ do
      xftpStartWorkers sndr (Just senderFiles)
      sfId <- xftpSendFile sndr 1 (CF.plain filePath) 2
      -- wait for progress events for 5 out of 6 chunks - at this point all chunks should be created on the server
      forM_ [1 .. 5 :: Integer] $ \_ -> do
        (_, _, SFPROG _ _) <- sfGet sndr
        pure ()
      pure sfId
    disconnectAgentClient sndr
    pure sfId

  dirEntries <- listDirectory senderFiles
  let prefixDir = fromJust $ find (isSuffixOf "_snd.xftp") dirEntries
      prefixPath = senderFiles </> prefixDir
      encPath = prefixPath </> "xftp.encrypted"
  doesDirectoryExist prefixPath `shouldReturn` True
  doesFileExist encPath `shouldReturn` True

  withXFTPServerThreadOn $ \_ -> do
    -- send file - should fail with AUTH error
    sndr' <- getSMPAgentClient' 2 agentCfg initAgentServers testDB
    runRight_ $ xftpStartWorkers sndr' (Just senderFiles)
    ("", sfId', SFERR (INTERNAL "XFTP {xftpErr = AUTH}")) <- sfGet sndr'
    sfId' `shouldBe` sfId

    -- prefix path should be removed after permanent error
    doesDirectoryExist prefixPath `shouldReturn` False
    doesFileExist encPath `shouldReturn` False

testXFTPAgentDelete :: HasCallStack => IO ()
testXFTPAgentDelete = withGlobalLogging logCfgNoLogs $
  withXFTPServer $ do
    filePath <- createRandomFile

    -- send file
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    (sfId, sndDescr, rfd1, rfd2) <- runRight $ testSend sndr filePath

    -- receive file
    rcp1 <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
    runRight_ . void $
      testReceive rcp1 rfd1 filePath

    length <$> listDirectory xftpServerFiles `shouldReturn` 6

    -- delete file
    runRight $ do
      xftpStartWorkers sndr (Just senderFiles)
      xftpDeleteSndFileRemote sndr 1 sfId sndDescr
      Nothing <- liftIO $ 100000 `timeout` sfGet sndr
      pure ()
    disconnectAgentClient rcp1

    threadDelay 1000000
    length <$> listDirectory xftpServerFiles `shouldReturn` 0

    -- receive file - should fail with AUTH error
    rcp2 <- getSMPAgentClient' 3 agentCfg initAgentServers testDB2
    runRight $ do
      xftpStartWorkers rcp2 (Just recipientFiles)
      rfId <- xftpReceiveFile rcp2 1 rfd2 Nothing
      ("", rfId', RFERR (INTERNAL "XFTP {xftpErr = AUTH}")) <- rfGet rcp2
      liftIO $ rfId' `shouldBe` rfId

testXFTPAgentDeleteRestore :: HasCallStack => IO ()
testXFTPAgentDeleteRestore = withGlobalLogging logCfgNoLogs $ do
  filePath <- createRandomFile

  (sfId, sndDescr, rfd2) <- withXFTPServerStoreLogOn $ \_ -> do
    -- send file
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    (sfId, sndDescr, rfd1, rfd2) <- runRight $ testSend sndr filePath

    -- receive file
    rcp1 <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
    runRight_ . void $
      testReceive rcp1 rfd1 filePath
    disconnectAgentClient rcp1
    disconnectAgentClient sndr
    pure (sfId, sndDescr, rfd2)

  -- delete file - should not succeed with server down
  sndr <- getSMPAgentClient' 3 agentCfg initAgentServers testDB
  runRight $ do
    xftpStartWorkers sndr (Just senderFiles)
    xftpDeleteSndFileRemote sndr 1 sfId sndDescr
    liftIO $ timeout 300000 (get sndr) `shouldReturn` Nothing -- wait for worker attempt
  disconnectAgentClient sndr

  threadDelay 300000
  length <$> listDirectory xftpServerFiles `shouldReturn` 6

  withXFTPServerStoreLogOn $ \_ -> do
    -- delete file - should succeed with server up
    sndr' <- getSMPAgentClient' 4 agentCfg initAgentServers testDB
    runRight_ $ xftpStartWorkers sndr' (Just senderFiles)

    threadDelay 1000000
    length <$> listDirectory xftpServerFiles `shouldReturn` 0

    -- receive file - should fail with AUTH error
    rcp2 <- getSMPAgentClient' 5 agentCfg initAgentServers testDB3
    runRight $ do
      xftpStartWorkers rcp2 (Just recipientFiles)
      rfId <- xftpReceiveFile rcp2 1 rfd2 Nothing
      ("", rfId', RFERR (INTERNAL "XFTP {xftpErr = AUTH}")) <- rfGet rcp2
      liftIO $ rfId' `shouldBe` rfId

testXFTPAgentDeleteOnServer :: HasCallStack => IO ()
testXFTPAgentDeleteOnServer = withGlobalLogging logCfgNoLogs $
  withXFTPServer $ do
    filePath1 <- createRandomFile' "testfile1"

    -- send file 1
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    (_, _, rfd1_1, rfd1_2) <- runRight $ testSend sndr filePath1

    -- receive file 1 successfully
    rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
    runRight_ . void $
      testReceive rcp rfd1_1 filePath1

    serverFiles <- listDirectory xftpServerFiles
    length serverFiles `shouldBe` 6

    -- delete file 1 on server from file system
    forM_ serverFiles (\file -> removeFile (xftpServerFiles </> file))

    threadDelay 1000000
    length <$> listDirectory xftpServerFiles `shouldReturn` 0

    -- create and send file 2
    filePath2 <- createRandomFile' "testfile2"
    (_, _, rfd2, _) <- runRight $ testSend sndr filePath2

    length <$> listDirectory xftpServerFiles `shouldReturn` 6

    runRight_ . void $ do
      -- receive file 1 again
      -- TODO should fail with AUTH error
      _rfId1 <- xftpReceiveFile rcp 1 rfd1_2 Nothing

      -- receive file 2
      testReceive' rcp rfd2 filePath2

testXFTPAgentExpiredOnServer :: HasCallStack => IO ()
testXFTPAgentExpiredOnServer = withGlobalLogging logCfgNoLogs $ do
  let fastExpiration = ExpirationConfig {ttl = 2, checkInterval = 1}
  withXFTPServerCfg testXFTPServerConfig {fileExpiration = Just fastExpiration} . const $ do
    filePath1 <- createRandomFile' "testfile1"

    -- send file 1
    sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
    (_, _, rfd1_1, rfd1_2) <- runRight $ testSend sndr filePath1

    -- receive file 1 successfully
    rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
    runRight_ . void $
      testReceive rcp rfd1_1 filePath1

    serverFiles <- listDirectory xftpServerFiles
    length serverFiles `shouldBe` 6

    -- wait until file 1 expires on server
    forM_ serverFiles (\file -> removeFile (xftpServerFiles </> file))

    threadDelay 3500000
    length <$> listDirectory xftpServerFiles `shouldReturn` 0

    -- receive file 1 again - should fail with AUTH error
    runRight $ do
      rfId <- xftpReceiveFile rcp 1 rfd1_2 Nothing
      ("", rfId', RFERR (INTERNAL "XFTP {xftpErr = AUTH}")) <- rfGet rcp
      liftIO $ rfId' `shouldBe` rfId

    -- create and send file 2
    filePath2 <- createRandomFile' "testfile2"
    (_, _, rfd2, _) <- runRight $ testSend sndr filePath2

    length <$> listDirectory xftpServerFiles `shouldReturn` 6

    -- receive file 2 successfully
    runRight_ . void $
      testReceive' rcp rfd2 filePath2

testXFTPAgentRequestAdditionalRecipientIDs :: HasCallStack => IO ()
testXFTPAgentRequestAdditionalRecipientIDs = withXFTPServer $ do
  filePath <- createRandomFile

  -- send file
  sndr <- getSMPAgentClient' 1 agentCfg initAgentServers testDB
  rfds <- runRight $ do
    xftpStartWorkers sndr (Just senderFiles)
    sfId <- xftpSendFile sndr 1 (CF.plain filePath) 500
    sfProgress sndr $ mb 18
    ("", sfId', SFDONE _sndDescr rfds) <- sfGet sndr
    liftIO $ do
      sfId' `shouldBe` sfId
      length rfds `shouldBe` 500
    pure rfds

  -- receive file using different descriptions
  -- ! revise number of recipients and indexes if xftpMaxRecipientsPerRequest is changed
  rcp <- getSMPAgentClient' 2 agentCfg initAgentServers testDB2
  runRight_ $ do
    void $ testReceive rcp (head rfds) filePath
    void $ testReceive rcp (rfds !! 99) filePath
    void $ testReceive rcp (rfds !! 299) filePath
    void $ testReceive rcp (rfds !! 499) filePath

testXFTPServerTest :: HasCallStack => Maybe BasicAuth -> XFTPServerWithAuth -> IO (Maybe ProtocolTestFailure)
testXFTPServerTest newFileBasicAuth srv =
  withXFTPServerCfg testXFTPServerConfig {newFileBasicAuth, xftpPort = xftpTestPort2} $ \_ -> do
    a <- getSMPAgentClient' 1 agentCfg initAgentServers testDB -- initially passed server is not running
    runRight $ testProtocolServer a 1 srv
