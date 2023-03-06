{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module XFTPAgent where

import AgentTests.FunctionalAPITests (get, runRight_)
import Control.Concurrent (threadDelay)
import Control.Monad.Except
import Data.Bifunctor (first)
import qualified Data.ByteString as LB
import SMPAgentClient (agentCfg, initAgentServers)
import Simplex.FileTransfer.Description
import Simplex.FileTransfer.Protocol (FileParty (..), checkParty)
import Simplex.Messaging.Agent (disconnectAgentClient, getSMPAgentClient, xftpReceiveFile)
import Simplex.Messaging.Agent.Protocol (ACommand (FRCVD), AgentErrorType (..))
import Simplex.Messaging.Encoding.String (StrEncoding (..))
import System.Directory (getFileSize)
import System.FilePath ((</>))
import Test.Hspec
import XFTPCLI
import XFTPClient

xftpAgentTests :: Spec
xftpAgentTests = around_ testBracket . describe "Functional API" $ do
  it "should receive file" testXFTPAgentReceive
  it "should resume receiving file after restart" testXFTPAgentReceiveRestore

testXFTPAgentReceive :: IO ()
testXFTPAgentReceive = withXFTPServer $ do
  -- send file using CLI
  let filePath = senderFiles </> "testfile"
  xftpCLI ["rand", filePath, "17mb"] `shouldReturn` ["File created: " <> filePath]
  file <- LB.readFile filePath
  getFileSize filePath `shouldReturn` mb 17
  let fdRcv = filePath <> ".xftp" </> "rcv1.xftp"
      fdSnd = filePath <> ".xftp" </> "snd.xftp.private"
  progress : sendResult <- xftpCLI ["send", filePath, senderFiles, "-s", testXFTPServerStr, "--tmp=tests/tmp"]
  progress `shouldSatisfy` uploadProgress
  sendResult
    `shouldBe` [ "Sender file description: " <> fdSnd,
                 "Pass file descriptions to the recipient(s):",
                 fdRcv
               ]
  -- receive file using agent
  rcp <- getSMPAgentClient agentCfg initAgentServers
  runRight_ $ do
    fd :: ValidFileDescription 'FPRecipient <- getFileDescription fdRcv
    fId <- xftpReceiveFile rcp 1 fd recipientFiles
    ("", "", FRCVD fId' path) <- get rcp
    liftIO $ do
      fId' `shouldBe` fId
      LB.readFile path `shouldReturn` file

getFileDescription :: FilePath -> ExceptT AgentErrorType IO (ValidFileDescription 'FPRecipient)
getFileDescription path = do
  fd :: AFileDescription <- ExceptT $ first (INTERNAL . ("Failed to parse file description: " <>)) . strDecode <$> LB.readFile path
  vfd <- liftEither . first INTERNAL $ validateFileDescription fd
  case vfd of
    AVFD fd' -> either (throwError . INTERNAL) pure $ checkParty fd'

testXFTPAgentReceiveRestore :: IO ()
testXFTPAgentReceiveRestore = do
  let filePath = senderFiles </> "testfile"
      fdRcv = filePath <> ".xftp" </> "rcv1.xftp"
      fdSnd = filePath <> ".xftp" </> "snd.xftp.private"

  withXFTPServerStoreLogOn $ \_ -> do
    -- send file using CLI
    xftpCLI ["rand", filePath, "17mb"] `shouldReturn` ["File created: " <> filePath]
    getFileSize filePath `shouldReturn` mb 17
    progress : sendResult <- xftpCLI ["send", filePath, senderFiles, "-s", testXFTPServerStr, "--tmp=tests/tmp"]
    progress `shouldSatisfy` uploadProgress
    sendResult
      `shouldBe` [ "Sender file description: " <> fdSnd,
                   "Pass file descriptions to the recipient(s):",
                   fdRcv
                 ]

  -- receive file using agent - should not succeed due to server being down
  rcp <- getSMPAgentClient agentCfg initAgentServers
  runRight_ $ do
    fd :: ValidFileDescription 'FPRecipient <- getFileDescription fdRcv
    void $ xftpReceiveFile rcp 1 fd recipientFiles
    liftIO $ threadDelay 1000000 -- wait for worker attempt
  disconnectAgentClient rcp

  rcp' <- getSMPAgentClient agentCfg initAgentServers
  withXFTPServerStoreLogOn $ \_ -> do
    -- receive file using agent - should succeed with server up
    ("", "", FRCVD fId' path) <- get rcp'
    liftIO $ do
      fId' `shouldBe` 1
      file <- LB.readFile filePath
      LB.readFile path `shouldReturn` file
