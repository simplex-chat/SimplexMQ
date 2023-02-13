{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}

module XFTPServerTests where

import AgentTests.FunctionalAPITests (runRight_)
import Control.Monad.IO.Class (liftIO)
import Crypto.Random (getRandomBytes)
import qualified Data.ByteString.Base64.URL as B64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Simplex.FileTransfer.Client
import Simplex.FileTransfer.Protocol (FileInfo (..))
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol (SenderId)
import System.Directory (createDirectoryIfMissing, removeDirectoryRecursive)
import System.IO (IOMode (..), withFile)
import Test.Hspec
import XFTPClient

xftpServerTests :: Spec
xftpServerTests =
  before_ (createDirectoryIfMissing False "tests/xftp-files")
    . after_ (removeDirectoryRecursive "tests/xftp-files")
    $ do
      fdescribe "XFTP file chunk delivery" testFileChunkDelivery

chSize :: Num n => n
chSize = 256 * 1024

createTestChunk :: FilePath -> IO ByteString
createTestChunk fp = do
  bytes <- getRandomBytes chSize
  withFile fp WriteMode $ \h -> B.hPut h bytes
  pure bytes

readChunk :: SenderId -> IO ByteString
readChunk sId = B.readFile ("tests/xftp-files/" <> B.unpack (B64.encode sId))

testFileChunkDelivery :: Spec
testFileChunkDelivery =
  it "should create, upload and receive file chunk" $ do
    (sndKey, spKey) <- C.generateSignatureKeyPair C.SEd25519
    (rcvKey, _rpKey) <- C.generateSignatureKeyPair C.SEd25519
    bytes <- createTestChunk "tests/tmp/chunk1"
    xftpTest $ \c -> runRight_ $ do
      let file = FileInfo {sndKey, size = chSize, digest = "abc="}
      (sId, _rIds) <- createXFTPChunk c spKey file [rcvKey]
      uploadXFTPChunk c spKey sId "tests/tmp/chunk1"
      liftIO $ readChunk sId `shouldReturn` bytes
      pure ()
