{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Simplex.FileTransfer.Types where

import qualified Data.Aeson.TH as J
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import Data.Int (Int64)
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word32)
import Simplex.FileTransfer.Client (XFTPChunkSpec (..))
import Simplex.FileTransfer.Description
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.File (CryptoFile (..))
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers
import Simplex.Messaging.Protocol (XFTPServer)
import System.FilePath ((</>))
import Simplex.Messaging.Agent.Store.DB (FromField (..), ToField (..), fromTextField_)

type RcvFileId = ByteString -- Agent entity ID

type SndFileId = ByteString -- Agent entity ID

authTagSize :: Int64
authTagSize = fromIntegral C.authTagSize

-- fileExtra is added to allow header extension in future versions
data FileHeader = FileHeader
  { fileName :: String,
    fileExtra :: Maybe String
  }
  deriving (Eq, Show)

instance Encoding FileHeader where
  smpEncode FileHeader {fileName, fileExtra} = smpEncode (fileName, fileExtra)
  smpP = do
    (fileName, fileExtra) <- smpP
    pure FileHeader {fileName, fileExtra}

type DBRcvFileId = Int64

data RcvFile = RcvFile
  { rcvFileId :: DBRcvFileId,
    rcvFileEntityId :: RcvFileId,
    userId :: Int64,
    size :: FileSize Int64,
    digest :: FileDigest,
    key :: C.SbKey,
    nonce :: C.CbNonce,
    chunkSize :: FileSize Word32,
    redirect :: Maybe RcvFileRedirect,
    chunks :: [RcvFileChunk],
    prefixPath :: FilePath,
    tmpPath :: Maybe FilePath,
    saveFile :: CryptoFile,
    status :: RcvFileStatus,
    deleted :: Bool
  }
  deriving (Show)

data RcvFileStatus
  = RFSReceiving
  | RFSReceived
  | RFSDecrypting
  | RFSComplete
  | RFSError
  deriving (Eq, Show)

instance FromField RcvFileStatus where fromField = fromTextField_ textDecode

instance ToField RcvFileStatus where toField = toField . textEncode

instance TextEncoding RcvFileStatus where
  textDecode = \case
    "receiving" -> Just RFSReceiving
    "received" -> Just RFSReceived
    "decrypting" -> Just RFSDecrypting
    "complete" -> Just RFSComplete
    "error" -> Just RFSError
    _ -> Nothing
  textEncode = \case
    RFSReceiving -> "receiving"
    RFSReceived -> "received"
    RFSDecrypting -> "decrypting"
    RFSComplete -> "complete"
    RFSError -> "error"

data RcvFileChunk = RcvFileChunk
  { rcvFileId :: DBRcvFileId,
    rcvFileEntityId :: RcvFileId,
    userId :: Int64,
    rcvChunkId :: Int64,
    chunkNo :: Int,
    chunkSize :: FileSize Word32,
    digest :: FileDigest,
    replicas :: [RcvFileChunkReplica],
    fileTmpPath :: FilePath,
    chunkTmpPath :: Maybe FilePath
  }
  deriving (Show)

data RcvFileChunkReplica = RcvFileChunkReplica
  { rcvChunkReplicaId :: Int64,
    server :: XFTPServer,
    replicaId :: ChunkReplicaId,
    replicaKey :: C.APrivateAuthKey,
    received :: Bool,
    delay :: Maybe Int64,
    retries :: Int
  }
  deriving (Show)

data RcvFileRedirect = RcvFileRedirect
  { redirectDbId :: DBRcvFileId,
    redirectEntityId :: RcvFileId,
    redirectFileInfo :: RedirectFileInfo
  }
  deriving (Show)

-- Sending files

type DBSndFileId = Int64

data SndFile = SndFile
  { sndFileId :: DBSndFileId,
    sndFileEntityId :: SndFileId,
    userId :: Int64,
    numRecipients :: Int,
    digest :: Maybe FileDigest,
    key :: C.SbKey,
    nonce :: C.CbNonce,
    chunks :: [SndFileChunk],
    srcFile :: CryptoFile,
    prefixPath :: Maybe FilePath,
    status :: SndFileStatus,
    deleted :: Bool,
    redirect :: Maybe RedirectFileInfo
  }
  deriving (Show)

sndFileEncPath :: FilePath -> FilePath
sndFileEncPath prefixPath = prefixPath </> "xftp.encrypted"

data SndFileStatus
  = SFSNew -- db record created
  | SFSEncrypting -- encryption started
  | SFSEncrypted -- encryption complete
  | SFSUploading -- all chunk replicas are created on servers
  | SFSComplete -- all chunk replicas are uploaded
  | SFSError -- permanent error
  deriving (Eq, Show)

instance FromField SndFileStatus where fromField = fromTextField_ textDecode

instance ToField SndFileStatus where toField = toField . textEncode

instance TextEncoding SndFileStatus where
  textDecode = \case
    "new" -> Just SFSNew
    "encrypting" -> Just SFSEncrypting
    "encrypted" -> Just SFSEncrypted
    "uploading" -> Just SFSUploading
    "complete" -> Just SFSComplete
    "error" -> Just SFSError
    _ -> Nothing
  textEncode = \case
    SFSNew -> "new"
    SFSEncrypting -> "encrypting"
    SFSEncrypted -> "encrypted"
    SFSUploading -> "uploading"
    SFSComplete -> "complete"
    SFSError -> "error"

data SndFileChunk = SndFileChunk
  { sndFileId :: DBSndFileId,
    sndFileEntityId :: SndFileId,
    userId :: Int64,
    numRecipients :: Int,
    sndChunkId :: Int64,
    chunkNo :: Int,
    chunkSpec :: XFTPChunkSpec,
    filePrefixPath :: FilePath,
    digest :: FileDigest,
    replicas :: [SndFileChunkReplica]
  }
  deriving (Show)

sndChunkSize :: SndFileChunk -> Word32
sndChunkSize SndFileChunk {chunkSpec = XFTPChunkSpec {chunkSize}} = chunkSize

data NewSndChunkReplica = NewSndChunkReplica
  { server :: XFTPServer,
    replicaId :: ChunkReplicaId,
    replicaKey :: C.APrivateAuthKey,
    rcvIdsKeys :: [(ChunkReplicaId, C.APrivateAuthKey)]
  }
  deriving (Show)

data SndFileChunkReplica = SndFileChunkReplica
  { sndChunkReplicaId :: Int64,
    server :: XFTPServer,
    replicaId :: ChunkReplicaId,
    replicaKey :: C.APrivateAuthKey,
    rcvIdsKeys :: [(ChunkReplicaId, C.APrivateAuthKey)],
    replicaStatus :: SndFileReplicaStatus,
    delay :: Maybe Int64,
    retries :: Int
  }
  deriving (Show)

data SndFileReplicaStatus
  = SFRSCreated
  | SFRSUploaded
  deriving (Eq, Show)

instance FromField SndFileReplicaStatus where fromField = fromTextField_ textDecode

instance ToField SndFileReplicaStatus where toField = toField . textEncode

instance TextEncoding SndFileReplicaStatus where
  textDecode = \case
    "created" -> Just SFRSCreated
    "uploaded" -> Just SFRSUploaded
    _ -> Nothing
  textEncode = \case
    SFRSCreated -> "created"
    SFRSUploaded -> "uploaded"

data DeletedSndChunkReplica = DeletedSndChunkReplica
  { deletedSndChunkReplicaId :: Int64,
    userId :: Int64,
    server :: XFTPServer,
    replicaId :: ChunkReplicaId,
    replicaKey :: C.APrivateAuthKey,
    chunkDigest :: FileDigest,
    delay :: Maybe Int64,
    retries :: Int
  }
  deriving (Show)

data SentRecipientReplica = SentRecipientReplica
  { chunkNo :: Int,
    server :: XFTPServer,
    rcvNo :: Int,
    replicaId :: ChunkReplicaId,
    replicaKey :: C.APrivateAuthKey,
    digest :: FileDigest,
    chunkSize :: FileSize Word32
  }

data FileErrorType
  = -- | cannot proceed with download from not approved relays without proxy
    NOT_APPROVED
  | -- | max file size exceeded
    SIZE
  | -- | bad redirect data
    REDIRECT {redirectError :: String}
  | -- | file crypto error
    FILE_IO {fileIOError :: String}
  | -- | file not found or was deleted
    NO_FILE
  deriving (Eq, Show)

instance StrEncoding FileErrorType where
  strP =
    A.takeTill (== ' ')
      >>= \case
        "NOT_APPROVED" -> pure NOT_APPROVED
        "SIZE" -> pure SIZE
        "REDIRECT" -> REDIRECT <$> (A.space *> textP)
        "FILE_IO" -> FILE_IO <$> (A.space *> textP)
        "NO_FILE" -> pure NO_FILE
        _ -> fail "bad FileErrorType"
  strEncode = \case
    NOT_APPROVED -> "NOT_APPROVED"
    SIZE -> "SIZE"
    REDIRECT e -> "REDIRECT " <> encodeUtf8 (T.pack e)
    FILE_IO e -> "FILE_IO " <> encodeUtf8 (T.pack e)
    NO_FILE -> "NO_FILE"

$(J.deriveJSON (sumTypeJSON id) ''FileErrorType)
