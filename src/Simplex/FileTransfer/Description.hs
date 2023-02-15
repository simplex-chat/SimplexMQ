{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.FileTransfer.Description
  ( FileDescription (..),
    ValidFileDescription,
    pattern ValidFileDescription,
    FileDigest (..),
    FileChunk (..),
    FileChunkReplica (..),
    FileSize (..),
    ChunkReplicaId (..),
    YAMLFileDescription (..), -- for tests
    YAMLServerReplicas (..), -- for tests
    validateFileDescription,
  )
where

import Control.Applicative (optional)
import Control.Monad ((<=<))
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as J
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Bifunctor (first)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Function (on)
import Data.Int (Int64)
import Data.List (foldl', groupBy, sortOn)
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe (fromMaybe)
import Data.Word (Word32)
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (parseAll)
import Simplex.Messaging.Protocol (XFTPServer)
import Simplex.Messaging.Util (bshow, (<$?>))

data FileDescription = FileDescription
  { size :: FileSize Int64,
    digest :: FileDigest,
    key :: C.SbKey,
    nonce :: C.CbNonce,
    chunkSize :: FileSize Word32,
    chunks :: [FileChunk]
  }
  deriving (Eq, Show)

newtype ValidFileDescription = ValidFD FileDescription

pattern ValidFileDescription :: FileDescription -> ValidFileDescription
pattern ValidFileDescription fd = ValidFD fd

newtype FileDigest = FileDigest {unFileDigest :: ByteString}
  deriving (Eq, Show)

instance StrEncoding FileDigest where
  strEncode (FileDigest fd) = strEncode fd
  strDecode s = FileDigest <$> strDecode s
  strP = FileDigest <$> strP

instance FromJSON FileDigest where
  parseJSON = strParseJSON "FileDigest"

instance ToJSON FileDigest where
  toJSON = strToJSON
  toEncoding = strToJEncoding

data FileChunk = FileChunk
  { chunkNo :: Int,
    chunkSize :: FileSize Word32,
    digest :: FileDigest,
    replicas :: [FileChunkReplica]
  }
  deriving (Eq, Show)

data FileChunkReplica = FileChunkReplica
  { server :: XFTPServer,
    rcvId :: ChunkReplicaId,
    rcvKey :: C.APrivateSignKey
  }
  deriving (Eq, Show)

newtype ChunkReplicaId = ChunkReplicaId {unChunkReplicaId :: ByteString}
  deriving (Eq, Show)

instance StrEncoding ChunkReplicaId where
  strEncode (ChunkReplicaId fid) = strEncode fid
  strP = ChunkReplicaId <$> strP

instance FromJSON ChunkReplicaId where
  parseJSON = strParseJSON "ChunkReplicaId"

instance ToJSON ChunkReplicaId where
  toJSON = strToJSON
  toEncoding = strToJEncoding

data YAMLFileDescription = YAMLFileDescription
  { size :: String,
    digest :: FileDigest,
    key :: C.SbKey,
    nonce :: C.CbNonce,
    chunkSize :: String,
    replicas :: [YAMLServerReplicas]
  }
  deriving (Eq, Show, Generic, FromJSON)

instance ToJSON YAMLFileDescription where
  toJSON = J.genericToJSON J.defaultOptions
  toEncoding = J.genericToEncoding J.defaultOptions

data YAMLServerReplicas = YAMLServerReplicas
  { server :: XFTPServer,
    chunks :: [String]
  }
  deriving (Eq, Show, Generic, FromJSON)

instance ToJSON YAMLServerReplicas where
  toJSON = J.genericToJSON J.defaultOptions
  toEncoding = J.genericToEncoding J.defaultOptions

data FileServerReplica = FileServerReplica
  { chunkNo :: Int,
    server :: XFTPServer,
    rcvId :: ChunkReplicaId,
    rcvKey :: C.APrivateSignKey,
    digest :: Maybe FileDigest,
    chunkSize :: Maybe (FileSize Word32)
  }
  deriving (Show)

instance StrEncoding FileDescription where
  strEncode = Y.encode . encodeFileDescription
  strDecode = decodeFileDescription <=< first show . Y.decodeEither'
  strP = strDecode <$?> A.takeByteString

validateFileDescription :: FileDescription -> Either String ValidFileDescription
validateFileDescription fd@FileDescription {size, chunks}
  | chunkNos /= [1 .. length chunks] = Left "chunk numbers are not sequential"
  | chunksSize chunks /= unFileSize size = Left "chunks total size is different than file size"
  | otherwise = Right $ ValidFD fd
  where
    chunkNos = map (chunkNo :: FileChunk -> Int) chunks
    chunksSize = fromIntegral . foldl' (\s FileChunk {chunkSize} -> s + unFileSize chunkSize) 0

encodeFileDescription :: FileDescription -> YAMLFileDescription
encodeFileDescription FileDescription {size, digest, key, nonce, chunkSize, chunks} =
  YAMLFileDescription
    { size = B.unpack $ strEncode size,
      digest,
      key,
      nonce,
      chunkSize = B.unpack $ strEncode chunkSize,
      replicas = encodeFileReplicas chunkSize chunks
    }

newtype FileSize a = FileSize {unFileSize :: a}
  deriving (Eq, Show)

instance (Integral a, Show a) => StrEncoding (FileSize a) where
  strEncode (FileSize b)
    | b' /= 0 = bshow b
    | kb' /= 0 = bshow kb <> "kb"
    | otherwise = bshow mb <> "mb"
    where
      (kb, b') = b `divMod` 1024
      (mb, kb') = kb `divMod` 1024
  strP =
    FileSize
      <$> A.choice
        [ (mb *) <$> A.decimal <* "mb",
          (kb *) <$> A.decimal <* "kb",
          A.decimal
        ]
    where
      kb = 1024
      mb = 1024 * kb

encodeFileReplicas :: FileSize Word32 -> [FileChunk] -> [YAMLServerReplicas]
encodeFileReplicas defChunkSize =
  map encodeServerReplicas
    . groupBy ((==) `on` server')
    . sortOn server'
    . unfoldChunksToReplicas defChunkSize
  where
    server' = server :: FileServerReplica -> XFTPServer
    encodeServerReplicas fs =
      YAMLServerReplicas
        { server = server' $ head fs, -- groupBy guarantees that fs is not empty
          chunks = map (B.unpack . encodeServerReplica) fs
        }

encodeServerReplica :: FileServerReplica -> ByteString
encodeServerReplica FileServerReplica {chunkNo, rcvId, rcvKey, digest, chunkSize} =
  bshow chunkNo
    <> ":"
    <> strEncode rcvId
    <> ":"
    <> strEncode rcvKey
    <> maybe "" ((":" <>) . strEncode) digest
    <> maybe "" ((":" <>) . strEncode) chunkSize

serverReplicaP :: XFTPServer -> Parser FileServerReplica
serverReplicaP server = do
  chunkNo <- A.decimal
  rcvId <- A.char ':' *> strP
  rcvKey <- A.char ':' *> strP
  digest <- optional (A.char ':' *> strP)
  chunkSize <- optional (A.char ':' *> strP)
  pure FileServerReplica {chunkNo, server, rcvId, rcvKey, digest, chunkSize}

unfoldChunksToReplicas :: FileSize Word32 -> [FileChunk] -> [FileServerReplica]
unfoldChunksToReplicas defChunkSize = concatMap chunkReplicas
  where
    chunkReplicas c@FileChunk {replicas} = zipWith (replicaToServerReplica c) [1 ..] replicas
    replicaToServerReplica :: FileChunk -> Int -> FileChunkReplica -> FileServerReplica
    replicaToServerReplica FileChunk {chunkNo, digest, chunkSize} replicaNo FileChunkReplica {server, rcvId, rcvKey} =
      let chunkSize' = if chunkSize /= defChunkSize && replicaNo == 1 then Just chunkSize else Nothing
          digest' = if replicaNo == 1 then Just digest else Nothing
       in FileServerReplica {chunkNo, server, rcvId, rcvKey, digest = digest', chunkSize = chunkSize'}

decodeFileDescription :: YAMLFileDescription -> Either String FileDescription
decodeFileDescription YAMLFileDescription {size, digest, key, nonce, chunkSize, replicas} = do
  size' <- strDecode $ B.pack size
  chunkSize' <- strDecode $ B.pack chunkSize
  replicas' <- decodeFileParts replicas
  chunks <- foldReplicasToChunks chunkSize' replicas'
  pure FileDescription {size = size', digest, key, nonce, chunkSize = chunkSize', chunks}
  where
    decodeFileParts = fmap concat . mapM decodeYAMLServerReplicas

decodeYAMLServerReplicas :: YAMLServerReplicas -> Either String [FileServerReplica]
decodeYAMLServerReplicas YAMLServerReplicas {server, chunks} =
  mapM (parseAll (serverReplicaP server) . B.pack) chunks

-- this function should fail if:
-- 1. no replica has digest or two replicas have different digests
-- 2. two replicas have different chunk sizes
foldReplicasToChunks :: FileSize Word32 -> [FileServerReplica] -> Either String [FileChunk]
foldReplicasToChunks defChunkSize fs = do
  sd <- foldSizesDigests fs
  -- TODO validate (check that chunks match) or in separate function
  sortOn (chunkNo :: FileChunk -> Int) . map reverseReplicas . M.elems <$> foldChunks sd fs
  where
    foldSizesDigests :: [FileServerReplica] -> Either String (Map Int (FileSize Word32), Map Int FileDigest)
    foldSizesDigests = foldl' addSizeDigest $ Right (M.empty, M.empty)
    addSizeDigest :: Either String (Map Int (FileSize Word32), Map Int FileDigest) -> FileServerReplica -> Either String (Map Int (FileSize Word32), Map Int FileDigest)
    addSizeDigest (Left e) _ = Left e
    addSizeDigest (Right (ms, md)) FileServerReplica {chunkNo, chunkSize, digest} =
      (,) <$> combineChunk ms chunkNo chunkSize <*> combineChunk md chunkNo digest
    combineChunk :: Eq a => Map Int a -> Int -> Maybe a -> Either String (Map Int a)
    combineChunk m _ Nothing = Right m
    combineChunk m chunkNo (Just value) = case M.lookup chunkNo m of
      Nothing -> Right $ M.insert chunkNo value m
      Just v -> if v == value then Right m else Left "different size or digest in chunk replicas"
    foldChunks :: (Map Int (FileSize Word32), Map Int FileDigest) -> [FileServerReplica] -> Either String (Map Int FileChunk)
    foldChunks sd = foldl' (addReplica sd) (Right M.empty)
    addReplica :: (Map Int (FileSize Word32), Map Int FileDigest) -> Either String (Map Int FileChunk) -> FileServerReplica -> Either String (Map Int FileChunk)
    addReplica _ (Left e) _ = Left e
    addReplica (ms, md) (Right cs) FileServerReplica {chunkNo, server, rcvId, rcvKey} = do
      case M.lookup chunkNo cs of
        Just chunk@FileChunk {replicas} ->
          let replica = FileChunkReplica {server, rcvId, rcvKey}
           in Right $ M.insert chunkNo ((chunk :: FileChunk) {replicas = replica : replicas}) cs
        _ -> do
          case M.lookup chunkNo md of
            Just digest' ->
              let replica = FileChunkReplica {server, rcvId, rcvKey}
                  chunkSize' = fromMaybe defChunkSize $ M.lookup chunkNo ms
                  chunk = FileChunk {chunkNo, digest = digest', chunkSize = chunkSize', replicas = [replica]}
               in Right $ M.insert chunkNo chunk cs
            _ -> Left "no digest for chunk"
    reverseReplicas c@FileChunk {replicas} = (c :: FileChunk) {replicas = reverse replicas}
