{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Simplex.Messaging.Protocol where

import Control.Applicative ((<|>))
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except
import qualified Crypto.Cipher.Types as AES
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteArray (xor)
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import Data.Kind
import Data.String
import Data.Time.Clock
import Data.Time.ISO8601
import Data.Word (Word32)
import GHC.IO.Handle.Internals (ioe_EOF)
import Network.Transport.Internal (encodeWord32)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Parsers
import Simplex.Messaging.Util
import System.IO

data Party = Broker | Recipient | Sender
  deriving (Show)

data SParty :: Party -> Type where
  SBroker :: SParty Broker
  SRecipient :: SParty Recipient
  SSender :: SParty Sender

data THandle = THandle
  { handle :: Handle,
    sendKey :: TransportKey,
    receiveKey :: TransportKey,
    blockSize :: Int
  }

data TransportKey = TransportKey
  { aesKey :: C.Key,
    baseIV :: C.IV,
    counter :: TVar Word32
  }

deriving instance Show (SParty a)

data Cmd = forall a. Cmd (SParty a) (Command a)

deriving instance Show Cmd

type Transmission = (CorrId, QueueId, Cmd)

type SignedTransmission = (C.Signature, Transmission)

type TransmissionOrError = (CorrId, QueueId, Either ErrorType Cmd)

type SignedTransmissionOrError = (C.Signature, TransmissionOrError)

type RawTransmission = (ByteString, ByteString, ByteString, ByteString)

type SignedRawTransmission = (C.Signature, ByteString)

type RecipientId = QueueId

type SenderId = QueueId

type QueueId = Encoded

data Command (a :: Party) where
  -- SMP recipient commands
  NEW :: RecipientPublicKey -> Command Recipient
  SUB :: Command Recipient
  KEY :: SenderPublicKey -> Command Recipient
  ACK :: Command Recipient
  OFF :: Command Recipient
  DEL :: Command Recipient
  -- SMP sender commands
  SEND :: MsgBody -> Command Sender
  PING :: Command Sender
  -- SMP broker commands (responses, messages, notifications)
  IDS :: RecipientId -> SenderId -> Command Broker
  MSG :: MsgId -> UTCTime -> MsgBody -> Command Broker
  END :: Command Broker
  OK :: Command Broker
  ERR :: ErrorType -> Command Broker
  PONG :: Command Broker

deriving instance Show (Command a)

deriving instance Eq (Command a)

type Encoded = ByteString

-- newtype to avoid accidentally changing order of transmission parts
newtype CorrId = CorrId {bs :: ByteString} deriving (Eq, Ord, Show)

instance IsString CorrId where
  fromString = CorrId . fromString

-- only used by Agent, kept here so its definition is close to respective public key
type RecipientPrivateKey = C.PrivateKey

type RecipientPublicKey = C.PublicKey

-- only used by Agent, kept here so its definition is close to respective public key
type SenderPrivateKey = C.PrivateKey

type SenderPublicKey = C.PublicKey

type MsgId = Encoded

type MsgBody = ByteString

data ErrorType = PROHIBITED | SYNTAX Int | SIZE | AUTH | INTERNAL | DUPLICATE deriving (Show, Eq)

errBadTransmission :: Int
errBadTransmission = 1

errBadSMPCommand :: Int
errBadSMPCommand = 2

errNoCredentials :: Int
errNoCredentials = 3

errHasCredentials :: Int
errHasCredentials = 4

errNoQueueId :: Int
errNoQueueId = 5

errMessageBody :: Int
errMessageBody = 6

transmissionP :: Parser RawTransmission
transmissionP = do
  signature <- segment
  corrId <- segment
  queueId <- segment
  command <- A.takeByteString
  return (signature, corrId, queueId, command)
  where
    segment = A.takeTill (== ' ') <* " "

commandP :: Parser Cmd
commandP =
  "NEW " *> newCmd
    <|> "IDS " *> idsResp
    <|> "SUB" $> Cmd SRecipient SUB
    <|> "KEY " *> keyCmd
    <|> "ACK" $> Cmd SRecipient ACK
    <|> "OFF" $> Cmd SRecipient OFF
    <|> "DEL" $> Cmd SRecipient DEL
    <|> "SEND " *> sendCmd
    <|> "PING" $> Cmd SSender PING
    <|> "MSG " *> message
    <|> "END" $> Cmd SBroker END
    <|> "OK" $> Cmd SBroker OK
    <|> "ERR " *> serverError
    <|> "PONG" $> Cmd SBroker PONG
  where
    newCmd = Cmd SRecipient . NEW <$> C.pubKeyP
    idsResp = Cmd SBroker <$> (IDS <$> (base64P <* A.space) <*> base64P)
    keyCmd = Cmd SRecipient . KEY <$> C.pubKeyP
    sendCmd = do
      size <- A.decimal <* A.space
      Cmd SSender . SEND <$> A.take size <* A.space
    message = do
      msgId <- base64P <* A.space
      ts <- tsISO8601P <* A.space
      size <- A.decimal <* A.space
      Cmd SBroker . MSG msgId ts <$> A.take size <* A.space
    serverError = Cmd SBroker . ERR <$> errorType
    errorType =
      "PROHIBITED" $> PROHIBITED
        <|> "SYNTAX " *> (SYNTAX <$> A.decimal)
        <|> "SIZE" $> SIZE
        <|> "AUTH" $> AUTH
        <|> "INTERNAL" $> INTERNAL

-- TODO ignore the end of block, no need to parse it
parseCommand :: ByteString -> Either ErrorType Cmd
parseCommand = parse (commandP <* " " <* A.takeByteString) $ SYNTAX errBadSMPCommand

serializeCommand :: Cmd -> ByteString
serializeCommand = \case
  Cmd SRecipient (NEW rKey) -> "NEW " <> C.serializePubKey rKey
  Cmd SRecipient (KEY sKey) -> "KEY " <> C.serializePubKey sKey
  Cmd SRecipient cmd -> bshow cmd
  Cmd SSender (SEND msgBody) -> "SEND " <> serializeMsg msgBody
  Cmd SSender PING -> "PING"
  Cmd SBroker (MSG msgId ts msgBody) ->
    B.unwords ["MSG", encode msgId, B.pack $ formatISO8601Millis ts, serializeMsg msgBody]
  Cmd SBroker (IDS rId sId) -> B.unwords ["IDS", encode rId, encode sId]
  Cmd SBroker (ERR err) -> "ERR " <> bshow err
  Cmd SBroker resp -> bshow resp
  where
    serializeMsg msgBody = bshow (B.length msgBody) <> " " <> msgBody <> " "

data TransportError = TransportCryptoError C.CryptoError | TransportParsingError
  deriving (Eq, Show)

tGetEncrypted :: THandle -> IO (Either TransportError RawTransmission)
tGetEncrypted THandle {handle = h, receiveKey, blockSize} =
  B.hGet h blockSize >>= decryptBlock receiveKey >>= \case
    Left e -> pure . Left $ TransportCryptoError e
    Right "" -> ioe_EOF
    Right msg -> pure $ parse transmissionP TransportParsingError msg

decryptBlock :: TransportKey -> ByteString -> IO (Either C.CryptoError ByteString)
decryptBlock tKey block = do
  let (authTag, msg') = B.splitAt C.authTagSize block
  ivBytes <- makeNextIV tKey
  runExceptT $ C.decryptAES (aesKey tKey) ivBytes msg' (C.bsToAuthTag authTag)

tPut :: THandle -> SignedRawTransmission -> IO (Either TransportError ())
tPut THandle {handle = h, sendKey, blockSize} (C.Signature sig, t) =
  encryptBlock sendKey size block >>= \case
    Left e -> return . Left $ TransportCryptoError e
    Right (authTag, msg) -> Right <$> B.hPut h (C.authTagToBS authTag <> msg)
  where
    size = blockSize - C.authTagSize
    block = encode sig <> " " <> t <> " "

encryptBlock :: TransportKey -> Int -> ByteString -> IO (Either C.CryptoError (AES.AuthTag, ByteString))
encryptBlock tKey size block = do
  ivBytes <- makeNextIV tKey
  runExceptT $ C.encryptAES (aesKey tKey) ivBytes size block

makeNextIV :: TransportKey -> IO C.IV
makeNextIV TransportKey {baseIV, counter} = atomically $ do
  c <- readTVar counter
  writeTVar counter $ c + 1
  pure $ iv c
  where
    (start, rest) = B.splitAt 4 $ C.unIV baseIV
    iv c = C.IV $ (start `xor` encodeWord32 c) <> rest

serializeTransmission :: Transmission -> ByteString
serializeTransmission (CorrId corrId, queueId, command) =
  B.intercalate " " [corrId, encode queueId, serializeCommand command]

fromClient :: Cmd -> Either ErrorType Cmd
fromClient = \case
  Cmd SBroker _ -> Left PROHIBITED
  cmd -> Right cmd

fromServer :: Cmd -> Either ErrorType Cmd
fromServer = \case
  cmd@(Cmd SBroker _) -> Right cmd
  _ -> Left PROHIBITED

-- | get client and server transmissions
-- `fromParty` is used to limit allowed senders - `fromClient` or `fromServer` should be used
tGet :: forall m. MonadIO m => (Cmd -> Either ErrorType Cmd) -> THandle -> m SignedTransmissionOrError
tGet fromParty th = do
  liftIO (tGetEncrypted th) >>= \case
    Right (signature, corrId, queueId, command) -> do
      let decodedTransmission = liftM2 (,corrId,,command) (decode signature) (decode queueId)
      either (const $ tError corrId) tParseValidate decodedTransmission
    Left _ -> tError ""
  where
    tError :: ByteString -> m SignedTransmissionOrError
    tError corrId = return (C.Signature B.empty, (CorrId corrId, B.empty, Left $ SYNTAX errBadTransmission))

    tParseValidate :: RawTransmission -> m SignedTransmissionOrError
    tParseValidate t@(sig, corrId, queueId, command) = do
      let cmd = parseCommand command >>= fromParty >>= tCredentials t
      return (C.Signature sig, (CorrId corrId, queueId, cmd))

    tCredentials :: RawTransmission -> Cmd -> Either ErrorType Cmd
    tCredentials (signature, _, queueId, _) cmd = case cmd of
      -- IDS response should not have queue ID
      Cmd SBroker (IDS _ _) -> Right cmd
      -- ERR response does not always have queue ID
      Cmd SBroker (ERR _) -> Right cmd
      -- PONG response should not have queue ID
      Cmd SBroker PONG
        | B.null queueId -> Right cmd
        | otherwise -> Left $ SYNTAX errHasCredentials
      -- other responses must have queue ID
      Cmd SBroker _
        | B.null queueId -> Left $ SYNTAX errNoQueueId
        | otherwise -> Right cmd
      -- NEW must NOT have signature or queue ID
      Cmd SRecipient (NEW _)
        | B.null signature -> Left $ SYNTAX errNoCredentials
        | not (B.null queueId) -> Left $ SYNTAX errHasCredentials
        | otherwise -> Right cmd
      -- SEND must have queue ID, signature is not always required
      Cmd SSender (SEND _)
        | B.null queueId -> Left $ SYNTAX errNoQueueId
        | otherwise -> Right cmd
      -- PING must not have queue ID or signature
      Cmd SSender PING
        | B.null queueId && B.null signature -> Right cmd
        | otherwise -> Left $ SYNTAX errHasCredentials
      -- other client commands must have both signature and queue ID
      Cmd SRecipient _
        | B.null signature || B.null queueId -> Left $ SYNTAX errNoCredentials
        | otherwise -> Right cmd
