{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Simplex.Messaging.Protocol where

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Except
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import Data.Kind
import Data.String
import Data.Time.Clock
import Data.Time.ISO8601
import GHC.Generics (Generic)
import Generic.Random (genericArbitraryU)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Parsers
import Simplex.Messaging.Transport
import Simplex.Messaging.Util
import Test.QuickCheck (Arbitrary (..))

data Party = Broker | Recipient | Sender
  deriving (Show)

data SParty :: Party -> Type where
  SBroker :: SParty Broker
  SRecipient :: SParty Recipient
  SSender :: SParty Sender

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
type RecipientPrivateKey = C.SafePrivateKey

type RecipientPublicKey = C.PublicKey

-- only used by Agent, kept here so its definition is close to respective public key
type SenderPrivateKey = C.SafePrivateKey

type SenderPublicKey = C.PublicKey

type MsgId = Encoded

type MsgBody = ByteString

data ErrorType
  = BLOCK
  | CMD CommandError
  | AUTH
  | NO_MSG
  | INTERNAL
  | DUPLICATE_ -- TODO remove, not part of SMP protocol
  deriving (Eq, Generic, Read, Show)

data CommandError
  = PROHIBITED
  | SYNTAX
  | NO_AUTH
  | HAS_AUTH
  | NO_QUEUE
  deriving (Eq, Generic, Read, Show)

instance Arbitrary ErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandError where arbitrary = genericArbitraryU

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
    serverError = Cmd SBroker . ERR <$> errorTypeP

-- TODO ignore the end of block, no need to parse it
parseCommand :: ByteString -> Either ErrorType Cmd
parseCommand = parse (commandP <* " " <* A.takeByteString) $ CMD SYNTAX

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
  Cmd SBroker (ERR err) -> "ERR " <> serializeErrorType err
  Cmd SBroker resp -> bshow resp
  where
    serializeMsg msgBody = bshow (B.length msgBody) <> " " <> msgBody <> " "

errorTypeP :: Parser ErrorType
errorTypeP = "CMD " *> (CMD <$> parseRead1) <|> parseRead1

serializeErrorType :: ErrorType -> ByteString
serializeErrorType = bshow

tPut :: THandle -> SignedRawTransmission -> IO (Either TransportError ())
tPut th (C.Signature sig, t) =
  tPutEncrypted th $ encode sig <> " " <> t <> " "

serializeTransmission :: Transmission -> ByteString
serializeTransmission (CorrId corrId, queueId, command) =
  B.intercalate " " [corrId, encode queueId, serializeCommand command]

fromClient :: Cmd -> Either ErrorType Cmd
fromClient = \case
  Cmd SBroker _ -> Left $ CMD PROHIBITED
  cmd -> Right cmd

fromServer :: Cmd -> Either ErrorType Cmd
fromServer = \case
  cmd@(Cmd SBroker _) -> Right cmd
  _ -> Left $ CMD PROHIBITED

tGetParse :: THandle -> IO (Either TransportError RawTransmission)
tGetParse th = (>>= parse transmissionP TEBadBlock) <$> tGetEncrypted th

-- | get client and server transmissions
-- `fromParty` is used to limit allowed senders - `fromClient` or `fromServer` should be used
tGet :: forall m. MonadIO m => (Cmd -> Either ErrorType Cmd) -> THandle -> m SignedTransmissionOrError
tGet fromParty th = liftIO (tGetParse th) >>= decodeParseValidate
  where
    decodeParseValidate :: Either TransportError RawTransmission -> m SignedTransmissionOrError
    decodeParseValidate = \case
      Right (signature, corrId, queueId, command) ->
        let decodedTransmission = liftM2 (,corrId,,command) (decode signature) (decode queueId)
         in either (const $ tError corrId) tParseValidate decodedTransmission
      Left _ -> tError ""

    tError :: ByteString -> m SignedTransmissionOrError
    tError corrId = return (C.Signature B.empty, (CorrId corrId, B.empty, Left BLOCK))

    tParseValidate :: RawTransmission -> m SignedTransmissionOrError
    tParseValidate t@(sig, corrId, queueId, command) = do
      let cmd = parseCommand command >>= fromParty >>= tCredentials t
      return (C.Signature sig, (CorrId corrId, queueId, cmd))

    tCredentials :: RawTransmission -> Cmd -> Either ErrorType Cmd
    tCredentials (signature, _, queueId, _) cmd = case cmd of
      -- IDS response must not have queue ID
      Cmd SBroker (IDS _ _) -> Right cmd
      -- ERR response does not always have queue ID
      Cmd SBroker (ERR _) -> Right cmd
      -- PONG response must not have queue ID
      Cmd SBroker PONG
        | B.null queueId -> Right cmd
        | otherwise -> Left $ CMD HAS_AUTH
      -- other responses must have queue ID
      Cmd SBroker _
        | B.null queueId -> Left $ CMD NO_QUEUE
        | otherwise -> Right cmd
      -- NEW must have signature but NOT queue ID
      Cmd SRecipient (NEW _)
        | B.null signature -> Left $ CMD NO_AUTH
        | not (B.null queueId) -> Left $ CMD HAS_AUTH
        | otherwise -> Right cmd
      -- SEND must have queue ID, signature is not always required
      Cmd SSender (SEND _)
        | B.null queueId -> Left $ CMD NO_QUEUE
        | otherwise -> Right cmd
      -- PING must not have queue ID or signature
      Cmd SSender PING
        | B.null queueId && B.null signature -> Right cmd
        | otherwise -> Left $ CMD HAS_AUTH
      -- other client commands must have both signature and queue ID
      Cmd SRecipient _
        | B.null signature || B.null queueId -> Left $ CMD NO_AUTH
        | otherwise -> Right cmd
