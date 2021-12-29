{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

-- |
-- Module      : Simplex.Messaging.Protocol
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- Types, parsers, serializers and functions to send and receive SMP protocol commands and responses.
--
-- See https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md
module Simplex.Messaging.Protocol
  ( -- * SMP protocol parameters
    clientVersion,
    maxMessageLength,
    e2eEncMessageLength,

    -- * SMP protocol types
    Command (..),
    CommandI (..),
    Party (..),
    ClientParty (..),
    Cmd (..),
    ClientCmd (..),
    SParty (..),
    QueueIdsKeys (..),
    ErrorType (..),
    CommandError (..),
    Transmission,
    BrokerTransmission,
    SignedTransmission,
    SentRawTransmission,
    SignedRawTransmission,
    EncMessage (..),
    PubHeader (..),
    ClientMessage (..),
    PrivHeader (..),
    CorrId (..),
    QueueId,
    RecipientId,
    SenderId,
    NotifierId,
    RcvPrivateSignKey,
    RcvPublicVerifyKey,
    RcvPublicDhKey,
    RcvDhSecret,
    SndPrivateSignKey,
    SndPublicVerifyKey,
    NtfPrivateSignKey,
    NtfPublicVerifyKey,
    Encoded,
    MsgId,
    MsgBody,

    -- * Parse and serialize
    serializeTransmission,
    serializeErrorType,
    transmissionP,
    errorTypeP,
    serializeEncMessage,
    encMessageP,
    serializeClientMessage,
    clientMessageP,

    -- * TCP transport functions
    tPut,
    tGet,
    fromClient,
    fromServer,
  )
where

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Except
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Bifunctor (first)
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Constraint (Dict (..))
import Data.Functor (($>))
import Data.Kind
import Data.Maybe (isNothing)
import Data.String
import Data.Time.Clock
import Data.Time.ISO8601
import Data.Type.Equality
import Data.Word (Word16)
import GHC.Generics (Generic)
import GHC.TypeLits (ErrorMessage (..), TypeError)
import Generic.Random (genericArbitraryU)
import Network.Transport.Internal (encodeWord16)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Parsers
import Simplex.Messaging.Transport (THandle (..), Transport, TransportError (..), tGetBlock, tPutBlock)
import Simplex.Messaging.Util
import Test.QuickCheck (Arbitrary (..))

clientVersion :: Word16
clientVersion = 1

maxMessageLength :: Int
maxMessageLength = 15968

e2eEncMessageLength :: Int
e2eEncMessageLength = 15842

-- | SMP protocol participants.
data Party = Broker | Recipient | Sender | Notifier
  deriving (Show)

-- | Singleton types for SMP protocol participants.
data SParty :: Party -> Type where
  SBroker :: SParty Broker
  SRecipient :: SParty Recipient
  SSender :: SParty Sender
  SNotifier :: SParty Notifier

instance TestEquality SParty where
  testEquality SBroker SBroker = Just Refl
  testEquality SRecipient SRecipient = Just Refl
  testEquality SSender SSender = Just Refl
  testEquality SNotifier SNotifier = Just Refl
  testEquality _ _ = Nothing

deriving instance Show (SParty p)

class PartyI (p :: Party) where sParty :: SParty p

instance PartyI Broker where sParty = SBroker

instance PartyI Recipient where sParty = SRecipient

instance PartyI Sender where sParty = SSender

instance PartyI Notifier where sParty = SNotifier

data ClientParty = forall p. IsClient p => CP (SParty p)

deriving instance Show ClientParty

-- | Type for command or response of any participant.
data Cmd = forall p. PartyI p => Cmd (SParty p) (Command p)

deriving instance Show Cmd

-- | Type for command or response of any participant.
data ClientCmd = forall p. (PartyI p, IsClient p) => ClientCmd (SParty p) (Command p)

class CommandI c where
  serializeCommand :: c -> ByteString
  commandP :: Parser c

-- | Parsed SMP transmission without signature, size and session ID.
type Transmission c = (CorrId, QueueId, c)

type BrokerTransmission = Transmission (Command Broker)

-- | signed parsed transmission, with original raw bytes and parsing error.
type SignedTransmission c = (Maybe C.ASignature, Signed, Transmission (Either ErrorType c))

type Signed = ByteString

-- | unparsed SMP transmission with signature.
data RawTransmission = RawTransmission
  { signature :: ByteString,
    signed :: ByteString,
    sessId :: ByteString,
    corrId :: ByteString,
    queueId :: ByteString,
    command :: ByteString
  }

-- | unparsed sent SMP transmission with signature, without session ID.
type SignedRawTransmission = (Maybe C.ASignature, ByteString, ByteString, ByteString)

-- | unparsed sent SMP transmission with signature.
type SentRawTransmission = (Maybe C.ASignature, ByteString)

-- | SMP queue ID for the recipient.
type RecipientId = QueueId

-- | SMP queue ID for the sender.
type SenderId = QueueId

-- | SMP queue ID for notifications.
type NotifierId = QueueId

-- | SMP queue ID on the server.
type QueueId = Encoded

-- | Parameterized type for SMP protocol commands from all participants.
data Command (a :: Party) where
  -- SMP recipient commands
  NEW :: RcvPublicVerifyKey -> RcvPublicDhKey -> Command Recipient
  SUB :: Command Recipient
  KEY :: SndPublicVerifyKey -> Command Recipient
  NKEY :: NtfPublicVerifyKey -> Command Recipient
  ACK :: Command Recipient
  OFF :: Command Recipient
  DEL :: Command Recipient
  -- SMP sender commands
  SEND :: MsgBody -> Command Sender
  PING :: Command Sender
  -- SMP notification subscriber commands
  NSUB :: Command Notifier
  -- SMP broker commands (responses, messages, notifications)
  IDS :: QueueIdsKeys -> Command Broker
  MSG :: MsgId -> UTCTime -> MsgBody -> Command Broker
  NID :: NotifierId -> Command Broker
  NMSG :: Command Broker
  END :: Command Broker
  OK :: Command Broker
  ERR :: ErrorType -> Command Broker
  PONG :: Command Broker

deriving instance Show (Command a)

deriving instance Eq (Command a)

type family IsClient p :: Constraint where
  IsClient Recipient = ()
  IsClient Sender = ()
  IsClient Notifier = ()
  IsClient p =
    (Int ~ Bool, TypeError (Text "Party " :<>: ShowType p :<>: Text " is not a Client"))

isClient :: SParty p -> Maybe (Dict (IsClient p))
isClient = \case
  SRecipient -> Just Dict
  SSender -> Just Dict
  SNotifier -> Just Dict
  _ -> Nothing

-- | SMP message body format
data EncMessage = EncMessage
  { emHeader :: PubHeader,
    emNonce :: C.CbNonce,
    emBody :: ByteString
  }

data PubHeader = PubHeader
  { phVersion :: Word16,
    phE2ePubDhKey :: C.PublicKeyX25519
  }

serializePubHeader :: PubHeader -> ByteString
serializePubHeader (PubHeader v k) = encodeWord16 v <> C.encodeLenKey' k

pubHeaderP :: Parser PubHeader
pubHeaderP = PubHeader <$> word16P <*> C.binaryLenKeyP

serializeEncMessage :: EncMessage -> ByteString
serializeEncMessage EncMessage {emHeader, emNonce, emBody} =
  serializePubHeader emHeader <> C.unCbNonce emNonce <> emBody

encMessageP :: Parser EncMessage
encMessageP = do
  emHeader <- pubHeaderP
  emNonce <- C.cbNonceP
  emBody <- A.takeByteString
  pure EncMessage {emHeader, emNonce, emBody}

data ClientMessage = ClientMessage PrivHeader ByteString

data PrivHeader
  = PHConfirmation C.APublicVerifyKey
  | PHEmpty

serializePrivHeader :: PrivHeader -> ByteString
serializePrivHeader = \case
  PHConfirmation k -> "K" <> C.encodeLenKey k
  PHEmpty -> " "

privHeaderP :: Parser PrivHeader
privHeaderP =
  A.anyChar >>= \case
    'K' -> PHConfirmation <$> C.binaryLenKeyP
    ' ' -> pure PHEmpty
    _ -> fail "invalid PrivHeader"

serializeClientMessage :: ClientMessage -> ByteString
serializeClientMessage (ClientMessage h msg) = serializePrivHeader h <> msg

clientMessageP :: Parser ClientMessage
clientMessageP = ClientMessage <$> privHeaderP <*> A.takeByteString

-- | Base-64 encoded string.
type Encoded = ByteString

-- | Transmission correlation ID.
newtype CorrId = CorrId {bs :: ByteString} deriving (Eq, Ord, Show)

instance IsString CorrId where
  fromString = CorrId . fromString

-- | Queue IDs and keys
data QueueIdsKeys = QIK
  { rcvId :: RecipientId,
    sndId :: SenderId,
    rcvPublicDhKey :: RcvPublicDhKey
  }
  deriving (Eq, Show)

-- | Recipient's private key used by the recipient to authorize (sign) SMP commands.
--
-- Only used by SMP agent, kept here so its definition is close to respective public key.
type RcvPrivateSignKey = C.APrivateSignKey

-- | Recipient's public key used by SMP server to verify authorization of SMP commands.
type RcvPublicVerifyKey = C.APublicVerifyKey

-- | Public key used for DH exchange to encrypt message bodies from server to recipient
type RcvPublicDhKey = C.PublicKeyX25519

-- | DH Secret used to encrypt message bodies from server to recipient
type RcvDhSecret = C.DhSecretX25519

-- | Sender's private key used by the recipient to authorize (sign) SMP commands.
--
-- Only used by SMP agent, kept here so its definition is close to respective public key.
type SndPrivateSignKey = C.APrivateSignKey

-- | Sender's public key used by SMP server to verify authorization of SMP commands.
type SndPublicVerifyKey = C.APublicVerifyKey

-- | Private key used by push notifications server to authorize (sign) LSTN command.
type NtfPrivateSignKey = C.APrivateSignKey

-- | Public key used by SMP server to verify authorization of LSTN command sent by push notifications server.
type NtfPublicVerifyKey = C.APublicVerifyKey

-- | SMP message server ID.
type MsgId = Encoded

-- | SMP message body.
type MsgBody = ByteString

-- | Type for protocol errors.
data ErrorType
  = -- | incorrect block format, encoding or signature size
    BLOCK
  | -- | incorrect SMP session ID (TLS Finished message / tls-unique binding RFC5929)
    SESSION
  | -- | SMP command is unknown or has invalid syntax
    CMD CommandError
  | -- | command authorization error - bad signature or non-existing SMP queue
    AUTH
  | -- | SMP queue capacity is exceeded on the server
    QUOTA
  | -- | ACK command is sent without message to be acknowledged
    NO_MSG
  | -- | sent message is too large (> maxMessageLength = 15968 bytes)
    LARGE_MSG
  | -- | internal server error
    INTERNAL
  | -- | used internally, never returned by the server (to be removed)
    DUPLICATE_ -- TODO remove, not part of SMP protocol
  deriving (Eq, Generic, Read, Show)

-- | SMP command error type.
data CommandError
  = -- | server response sent from client or vice versa
    PROHIBITED
  | -- | error parsing command
    SYNTAX
  | -- | transmission has no required credentials (signature or queue ID)
    NO_AUTH
  | -- | transmission has credentials that are not allowed for this command
    HAS_AUTH
  | -- | transmission has no required queue ID
    NO_QUEUE
  deriving (Eq, Generic, Read, Show)

instance Arbitrary ErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandError where arbitrary = genericArbitraryU

-- | SMP transmission parser.
transmissionP :: Parser RawTransmission
transmissionP = do
  signature <- segment
  signed <- A.takeByteString
  either fail pure $ parseAll (trn signature signed) signed
  where
    segment = A.takeTill (== ' ') <* A.space
    trn signature signed = do
      sessId <- segment
      corrId <- segment
      queueId <- segment
      command <- A.takeByteString
      pure RawTransmission {signature, signed, sessId, corrId, queueId, command}

instance CommandI Cmd where
  serializeCommand (Cmd _ cmd) = serializeCommand cmd
  commandP =
    "NEW " *> newCmd
      <|> "IDS " *> idsResp
      <|> "SUB" $> Cmd SRecipient SUB
      <|> "KEY " *> keyCmd
      <|> "NKEY " *> nKeyCmd
      <|> "NID " *> nIdsResp
      <|> "ACK" $> Cmd SRecipient ACK
      <|> "OFF" $> Cmd SRecipient OFF
      <|> "DEL" $> Cmd SRecipient DEL
      <|> "SEND " *> sendCmd
      <|> "PING" $> Cmd SSender PING
      <|> "NSUB" $> Cmd SNotifier NSUB
      <|> "MSG " *> message
      <|> "NMSG" $> Cmd SBroker NMSG
      <|> "END" $> Cmd SBroker END
      <|> "OK" $> Cmd SBroker OK
      <|> "ERR " *> serverError
      <|> "PONG" $> Cmd SBroker PONG
    where
      newCmd = Cmd SRecipient <$> (NEW <$> C.strPubKeyP <* A.space <*> C.strPubKeyP)
      idsResp = Cmd SBroker . IDS <$> qik
      qik = QIK <$> base64P <* A.space <*> base64P <* A.space <*> C.strPubKeyP
      nIdsResp = Cmd SBroker . NID <$> base64P
      keyCmd = Cmd SRecipient . KEY <$> C.strPubKeyP
      nKeyCmd = Cmd SRecipient . NKEY <$> C.strPubKeyP
      sendCmd = Cmd SSender . SEND <$> A.takeByteString
      message = do
        msgId <- base64P <* A.space
        ts <- tsISO8601P <* A.space
        Cmd SBroker . MSG msgId ts <$> A.takeByteString
      serverError = Cmd SBroker . ERR <$> errorTypeP

instance CommandI ClientCmd where
  serializeCommand (ClientCmd _ cmd) = serializeCommand cmd
  commandP = clientCmd <$?> commandP
    where
      clientCmd :: Cmd -> Either String ClientCmd
      clientCmd (Cmd p cmd) = case isClient p of
        Just Dict -> Right (ClientCmd p cmd)
        _ -> Left "not a client command"

-- | Parse SMP command.
parseCommand :: ByteString -> Either ErrorType Cmd
parseCommand = parse commandP $ CMD SYNTAX

instance PartyI p => CommandI (Command p) where
  commandP = command' <$?> commandP
    where
      command' :: Cmd -> Either String (Command p)
      command' (Cmd p cmd) = case testEquality p $ sParty @p of
        Just Refl -> Right cmd
        _ -> Left "bad command party"
  serializeCommand = \case
    NEW rKey dhKey -> B.unwords ["NEW", C.serializePubKey rKey, C.serializePubKey' dhKey]
    KEY sKey -> "KEY " <> C.serializePubKey sKey
    NKEY nKey -> "NKEY " <> C.serializePubKey nKey
    SUB -> "SUB"
    ACK -> "ACK"
    OFF -> "OFF"
    DEL -> "DEL"
    SEND msgBody -> "SEND " <> msgBody
    PING -> "PING"
    NSUB -> "NSUB"
    MSG msgId ts msgBody ->
      B.unwords ["MSG", encode msgId, B.pack $ formatISO8601Millis ts, msgBody]
    IDS (QIK rcvId sndId srvDh) ->
      B.unwords ["IDS", encode rcvId, encode sndId, C.serializePubKey' srvDh]
    NID nId -> "NID " <> encode nId
    ERR err -> "ERR " <> serializeErrorType err
    NMSG -> "NMSG"
    END -> "END"
    OK -> "OK"
    PONG -> "PONG"

-- | SMP error parser.
errorTypeP :: Parser ErrorType
errorTypeP = "CMD " *> (CMD <$> parseRead1) <|> parseRead1

-- | Serialize SMP error.
serializeErrorType :: ErrorType -> ByteString
serializeErrorType = bshow

-- | Send signed SMP transmission to TCP transport.
tPut :: Transport c => THandle c -> SentRawTransmission -> IO (Either TransportError ())
tPut th (sig, t) = tPutBlock th $ C.serializeSignature sig <> " " <> t

serializeTransmission :: CommandI c => ByteString -> Transmission c -> ByteString
serializeTransmission sessionId (CorrId corrId, queueId, command) =
  B.unwords [sessionId, corrId, encode queueId, serializeCommand command]

-- | Validate that it is an SMP client command, used with 'tGet' by 'Simplex.Messaging.Server'.
fromClient :: Cmd -> Either ErrorType ClientCmd
fromClient (Cmd p cmd) = case isClient p of
  Just Dict -> Right $ ClientCmd p cmd
  Nothing -> Left $ CMD PROHIBITED

-- | Validate that it is an SMP server command, used with 'tGet' by 'Simplex.Messaging.Client'.
fromServer :: Cmd -> Either ErrorType (Command Broker)
fromServer = \case
  Cmd SBroker cmd -> Right cmd
  _ -> Left $ CMD PROHIBITED

-- | Receive and parse transmission from the TCP transport (ignoring any trailing padding).
tGetParse :: Transport c => THandle c -> IO (Either TransportError RawTransmission)
tGetParse th = (parseTransmission =<<) <$> tGetBlock th
  where
    parseTransmission = first (const TEBadBlock) . A.parseOnly transmissionP

-- | Receive client and server transmissions.
--
-- The first argument is used to limit allowed senders.
-- 'fromClient' or 'fromServer' should be used here.
tGet :: forall c m cmd. (Transport c, MonadIO m) => (Cmd -> Either ErrorType cmd) -> THandle c -> m (SignedTransmission cmd)
tGet fromParty th@THandle {sessionId} = liftIO (tGetParse th) >>= decodeParseValidate
  where
    decodeParseValidate :: Either TransportError RawTransmission -> m (SignedTransmission cmd)
    decodeParseValidate = \case
      Right RawTransmission {signature, signed, sessId, corrId, queueId, command}
        | sessId == sessionId ->
          let decodedTransmission = liftM2 (,corrId,,command) (C.decodeSignature =<< decode signature) (decode queueId)
           in either (const $ tError corrId) (tParseValidate signed) decodedTransmission
        | otherwise -> pure (Nothing, "", (CorrId corrId, "", Left SESSION))
      Left _ -> tError ""

    tError :: ByteString -> m (SignedTransmission cmd)
    tError corrId = pure (Nothing, "", (CorrId corrId, "", Left BLOCK))

    tParseValidate :: ByteString -> SignedRawTransmission -> m (SignedTransmission cmd)
    tParseValidate signed t@(sig, corrId, queueId, command) = do
      let cmd = parseCommand command >>= tCredentials t >>= fromParty
      return (sig, signed, (CorrId corrId, queueId, cmd))

    tCredentials :: SignedRawTransmission -> Cmd -> Either ErrorType Cmd
    tCredentials (sig, _, queueId, _) cmd = case cmd of
      -- IDS response must not have queue ID
      Cmd SBroker (IDS _) -> Right cmd
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
      Cmd SRecipient NEW {}
        | isNothing sig -> Left $ CMD NO_AUTH
        | not (B.null queueId) -> Left $ CMD HAS_AUTH
        | otherwise -> Right cmd
      -- SEND must have queue ID, signature is not always required
      Cmd SSender (SEND _)
        | B.null queueId -> Left $ CMD NO_QUEUE
        | otherwise -> Right cmd
      -- PING must not have queue ID or signature
      Cmd SSender PING
        | isNothing sig && B.null queueId -> Right cmd
        | otherwise -> Left $ CMD HAS_AUTH
      -- other client commands must have both signature and queue ID
      Cmd _ _
        | isNothing sig || B.null queueId -> Left $ CMD NO_AUTH
        | otherwise -> Right cmd
