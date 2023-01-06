{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

{-# HLINT ignore "Use newtype instead of data" #-}

-- |
-- Module      : Simplex.Messaging.ProtocolEncoding
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
    supportedSMPClientVRange,
    maxMessageLength,
    e2eEncConfirmationLength,
    e2eEncMessageLength,

    -- * SMP protocol types
    ProtocolEncoding (..),
    Command (..),
    Party (..),
    Cmd (..),
    BrokerMsg (..),
    SParty (..),
    PartyI (..),
    QueueIdsKeys (..),
    ErrorType (..),
    CommandError (..),
    Transmission,
    SignedTransmission,
    SentRawTransmission,
    SignedRawTransmission,
    ClientMsgEnvelope (..),
    PubHeader (..),
    ClientMessage (..),
    PrivHeader (..),
    Protocol (..),
    ProtocolType (..),
    AProtocolType (..),
    ProtocolTypeI (..),
    ProtocolServer (..),
    ProtoServer,
    SMPServer,
    pattern SMPServer,
    SMPServerWithAuth,
    NtfServer,
    pattern NtfServer,
    ProtoServerWithAuth (..),
    BasicAuth (..),
    SrvLoc (..),
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
    RcvNtfPublicDhKey,
    RcvNtfDhSecret,
    Message (..),
    RcvMessage (..),
    MsgId,
    MsgBody,
    MaxMessageLen,
    MaxRcvMessageLen,
    EncRcvMsgBody (..),
    RcvMsgBody (..),
    ClientRcvMsgBody (..),
    EncNMsgMeta,
    SMPMsgMeta (..),
    NMsgMeta (..),
    MsgFlags (..),
    rcvMessageMeta,
    noMsgFlags,

    -- * Parse and serialize
    ProtocolMsgTag (..),
    messageTagP,
    encodeTransmission,
    transmissionP,
    _smpP,
    encodeRcvMsgBody,
    clientRcvMsgBodyP,
    legacyEncodeServer,
    legacyServerP,
    legacyStrEncodeServer,
    sameSrvAddr,
    sameSrvAddr',
    noAuthSrv,

    -- * TCP transport functions
    tPut,
    tGet,

    -- * exports for tests
    CommandTag (..),
    BrokerMsgTag (..),
  )
where

import Control.Applicative (optional, (<|>))
import Control.Monad.Except
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Aeson as J
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Char (isPrint, isSpace)
import Data.Functor (($>))
import Data.Kind
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import Data.Maybe (isJust, isNothing)
import Data.String
import Data.Time.Clock.System (SystemTime (..))
import Data.Type.Equality
import GHC.Generics (Generic)
import GHC.TypeLits (type (+))
import Generic.Random (genericArbitraryU)
import Network.Socket (HostName, ServiceName)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers
import Simplex.Messaging.Transport
import Simplex.Messaging.Transport.Client (TransportHost, TransportHosts (..))
import Simplex.Messaging.Util (bshow, (<$?>))
import Simplex.Messaging.Version
import Test.QuickCheck (Arbitrary (..))

currentSMPClientVersion :: Version
currentSMPClientVersion = 2

supportedSMPClientVRange :: VersionRange
supportedSMPClientVRange = mkVersionRange 1 currentSMPClientVersion

maxMessageLength :: Int
maxMessageLength = 16088

type MaxMessageLen = 16088

-- 16 extra bytes: 8 for timestamp and 8 for flags (7 flags and the space, only 1 flag is currently used)
type MaxRcvMessageLen = MaxMessageLen + 16 -- 16104, the padded size is 16106

-- it is shorter to allow per-queue e2e encryption DH key in the "public" header
e2eEncConfirmationLength :: Int
e2eEncConfirmationLength = 15936

e2eEncMessageLength :: Int
e2eEncMessageLength = 16032

-- | SMP protocol clients
data Party = Recipient | Sender | Notifier
  deriving (Show)

-- | Singleton types for SMP protocol clients
data SParty :: Party -> Type where
  SRecipient :: SParty Recipient
  SSender :: SParty Sender
  SNotifier :: SParty Notifier

instance TestEquality SParty where
  testEquality SRecipient SRecipient = Just Refl
  testEquality SSender SSender = Just Refl
  testEquality SNotifier SNotifier = Just Refl
  testEquality _ _ = Nothing

deriving instance Show (SParty p)

class PartyI (p :: Party) where sParty :: SParty p

instance PartyI Recipient where sParty = SRecipient

instance PartyI Sender where sParty = SSender

instance PartyI Notifier where sParty = SNotifier

-- | Type for client command of any participant.
data Cmd = forall p. PartyI p => Cmd (SParty p) (Command p)

deriving instance Show Cmd

-- | Parsed SMP transmission without signature, size and session ID.
type Transmission c = (CorrId, EntityId, c)

-- | signed parsed transmission, with original raw bytes and parsing error.
type SignedTransmission c = (Maybe C.ASignature, Signed, Transmission (Either ErrorType c))

type Signed = ByteString

-- | unparsed SMP transmission with signature.
data RawTransmission = RawTransmission
  { signature :: ByteString,
    signed :: ByteString,
    sessId :: SessionId,
    corrId :: ByteString,
    entityId :: ByteString,
    command :: ByteString
  }
  deriving (Show)

-- | unparsed sent SMP transmission with signature, without session ID.
type SignedRawTransmission = (Maybe C.ASignature, SessionId, ByteString, ByteString)

-- | unparsed sent SMP transmission with signature.
type SentRawTransmission = (Maybe C.ASignature, ByteString)

-- | SMP queue ID for the recipient.
type RecipientId = QueueId

-- | SMP queue ID for the sender.
type SenderId = QueueId

-- | SMP queue ID for notifications.
type NotifierId = QueueId

-- | SMP queue ID on the server.
type QueueId = EntityId

type EntityId = ByteString

-- | Parameterized type for SMP protocol commands from all clients.
data Command (p :: Party) where
  -- SMP recipient commands
  NEW :: RcvPublicVerifyKey -> RcvPublicDhKey -> Maybe BasicAuth -> Command Recipient
  SUB :: Command Recipient
  KEY :: SndPublicVerifyKey -> Command Recipient
  NKEY :: NtfPublicVerifyKey -> RcvNtfPublicDhKey -> Command Recipient
  NDEL :: Command Recipient
  GET :: Command Recipient
  -- ACK v1 has to be supported for encoding/decoding
  -- ACK :: Command Recipient
  ACK :: MsgId -> Command Recipient
  OFF :: Command Recipient
  DEL :: Command Recipient
  -- SMP sender commands
  -- SEND v1 has to be supported for encoding/decoding
  -- SEND :: MsgBody -> Command Sender
  SEND :: MsgFlags -> MsgBody -> Command Sender
  PING :: Command Sender
  -- SMP notification subscriber commands
  NSUB :: Command Notifier

deriving instance Show (Command p)

deriving instance Eq (Command p)

data BrokerMsg where
  -- SMP broker messages (responses, client messages, notifications)
  IDS :: QueueIdsKeys -> BrokerMsg
  -- MSG v1/2 has to be supported for encoding/decoding
  -- v1: MSG :: MsgId -> SystemTime -> MsgBody -> BrokerMsg
  -- v2: MsgId -> SystemTime -> MsgFlags -> MsgBody -> BrokerMsg
  MSG :: RcvMessage -> BrokerMsg
  NID :: NotifierId -> RcvNtfPublicDhKey -> BrokerMsg
  NMSG :: C.CbNonce -> EncNMsgMeta -> BrokerMsg
  END :: BrokerMsg
  OK :: BrokerMsg
  ERR :: ErrorType -> BrokerMsg
  PONG :: BrokerMsg
  deriving (Eq, Show)

data RcvMessage = RcvMessage
  { msgId :: MsgId,
    msgTs :: SystemTime,
    msgFlags :: MsgFlags,
    msgBody :: EncRcvMsgBody -- e2e encrypted, with extra encryption for recipient
  }
  deriving (Eq, Show)

-- | received message without server/recipient encryption
data Message
  = Message
      { msgId :: MsgId,
        msgTs :: SystemTime,
        msgFlags :: MsgFlags,
        msgBody :: C.MaxLenBS MaxMessageLen
      }
  | MessageQuota
      { msgId :: MsgId,
        msgTs :: SystemTime
      }

instance StrEncoding RcvMessage where
  strEncode RcvMessage {msgId, msgTs, msgFlags, msgBody = EncRcvMsgBody body} =
    B.unwords
      [ strEncode msgId,
        strEncode msgTs,
        "flags=" <> strEncode msgFlags,
        strEncode body
      ]
  strP = do
    msgId <- strP_
    msgTs <- strP_
    msgFlags <- ("flags=" *> strP_) <|> pure noMsgFlags
    msgBody <- EncRcvMsgBody <$> strP
    pure RcvMessage {msgId, msgTs, msgFlags, msgBody}

newtype EncRcvMsgBody = EncRcvMsgBody ByteString
  deriving (Eq, Show)

data RcvMsgBody
  = RcvMsgBody
      { msgTs :: SystemTime,
        msgFlags :: MsgFlags,
        msgBody :: C.MaxLenBS MaxMessageLen
      }
  | RcvMsgQuota
      { msgTs :: SystemTime
      }

msgQuotaTag :: ByteString
msgQuotaTag = "QUOTA"

encodeRcvMsgBody :: RcvMsgBody -> C.MaxLenBS MaxRcvMessageLen
encodeRcvMsgBody = \case
  RcvMsgBody {msgTs, msgFlags, msgBody} ->
    let rcvMeta :: C.MaxLenBS 16 = C.unsafeMaxLenBS $ smpEncode (msgTs, msgFlags, ' ')
     in C.appendMaxLenBS rcvMeta msgBody
  RcvMsgQuota {msgTs} ->
    C.unsafeMaxLenBS $ msgQuotaTag <> " " <> smpEncode msgTs

data ClientRcvMsgBody
  = ClientRcvMsgBody
      { msgTs :: SystemTime,
        msgFlags :: MsgFlags,
        msgBody :: ByteString
      }
  | ClientRcvMsgQuota
      { msgTs :: SystemTime
      }

clientRcvMsgBodyP :: Parser ClientRcvMsgBody
clientRcvMsgBodyP = msgQuotaP <|> msgBodyP
  where
    msgQuotaP = A.string msgQuotaTag *> (ClientRcvMsgQuota <$> _smpP)
    msgBodyP = do
      msgTs <- smpP
      msgFlags <- smpP
      Tail msgBody <- _smpP
      pure ClientRcvMsgBody {msgTs, msgFlags, msgBody}

instance StrEncoding Message where
  strEncode = \case
    Message {msgId, msgTs, msgFlags, msgBody} ->
      B.unwords
        [ strEncode msgId,
          strEncode msgTs,
          "flags=" <> strEncode msgFlags,
          strEncode msgBody
        ]
    MessageQuota {msgId, msgTs} ->
      B.unwords
        [ strEncode msgId,
          strEncode msgTs,
          "quota"
        ]
  strP = do
    msgId <- strP_
    msgTs <- strP_
    msgQuotaP msgId msgTs <|> msgP msgId msgTs
    where
      msgQuotaP msgId msgTs = "quota" $> MessageQuota {msgId, msgTs}
      msgP msgId msgTs = do
        msgFlags <- ("flags=" *> strP_) <|> pure noMsgFlags
        msgBody <- strP
        pure Message {msgId, msgTs, msgFlags, msgBody}

type EncNMsgMeta = ByteString

data SMPMsgMeta = SMPMsgMeta
  { msgId :: MsgId,
    msgTs :: SystemTime,
    msgFlags :: MsgFlags
  }
  deriving (Show)

rcvMessageMeta :: MsgId -> ClientRcvMsgBody -> SMPMsgMeta
rcvMessageMeta msgId = \case
  ClientRcvMsgBody {msgTs, msgFlags} -> SMPMsgMeta {msgId, msgTs, msgFlags}
  ClientRcvMsgQuota {msgTs} -> SMPMsgMeta {msgId, msgTs, msgFlags = noMsgFlags}

data NMsgMeta = NMsgMeta
  { msgId :: MsgId,
    msgTs :: SystemTime
  }
  deriving (Show)

instance Encoding NMsgMeta where
  smpEncode NMsgMeta {msgId, msgTs} =
    smpEncode (msgId, msgTs)
  smpP = do
    -- Tail here is to allow extension in the future clients/servers
    (msgId, msgTs, Tail _) <- smpP
    pure NMsgMeta {msgId, msgTs}

-- it must be data for correct JSON encoding
data MsgFlags = MsgFlags {notification :: Bool}
  deriving (Eq, Show, Generic)

instance ToJSON MsgFlags where toEncoding = J.genericToEncoding J.defaultOptions

-- this encoding should not become bigger than 7 bytes (currently it is 1 byte)
instance Encoding MsgFlags where
  smpEncode MsgFlags {notification} = smpEncode notification
  smpP = do
    notification <- smpP <* A.takeTill (== ' ')
    pure MsgFlags {notification}

instance StrEncoding MsgFlags where
  strEncode = smpEncode
  {-# INLINE strEncode #-}
  strP = smpP
  {-# INLINE strP #-}

noMsgFlags :: MsgFlags
noMsgFlags = MsgFlags {notification = False}

-- * SMP command tags

data CommandTag (p :: Party) where
  NEW_ :: CommandTag Recipient
  SUB_ :: CommandTag Recipient
  KEY_ :: CommandTag Recipient
  NKEY_ :: CommandTag Recipient
  NDEL_ :: CommandTag Recipient
  GET_ :: CommandTag Recipient
  ACK_ :: CommandTag Recipient
  OFF_ :: CommandTag Recipient
  DEL_ :: CommandTag Recipient
  SEND_ :: CommandTag Sender
  PING_ :: CommandTag Sender
  NSUB_ :: CommandTag Notifier

data CmdTag = forall p. PartyI p => CT (SParty p) (CommandTag p)

deriving instance Show (CommandTag p)

deriving instance Show CmdTag

data BrokerMsgTag
  = IDS_
  | MSG_
  | NID_
  | NMSG_
  | END_
  | OK_
  | ERR_
  | PONG_
  deriving (Show)

class ProtocolMsgTag t where
  decodeTag :: ByteString -> Maybe t

messageTagP :: ProtocolMsgTag t => Parser t
messageTagP =
  maybe (fail "bad message") pure . decodeTag
    =<< (A.takeTill (== ' ') <* optional A.space)

instance PartyI p => Encoding (CommandTag p) where
  smpEncode = \case
    NEW_ -> "NEW"
    SUB_ -> "SUB"
    KEY_ -> "KEY"
    NKEY_ -> "NKEY"
    NDEL_ -> "NDEL"
    GET_ -> "GET"
    ACK_ -> "ACK"
    OFF_ -> "OFF"
    DEL_ -> "DEL"
    SEND_ -> "SEND"
    PING_ -> "PING"
    NSUB_ -> "NSUB"
  smpP = messageTagP

instance ProtocolMsgTag CmdTag where
  decodeTag = \case
    "NEW" -> Just $ CT SRecipient NEW_
    "SUB" -> Just $ CT SRecipient SUB_
    "KEY" -> Just $ CT SRecipient KEY_
    "NKEY" -> Just $ CT SRecipient NKEY_
    "NDEL" -> Just $ CT SRecipient NDEL_
    "GET" -> Just $ CT SRecipient GET_
    "ACK" -> Just $ CT SRecipient ACK_
    "OFF" -> Just $ CT SRecipient OFF_
    "DEL" -> Just $ CT SRecipient DEL_
    "SEND" -> Just $ CT SSender SEND_
    "PING" -> Just $ CT SSender PING_
    "NSUB" -> Just $ CT SNotifier NSUB_
    _ -> Nothing

instance Encoding CmdTag where
  smpEncode (CT _ t) = smpEncode t
  smpP = messageTagP

instance PartyI p => ProtocolMsgTag (CommandTag p) where
  decodeTag s = decodeTag s >>= (\(CT _ t) -> checkParty' t)

instance Encoding BrokerMsgTag where
  smpEncode = \case
    IDS_ -> "IDS"
    MSG_ -> "MSG"
    NID_ -> "NID"
    NMSG_ -> "NMSG"
    END_ -> "END"
    OK_ -> "OK"
    ERR_ -> "ERR"
    PONG_ -> "PONG"
  smpP = messageTagP

instance ProtocolMsgTag BrokerMsgTag where
  decodeTag = \case
    "IDS" -> Just IDS_
    "MSG" -> Just MSG_
    "NID" -> Just NID_
    "NMSG" -> Just NMSG_
    "END" -> Just END_
    "OK" -> Just OK_
    "ERR" -> Just ERR_
    "PONG" -> Just PONG_
    _ -> Nothing

-- | SMP message body format
data ClientMsgEnvelope = ClientMsgEnvelope
  { cmHeader :: PubHeader,
    cmNonce :: C.CbNonce,
    cmEncBody :: ByteString
  }
  deriving (Show)

data PubHeader = PubHeader
  { phVersion :: Version,
    phE2ePubDhKey :: Maybe C.PublicKeyX25519
  }
  deriving (Show)

instance Encoding PubHeader where
  smpEncode (PubHeader v k) = smpEncode (v, k)
  smpP = PubHeader <$> smpP <*> smpP

instance Encoding ClientMsgEnvelope where
  smpEncode ClientMsgEnvelope {cmHeader, cmNonce, cmEncBody} =
    smpEncode (cmHeader, cmNonce, Tail cmEncBody)
  smpP = do
    (cmHeader, cmNonce, Tail cmEncBody) <- smpP
    pure ClientMsgEnvelope {cmHeader, cmNonce, cmEncBody}

data ClientMessage = ClientMessage PrivHeader ByteString

data PrivHeader
  = PHConfirmation C.APublicVerifyKey
  | PHEmpty
  deriving (Show)

instance Encoding PrivHeader where
  smpEncode = \case
    PHConfirmation k -> "K" <> smpEncode k
    PHEmpty -> "_"
  smpP =
    A.anyChar >>= \case
      'K' -> PHConfirmation <$> smpP
      '_' -> pure PHEmpty
      _ -> fail "invalid PrivHeader"

instance Encoding ClientMessage where
  smpEncode (ClientMessage h msg) = smpEncode h <> msg
  smpP = ClientMessage <$> smpP <*> A.takeByteString

type SMPServer = ProtocolServer 'PSMP

pattern SMPServer :: NonEmpty TransportHost -> ServiceName -> C.KeyHash -> ProtocolServer 'PSMP
pattern SMPServer host port keyHash = ProtocolServer SPSMP host port keyHash

{-# COMPLETE SMPServer #-}

type SMPServerWithAuth = ProtoServerWithAuth 'PSMP

type NtfServer = ProtocolServer 'PNTF

pattern NtfServer :: NonEmpty TransportHost -> ServiceName -> C.KeyHash -> ProtocolServer 'PNTF
pattern NtfServer host port keyHash = ProtocolServer SPNTF host port keyHash

{-# COMPLETE NtfServer #-}

sameSrvAddr' :: ProtoServerWithAuth p -> ProtoServerWithAuth p -> Bool
sameSrvAddr' (ProtoServerWithAuth srv _) (ProtoServerWithAuth srv' _) = sameSrvAddr srv srv'
{-# INLINE sameSrvAddr' #-}

sameSrvAddr :: ProtocolServer p -> ProtocolServer p -> Bool
sameSrvAddr ProtocolServer {host, port} ProtocolServer {host = h', port = p'} = host == h' && port == p'
{-# INLINE sameSrvAddr #-}

data ProtocolType = PSMP | PNTF
  deriving (Eq, Ord, Show)

instance StrEncoding ProtocolType where
  strEncode = \case
    PSMP -> "smp"
    PNTF -> "ntf"
  strP =
    A.takeTill (\c -> c == ':' || c == ' ') >>= \case
      "smp" -> pure PSMP
      "ntf" -> pure PNTF
      _ -> fail "bad ProtocolType"

data SProtocolType (p :: ProtocolType) where
  SPSMP :: SProtocolType 'PSMP
  SPNTF :: SProtocolType 'PNTF

deriving instance Eq (SProtocolType p)

deriving instance Ord (SProtocolType p)

deriving instance Show (SProtocolType p)

data AProtocolType = forall p. ProtocolTypeI p => AProtocolType (SProtocolType p)

deriving instance Show AProtocolType

instance Eq AProtocolType where
  AProtocolType p == AProtocolType p' = isJust $ testEquality p p'

instance TestEquality SProtocolType where
  testEquality SPSMP SPSMP = Just Refl
  testEquality SPNTF SPNTF = Just Refl
  testEquality _ _ = Nothing

protocolType :: SProtocolType p -> ProtocolType
protocolType = \case
  SPSMP -> PSMP
  SPNTF -> PNTF

aProtocolType :: ProtocolType -> AProtocolType
aProtocolType = \case
  PSMP -> AProtocolType SPSMP
  PNTF -> AProtocolType SPNTF

instance ProtocolTypeI p => StrEncoding (SProtocolType p) where
  strEncode = strEncode . protocolType
  strP = (\(AProtocolType p) -> checkProtocolType p) <$?> strP

instance StrEncoding AProtocolType where
  strEncode (AProtocolType p) = strEncode p
  strP = aProtocolType <$> strP

instance ToJSON AProtocolType where
  toEncoding = strToJEncoding
  toJSON = strToJSON

checkProtocolType :: forall t p p'. (ProtocolTypeI p, ProtocolTypeI p') => t p' -> Either String (t p)
checkProtocolType p = case testEquality (protocolTypeI @p) (protocolTypeI @p') of
  Just Refl -> Right p
  Nothing -> Left "bad ProtocolType"

class ProtocolTypeI (p :: ProtocolType) where
  protocolTypeI :: SProtocolType p

instance ProtocolTypeI 'PSMP where protocolTypeI = SPSMP

instance ProtocolTypeI 'PNTF where protocolTypeI = SPNTF

-- | server location and transport key digest (hash).
data ProtocolServer p = ProtocolServer
  { scheme :: SProtocolType p,
    host :: NonEmpty TransportHost,
    port :: ServiceName,
    keyHash :: C.KeyHash
  }
  deriving (Eq, Ord, Show)

instance ProtocolTypeI p => IsString (ProtocolServer p) where
  fromString = parseString strDecode

instance ProtocolTypeI p => Encoding (ProtocolServer p) where
  smpEncode ProtocolServer {host, port, keyHash} =
    smpEncode (host, port, keyHash)
  smpP = do
    (host, port, keyHash) <- smpP
    pure ProtocolServer {scheme = protocolTypeI @p, host, port, keyHash}

instance ProtocolTypeI p => StrEncoding (ProtocolServer p) where
  strEncode ProtocolServer {scheme, host, port, keyHash} =
    strEncodeServer scheme (strEncode host) port keyHash Nothing
  strP =
    serverStrP >>= \case
      (srv, Nothing) -> pure srv
      _ -> fail "ProtocolServer with basic auth not allowed"

instance ProtocolTypeI p => ToJSON (ProtocolServer p) where
  toJSON = strToJSON
  toEncoding = strToJEncoding

newtype BasicAuth = BasicAuth {unBasicAuth :: ByteString}
  deriving (Eq, Show)

instance IsString BasicAuth where fromString = BasicAuth . B.pack

instance Encoding BasicAuth where
  smpEncode (BasicAuth s) = smpEncode s
  smpP = basicAuth <$?> smpP

instance StrEncoding BasicAuth where
  strEncode (BasicAuth s) = s
  strP = basicAuth <$?> A.takeWhile1 (/= '@')

basicAuth :: ByteString -> Either String BasicAuth
basicAuth s
  | B.all valid s = Right $ BasicAuth s
  | otherwise = Left "invalid character in BasicAuth"
  where
    valid c = isPrint c && not (isSpace c) && c /= '@' && c /= ':' && c /= '/'

data ProtoServerWithAuth p = ProtoServerWithAuth (ProtocolServer p) (Maybe BasicAuth)
  deriving (Show)

instance ProtocolTypeI p => IsString (ProtoServerWithAuth p) where
  fromString = parseString strDecode

instance ProtocolTypeI p => StrEncoding (ProtoServerWithAuth p) where
  strEncode (ProtoServerWithAuth ProtocolServer {scheme, host, port, keyHash} auth_) =
    strEncodeServer scheme (strEncode host) port keyHash auth_
  strP = uncurry ProtoServerWithAuth <$> serverStrP

instance ProtocolTypeI p => ToJSON (ProtoServerWithAuth p) where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance ProtocolTypeI p => FromJSON (ProtoServerWithAuth p) where
  parseJSON = strParseJSON "ProtoServerWithAuth"

noAuthSrv :: ProtocolServer p -> ProtoServerWithAuth p
noAuthSrv srv = ProtoServerWithAuth srv Nothing

legacyEncodeServer :: ProtocolServer p -> ByteString
legacyEncodeServer ProtocolServer {host, port, keyHash} =
  smpEncode (L.head host, port, keyHash)

legacyServerP :: forall p. ProtocolTypeI p => Parser (ProtocolServer p)
legacyServerP = do
  (h, port, keyHash) <- smpP
  pure ProtocolServer {scheme = protocolTypeI @p, host = [h], port, keyHash}

legacyStrEncodeServer :: ProtocolTypeI p => ProtocolServer p -> ByteString
legacyStrEncodeServer ProtocolServer {scheme, host, port, keyHash} =
  strEncodeServer scheme (strEncode $ L.head host) port keyHash Nothing

strEncodeServer :: ProtocolTypeI p => SProtocolType p -> ByteString -> ServiceName -> C.KeyHash -> Maybe BasicAuth -> ByteString
strEncodeServer scheme host port keyHash auth_ =
  strEncode scheme <> "://" <> strEncode keyHash <> maybe "" ((":" <>) . strEncode) auth_ <> "@" <> host <> portStr
  where
    portStr = B.pack $ if null port then "" else ':' : port

serverStrP :: ProtocolTypeI p => Parser (ProtocolServer p, Maybe BasicAuth)
serverStrP = do
  scheme <- strP <* "://"
  keyHash <- strP
  auth_ <- optional $ A.char ':' *> strP
  TransportHosts host <- A.char '@' *> strP
  port <- portP <|> pure ""
  pure (ProtocolServer {scheme, host, port, keyHash}, auth_)
  where
    portP = show <$> (A.char ':' *> (A.decimal :: Parser Int))

data SrvLoc = SrvLoc HostName ServiceName
  deriving (Eq, Ord, Show)

instance StrEncoding SrvLoc where
  strEncode (SrvLoc host port) = B.pack $ host <> if null port then "" else ':' : port
  strP = SrvLoc <$> host <*> (port <|> pure "")
    where
      host = B.unpack <$> A.takeWhile1 (A.notInClass ":#,;/ ")
      port = show <$> (A.char ':' *> (A.decimal :: Parser Int))

-- | Transmission correlation ID.
newtype CorrId = CorrId {bs :: ByteString} deriving (Eq, Ord, Show)

instance IsString CorrId where
  fromString = CorrId . fromString

instance StrEncoding CorrId where
  strEncode (CorrId cId) = strEncode cId
  strDecode s = CorrId <$> strDecode s
  strP = CorrId <$> strP

instance ToJSON CorrId where
  toJSON = strToJSON
  toEncoding = strToJEncoding

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

-- | Private key used by push notifications server to authorize (sign) NSUB command.
type NtfPrivateSignKey = C.APrivateSignKey

-- | Public key used by SMP server to verify authorization of NSUB command sent by push notifications server.
type NtfPublicVerifyKey = C.APublicVerifyKey

-- | Public key used for DH exchange to encrypt notification metadata from server to recipient
type RcvNtfPublicDhKey = C.PublicKeyX25519

-- | DH Secret used to encrypt notification metadata from server to recipient
type RcvNtfDhSecret = C.DhSecretX25519

-- | SMP message server ID.
type MsgId = ByteString

-- | SMP message body.
type MsgBody = ByteString

-- | Type for protocol errors.
data ErrorType
  = -- | incorrect block format, encoding or signature size
    BLOCK
  | -- | incorrect SMP session ID (TLS Finished message / tls-unique binding RFC5929)
    SESSION
  | -- | SMP command is unknown or has invalid syntax
    CMD {cmdErr :: CommandError}
  | -- | command authorization error - bad signature or non-existing SMP queue
    AUTH
  | -- | SMP queue capacity is exceeded on the server
    QUOTA
  | -- | ACK command is sent without message to be acknowledged
    NO_MSG
  | -- | sent message is too large (> maxMessageLength = 16088 bytes)
    LARGE_MSG
  | -- | internal server error
    INTERNAL
  | -- | used internally, never returned by the server (to be removed)
    DUPLICATE_ -- not part of SMP protocol, used internally
  deriving (Eq, Generic, Read, Show)

instance ToJSON ErrorType where
  toJSON = J.genericToJSON $ sumTypeJSON id
  toEncoding = J.genericToEncoding $ sumTypeJSON id

instance StrEncoding ErrorType where
  strEncode = \case
    CMD e -> "CMD " <> bshow e
    e -> bshow e
  strP = "CMD " *> (CMD <$> parseRead1) <|> parseRead1

-- | SMP command error type.
data CommandError
  = -- | unknown command
    UNKNOWN
  | -- | error parsing command
    SYNTAX
  | -- | command is not allowed (SUB/GET cannot be used with the same queue in the same TCP connection)
    PROHIBITED
  | -- | transmission has no required credentials (signature or queue ID)
    NO_AUTH
  | -- | transmission has credentials that are not allowed for this command
    HAS_AUTH
  | -- | transmission has no required entity ID (e.g. SMP queue)
    NO_ENTITY
  deriving (Eq, Generic, Read, Show)

instance ToJSON CommandError where
  toJSON = J.genericToJSON $ sumTypeJSON id
  toEncoding = J.genericToEncoding $ sumTypeJSON id

instance Arbitrary ErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandError where arbitrary = genericArbitraryU

-- | SMP transmission parser.
transmissionP :: Parser RawTransmission
transmissionP = do
  signature <- smpP
  signed <- A.takeByteString
  either fail pure $ parseAll (trn signature signed) signed
  where
    trn signature signed = do
      sessId <- smpP
      corrId <- smpP
      entityId <- smpP
      command <- A.takeByteString
      pure RawTransmission {signature, signed, sessId, corrId, entityId, command}

class (ProtocolEncoding msg, ProtocolEncoding (ProtoCommand msg), Show msg) => Protocol msg where
  type ProtoCommand msg = cmd | cmd -> msg
  type ProtoType msg = (sch :: ProtocolType) | sch -> msg
  protocolClientHandshake :: forall c. Transport c => c -> C.KeyHash -> VersionRange -> ExceptT TransportError IO (THandle c)
  protocolPing :: ProtoCommand msg
  protocolError :: msg -> Maybe ErrorType

type ProtoServer msg = ProtocolServer (ProtoType msg)

instance Protocol BrokerMsg where
  type ProtoCommand BrokerMsg = Cmd
  type ProtoType BrokerMsg = 'PSMP
  protocolClientHandshake = smpClientHandshake
  protocolPing = Cmd SSender PING
  protocolError = \case
    ERR e -> Just e
    _ -> Nothing

class ProtocolMsgTag (Tag msg) => ProtocolEncoding msg where
  type Tag msg
  encodeProtocol :: Version -> msg -> ByteString
  protocolP :: Version -> Tag msg -> Parser msg
  checkCredentials :: SignedRawTransmission -> msg -> Either ErrorType msg

instance PartyI p => ProtocolEncoding (Command p) where
  type Tag (Command p) = CommandTag p
  encodeProtocol v = \case
    NEW rKey dhKey auth_ -> case auth_ of
      Just auth
        | v >= 5 -> new <> e ('A', auth)
        | otherwise -> new
      _ -> new
      where
        new = e (NEW_, ' ', rKey, dhKey)
    SUB -> e SUB_
    KEY k -> e (KEY_, ' ', k)
    NKEY k dhKey -> e (NKEY_, ' ', k, dhKey)
    NDEL -> e NDEL_
    GET -> e GET_
    ACK msgId
      | v == 1 -> e ACK_
      | otherwise -> e (ACK_, ' ', msgId)
    OFF -> e OFF_
    DEL -> e DEL_
    SEND flags msg
      | v == 1 -> e (SEND_, ' ', Tail msg)
      | otherwise -> e (SEND_, ' ', flags, ' ', Tail msg)
    PING -> e PING_
    NSUB -> e NSUB_
    where
      e :: Encoding a => a -> ByteString
      e = smpEncode

  protocolP v tag = (\(Cmd _ c) -> checkParty c) <$?> protocolP v (CT (sParty @p) tag)

  checkCredentials (sig, _, queueId, _) cmd = case cmd of
    -- NEW must have signature but NOT queue ID
    NEW {}
      | isNothing sig -> Left $ CMD NO_AUTH
      | not (B.null queueId) -> Left $ CMD HAS_AUTH
      | otherwise -> Right cmd
    -- SEND must have queue ID, signature is not always required
    SEND {}
      | B.null queueId -> Left $ CMD NO_ENTITY
      | otherwise -> Right cmd
    -- PING must not have queue ID or signature
    PING
      | isNothing sig && B.null queueId -> Right cmd
      | otherwise -> Left $ CMD HAS_AUTH
    -- other client commands must have both signature and queue ID
    _
      | isNothing sig || B.null queueId -> Left $ CMD NO_AUTH
      | otherwise -> Right cmd

instance ProtocolEncoding Cmd where
  type Tag Cmd = CmdTag
  encodeProtocol v (Cmd _ c) = encodeProtocol v c

  protocolP v = \case
    CT SRecipient tag ->
      Cmd SRecipient <$> case tag of
        NEW_
          | v >= 5 -> new <*> optional (A.char 'A' *> smpP)
          | otherwise -> new <*> pure Nothing
          where
            new = NEW <$> _smpP <*> smpP
        SUB_ -> pure SUB
        KEY_ -> KEY <$> _smpP
        NKEY_ -> NKEY <$> _smpP <*> smpP
        NDEL_ -> pure NDEL
        GET_ -> pure GET
        ACK_
          | v == 1 -> pure $ ACK ""
          | otherwise -> ACK <$> _smpP
        OFF_ -> pure OFF
        DEL_ -> pure DEL
    CT SSender tag ->
      Cmd SSender <$> case tag of
        SEND_
          | v == 1 -> SEND noMsgFlags <$> (unTail <$> _smpP)
          | otherwise -> SEND <$> _smpP <*> (unTail <$> _smpP)
        PING_ -> pure PING
    CT SNotifier NSUB_ -> pure $ Cmd SNotifier NSUB

  checkCredentials t (Cmd p c) = Cmd p <$> checkCredentials t c

instance ProtocolEncoding BrokerMsg where
  type Tag BrokerMsg = BrokerMsgTag
  encodeProtocol v = \case
    IDS (QIK rcvId sndId srvDh) -> e (IDS_, ' ', rcvId, sndId, srvDh)
    MSG RcvMessage {msgId, msgTs, msgFlags, msgBody = EncRcvMsgBody body}
      | v == 1 -> e (MSG_, ' ', msgId, msgTs, Tail body)
      | v == 2 -> e (MSG_, ' ', msgId, msgTs, msgFlags, ' ', Tail body)
      | otherwise -> e (MSG_, ' ', msgId, Tail body)
    NID nId srvNtfDh -> e (NID_, ' ', nId, srvNtfDh)
    NMSG nmsgNonce encNMsgMeta -> e (NMSG_, ' ', nmsgNonce, encNMsgMeta)
    END -> e END_
    OK -> e OK_
    ERR err -> e (ERR_, ' ', err)
    PONG -> e PONG_
    where
      e :: Encoding a => a -> ByteString
      e = smpEncode

  protocolP v = \case
    MSG_ -> do
      msgId <- _smpP
      MSG <$> case v of
        1 -> RcvMessage msgId <$> smpP <*> pure noMsgFlags <*> bodyP
        2 -> RcvMessage msgId <$> smpP <*> smpP <*> (A.space *> bodyP)
        _ -> RcvMessage msgId (MkSystemTime 0 0) noMsgFlags <$> bodyP
      where
        bodyP = EncRcvMsgBody . unTail <$> smpP
    IDS_ -> IDS <$> (QIK <$> _smpP <*> smpP <*> smpP)
    NID_ -> NID <$> _smpP <*> smpP
    NMSG_ -> NMSG <$> _smpP <*> smpP
    END_ -> pure END
    OK_ -> pure OK
    ERR_ -> ERR <$> _smpP
    PONG_ -> pure PONG

  checkCredentials (_, _, queueId, _) cmd = case cmd of
    -- IDS response should not have queue ID
    IDS _ -> Right cmd
    -- ERR response does not always have queue ID
    ERR _ -> Right cmd
    -- PONG response must not have queue ID
    PONG
      | B.null queueId -> Right cmd
      | otherwise -> Left $ CMD HAS_AUTH
    -- other broker responses must have queue ID
    _
      | B.null queueId -> Left $ CMD NO_ENTITY
      | otherwise -> Right cmd

_smpP :: Encoding a => Parser a
_smpP = A.space *> smpP

-- | Parse SMP protocol commands and broker messages
parseProtocol :: ProtocolEncoding msg => Version -> ByteString -> Either ErrorType msg
parseProtocol v s =
  let (tag, params) = B.break (== ' ') s
   in case decodeTag tag of
        Just cmd -> parse (protocolP v cmd) (CMD SYNTAX) params
        Nothing -> Left $ CMD UNKNOWN

checkParty :: forall t p p'. (PartyI p, PartyI p') => t p' -> Either String (t p)
checkParty c = case testEquality (sParty @p) (sParty @p') of
  Just Refl -> Right c
  Nothing -> Left "bad command party"

checkParty' :: forall t p p'. (PartyI p, PartyI p') => t p' -> Maybe (t p)
checkParty' c = case testEquality (sParty @p) (sParty @p') of
  Just Refl -> Just c
  _ -> Nothing

instance Encoding ErrorType where
  smpEncode = \case
    BLOCK -> "BLOCK"
    SESSION -> "SESSION"
    CMD err -> "CMD " <> smpEncode err
    AUTH -> "AUTH"
    QUOTA -> "QUOTA"
    NO_MSG -> "NO_MSG"
    LARGE_MSG -> "LARGE_MSG"
    INTERNAL -> "INTERNAL"
    DUPLICATE_ -> "DUPLICATE_"

  smpP =
    A.takeTill (== ' ') >>= \case
      "BLOCK" -> pure BLOCK
      "SESSION" -> pure SESSION
      "CMD" -> CMD <$> _smpP
      "AUTH" -> pure AUTH
      "QUOTA" -> pure QUOTA
      "NO_MSG" -> pure NO_MSG
      "LARGE_MSG" -> pure LARGE_MSG
      "INTERNAL" -> pure INTERNAL
      "DUPLICATE_" -> pure DUPLICATE_
      _ -> fail "bad error type"

instance Encoding CommandError where
  smpEncode e = case e of
    UNKNOWN -> "UNKNOWN"
    SYNTAX -> "SYNTAX"
    PROHIBITED -> "PROHIBITED"
    NO_AUTH -> "NO_AUTH"
    HAS_AUTH -> "HAS_AUTH"
    NO_ENTITY -> "NO_ENTITY"
  smpP =
    A.takeTill (== ' ') >>= \case
      "UNKNOWN" -> pure UNKNOWN
      "SYNTAX" -> pure SYNTAX
      "PROHIBITED" -> pure PROHIBITED
      "NO_AUTH" -> pure NO_AUTH
      "HAS_AUTH" -> pure HAS_AUTH
      "NO_ENTITY" -> pure NO_ENTITY
      "NO_QUEUE" -> pure NO_ENTITY
      _ -> fail "bad command error type"

-- | Send signed SMP transmission to TCP transport.
tPut :: Transport c => THandle c -> NonEmpty SentRawTransmission -> IO [Either TransportError ()]
tPut th trs
  | batch th = tPutBatch [] $ L.map tEncode trs
  | otherwise = forM (L.toList trs) $ tPutLog . tEncode
  where
    tPutBatch :: [Either TransportError ()] -> NonEmpty ByteString -> IO [Either TransportError ()]
    tPutBatch rs ts = do
      let (n, s, ts_) = encodeBatch 0 "" ts
      r <- if n == 0 then largeMsg else replicate n <$> tPutLog (lenEncode n `B.cons` s)
      let rs' = rs <> r
      case ts_ of
        Just ts' -> tPutBatch rs' ts'
        _ -> pure rs'
    largeMsg = putStrLn "tPut error: large message" >> pure [Left TELargeMsg]
    tPutLog s = do
      r <- tPutBlock th s
      case r of
        Left e -> putStrLn ("tPut error: " <> show e)
        _ -> pure ()
      pure r
    encodeBatch :: Int -> ByteString -> NonEmpty ByteString -> (Int, ByteString, Maybe (NonEmpty ByteString))
    encodeBatch n s ts@(t :| ts_)
      | n == 255 = (n, s, Just ts)
      | otherwise =
        let s' = s <> smpEncode (Large t)
            n' = n + 1
         in if B.length s' > blockSize th - 1 -- one byte is reserved for the number of messages in the batch
              then (n,s,) $ if n == 0 then L.nonEmpty ts_ else Just ts
              else case L.nonEmpty ts_ of
                Just ts' -> encodeBatch n' s' ts'
                _ -> (n', s', Nothing)
    tEncode (sig, tr) = smpEncode (C.signatureBytes sig) <> tr

encodeTransmission :: ProtocolEncoding c => Version -> ByteString -> Transmission c -> ByteString
encodeTransmission v sessionId (CorrId corrId, queueId, command) =
  smpEncode (sessionId, corrId, queueId) <> encodeProtocol v command

-- | Receive and parse transmission from the TCP transport (ignoring any trailing padding).
tGetParse :: Transport c => THandle c -> IO (NonEmpty (Either TransportError RawTransmission))
tGetParse th
  | batch th = either ((:| []) . Left) id <$> runExceptT getBatch
  | otherwise = (:| []) . (parse transmissionP TEBadBlock =<<) <$> tGetBlock th
  where
    getBatch :: ExceptT TransportError IO (NonEmpty (Either TransportError RawTransmission))
    getBatch = do
      s <- ExceptT $ tGetBlock th
      ts <- liftEither $ parse smpP TEBadBlock s
      pure $ L.map (\(Large t) -> parse transmissionP TEBadBlock t) ts

-- | Receive client and server transmissions (determined by `cmd` type).
tGet :: forall cmd c m. (ProtocolEncoding cmd, Transport c, MonadIO m) => THandle c -> m (NonEmpty (SignedTransmission cmd))
tGet th@THandle {sessionId, thVersion = v} = liftIO (tGetParse th) >>= mapM decodeParseValidate
  where
    decodeParseValidate :: Either TransportError RawTransmission -> m (SignedTransmission cmd)
    decodeParseValidate = \case
      Right RawTransmission {signature, signed, sessId, corrId, entityId, command}
        | sessId == sessionId ->
          let decodedTransmission = (,corrId,entityId,command) <$> C.decodeSignature signature
           in either (const $ tError corrId) (tParseValidate signed) decodedTransmission
        | otherwise -> pure (Nothing, "", (CorrId corrId, "", Left SESSION))
      Left _ -> tError ""

    tError :: ByteString -> m (SignedTransmission cmd)
    tError corrId = pure (Nothing, "", (CorrId corrId, "", Left BLOCK))

    tParseValidate :: ByteString -> SignedRawTransmission -> m (SignedTransmission cmd)
    tParseValidate signed t@(sig, corrId, entityId, command) = do
      let cmd = parseProtocol v command >>= checkCredentials t
      pure (sig, signed, (CorrId corrId, entityId, cmd))
