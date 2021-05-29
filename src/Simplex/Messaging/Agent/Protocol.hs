{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

-- |
-- Module      : Simplex.Messaging.Agent.Protocol
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- Types, parsers, serializers and functions to send and receive SMP agent protocol commands and responses.
--
-- See https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md
module Simplex.Messaging.Agent.Protocol
  ( -- * SMP agent protocol types
    Entity (..),
    EntityTag (..),
    AnEntity (..),
    EntityCommand,
    entityCommand,
    ACommand (..),
    AParty (..),
    APartyCmd (..),
    SAParty (..),
    SMPMessage (..),
    AMessage (..),
    SMPServer (..),
    SMPQueueInfo (..),
    AgentErrorType (..),
    CommandErrorType (..),
    ConnectionErrorType (..),
    BrokerErrorType (..),
    SMPAgentError (..),
    ATransmission (..),
    ATransmissionOrError (..),
    ARawTransmission,
    ConnAlias,
    ReplyMode (..),
    AckMode (..),
    OnOff (..),
    MsgIntegrity (..),
    MsgErrorType (..),
    QueueStatus (..),
    SignatureKey,
    VerificationKey,
    EncryptionKey,
    DecryptionKey,
    ACorrId,
    AgentMsgId,

    -- * Parse and serialize
    serializeCommand,
    serializeEntity,
    serializeSMPMessage,
    serializeMsgIntegrity,
    serializeServer,
    serializeSmpQueueInfo,
    serializeAgentError,
    commandP,
    entityP,
    parseSMPMessage,
    smpServerP,
    smpQueueInfoP,
    msgIntegrityP,
    agentErrorTypeP,

    -- * TCP transport functions
    tPut,
    tGet,
    tPutRaw,
    tGetRaw,
  )
where

import Control.Applicative (optional, (<|>))
import Control.Monad.IO.Class
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Constraint (Dict (..))
import Data.Functor (($>))
import Data.Int (Int64)
import Data.Kind (Constraint, Type)
import Data.Maybe (isJust)
import Data.String (IsString (..))
import Data.Time.Clock (UTCTime)
import Data.Time.ISO8601
import Data.Type.Equality
import Data.Typeable ()
import GHC.Generics (Generic)
import Generic.Random (genericArbitraryU)
import Network.Socket (HostName, ServiceName)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Parsers
import Simplex.Messaging.Protocol
  ( ErrorType,
    MsgBody,
    MsgId,
    SenderPublicKey,
  )
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Transport (Transport (..), TransportError, serializeTransportError, transportErrorP)
import Simplex.Messaging.Util
import Test.QuickCheck (Arbitrary (..))
import Text.Read
import UnliftIO.Exception

-- | Raw (unparsed) SMP agent protocol transmission.
type ARawTransmission = (ByteString, ByteString, ByteString)

-- | Parsed SMP agent protocol transmission.
data ATransmission p = forall t c. EntityCommand t c => ATransmission ACorrId (Entity t) (ACommand p c)

-- | SMP agent protocol transmission or transmission error.
data ATransmissionOrError p = forall t c. EntityCommand t c => ATransmissionOrError ACorrId (Entity t) (Either AgentErrorType (ACommand p c))

deriving instance Show (ATransmissionOrError p)

type ACorrId = ByteString

-- | SMP agent protocol participants.
data AParty = Agent | Client
  deriving (Eq, Show)

-- | Singleton types for SMP agent protocol participants.
data SAParty :: AParty -> Type where
  SAgent :: SAParty Agent
  SClient :: SAParty Client

deriving instance Show (SAParty p)

deriving instance Eq (SAParty p)

instance TestEquality SAParty where
  testEquality SAgent SAgent = Just Refl
  testEquality SClient SClient = Just Refl
  testEquality _ _ = Nothing

-- | SMP agent protocol entity types
data EntityTag = Conn_ | OpenConn_ | Broadcast_ | AGroup_

data Entity :: EntityTag -> Type where
  Conn :: ByteString -> Entity Conn_
  OpenConn :: ByteString -> Entity OpenConn_
  BroadCast :: ByteString -> Entity Broadcast_
  AGroup :: ByteString -> Entity AGroup_

deriving instance Show (Entity t)

entityId :: Entity t -> ByteString
entityId = \case
  Conn bs -> bs
  OpenConn bs -> bs
  BroadCast bs -> bs
  AGroup bs -> bs

data AnEntity = forall t. AE (Entity t)

data ACmd = forall (p :: AParty) (c :: ACmdTag). ACmd (SAParty p) (ACommand p c)

deriving instance Show ACmd

data APartyCmd (p :: AParty) = forall c. APartyCmd (ACommand p c)

instance Eq (APartyCmd p) where
  APartyCmd c1 == APartyCmd c2 = isJust $ testEquality c1 c2

deriving instance Show (APartyCmd p)

type family EntityCommand (t :: EntityTag) (c :: ACmdTag) :: Constraint where
  EntityCommand Conn_ NEW_ = ()
  EntityCommand Conn_ INV_ = ()
  EntityCommand Conn_ JOIN_ = ()
  EntityCommand Conn_ CON_ = ()
  EntityCommand Conn_ SUB_ = ()
  EntityCommand Conn_ SUBALL_ = ()
  EntityCommand Conn_ END_ = ()
  EntityCommand Conn_ SEND_ = ()
  EntityCommand Conn_ SENT_ = ()
  EntityCommand Conn_ MSG_ = ()
  EntityCommand Conn_ OFF_ = ()
  EntityCommand Conn_ DEL_ = ()
  EntityCommand Conn_ OK_ = ()
  EntityCommand Conn_ ERR_ = ()
  EntityCommand _ ERR_ = ()

entityCommand :: Entity t -> ACommand p c -> Maybe (Dict (EntityCommand t c))
entityCommand = \case
  Conn _ -> \case
    NEW -> Just Dict
    INV _ -> Just Dict
    JOIN {} -> Just Dict
    CON -> Just Dict
    SUB -> Just Dict
    SUBALL -> Just Dict
    END -> Just Dict
    SEND _ -> Just Dict
    SENT _ -> Just Dict
    MSG {} -> Just Dict
    OFF -> Just Dict
    DEL -> Just Dict
    OK -> Just Dict
    ERR _ -> Just Dict
  _ -> \case
    ERR _ -> Just Dict
    _ -> Nothing

data ACmdTag
  = NEW_
  | INV_
  | JOIN_
  | CON_
  | SUB_
  | SUBALL_
  | END_
  | SEND_
  | SENT_
  | MSG_
  | OFF_
  | DEL_
  | OK_
  | ERR_

-- | Parameterized type for SMP agent protocol commands and responses from all participants.
data ACommand (p :: AParty) (c :: ACmdTag) where
  NEW :: ACommand Client NEW_ -- response INV
  INV :: SMPQueueInfo -> ACommand Agent INV_
  JOIN :: SMPQueueInfo -> ReplyMode -> ACommand Client JOIN_ -- response OK
  CON :: ACommand Agent CON_ -- notification that connection is established
  -- TODO currently it automatically allows whoever sends the confirmation
  -- CONF :: OtherPartyId -> ACommand Agent
  -- LET :: OtherPartyId -> ACommand Client
  SUB :: ACommand Client SUB_
  SUBALL :: ACommand Client SUBALL_ -- TODO should be moved to chat protocol - hack for subscribing to all
  END :: ACommand Agent END_
  -- QST :: QueueDirection -> ACommand Client
  -- STAT :: QueueDirection -> Maybe QueueStatus -> Maybe SubMode -> ACommand Agent
  SEND :: MsgBody -> ACommand Client SEND_
  SENT :: AgentMsgId -> ACommand Agent SENT_
  MSG ::
    { recipientMeta :: (AgentMsgId, UTCTime),
      brokerMeta :: (MsgId, UTCTime),
      senderMeta :: (AgentMsgId, UTCTime),
      msgIntegrity :: MsgIntegrity,
      msgBody :: MsgBody
    } ->
    ACommand Agent MSG_
  -- ACK :: AgentMsgId -> ACommand Client
  -- RCVD :: AgentMsgId -> ACommand Agent
  OFF :: ACommand Client MSG_
  DEL :: ACommand Client DEL_
  OK :: ACommand Agent OK_
  ERR :: AgentErrorType -> ACommand Agent ERR_

deriving instance Eq (ACommand p c)

deriving instance Show (ACommand p c)

instance TestEquality (ACommand p) where
  testEquality NEW NEW = Just Refl
  testEquality c@INV {} c'@INV {} = refl c c'
  testEquality c@JOIN {} c'@JOIN {} = refl c c'
  testEquality CON CON = Just Refl
  testEquality SUB SUB = Just Refl
  testEquality SUBALL SUBALL = Just Refl
  testEquality END END = Just Refl
  testEquality c@SEND {} c'@SEND {} = refl c c'
  testEquality c@SENT {} c'@SENT {} = refl c c'
  testEquality c@MSG {} c'@MSG {} = refl c c'
  testEquality OFF OFF = Just Refl
  testEquality DEL DEL = Just Refl
  testEquality OK OK = Just Refl
  testEquality c@ERR {} c'@ERR {} = refl c c'
  testEquality _ _ = Nothing

refl :: Eq (f a) => f a -> f a -> Maybe (a :~: a)
refl x x' = if x == x' then Just Refl else Nothing

-- | SMP message formats.
data SMPMessage
  = -- | SMP confirmation
    -- (see <https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#send-message SMP protocol>)
    SMPConfirmation SenderPublicKey
  | -- | Agent message header and envelope for client messages
    -- (see <https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md#messages-between-smp-agents SMP agent protocol>)
    SMPMessage
      { -- | sequential ID assigned by the sending agent
        senderMsgId :: AgentMsgId,
        -- | timestamp from the sending agent
        senderTimestamp :: SenderTimestamp,
        -- | digest of the previous message
        previousMsgHash :: ByteString,
        -- | messages sent between agents once queue is secured
        agentMessage :: AMessage
      }
  deriving (Show)

-- | Messages sent between SMP agents once SMP queue is secured.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md#messages-between-smp-agents
data AMessage where
  -- | the first message in the queue to validate it is secured
  HELLO :: VerificationKey -> AckMode -> AMessage
  -- | reply queue information
  REPLY :: SMPQueueInfo -> AMessage
  -- | agent envelope for the client message
  A_MSG :: MsgBody -> AMessage
  deriving (Show)

-- | Parse SMP message.
parseSMPMessage :: ByteString -> Either AgentErrorType SMPMessage
parseSMPMessage = parse (smpMessageP <* A.endOfLine) $ AGENT A_MESSAGE
  where
    smpMessageP :: Parser SMPMessage
    smpMessageP =
      smpConfirmationP <* A.endOfLine
        <|> A.endOfLine *> smpClientMessageP

    smpConfirmationP :: Parser SMPMessage
    smpConfirmationP = SMPConfirmation <$> ("KEY " *> C.pubKeyP <* A.endOfLine)

    smpClientMessageP :: Parser SMPMessage
    smpClientMessageP =
      SMPMessage
        <$> A.decimal <* A.space
        <*> tsISO8601P <* A.space
        -- TODO previous message hash should become mandatory when we support HELLO and REPLY
        -- (for HELLO it would be the hash of SMPConfirmation)
        <*> (base64P <|> pure "") <* A.endOfLine
        <*> agentMessageP

-- | Serialize SMP message.
serializeSMPMessage :: SMPMessage -> ByteString
serializeSMPMessage = \case
  SMPConfirmation sKey -> smpMessage ("KEY " <> C.serializePubKey sKey) "" ""
  SMPMessage {senderMsgId, senderTimestamp, previousMsgHash, agentMessage} ->
    let header = messageHeader senderMsgId senderTimestamp previousMsgHash
        body = serializeAgentMessage agentMessage
     in smpMessage "" header body
  where
    messageHeader msgId ts prevMsgHash =
      B.unwords [bshow msgId, B.pack $ formatISO8601Millis ts, encode prevMsgHash]
    smpMessage smpHeader aHeader aBody = B.intercalate "\n" [smpHeader, aHeader, aBody, ""]

agentMessageP :: Parser AMessage
agentMessageP =
  "HELLO " *> hello
    <|> "REPLY " *> reply
    <|> "MSG " *> a_msg
  where
    hello = HELLO <$> C.pubKeyP <*> ackMode
    reply = REPLY <$> smpQueueInfoP
    a_msg = do
      size :: Int <- A.decimal <* A.endOfLine
      A_MSG <$> A.take size <* A.endOfLine
    ackMode = AckMode <$> (" NO_ACK" $> Off <|> pure On)

-- | SMP queue information parser.
smpQueueInfoP :: Parser SMPQueueInfo
smpQueueInfoP =
  "smp::" *> (SMPQueueInfo <$> smpServerP <* "::" <*> base64P <* "::" <*> C.pubKeyP)

-- | SMP server location parser.
smpServerP :: Parser SMPServer
smpServerP = SMPServer <$> server <*> optional port <*> optional kHash
  where
    server = B.unpack <$> A.takeWhile1 (A.notInClass ":# ")
    port = A.char ':' *> (B.unpack <$> A.takeWhile1 A.isDigit)
    kHash = C.KeyHash <$> (A.char '#' *> base64P)

serializeAgentMessage :: AMessage -> ByteString
serializeAgentMessage = \case
  HELLO verifyKey ackMode -> "HELLO " <> C.serializePubKey verifyKey <> if ackMode == AckMode Off then " NO_ACK" else ""
  REPLY qInfo -> "REPLY " <> serializeSmpQueueInfo qInfo
  A_MSG body -> "MSG " <> serializeMsg body <> "\n"

-- | Serialize SMP queue information that is sent out-of-band.
serializeSmpQueueInfo :: SMPQueueInfo -> ByteString
serializeSmpQueueInfo (SMPQueueInfo srv qId ek) =
  B.intercalate "::" ["smp", serializeServer srv, encode qId, C.serializePubKey ek]

-- | Serialize SMP server location.
serializeServer :: SMPServer -> ByteString
serializeServer SMPServer {host, port, keyHash} =
  B.pack $ host <> maybe "" (':' :) port <> maybe "" (('#' :) . B.unpack . encode . C.unKeyHash) keyHash

-- | SMP server location and transport key digest (hash).
data SMPServer = SMPServer
  { host :: HostName,
    port :: Maybe ServiceName,
    keyHash :: Maybe C.KeyHash
  }
  deriving (Eq, Ord, Show)

instance IsString SMPServer where
  fromString = parseString . parseAll $ smpServerP

-- | SMP agent connection alias.
type ConnAlias = ByteString

-- | Connection modes.
data OnOff = On | Off deriving (Eq, Show, Read)

-- | Message acknowledgement mode of the connection.
newtype AckMode = AckMode OnOff deriving (Eq, Show)

-- | SMP queue information sent out-of-band.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#out-of-band-messages
data SMPQueueInfo = SMPQueueInfo SMPServer SMP.SenderId EncryptionKey
  deriving (Eq, Show)

-- | Connection reply mode (used in JOIN command).
newtype ReplyMode = ReplyMode OnOff deriving (Eq, Show)

-- | Public key used to E2E encrypt SMP messages.
type EncryptionKey = C.PublicKey

-- | Private key used to E2E decrypt SMP messages.
type DecryptionKey = C.SafePrivateKey

-- | Private key used to sign SMP commands
type SignatureKey = C.SafePrivateKey

-- | Public key used by SMP server to authorize (verify) SMP commands.
type VerificationKey = C.PublicKey

data QueueDirection = SND | RCV deriving (Show)

-- | SMP queue status.
data QueueStatus
  = -- | queue is created
    New
  | -- | queue is confirmed by the sender
    Confirmed
  | -- | queue is secured with sender key (only used by the queue recipient)
    Secured
  | -- | queue is active
    Active
  | -- | queue is disabled (only used by the queue recipient)
    Disabled
  deriving (Eq, Show, Read)

type AgentMsgId = Int64

type SenderTimestamp = UTCTime

-- | Result of received message integrity validation.
data MsgIntegrity = MsgOk | MsgError MsgErrorType
  deriving (Eq, Show)

-- | Error of message integrity validation.
data MsgErrorType = MsgSkipped AgentMsgId AgentMsgId | MsgBadId AgentMsgId | MsgBadHash | MsgDuplicate
  deriving (Eq, Show)

-- | Error type used in errors sent to agent clients.
data AgentErrorType
  = -- | command or response error
    CMD CommandErrorType
  | -- | connection errors
    CONN ConnectionErrorType
  | -- | SMP protocol errors forwarded to agent clients
    SMP ErrorType
  | -- | SMP server errors
    BROKER BrokerErrorType
  | -- | errors of other agents
    AGENT SMPAgentError
  | -- | agent implementation or dependency errors
    INTERNAL String
  deriving (Eq, Generic, Read, Show, Exception)

-- | SMP agent protocol command or response error.
data CommandErrorType
  = -- | command is prohibited in this context
    PROHIBITED
  | -- | command is not supported by this entity
    ENTITY
  | -- | command syntax is invalid
    SYNTAX
  | -- | cannot parse entity
    BAD_ENTITY
  | -- | entity ID is required with this command
    NO_ENTITY
  | -- | message size is not correct (no terminating space)
    SIZE
  | -- | message does not fit in SMP block
    LARGE
  deriving (Eq, Generic, Read, Show, Exception)

-- | Connection error.
data ConnectionErrorType
  = -- | connection alias is not in the database
    UNKNOWN
  | -- | connection alias already exists
    DUPLICATE
  | -- | connection is simplex, but operation requires another queue
    SIMPLEX
  deriving (Eq, Generic, Read, Show, Exception)

-- | SMP server errors.
data BrokerErrorType
  = -- | invalid server response (failed to parse)
    RESPONSE ErrorType
  | -- | unexpected response
    UNEXPECTED
  | -- | network error
    NETWORK
  | -- | handshake or other transport error
    TRANSPORT TransportError
  | -- | command response timeout
    TIMEOUT
  deriving (Eq, Generic, Read, Show, Exception)

-- | Errors of another SMP agent.
data SMPAgentError
  = -- | possibly should include bytestring that failed to parse
    A_MESSAGE
  | -- | possibly should include the prohibited SMP/agent message
    A_PROHIBITED
  | -- | cannot RSA/AES-decrypt or parse decrypted header
    A_ENCRYPTION
  | -- | invalid RSA signature
    A_SIGNATURE
  deriving (Eq, Generic, Read, Show, Exception)

instance Arbitrary AgentErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandErrorType where arbitrary = genericArbitraryU

instance Arbitrary ConnectionErrorType where arbitrary = genericArbitraryU

instance Arbitrary BrokerErrorType where arbitrary = genericArbitraryU

instance Arbitrary SMPAgentError where arbitrary = genericArbitraryU

entityP :: Parser AnEntity
entityP =
  ($)
    <$> ( "C:" $> AE . Conn
            <|> "O:" $> AE . OpenConn
            <|> "B:" $> AE . BroadCast
            <|> "G:" $> AE . AGroup
        )
    <*> A.takeTill (== ' ')

serializeEntity :: Entity t -> ByteString
serializeEntity = \case
  Conn s -> "C:" <> s
  OpenConn s -> "O:" <> s
  BroadCast s -> "B:" <> s
  AGroup s -> "G:" <> s

-- | SMP agent command and response parser
commandP :: Parser ACmd
commandP =
  "NEW" $> ACmd SClient NEW
    <|> "INV " *> invResp
    <|> "JOIN " *> joinCmd
    <|> "SUB" $> ACmd SClient SUB
    <|> "SUBALL" $> ACmd SClient SUBALL -- TODO remove - hack for subscribing to all
    <|> "END" $> ACmd SAgent END
    <|> "SEND " *> sendCmd
    <|> "SENT " *> sentResp
    <|> "MSG " *> message
    <|> "OFF" $> ACmd SClient OFF
    <|> "DEL" $> ACmd SClient DEL
    <|> "ERR " *> agentError
    <|> "CON" $> ACmd SAgent CON
    <|> "OK" $> ACmd SAgent OK
  where
    invResp = ACmd SAgent . INV <$> smpQueueInfoP
    joinCmd = ACmd SClient <$> (JOIN <$> smpQueueInfoP <*> replyMode)
    sendCmd = ACmd SClient . SEND <$> A.takeByteString
    sentResp = ACmd SAgent . SENT <$> A.decimal
    message = do
      msgIntegrity <- msgIntegrityP <* A.space
      recipientMeta <- "R=" *> partyMeta A.decimal
      brokerMeta <- "B=" *> partyMeta base64P
      senderMeta <- "S=" *> partyMeta A.decimal
      msgBody <- A.takeByteString
      return $ ACmd SAgent MSG {recipientMeta, brokerMeta, senderMeta, msgIntegrity, msgBody}
    replyMode = ReplyMode <$> (" NO_REPLY" $> Off <|> pure On)
    partyMeta idParser = (,) <$> idParser <* "," <*> tsISO8601P <* A.space
    agentError = ACmd SAgent . ERR <$> agentErrorTypeP

-- | Message integrity validation result parser.
msgIntegrityP :: Parser MsgIntegrity
msgIntegrityP = "OK" $> MsgOk <|> "ERR " *> (MsgError <$> msgErrorType)
  where
    msgErrorType =
      "ID " *> (MsgBadId <$> A.decimal)
        <|> "IDS " *> (MsgSkipped <$> A.decimal <* A.space <*> A.decimal)
        <|> "HASH" $> MsgBadHash
        <|> "DUPLICATE" $> MsgDuplicate

parseCommand :: ByteString -> Either AgentErrorType ACmd
parseCommand = parse commandP $ CMD SYNTAX

-- | Serialize SMP agent command.
serializeCommand :: ACommand p c -> ByteString
serializeCommand = \case
  NEW -> "NEW"
  INV qInfo -> "INV " <> serializeSmpQueueInfo qInfo
  JOIN qInfo rMode -> "JOIN " <> serializeSmpQueueInfo qInfo <> replyMode rMode
  SUB -> "SUB"
  SUBALL -> "SUBALL" -- TODO remove - hack for subscribing to all
  END -> "END"
  SEND msgBody -> "SEND " <> serializeMsg msgBody
  SENT mId -> "SENT " <> bshow mId
  MSG {recipientMeta = (rmId, rTs), brokerMeta = (bmId, bTs), senderMeta = (smId, sTs), msgIntegrity, msgBody} ->
    B.unwords
      [ "MSG",
        serializeMsgIntegrity msgIntegrity,
        "R=" <> bshow rmId <> "," <> showTs rTs,
        "B=" <> encode bmId <> "," <> showTs bTs,
        "S=" <> bshow smId <> "," <> showTs sTs,
        serializeMsg msgBody
      ]
  OFF -> "OFF"
  DEL -> "DEL"
  CON -> "CON"
  ERR e -> "ERR " <> serializeAgentError e
  OK -> "OK"
  where
    replyMode :: ReplyMode -> ByteString
    replyMode = \case
      ReplyMode Off -> " NO_REPLY"
      ReplyMode On -> ""
    showTs :: UTCTime -> ByteString
    showTs = B.pack . formatISO8601Millis

-- | Serialize message integrity validation result.
serializeMsgIntegrity :: MsgIntegrity -> ByteString
serializeMsgIntegrity = \case
  MsgOk -> "OK"
  MsgError e ->
    "ERR " <> case e of
      MsgSkipped fromMsgId toMsgId ->
        B.unwords ["NO_ID", bshow fromMsgId, bshow toMsgId]
      MsgBadId aMsgId -> "ID " <> bshow aMsgId
      MsgBadHash -> "HASH"
      MsgDuplicate -> "DUPLICATE"

-- | SMP agent protocol error parser.
agentErrorTypeP :: Parser AgentErrorType
agentErrorTypeP =
  "SMP " *> (SMP <$> SMP.errorTypeP)
    <|> "BROKER RESPONSE " *> (BROKER . RESPONSE <$> SMP.errorTypeP)
    <|> "BROKER TRANSPORT " *> (BROKER . TRANSPORT <$> transportErrorP)
    <|> "INTERNAL " *> (INTERNAL <$> parseRead A.takeByteString)
    <|> parseRead2

-- | Serialize SMP agent protocol error.
serializeAgentError :: AgentErrorType -> ByteString
serializeAgentError = \case
  SMP e -> "SMP " <> SMP.serializeErrorType e
  BROKER (RESPONSE e) -> "BROKER RESPONSE " <> SMP.serializeErrorType e
  BROKER (TRANSPORT e) -> "BROKER TRANSPORT " <> serializeTransportError e
  e -> bshow e

serializeMsg :: ByteString -> ByteString
serializeMsg body = bshow (B.length body) <> "\n" <> body

-- | Send raw (unparsed) SMP agent protocol transmission to TCP connection.
tPutRaw :: Transport c => c -> ARawTransmission -> IO ()
tPutRaw h (corrId, entity, command) = do
  putLn h corrId
  putLn h entity
  putLn h command

-- | Receive raw (unparsed) SMP agent protocol transmission from TCP connection.
tGetRaw :: Transport c => c -> IO ARawTransmission
tGetRaw h = (,,) <$> getLn h <*> getLn h <*> getLn h

-- | Send SMP agent protocol command (or response) to TCP connection.
tPut :: (Transport c, MonadIO m) => c -> ATransmission p -> m ()
tPut h (ATransmission corrId ent cmd) =
  liftIO $ tPutRaw h (corrId, serializeEntity ent, serializeCommand cmd)

-- | Receive client and agent transmissions from TCP connection.
tGet :: forall c m p. (Transport c, MonadIO m) => SAParty p -> c -> m (ATransmissionOrError p)
tGet party h = liftIO (tGetRaw h) >>= tParseLoadBody
  where
    tParseLoadBody :: ARawTransmission -> m (ATransmissionOrError p)
    tParseLoadBody (corrId, entityStr, command) =
      case parseAll entityP entityStr of
        Left _ -> pure $ ATransmissionOrError @_ @_ @ERR_ corrId (Conn "") $ Left $ CMD BAD_ENTITY
        Right entity -> do
          let cmd = parseCommand command >>= fromParty >>= hasEntityId entity
          makeTransmission corrId entity <$> either (pure . Left) cmdWithMsgBody cmd

    fromParty :: ACmd -> Either AgentErrorType (APartyCmd p)
    fromParty (ACmd (p :: p1) cmd) = case testEquality party p of
      Just Refl -> Right $ APartyCmd cmd
      _ -> Left $ CMD PROHIBITED

    hasEntityId :: AnEntity -> APartyCmd p -> Either AgentErrorType (APartyCmd p)
    hasEntityId (AE entity) (APartyCmd cmd) =
      APartyCmd <$> case cmd of
        -- NEW and JOIN have optional entity
        NEW -> Right cmd
        JOIN _ _ -> Right cmd
        -- ERROR response does not always have entity
        ERR _ -> Right cmd
        -- other responses must have entity
        _
          | B.null (entityId entity) -> Left $ CMD NO_ENTITY
          | otherwise -> Right cmd

    makeTransmission :: ACorrId -> AnEntity -> Either AgentErrorType (APartyCmd p) -> ATransmissionOrError p
    makeTransmission corrId (AE entity) = \case
      Left e -> err e
      Right (APartyCmd cmd) -> case entityCommand entity cmd of
        Just Dict -> ATransmissionOrError corrId entity $ Right cmd
        _ -> err $ CMD ENTITY
      where
        err e = ATransmissionOrError @_ @_ @ERR_ corrId entity $ Left e

    cmdWithMsgBody :: APartyCmd p -> m (Either AgentErrorType (APartyCmd p))
    cmdWithMsgBody (APartyCmd cmd) =
      APartyCmd <$$> case cmd of
        SEND body -> SEND <$$> getMsgBody body
        MSG agentMsgId srvTS agentTS integrity body -> MSG agentMsgId srvTS agentTS integrity <$$> getMsgBody body
        _ -> pure $ Right cmd

    -- TODO refactor with server
    getMsgBody :: MsgBody -> m (Either AgentErrorType MsgBody)
    getMsgBody msgBody =
      case B.unpack msgBody of
        ':' : body -> return . Right $ B.pack body
        str -> case readMaybe str :: Maybe Int of
          Just size -> liftIO $ do
            body <- cGet h size
            s <- getLn h
            return $ if B.null s then Right body else Left $ CMD SIZE
          Nothing -> return . Left $ CMD SYNTAX
