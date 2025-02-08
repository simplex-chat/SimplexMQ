{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
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
  ( -- * Protocol parameters
    VersionSMPA,
    VersionRangeSMPA,
    pattern VersionSMPA,
    duplexHandshakeSMPAgentVersion,
    ratchetSyncSMPAgentVersion,
    deliveryRcptsSMPAgentVersion,
    pqdrSMPAgentVersion,
    sndAuthKeySMPAgentVersion,
    ratchetOnConfSMPAgentVersion,
    currentSMPAgentVersion,
    supportedSMPAgentVRange,
    e2eEncConnInfoLength,
    e2eEncAgentMsgLength,

    -- * SMP agent protocol types
    ConnInfo,
    SndQueueSecured,
    AEntityId,
    ACommand (..),
    AEvent (..),
    AEvt (..),
    ACommandTag (..),
    AEventTag (..),
    AEvtTag (..),
    aCommandTag,
    aEventTag,
    AEntity (..),
    SAEntity (..),
    AEntityI (..),
    MsgHash,
    MsgMeta (..),
    RcvQueueInfo (..),
    SndQueueInfo (..),
    ConnectionStats (..),
    SwitchPhase (..),
    RcvSwitchStatus (..),
    SndSwitchStatus (..),
    QueueDirection (..),
    RatchetSyncState (..),
    SMPConfirmation (..),
    AgentMsgEnvelope (..),
    AgentMessage (..),
    AgentMessageType (..),
    APrivHeader (..),
    AMessage (..),
    AMessageReceipt (..),
    MsgReceipt (..),
    MsgReceiptInfo,
    MsgReceiptStatus (..),
    SndQAddr,
    SMPServer,
    pattern SMPServer,
    pattern ProtoServerWithAuth,
    SMPServerWithAuth,
    SrvLoc (..),
    SMPQueue (..),
    qAddress,
    sameQueue,
    sameQAddress,
    noAuthSrv,
    SMPQueueUri (..),
    SMPQueueInfo (..),
    SMPQueueAddress (..),
    ConnectionMode (..),
    SConnectionMode (..),
    AConnectionMode (..),
    cmInvitation,
    cmContact,
    ConnectionModeI (..),
    ConnectionRequestUri (..),
    AConnectionRequestUri (..),
    ConnReqUriData (..),
    CRClientData,
    ServiceScheme,
    sameConnReqContact,
    simplexChat,
    connReqUriP',
    AgentErrorType (..),
    CommandErrorType (..),
    ConnectionErrorType (..),
    BrokerErrorType (..),
    SMPAgentError (..),
    AgentCryptoError (..),
    cryptoErrToSyncState,
    ATransmission,
    ConnId,
    ConfirmationId,
    InvitationId,
    MsgIntegrity (..),
    MsgErrorType (..),
    QueueStatus (..),
    UserId,
    ACorrId,
    AgentMsgId,
    NotificationsMode (..),
    NotificationInfo (..),

    -- * Encode/decode
    serializeCommand,
    connMode,
    connMode',
    dbCommandP,
    connModeT,
    serializeQueueStatus,
    queueStatusT,
    agentMessageType,
    extraSMPServerHosts,
    updateSMPServerHosts,
  )
where

import Control.Applicative (optional, (<|>))
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Aeson.TH as J
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import Data.Int (Int64)
import Data.Kind (Type)
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Text.Encoding (decodeLatin1, encodeUtf8)
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.System (SystemTime)
import Data.Type.Equality
import Data.Typeable ()
import Data.Word (Word16, Word32)
import Simplex.Messaging.Agent.Store.DB (FromField (..), ToField (..), blobFieldDecoder, fromTextField_)
import Simplex.FileTransfer.Description
import Simplex.FileTransfer.Protocol (FileParty (..))
import Simplex.FileTransfer.Transport (XFTPErrorType)
import Simplex.FileTransfer.Types (FileErrorType)
import Simplex.Messaging.Agent.QueryString
import Simplex.Messaging.Client (ProxyClientError)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet
  ( InitialKeys (..),
    PQEncryption (..),
    PQSupport,
    RcvE2ERatchetParams,
    RcvE2ERatchetParamsUri,
    SndE2ERatchetParams,
    pattern PQSupportOff,
    pattern PQSupportOn,
  )
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers
import Simplex.Messaging.Protocol
  ( AProtocolType,
    BrokerErrorType (..),
    ErrorType,
    MsgBody,
    MsgFlags,
    MsgId,
    NMsgMeta,
    ProtocolServer (..),
    SMPClientVersion,
    SMPServer,
    SMPServerWithAuth,
    SndPublicAuthKey,
    SubscriptionMode,
    VersionRangeSMPC,
    VersionSMPC,
    initialSMPClientVersion,
    legacyEncodeServer,
    legacyServerP,
    legacyStrEncodeServer,
    noAuthSrv,
    sameSrvAddr,
    sndAuthKeySMPClientVersion,
    srvHostnamesSMPClientVersion,
    pattern ProtoServerWithAuth,
    pattern SMPServer,
  )
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.ServiceScheme
import Simplex.Messaging.Transport.Client (TransportHost, TransportHosts_ (..))
import Simplex.Messaging.Util
import Simplex.Messaging.Version
import Simplex.Messaging.Version.Internal
import Simplex.RemoteControl.Types
import UnliftIO.Exception (Exception)

-- SMP agent protocol version history:
-- 1 - binary protocol encoding (1/1/2022)
-- 2 - "duplex" (more efficient) connection handshake (6/9/2022)
-- 3 - support ratchet renegotiation (6/30/2023)
-- 4 - delivery receipts (7/13/2023)
-- 5 - post-quantum double ratchet (3/14/2024)
-- 6 - secure reply queues with provided keys (6/14/2024)

data SMPAgentVersion

instance VersionScope SMPAgentVersion

type VersionSMPA = Version SMPAgentVersion

type VersionRangeSMPA = VersionRange SMPAgentVersion

pattern VersionSMPA :: Word16 -> VersionSMPA
pattern VersionSMPA v = Version v

duplexHandshakeSMPAgentVersion :: VersionSMPA
duplexHandshakeSMPAgentVersion = VersionSMPA 2

ratchetSyncSMPAgentVersion :: VersionSMPA
ratchetSyncSMPAgentVersion = VersionSMPA 3

deliveryRcptsSMPAgentVersion :: VersionSMPA
deliveryRcptsSMPAgentVersion = VersionSMPA 4

pqdrSMPAgentVersion :: VersionSMPA
pqdrSMPAgentVersion = VersionSMPA 5

sndAuthKeySMPAgentVersion :: VersionSMPA
sndAuthKeySMPAgentVersion = VersionSMPA 6

ratchetOnConfSMPAgentVersion :: VersionSMPA
ratchetOnConfSMPAgentVersion = VersionSMPA 7

minSupportedSMPAgentVersion :: VersionSMPA
minSupportedSMPAgentVersion = duplexHandshakeSMPAgentVersion

currentSMPAgentVersion :: VersionSMPA
currentSMPAgentVersion = VersionSMPA 7

supportedSMPAgentVRange :: VersionRangeSMPA
supportedSMPAgentVRange = mkVersionRange minSupportedSMPAgentVersion currentSMPAgentVersion

-- it is shorter to allow all handshake headers,
-- including E2E (double-ratchet) parameters and
-- signing key of the sender for the server
e2eEncConnInfoLength :: VersionSMPA -> PQSupport -> Int
e2eEncConnInfoLength v = \case
  -- reduced by 3726 (roughly the increase of message ratchet header size + key and ciphertext in reply link)
  PQSupportOn | v >= pqdrSMPAgentVersion -> 11106
  _ -> 14832

e2eEncAgentMsgLength :: VersionSMPA -> PQSupport -> Int
e2eEncAgentMsgLength v = \case
  -- reduced by 2222 (the increase of message ratchet header size)
  PQSupportOn | v >= pqdrSMPAgentVersion -> 13618
  _ -> 15840

-- | SMP agent event
type ATransmission = (ACorrId, AEntityId, AEvt)

type UserId = Int64

type AEntityId = ByteString

type ACorrId = ByteString

data AEntity = AEConn | AERcvFile | AESndFile | AENone
  deriving (Eq, Show)

data SAEntity :: AEntity -> Type where
  SAEConn :: SAEntity AEConn
  SAERcvFile :: SAEntity AERcvFile
  SAESndFile :: SAEntity AESndFile
  SAENone :: SAEntity AENone

deriving instance Show (SAEntity e)

instance TestEquality SAEntity where
  testEquality SAEConn SAEConn = Just Refl
  testEquality SAERcvFile SAERcvFile = Just Refl
  testEquality SAESndFile SAESndFile = Just Refl
  testEquality SAENone SAENone = Just Refl
  testEquality _ _ = Nothing

class AEntityI (e :: AEntity) where sAEntity :: SAEntity e

instance AEntityI AEConn where sAEntity = SAEConn

instance AEntityI AERcvFile where sAEntity = SAERcvFile

instance AEntityI AESndFile where sAEntity = SAESndFile

instance AEntityI AENone where sAEntity = SAENone

data AEvt = forall e. AEntityI e => AEvt (SAEntity e) (AEvent e)

instance Eq AEvt where
  AEvt e evt == AEvt e' evt' = case testEquality e e' of
    Just Refl -> evt == evt'
    Nothing -> False

deriving instance Show AEvt

type ConnInfo = ByteString

type SndQueueSecured = Bool

-- | Parameterized type for SMP agent events
data AEvent (e :: AEntity) where
  INV :: AConnectionRequestUri -> AEvent AEConn
  CONF :: ConfirmationId -> PQSupport -> [SMPServer] -> ConnInfo -> AEvent AEConn -- ConnInfo is from sender, [SMPServer] will be empty only in v1 handshake
  REQ :: InvitationId -> PQSupport -> NonEmpty SMPServer -> ConnInfo -> AEvent AEConn -- ConnInfo is from sender
  INFO :: PQSupport -> ConnInfo -> AEvent AEConn
  CON :: PQEncryption -> AEvent AEConn -- notification that connection is established
  END :: AEvent AEConn
  DELD :: AEvent AEConn
  CONNECT :: AProtocolType -> TransportHost -> AEvent AENone
  DISCONNECT :: AProtocolType -> TransportHost -> AEvent AENone
  DOWN :: SMPServer -> [ConnId] -> AEvent AENone
  UP :: SMPServer -> [ConnId] -> AEvent AENone
  SWITCH :: QueueDirection -> SwitchPhase -> ConnectionStats -> AEvent AEConn
  RSYNC :: RatchetSyncState -> Maybe AgentCryptoError -> ConnectionStats -> AEvent AEConn
  SENT :: AgentMsgId -> Maybe SMPServer -> AEvent AEConn
  MWARN :: AgentMsgId -> AgentErrorType -> AEvent AEConn
  MERR :: AgentMsgId -> AgentErrorType -> AEvent AEConn
  MERRS :: NonEmpty AgentMsgId -> AgentErrorType -> AEvent AEConn
  MSG :: MsgMeta -> MsgFlags -> MsgBody -> AEvent AEConn
  MSGNTF :: MsgId -> Maybe UTCTime -> AEvent AEConn
  RCVD :: MsgMeta -> NonEmpty MsgReceipt -> AEvent AEConn
  QCONT :: AEvent AEConn
  DEL_RCVQS :: NonEmpty (ConnId, SMPServer, SMP.RecipientId, Maybe AgentErrorType) -> AEvent AEConn
  DEL_CONNS :: NonEmpty ConnId -> AEvent AEConn
  DEL_USER :: Int64 -> AEvent AENone
  STAT :: ConnectionStats -> AEvent AEConn
  OK :: AEvent AEConn
  JOINED :: SndQueueSecured -> AEvent AEConn
  ERR :: AgentErrorType -> AEvent AEConn
  ERRS :: [(ConnId, AgentErrorType)] -> AEvent AENone
  SUSPENDED :: AEvent AENone
  RFPROG :: Int64 -> Int64 -> AEvent AERcvFile
  RFDONE :: FilePath -> AEvent AERcvFile
  RFERR :: AgentErrorType -> AEvent AERcvFile
  RFWARN :: AgentErrorType -> AEvent AERcvFile
  SFPROG :: Int64 -> Int64 -> AEvent AESndFile
  SFDONE :: ValidFileDescription 'FSender -> [ValidFileDescription 'FRecipient] -> AEvent AESndFile
  SFERR :: AgentErrorType -> AEvent AESndFile
  SFWARN :: AgentErrorType -> AEvent AESndFile

deriving instance Eq (AEvent e)

deriving instance Show (AEvent e)

data AEvtTag = forall e. AEntityI e => AEvtTag (SAEntity e) (AEventTag e)

instance Eq AEvtTag where
  AEvtTag e evt == AEvtTag e' evt' = case testEquality e e' of
    Just Refl -> evt == evt'
    Nothing -> False

deriving instance Show AEvtTag

data ACommand
  = NEW Bool AConnectionMode InitialKeys SubscriptionMode -- response INV
  | JOIN Bool AConnectionRequestUri PQSupport SubscriptionMode ConnInfo
  | LET ConfirmationId ConnInfo -- ConnInfo is from client
  | ACK AgentMsgId (Maybe MsgReceiptInfo)
  | SWCH
  | DEL
  deriving (Eq, Show)

data ACommandTag
  = NEW_
  | JOIN_
  | LET_
  | ACK_
  | SWCH_
  | DEL_
  deriving (Show)

data AEventTag (e :: AEntity) where
  INV_ :: AEventTag AEConn
  CONF_ :: AEventTag AEConn
  REQ_ :: AEventTag AEConn
  INFO_ :: AEventTag AEConn
  CON_ :: AEventTag AEConn
  END_ :: AEventTag AEConn
  DELD_ :: AEventTag AEConn
  CONNECT_ :: AEventTag AENone
  DISCONNECT_ :: AEventTag AENone
  DOWN_ :: AEventTag AENone
  UP_ :: AEventTag AENone
  SWITCH_ :: AEventTag AEConn
  RSYNC_ :: AEventTag AEConn
  SENT_ :: AEventTag AEConn
  MWARN_ :: AEventTag AEConn
  MERR_ :: AEventTag AEConn
  MERRS_ :: AEventTag AEConn
  MSG_ :: AEventTag AEConn
  MSGNTF_ :: AEventTag AEConn
  RCVD_ :: AEventTag AEConn
  QCONT_ :: AEventTag AEConn
  DEL_RCVQS_ :: AEventTag AEConn
  DEL_CONNS_ :: AEventTag AEConn
  DEL_USER_ :: AEventTag AENone
  STAT_ :: AEventTag AEConn
  OK_ :: AEventTag AEConn
  JOINED_ :: AEventTag AEConn
  ERR_ :: AEventTag AEConn
  ERRS_ :: AEventTag AENone
  SUSPENDED_ :: AEventTag AENone
  -- XFTP commands and responses
  RFDONE_ :: AEventTag AERcvFile
  RFPROG_ :: AEventTag AERcvFile
  RFERR_ :: AEventTag AERcvFile
  RFWARN_ :: AEventTag AERcvFile
  SFPROG_ :: AEventTag AESndFile
  SFDONE_ :: AEventTag AESndFile
  SFERR_ :: AEventTag AESndFile
  SFWARN_ :: AEventTag AESndFile

deriving instance Eq (AEventTag e)

deriving instance Show (AEventTag e)

aCommandTag :: ACommand -> ACommandTag
aCommandTag = \case
  NEW {} -> NEW_
  JOIN {} -> JOIN_
  LET {} -> LET_
  ACK {} -> ACK_
  SWCH -> SWCH_
  DEL -> DEL_

aEventTag :: AEvent e -> AEventTag e
aEventTag = \case
  INV _ -> INV_
  CONF {} -> CONF_
  REQ {} -> REQ_
  INFO {} -> INFO_
  CON _ -> CON_
  END -> END_
  DELD -> DELD_
  CONNECT {} -> CONNECT_
  DISCONNECT {} -> DISCONNECT_
  DOWN {} -> DOWN_
  UP {} -> UP_
  SWITCH {} -> SWITCH_
  RSYNC {} -> RSYNC_
  SENT {} -> SENT_
  MWARN {} -> MWARN_
  MERR {} -> MERR_
  MERRS {} -> MERRS_
  MSG {} -> MSG_
  MSGNTF {} -> MSGNTF_
  RCVD {} -> RCVD_
  QCONT -> QCONT_
  DEL_RCVQS _ -> DEL_RCVQS_
  DEL_CONNS _ -> DEL_CONNS_
  DEL_USER _ -> DEL_USER_
  STAT _ -> STAT_
  OK -> OK_
  JOINED _ -> JOINED_
  ERR _ -> ERR_
  ERRS _ -> ERRS_
  SUSPENDED -> SUSPENDED_
  RFPROG {} -> RFPROG_
  RFDONE {} -> RFDONE_
  RFERR {} -> RFERR_
  RFWARN {} -> RFWARN_
  SFPROG {} -> SFPROG_
  SFDONE {} -> SFDONE_
  SFERR {} -> SFERR_
  SFWARN {} -> SFWARN_

data QueueDirection = QDRcv | QDSnd
  deriving (Eq, Show)

data SwitchPhase = SPStarted | SPConfirmed | SPSecured | SPCompleted
  deriving (Eq, Show)

data RcvSwitchStatus
  = RSSwitchStarted
  | RSSendingQADD
  | RSSendingQUSE
  | RSReceivedMessage
  deriving (Eq, Show)

instance StrEncoding RcvSwitchStatus where
  strEncode = \case
    RSSwitchStarted -> "switch_started"
    RSSendingQADD -> "sending_qadd"
    RSSendingQUSE -> "sending_quse"
    RSReceivedMessage -> "received_message"
  strP =
    A.takeTill (== ' ') >>= \case
      "switch_started" -> pure RSSwitchStarted
      "sending_qadd" -> pure RSSendingQADD
      "sending_quse" -> pure RSSendingQUSE
      "received_message" -> pure RSReceivedMessage
      _ -> fail "bad RcvSwitchStatus"

instance ToField RcvSwitchStatus where toField = toField . decodeLatin1 . strEncode

instance FromField RcvSwitchStatus where fromField = fromTextField_ $ eitherToMaybe . strDecode . encodeUtf8

instance ToJSON RcvSwitchStatus where
  toEncoding = strToJEncoding
  toJSON = strToJSON

instance FromJSON RcvSwitchStatus where
  parseJSON = strParseJSON "RcvSwitchStatus"

data SndSwitchStatus
  = SSSendingQKEY
  | SSSendingQTEST
  deriving (Eq, Show)

instance StrEncoding SndSwitchStatus where
  strEncode = \case
    SSSendingQKEY -> "sending_qkey"
    SSSendingQTEST -> "sending_qtest"
  strP =
    A.takeTill (== ' ') >>= \case
      "sending_qkey" -> pure SSSendingQKEY
      "sending_qtest" -> pure SSSendingQTEST
      _ -> fail "bad SndSwitchStatus"

instance ToField SndSwitchStatus where toField = toField . decodeLatin1 . strEncode

instance FromField SndSwitchStatus where fromField = fromTextField_ $ eitherToMaybe . strDecode . encodeUtf8

instance ToJSON SndSwitchStatus where
  toEncoding = strToJEncoding
  toJSON = strToJSON

instance FromJSON SndSwitchStatus where
  parseJSON = strParseJSON "SndSwitchStatus"

data RatchetSyncState
  = RSOk
  | RSAllowed
  | RSRequired
  | RSStarted
  | RSAgreed
  deriving (Eq, Show)

instance StrEncoding RatchetSyncState where
  strEncode = \case
    RSOk -> "ok"
    RSAllowed -> "allowed"
    RSRequired -> "required"
    RSStarted -> "started"
    RSAgreed -> "agreed"
  strP =
    A.takeTill (== ' ') >>= \case
      "ok" -> pure RSOk
      "allowed" -> pure RSAllowed
      "required" -> pure RSRequired
      "started" -> pure RSStarted
      "agreed" -> pure RSAgreed
      _ -> fail "bad RatchetSyncState"

instance FromField RatchetSyncState where fromField = fromTextField_ $ eitherToMaybe . strDecode . encodeUtf8

instance ToField RatchetSyncState where toField = toField . decodeLatin1 . strEncode

instance ToJSON RatchetSyncState where
  toEncoding = strToJEncoding
  toJSON = strToJSON

instance FromJSON RatchetSyncState where
  parseJSON = strParseJSON "RatchetSyncState"

data RcvQueueInfo = RcvQueueInfo
  { rcvServer :: SMPServer,
    rcvSwitchStatus :: Maybe RcvSwitchStatus,
    canAbortSwitch :: Bool
  }
  deriving (Eq, Show)

data SndQueueInfo = SndQueueInfo
  { sndServer :: SMPServer,
    sndSwitchStatus :: Maybe SndSwitchStatus
  }
  deriving (Eq, Show)

data ConnectionStats = ConnectionStats
  { connAgentVersion :: VersionSMPA,
    rcvQueuesInfo :: [RcvQueueInfo],
    sndQueuesInfo :: [SndQueueInfo],
    ratchetSyncState :: RatchetSyncState,
    ratchetSyncSupported :: Bool
  }
  deriving (Eq, Show)

data NotificationsMode = NMPeriodic | NMInstant
  deriving (Eq, Show)

instance StrEncoding NotificationsMode where
  strEncode = \case
    NMPeriodic -> "PERIODIC"
    NMInstant -> "INSTANT"
  strP =
    A.takeTill (== ' ') >>= \case
      "PERIODIC" -> pure NMPeriodic
      "INSTANT" -> pure NMInstant
      _ -> fail "bad NotificationsMode"

instance ToJSON NotificationsMode where
  toEncoding = strToJEncoding
  toJSON = strToJSON

instance FromJSON NotificationsMode where
  parseJSON = strParseJSON "NotificationsMode"

instance ToField NotificationsMode where toField = toField . strEncode

instance FromField NotificationsMode where fromField = blobFieldDecoder $ parseAll strP

data NotificationInfo = NotificationInfo
  { ntfConnId :: ConnId,
    ntfTs :: SystemTime,
    ntfMsgMeta :: Maybe NMsgMeta
  }
  deriving (Show)

data ConnectionMode = CMInvitation | CMContact
  deriving (Eq, Show)

data SConnectionMode (m :: ConnectionMode) where
  SCMInvitation :: SConnectionMode CMInvitation
  SCMContact :: SConnectionMode CMContact

deriving instance Eq (SConnectionMode m)

deriving instance Show (SConnectionMode m)

instance TestEquality SConnectionMode where
  testEquality SCMInvitation SCMInvitation = Just Refl
  testEquality SCMContact SCMContact = Just Refl
  testEquality _ _ = Nothing

data AConnectionMode = forall m. ConnectionModeI m => ACM (SConnectionMode m)

instance Eq AConnectionMode where
  ACM m == ACM m' = isJust $ testEquality m m'

cmInvitation :: AConnectionMode
cmInvitation = ACM SCMInvitation

cmContact :: AConnectionMode
cmContact = ACM SCMContact

deriving instance Show AConnectionMode

connMode :: SConnectionMode m -> ConnectionMode
connMode SCMInvitation = CMInvitation
connMode SCMContact = CMContact

connMode' :: ConnectionMode -> AConnectionMode
connMode' CMInvitation = cmInvitation
connMode' CMContact = cmContact

class ConnectionModeI (m :: ConnectionMode) where sConnectionMode :: SConnectionMode m

instance ConnectionModeI CMInvitation where sConnectionMode = SCMInvitation

instance ConnectionModeI CMContact where sConnectionMode = SCMContact

type MsgHash = ByteString

-- | Agent message metadata sent to the client
data MsgMeta = MsgMeta
  { integrity :: MsgIntegrity,
    recipient :: (AgentMsgId, UTCTime),
    broker :: (MsgId, UTCTime),
    sndMsgId :: AgentMsgId,
    pqEncryption :: PQEncryption
  }
  deriving (Eq, Show)

data SMPConfirmation = SMPConfirmation
  { -- | sender's public key to use for authentication of sender's commands at the recepient's server
    senderKey :: Maybe SndPublicAuthKey,
    -- | sender's DH public key for simple per-queue e2e encryption
    e2ePubKey :: C.PublicKeyX25519,
    -- | sender's information to be associated with the connection, e.g. sender's profile information
    connInfo :: ConnInfo,
    -- | optional reply queues included in confirmation (added in agent protocol v2)
    smpReplyQueues :: [SMPQueueInfo],
    -- | SMP client version
    smpClientVersion :: VersionSMPC
  }
  deriving (Show)

data AgentMsgEnvelope
  = AgentConfirmation
      { agentVersion :: VersionSMPA,
        e2eEncryption_ :: Maybe (SndE2ERatchetParams 'C.X448),
        encConnInfo :: ByteString
      }
  | AgentMsgEnvelope
      { agentVersion :: VersionSMPA,
        encAgentMessage :: ByteString
      }
  | AgentInvitation -- the connInfo in contactInvite is only encrypted with per-queue E2E, not with double ratchet,
      { agentVersion :: VersionSMPA,
        connReq :: ConnectionRequestUri 'CMInvitation,
        connInfo :: ByteString -- this message is only encrypted with per-queue E2E, not with double ratchet,
      }
  | AgentRatchetKey
      { agentVersion :: VersionSMPA,
        e2eEncryption :: RcvE2ERatchetParams 'C.X448,
        info :: ByteString
      }
  deriving (Show)

instance Encoding AgentMsgEnvelope where
  smpEncode = \case
    AgentConfirmation {agentVersion, e2eEncryption_, encConnInfo} ->
      smpEncode (agentVersion, 'C', e2eEncryption_, Tail encConnInfo)
    AgentMsgEnvelope {agentVersion, encAgentMessage} ->
      smpEncode (agentVersion, 'M', Tail encAgentMessage)
    AgentInvitation {agentVersion, connReq, connInfo} ->
      smpEncode (agentVersion, 'I', Large $ strEncode connReq, Tail connInfo)
    AgentRatchetKey {agentVersion, e2eEncryption, info} ->
      smpEncode (agentVersion, 'R', e2eEncryption, Tail info)
  smpP = do
    agentVersion <- smpP
    smpP >>= \case
      'C' -> do
        (e2eEncryption_, Tail encConnInfo) <- smpP
        pure AgentConfirmation {agentVersion, e2eEncryption_, encConnInfo}
      'M' -> do
        Tail encAgentMessage <- smpP
        pure AgentMsgEnvelope {agentVersion, encAgentMessage}
      'I' -> do
        connReq <- strDecode . unLarge <$?> smpP
        Tail connInfo <- smpP
        pure AgentInvitation {agentVersion, connReq, connInfo}
      'R' -> do
        e2eEncryption <- smpP
        Tail info <- smpP
        pure AgentRatchetKey {agentVersion, e2eEncryption, info}
      _ -> fail "bad AgentMsgEnvelope"

-- SMP agent message formats (after double ratchet decryption,
-- or in case of AgentInvitation - in plain text body)
-- AgentRatchetInfo is not encrypted with double ratchet, but with per-queue E2E encryption
data AgentMessage
  = -- used by the initiating party when confirming reply queue
    AgentConnInfo ConnInfo
  | -- AgentConnInfoReply is used by accepting party in duplexHandshake mode (v2), allowing to include reply queue(s) in the initial confirmation.
    -- It made removed REPLY message unnecessary.
    AgentConnInfoReply (NonEmpty SMPQueueInfo) ConnInfo
  | AgentRatchetInfo ByteString
  | AgentMessage APrivHeader AMessage
  deriving (Show)

instance Encoding AgentMessage where
  smpEncode = \case
    AgentConnInfo cInfo -> smpEncode ('I', Tail cInfo)
    AgentConnInfoReply smpQueues cInfo -> smpEncode ('D', smpQueues, Tail cInfo) -- 'D' stands for "duplex"
    AgentRatchetInfo info -> smpEncode ('R', Tail info)
    AgentMessage hdr aMsg -> smpEncode ('M', hdr, aMsg)
  smpP =
    smpP >>= \case
      'I' -> AgentConnInfo . unTail <$> smpP
      'D' -> AgentConnInfoReply <$> smpP <*> (unTail <$> smpP)
      'R' -> AgentRatchetInfo . unTail <$> smpP
      'M' -> AgentMessage <$> smpP <*> smpP
      _ -> fail "bad AgentMessage"

-- internal type for storing message type in the database
data AgentMessageType
  = AM_CONN_INFO
  | AM_CONN_INFO_REPLY
  | AM_RATCHET_INFO
  | AM_HELLO_
  | AM_A_MSG_
  | AM_A_RCVD_
  | AM_QCONT_
  | AM_QADD_
  | AM_QKEY_
  | AM_QUSE_
  | AM_QTEST_
  | AM_EREADY_
  deriving (Eq, Show)

instance Encoding AgentMessageType where
  smpEncode = \case
    AM_CONN_INFO -> "C"
    AM_CONN_INFO_REPLY -> "D"
    AM_RATCHET_INFO -> "S"
    AM_HELLO_ -> "H"
    AM_A_MSG_ -> "M"
    AM_A_RCVD_ -> "V"
    AM_QCONT_ -> "QC"
    AM_QADD_ -> "QA"
    AM_QKEY_ -> "QK"
    AM_QUSE_ -> "QU"
    AM_QTEST_ -> "QT"
    AM_EREADY_ -> "E"
  smpP =
    A.anyChar >>= \case
      'C' -> pure AM_CONN_INFO
      'D' -> pure AM_CONN_INFO_REPLY
      'S' -> pure AM_RATCHET_INFO
      'H' -> pure AM_HELLO_
      'M' -> pure AM_A_MSG_
      'V' -> pure AM_A_RCVD_
      'Q' ->
        A.anyChar >>= \case
          'C' -> pure AM_QCONT_
          'A' -> pure AM_QADD_
          'K' -> pure AM_QKEY_
          'U' -> pure AM_QUSE_
          'T' -> pure AM_QTEST_
          _ -> fail "bad AgentMessageType"
      'E' -> pure AM_EREADY_
      _ -> fail "bad AgentMessageType"

agentMessageType :: AgentMessage -> AgentMessageType
agentMessageType = \case
  AgentConnInfo _ -> AM_CONN_INFO
  AgentConnInfoReply {} -> AM_CONN_INFO_REPLY
  AgentRatchetInfo _ -> AM_RATCHET_INFO
  AgentMessage _ aMsg -> case aMsg of
    -- HELLO is used both in v1 and in v2, but differently.
    -- - in v1 (and, possibly, in v2 for simplex connections) can be sent multiple times,
    --   until the queue is secured - the OK response from the server instead of initial AUTH errors confirms it.
    -- - in v2 duplexHandshake it is sent only once, when it is known that the queue was secured.
    HELLO -> AM_HELLO_
    A_MSG _ -> AM_A_MSG_
    A_RCVD {} -> AM_A_RCVD_
    A_QCONT _ -> AM_QCONT_
    QADD _ -> AM_QADD_
    QKEY _ -> AM_QKEY_
    QUSE _ -> AM_QUSE_
    QTEST _ -> AM_QTEST_
    EREADY _ -> AM_EREADY_

data APrivHeader = APrivHeader
  { -- | sequential ID assigned by the sending agent
    sndMsgId :: AgentMsgId,
    -- | digest of the previous message
    prevMsgHash :: MsgHash
  }
  deriving (Show)

instance Encoding APrivHeader where
  smpEncode APrivHeader {sndMsgId, prevMsgHash} =
    smpEncode (sndMsgId, prevMsgHash)
  smpP = APrivHeader <$> smpP <*> smpP

data AMsgType
  = HELLO_
  | A_MSG_
  | A_RCVD_
  | A_QCONT_
  | QADD_
  | QKEY_
  | QUSE_
  | QTEST_
  | EREADY_
  deriving (Eq)

instance Encoding AMsgType where
  smpEncode = \case
    HELLO_ -> "H"
    A_MSG_ -> "M"
    A_RCVD_ -> "V"
    A_QCONT_ -> "QC"
    QADD_ -> "QA"
    QKEY_ -> "QK"
    QUSE_ -> "QU"
    QTEST_ -> "QT"
    EREADY_ -> "E"
  smpP =
    A.anyChar >>= \case
      'H' -> pure HELLO_
      'M' -> pure A_MSG_
      'V' -> pure A_RCVD_
      'Q' ->
        A.anyChar >>= \case
          'C' -> pure A_QCONT_
          'A' -> pure QADD_
          'K' -> pure QKEY_
          'U' -> pure QUSE_
          'T' -> pure QTEST_
          _ -> fail "bad AMsgType"
      'E' -> pure EREADY_
      _ -> fail "bad AMsgType"

-- | Messages sent between SMP agents once SMP queue is secured.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md#messages-between-smp-agents
data AMessage
  = -- | the first message in the queue to validate it is secured
    HELLO
  | -- | agent envelope for the client message
    A_MSG MsgBody
  | -- | agent envelope for delivery receipt
    A_RCVD (NonEmpty AMessageReceipt)
  | -- | the message instructing the client to continue sending messages (after ERR QUOTA)
    A_QCONT SndQAddr
  | -- add queue to connection (sent by recipient), with optional address of the replaced queue
    QADD (NonEmpty (SMPQueueUri, Maybe SndQAddr))
  | -- key to secure the added queues and agree e2e encryption key (sent by sender)
    QKEY (NonEmpty (SMPQueueInfo, SndPublicAuthKey))
  | -- inform that the queues are ready to use (sent by recipient)
    QUSE (NonEmpty (SndQAddr, Bool))
  | -- sent by the sender to test new queues and to complete switching
    QTEST (NonEmpty SndQAddr)
  | -- ratchet re-synchronization is complete, with last decrypted sender message id (recipient's `last_external_snd_msg_id`)
    EREADY AgentMsgId
  deriving (Show)

-- | this type is used to send as part of the protocol between different clients
-- TODO possibly, rename fields and types referring to external and internal IDs to make them different
data AMessageReceipt = AMessageReceipt
  { agentMsgId :: AgentMsgId, -- this is an external snd message ID referenced by the message recipient
    msgHash :: MsgHash,
    rcptInfo :: MsgReceiptInfo
  }
  deriving (Show)

-- | this type is used as part of agent protocol to communicate with the user application
data MsgReceipt = MsgReceipt
  { agentMsgId :: AgentMsgId, -- this is an internal agent message ID of received message
    msgRcptStatus :: MsgReceiptStatus
  }
  deriving (Eq, Show)

data MsgReceiptStatus = MROk | MRBadMsgHash
  deriving (Eq, Show)

instance StrEncoding MsgReceiptStatus where
  strEncode = \case
    MROk -> "ok"
    MRBadMsgHash -> "badMsgHash"
  strP =
    A.takeWhile1 (/= ' ') >>= \case
      "ok" -> pure MROk
      "badMsgHash" -> pure MRBadMsgHash
      _ -> fail "bad MsgReceiptStatus"

instance ToJSON MsgReceiptStatus where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance FromJSON MsgReceiptStatus where
  parseJSON = strParseJSON "MsgReceiptStatus"

type MsgReceiptInfo = ByteString

type SndQAddr = (SMPServer, SMP.SenderId)

instance Encoding AMessage where
  smpEncode = \case
    HELLO -> smpEncode HELLO_
    A_MSG body -> smpEncode (A_MSG_, Tail body)
    A_RCVD mrs -> smpEncode (A_RCVD_, mrs)
    A_QCONT addr -> smpEncode (A_QCONT_, addr)
    QADD qs -> smpEncode (QADD_, qs)
    QKEY qs -> smpEncode (QKEY_, qs)
    QUSE qs -> smpEncode (QUSE_, qs)
    QTEST qs -> smpEncode (QTEST_, qs)
    EREADY lastDecryptedMsgId -> smpEncode (EREADY_, lastDecryptedMsgId)
  smpP =
    smpP
      >>= \case
        HELLO_ -> pure HELLO
        A_MSG_ -> A_MSG . unTail <$> smpP
        A_RCVD_ -> A_RCVD <$> smpP
        A_QCONT_ -> A_QCONT <$> smpP
        QADD_ -> QADD <$> smpP
        QKEY_ -> QKEY <$> smpP
        QUSE_ -> QUSE <$> smpP
        QTEST_ -> QTEST <$> smpP
        EREADY_ -> EREADY <$> smpP

instance Encoding AMessageReceipt where
  smpEncode AMessageReceipt {agentMsgId, msgHash, rcptInfo} =
    smpEncode (agentMsgId, msgHash, Large rcptInfo)
  smpP = do
    (agentMsgId, msgHash, Large rcptInfo) <- smpP
    pure AMessageReceipt {agentMsgId, msgHash, rcptInfo}

instance ConnectionModeI m => StrEncoding (ConnectionRequestUri m) where
  strEncode = \case
    CRInvitationUri crData e2eParams -> crEncode "invitation" crData (Just e2eParams)
    CRContactUri crData -> crEncode "contact" crData Nothing
    where
      crEncode :: ByteString -> ConnReqUriData -> Maybe (RcvE2ERatchetParamsUri 'C.X448) -> ByteString
      crEncode crMode ConnReqUriData {crScheme, crAgentVRange, crSmpQueues, crClientData} e2eParams =
        strEncode crScheme <> "/" <> crMode <> "#/?" <> queryStr
        where
          queryStr =
            strEncode . QSP QEscape $
              -- semicolon is used to separate SMP queues because comma is used to separate server address hostnames
              [("v", strEncode crAgentVRange), ("smp", B.intercalate ";" $ map strEncode $ L.toList crSmpQueues)]
                <> maybe [] (\e2e -> [("e2e", strEncode e2e)]) e2eParams
                <> maybe [] (\cd -> [("data", encodeUtf8 cd)]) crClientData
  strP = connReqUriP' (Just SSSimplex)

connReqUriP' :: forall m. ConnectionModeI m => Maybe ServiceScheme -> Parser (ConnectionRequestUri m)
connReqUriP' overrideScheme = do
  ACR m cr <- connReqUriP overrideScheme
  case testEquality m $ sConnectionMode @m of
    Just Refl -> pure cr
    _ -> fail "bad connection request mode"

instance StrEncoding AConnectionRequestUri where
  strEncode (ACR _ cr) = strEncode cr
  strP = connReqUriP (Just SSSimplex)

connReqUriP :: Maybe ServiceScheme -> Parser AConnectionRequestUri
connReqUriP overrideScheme = do
  crScheme <- (`fromMaybe` overrideScheme) <$> strP
  crMode <- A.char '/' *> crModeP <* optional (A.char '/') <* "#/?"
  query <- strP
  aVRange <- queryParam "v" query
  crSmpQueues <- queryParamParser queuesP "smp" query
  let crClientData = safeDecodeUtf8 <$> queryParamStr "data" query
      crData = ConnReqUriData {crScheme, crAgentVRange = aVRange, crSmpQueues, crClientData}
  case crMode of
    CMInvitation -> do
      crE2eParams <- queryParam "e2e" query
      pure . ACR SCMInvitation $ CRInvitationUri crData crE2eParams
    -- contact links are adjusted to the minimum version supported by the agent
    -- to preserve compatibility with the old links published online
    CMContact -> pure . ACR SCMContact $ CRContactUri crData {crAgentVRange = adjustAgentVRange aVRange}
  where
    crModeP = "invitation" $> CMInvitation <|> "contact" $> CMContact
    -- semicolon is used to separate SMP queues because comma is used to separate server address hostnames
    queuesP = L.fromList <$> (strDecode <$?> A.takeTill (== ';')) `A.sepBy1'` A.char ';'
    adjustAgentVRange vr =
      let v = max minSupportedSMPAgentVersion $ minVersion vr
       in fromMaybe vr $ safeVersionRange v (max v $ maxVersion vr)

instance ConnectionModeI m => FromJSON (ConnectionRequestUri m) where
  parseJSON = strParseJSON "ConnectionRequestUri"

instance ConnectionModeI m => ToJSON (ConnectionRequestUri m) where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance FromJSON AConnectionRequestUri where
  parseJSON = strParseJSON "ConnectionRequestUri"

instance ToJSON AConnectionRequestUri where
  toJSON = strToJSON
  toEncoding = strToJEncoding

-- debug :: Show a => String -> a -> a
-- debug name value = unsafePerformIO (putStrLn $ name <> ": " <> show value) `seq` value
-- {-# INLINE debug #-}

instance StrEncoding ConnectionMode where
  strEncode = \case
    CMInvitation -> "INV"
    CMContact -> "CON"
  strP = "INV" $> CMInvitation <|> "CON" $> CMContact

instance StrEncoding AConnectionMode where
  strEncode (ACM cMode) = strEncode $ connMode cMode
  strP = connMode' <$> strP

connModeT :: Text -> Maybe ConnectionMode
connModeT = \case
  "INV" -> Just CMInvitation
  "CON" -> Just CMContact
  _ -> Nothing

-- | SMP agent connection ID.
type ConnId = ByteString

type ConfirmationId = ByteString

type InvitationId = ByteString

extraSMPServerHosts :: Map TransportHost TransportHost
extraSMPServerHosts =
  M.fromList
    [ ("smp4.simplex.im", "o5vmywmrnaxalvz6wi3zicyftgio6psuvyniis6gco6bp6ekl4cqj4id.onion"),
      ("smp5.simplex.im", "jjbyvoemxysm7qxap7m5d5m35jzv5qq6gnlv7s4rsn7tdwwmuqciwpid.onion"),
      ("smp6.simplex.im", "bylepyau3ty4czmn77q4fglvperknl4bi2eb2fdy2bh4jxtf32kf73yd.onion"),
      ("smp8.simplex.im", "beccx4yfxxbvyhqypaavemqurytl6hozr47wfc7uuecacjqdvwpw2xid.onion"),
      ("smp9.simplex.im", "jssqzccmrcws6bhmn77vgmhfjmhwlyr3u7puw4erkyoosywgl67slqqd.onion"),
      ("smp10.simplex.im", "rb2pbttocvnbrngnwziclp2f4ckjq65kebafws6g4hy22cdaiv5dwjqd.onion")
    ]

updateSMPServerHosts :: SMPServer -> SMPServer
updateSMPServerHosts srv@ProtocolServer {host} = case host of
  h :| [] -> case M.lookup h extraSMPServerHosts of
    Just h' -> srv {host = [h, h']}
    _ -> srv
  _ -> srv

class SMPQueue q where
  qServer :: q -> SMPServer
  queueId :: q -> SMP.QueueId

qAddress :: SMPQueue q => q -> (SMPServer, SMP.QueueId)
qAddress q = (qServer q, queueId q)
{-# INLINE qAddress #-}

sameQueue :: SMPQueue q => (SMPServer, SMP.QueueId) -> q -> Bool
sameQueue addr q = sameQAddress addr (qAddress q)
{-# INLINE sameQueue #-}

data SMPQueueInfo = SMPQueueInfo {clientVersion :: VersionSMPC, queueAddress :: SMPQueueAddress}
  deriving (Eq, Show)

instance Encoding SMPQueueInfo where
  smpEncode (SMPQueueInfo clientVersion SMPQueueAddress {smpServer, senderId, dhPublicKey, sndSecure})
    | clientVersion >= sndAuthKeySMPClientVersion && sndSecure = smpEncode (clientVersion, smpServer, senderId, dhPublicKey, sndSecure)
    | clientVersion > initialSMPClientVersion = smpEncode (clientVersion, smpServer, senderId, dhPublicKey)
    | otherwise = smpEncode clientVersion <> legacyEncodeServer smpServer <> smpEncode (senderId, dhPublicKey)
  smpP = do
    clientVersion <- smpP
    smpServer <- if clientVersion > initialSMPClientVersion then smpP else updateSMPServerHosts <$> legacyServerP
    (senderId, dhPublicKey) <- smpP
    sndSecure <- fromMaybe False <$> optional smpP
    pure $ SMPQueueInfo clientVersion SMPQueueAddress {smpServer, senderId, dhPublicKey, sndSecure}

-- This instance seems contrived and there was a temptation to split a common part of both types.
-- But this is created to allow backward and forward compatibility where SMPQueueUri
-- could have more fields to convert to different versions of SMPQueueInfo in a different way,
-- and this instance would become non-trivial.
instance VersionI SMPClientVersion SMPQueueInfo where
  type VersionRangeT SMPClientVersion SMPQueueInfo = SMPQueueUri
  version = clientVersion
  toVersionRangeT (SMPQueueInfo _v addr) vr = SMPQueueUri vr addr

instance VersionRangeI SMPClientVersion SMPQueueUri where
  type VersionT SMPClientVersion SMPQueueUri = SMPQueueInfo
  versionRange = clientVRange
  toVersionT (SMPQueueUri _vr addr) v = SMPQueueInfo v addr
  toVersionRange (SMPQueueUri _vr addr) vr = SMPQueueUri vr addr

-- | SMP queue information sent out-of-band.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#out-of-band-messages
data SMPQueueUri = SMPQueueUri {clientVRange :: VersionRangeSMPC, queueAddress :: SMPQueueAddress}
  deriving (Eq, Show)

data SMPQueueAddress = SMPQueueAddress
  { smpServer :: SMPServer,
    senderId :: SMP.SenderId,
    dhPublicKey :: C.PublicKeyX25519,
    sndSecure :: Bool
  }
  deriving (Eq, Show)

instance SMPQueue SMPQueueUri where
  qServer SMPQueueUri {queueAddress} = qServer queueAddress
  {-# INLINE qServer #-}
  queueId SMPQueueUri {queueAddress} = queueId queueAddress
  {-# INLINE queueId #-}

instance SMPQueue SMPQueueInfo where
  qServer SMPQueueInfo {queueAddress} = qServer queueAddress
  {-# INLINE qServer #-}
  queueId SMPQueueInfo {queueAddress} = queueId queueAddress
  {-# INLINE queueId #-}

instance SMPQueue SMPQueueAddress where
  qServer SMPQueueAddress {smpServer} = smpServer
  {-# INLINE qServer #-}
  queueId SMPQueueAddress {senderId} = senderId
  {-# INLINE queueId #-}

sameQAddress :: (SMPServer, SMP.QueueId) -> (SMPServer, SMP.QueueId) -> Bool
sameQAddress (srv, qId) (srv', qId') = sameSrvAddr srv srv' && qId == qId'
{-# INLINE sameQAddress #-}

instance StrEncoding SMPQueueUri where
  strEncode (SMPQueueUri vr SMPQueueAddress {smpServer = srv, senderId = qId, dhPublicKey, sndSecure})
    | minVersion vr >= srvHostnamesSMPClientVersion = strEncode srv <> "/" <> strEncode qId <> "#/?" <> query queryParams
    | otherwise = legacyStrEncodeServer srv <> "/" <> strEncode qId <> "#/?" <> query (queryParams <> srvParam)
    where
      query = strEncode . QSP QEscape
      queryParams = [("v", strEncode vr), ("dh", strEncode dhPublicKey)] <> [("k", "s") | sndSecure]
      srvParam = [("srv", strEncode $ TransportHosts_ hs) | not (null hs)]
      hs = L.tail $ host srv
  strP = do
    srv@ProtocolServer {host = h :| host} <- strP <* A.char '/'
    senderId <- strP <* optional (A.char '/') <* A.char '#'
    (vr, hs, dhPublicKey, sndSecure) <- versioned <|> unversioned
    let srv' = srv {host = h :| host <> hs}
        smpServer = if maxVersion vr < srvHostnamesSMPClientVersion then updateSMPServerHosts srv' else srv'
    pure $ SMPQueueUri vr SMPQueueAddress {smpServer, senderId, dhPublicKey, sndSecure}
    where
      unversioned = (versionToRange initialSMPClientVersion,[],,False) <$> strP <* A.endOfInput
      versioned = do
        dhKey_ <- optional strP
        query <- optional (A.char '/') *> A.char '?' *> strP
        vr <- queryParam "v" query
        dhKey <- maybe (queryParam "dh" query) pure dhKey_
        hs_ <- queryParam_ "srv" query
        let sndSecure = queryParamStr "k" query == Just "s"
        pure (vr, maybe [] thList_ hs_, dhKey, sndSecure)

instance Encoding SMPQueueUri where
  smpEncode (SMPQueueUri clientVRange SMPQueueAddress {smpServer, senderId, dhPublicKey, sndSecure})
    | maxVersion clientVRange >= sndAuthKeySMPClientVersion && sndSecure =
        smpEncode (clientVRange, smpServer, senderId, dhPublicKey, sndSecure)
    | otherwise =
        smpEncode (clientVRange, smpServer, senderId, dhPublicKey)
  smpP = do
    (clientVRange, smpServer, senderId, dhPublicKey) <- smpP
    sndSecure <- fromMaybe False <$> optional smpP
    pure $ SMPQueueUri clientVRange SMPQueueAddress {smpServer, senderId, dhPublicKey, sndSecure}

data ConnectionRequestUri (m :: ConnectionMode) where
  CRInvitationUri :: ConnReqUriData -> RcvE2ERatchetParamsUri 'C.X448 -> ConnectionRequestUri CMInvitation
  -- contact connection request does NOT contain E2E encryption parameters for double ratchet -
  -- they are passed in AgentInvitation message
  CRContactUri :: ConnReqUriData -> ConnectionRequestUri CMContact

deriving instance Eq (ConnectionRequestUri m)

deriving instance Show (ConnectionRequestUri m)

data AConnectionRequestUri = forall m. ConnectionModeI m => ACR (SConnectionMode m) (ConnectionRequestUri m)

instance Eq AConnectionRequestUri where
  ACR m cr == ACR m' cr' = case testEquality m m' of
    Just Refl -> cr == cr'
    _ -> False

deriving instance Show AConnectionRequestUri

sameConnReqContact :: ConnectionRequestUri 'CMContact -> ConnectionRequestUri 'CMContact -> Bool
sameConnReqContact (CRContactUri ConnReqUriData {crSmpQueues = qs}) (CRContactUri ConnReqUriData {crSmpQueues = qs'}) =
  L.length qs == L.length qs' && all same (L.zip qs qs')
  where
    same (q, q') = sameQAddress (qAddress q) (qAddress q')

data ConnReqUriData = ConnReqUriData
  { crScheme :: ServiceScheme,
    crAgentVRange :: VersionRangeSMPA,
    crSmpQueues :: NonEmpty SMPQueueUri,
    crClientData :: Maybe CRClientData
  }
  deriving (Eq, Show)

type CRClientData = Text

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

serializeQueueStatus :: QueueStatus -> Text
serializeQueueStatus = \case
  New -> "new"
  Confirmed -> "confirmed"
  Secured -> "secured"
  Active -> "active"
  Disabled -> "disabled"

queueStatusT :: Text -> Maybe QueueStatus
queueStatusT = \case
  "new" -> Just New
  "confirmed" -> Just Confirmed
  "secured" -> Just Secured
  "active" -> Just Active
  "disabled" -> Just Disabled
  _ -> Nothing

type AgentMsgId = Int64

-- | Result of received message integrity validation.
data MsgIntegrity = MsgOk | MsgError {errorInfo :: MsgErrorType}
  deriving (Eq, Show)

instance StrEncoding MsgIntegrity where
  strP = "OK" $> MsgOk <|> "ERR " *> (MsgError <$> strP)
  strEncode = \case
    MsgOk -> "OK"
    MsgError e -> "ERR " <> strEncode e

-- | Error of message integrity validation.
data MsgErrorType
  = MsgSkipped {fromMsgId :: AgentMsgId, toMsgId :: AgentMsgId}
  | MsgBadId {msgId :: AgentMsgId}
  | MsgBadHash
  | MsgDuplicate
  deriving (Eq, Show)

instance StrEncoding MsgErrorType where
  strP =
    "ID " *> (MsgBadId <$> A.decimal)
      <|> "NO_ID " *> (MsgSkipped <$> A.decimal <* A.space <*> A.decimal)
      <|> "HASH" $> MsgBadHash
      <|> "DUPLICATE" $> MsgDuplicate
  strEncode = \case
    MsgSkipped fromMsgId toMsgId ->
      B.unwords ["NO_ID", bshow fromMsgId, bshow toMsgId]
    MsgBadId aMsgId -> "ID " <> bshow aMsgId
    MsgBadHash -> "HASH"
    MsgDuplicate -> "DUPLICATE"

-- | Error type used in errors sent to agent clients.
data AgentErrorType
  = -- | command or response error
    CMD {cmdErr :: CommandErrorType, errContext :: String}
  | -- | connection errors
    CONN {connErr :: ConnectionErrorType}
  | -- | user not found in database
    NO_USER
  | -- | SMP protocol errors forwarded to agent clients
    SMP {serverAddress :: String, smpErr :: ErrorType}
  | -- | NTF protocol errors forwarded to agent clients
    NTF {serverAddress :: String, ntfErr :: ErrorType}
  | -- | XFTP protocol errors forwarded to agent clients
    XFTP {serverAddress :: String, xftpErr :: XFTPErrorType}
  | -- | XFTP agent errors
    FILE {fileErr :: FileErrorType}
  | -- | SMP proxy errors
    PROXY {proxyServer :: String, relayServer :: String, proxyErr :: ProxyClientError}
  | -- | XRCP protocol errors forwarded to agent clients
    RCP {rcpErr :: RCErrorType}
  | -- | SMP server errors
    BROKER {brokerAddress :: String, brokerErr :: BrokerErrorType}
  | -- | errors of other agents
    AGENT {agentErr :: SMPAgentError}
  | -- | agent implementation or dependency errors
    INTERNAL {internalErr :: String}
  | -- | critical agent errors that should be shown to the user, optionally with restart button
    CRITICAL {offerRestart :: Bool, criticalErr :: String}
  | -- | agent inactive
    INACTIVE
  deriving (Eq, Show, Exception)

-- | SMP agent protocol command or response error.
data CommandErrorType
  = -- | command is prohibited in this context
    PROHIBITED
  | -- | command syntax is invalid
    SYNTAX
  | -- | entity ID is required with this command
    NO_CONN
  | -- | message size is not correct (no terminating space)
    SIZE
  | -- | message does not fit in SMP block
    LARGE
  deriving (Eq, Read, Show, Exception)

-- | Connection error.
data ConnectionErrorType
  = -- | connection is not in the database
    NOT_FOUND
  | -- | connection already exists
    DUPLICATE
  | -- | connection is simplex, but operation requires another queue
    SIMPLEX
  | -- | connection not accepted on join HELLO after timeout
    NOT_ACCEPTED
  | -- | connection not available on reply confirmation/HELLO after timeout
    NOT_AVAILABLE
  deriving (Eq, Read, Show, Exception)

-- | Errors of another SMP agent.
data SMPAgentError
  = -- | client or agent message that failed to parse
    A_MESSAGE
  | -- | prohibited SMP/agent message
    A_PROHIBITED {prohibitedErr :: String}
  | -- | incompatible version of SMP client, agent or encryption protocols
    A_VERSION
  | -- | cannot decrypt message
    A_CRYPTO {cryptoErr :: AgentCryptoError}
  | -- | duplicate message - this error is detected by ratchet decryption - this message will be ignored and not shown
    -- it may also indicate a loss of ratchet synchronization (when only one message is sent via copied ratchet)
    A_DUPLICATE
  | -- | error in the message to add/delete/etc queue in connection
    A_QUEUE {queueErr :: String}
  deriving (Eq, Read, Show, Exception)

data AgentCryptoError
  = -- | AES decryption error
    DECRYPT_AES
  | -- CryptoBox decryption error
    DECRYPT_CB
  | -- | can't decrypt ratchet header, possibly ratchet out of sync due to device change
    RATCHET_HEADER
  | -- | earlier message number (or, possibly, skipped message that failed to decrypt?)
    RATCHET_EARLIER Word32
  | -- | too many skipped messages
    RATCHET_SKIPPED Word32
  | -- | ratchet synchronization error
    RATCHET_SYNC
  deriving (Eq, Read, Show, Exception)

cryptoErrToSyncState :: AgentCryptoError -> RatchetSyncState
cryptoErrToSyncState = \case
  DECRYPT_AES -> RSAllowed
  DECRYPT_CB -> RSAllowed
  RATCHET_HEADER -> RSRequired
  RATCHET_EARLIER _ -> RSAllowed
  RATCHET_SKIPPED _ -> RSRequired
  RATCHET_SYNC -> RSRequired

-- | SMP agent command and response parser for commands stored in db (fully parses binary bodies)
dbCommandP :: Parser ACommand
dbCommandP = commandP $ A.take =<< (A.decimal <* "\n")

instance StrEncoding ACommandTag where
  strP =
    A.takeTill (== ' ') >>= \case
      "NEW" -> pure NEW_
      "JOIN" -> pure JOIN_
      "LET" -> pure LET_
      "ACK" -> pure ACK_
      "SWCH" -> pure SWCH_
      "DEL" -> pure DEL_
      _ -> fail "bad ACommandTag"
  strEncode = \case
    NEW_ -> "NEW"
    JOIN_ -> "JOIN"
    LET_ -> "LET"
    ACK_ -> "ACK"
    SWCH_ -> "SWCH"
    DEL_ -> "DEL"

commandP :: Parser ByteString -> Parser ACommand
commandP binaryP =
  strP
    >>= \case
      NEW_ -> s (NEW <$> strP_ <*> strP_ <*> pqIKP <*> (strP <|> pure SMP.SMSubscribe))
      JOIN_ -> s (JOIN <$> strP_ <*> strP_ <*> pqSupP <*> (strP_ <|> pure SMP.SMSubscribe) <*> binaryP)
      LET_ -> s (LET <$> A.takeTill (== ' ') <* A.space <*> binaryP)
      ACK_ -> s (ACK <$> A.decimal <*> optional (A.space *> binaryP))
      SWCH_ -> pure SWCH
      DEL_ -> pure DEL
  where
    s :: Parser a -> Parser a
    s p = A.space *> p
    pqIKP :: Parser InitialKeys
    pqIKP = strP_ <|> pure (IKNoPQ PQSupportOff)
    pqSupP :: Parser PQSupport
    pqSupP = strP_ <|> pure PQSupportOff

-- | Serialize SMP agent command.
serializeCommand :: ACommand -> ByteString
serializeCommand = \case
  NEW ntfs cMode pqIK subMode -> s (NEW_, ntfs, cMode, pqIK, subMode)
  JOIN ntfs cReq pqSup subMode cInfo -> s (JOIN_, ntfs, cReq, pqSup, subMode, Str $ serializeBinary cInfo)
  LET confId cInfo -> B.unwords [s LET_, confId, serializeBinary cInfo]
  ACK mId rcptInfo_ -> s (ACK_, mId) <> maybe "" (B.cons ' ' . serializeBinary) rcptInfo_
  SWCH -> s SWCH_
  DEL -> s DEL_
  where
    s :: StrEncoding a => a -> ByteString
    s = strEncode

serializeBinary :: ByteString -> ByteString
serializeBinary body = bshow (B.length body) <> "\n" <> body

$(J.deriveJSON defaultJSON ''RcvQueueInfo)

$(J.deriveJSON defaultJSON ''SndQueueInfo)

$(J.deriveJSON defaultJSON ''ConnectionStats)

$(J.deriveJSON (sumTypeJSON fstToLower) ''MsgErrorType)

$(J.deriveJSON (sumTypeJSON fstToLower) ''MsgIntegrity)

$(J.deriveJSON (sumTypeJSON id) ''CommandErrorType)

$(J.deriveJSON (sumTypeJSON id) ''ConnectionErrorType)

$(J.deriveJSON (sumTypeJSON id) ''AgentCryptoError)

$(J.deriveJSON (sumTypeJSON id) ''SMPAgentError)

$(J.deriveJSON (sumTypeJSON id) ''AgentErrorType)

$(J.deriveJSON (enumJSON $ dropPrefix "QD") ''QueueDirection)

$(J.deriveJSON (enumJSON $ dropPrefix "SP") ''SwitchPhase)
