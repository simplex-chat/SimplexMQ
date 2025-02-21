{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Simplex.Messaging.Agent.Store where

import Control.Exception (Exception)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.List (find)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as L
import Data.Maybe (isJust)
import Data.Time (UTCTime)
import Data.Type.Equality
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval (RI2State)
import Simplex.Messaging.Agent.Store.Common
import Simplex.Messaging.Agent.Store.Interface (createDBStore)
import Simplex.Messaging.Agent.Store.Migrations.App (appMigrations)
import Simplex.Messaging.Agent.Store.Shared (MigrationConfirmation (..), MigrationError (..))
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet (MsgEncryptKeyX448, PQEncryption, PQSupport, RatchetX448)
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
  ( MsgBody,
    MsgFlags,
    MsgId,
    NotifierId,
    NtfPrivateAuthKey,
    NtfPublicAuthKey,
    RcvDhSecret,
    RcvNtfDhSecret,
    RcvPrivateAuthKey,
    SenderCanSecure,
    SndPrivateAuthKey,
    SndPublicAuthKey,
    VersionSMPC,
  )
import qualified Simplex.Messaging.Protocol as SMP

createStore :: DBOpts -> MigrationConfirmation -> IO (Either MigrationError DBStore)
createStore dbOpts = createDBStore dbOpts appMigrations

-- * Queue types

data QueueStored = QSStored | QSNew

data SQueueStored (q :: QueueStored) where
  SQSStored :: SQueueStored 'QSStored
  SQSNew :: SQueueStored 'QSNew

data DBQueueId (q :: QueueStored) where
  DBQueueId :: Int64 -> DBQueueId 'QSStored
  DBNewQueue :: DBQueueId 'QSNew

deriving instance Show (DBQueueId q)

type RcvQueue = StoredRcvQueue 'QSStored

type NewRcvQueue = StoredRcvQueue 'QSNew

-- | A receive queue. SMP queue through which the agent receives messages from a sender.
data StoredRcvQueue (q :: QueueStored) = RcvQueue
  { userId :: UserId,
    connId :: ConnId,
    server :: SMPServer,
    -- | recipient queue ID
    rcvId :: SMP.RecipientId,
    -- | key used by the recipient to authorize transmissions
    rcvPrivateKey :: RcvPrivateAuthKey,
    -- | shared DH secret used to encrypt/decrypt message bodies from server to recipient
    rcvDhSecret :: RcvDhSecret,
    -- | private DH key related to public sent to sender out-of-band (to agree simple per-queue e2e)
    e2ePrivKey :: C.PrivateKeyX25519,
    -- | public sender's DH key and agreed shared DH secret for simple per-queue e2e
    e2eDhSecret :: Maybe C.DhSecretX25519,
    -- | sender queue ID
    sndId :: SMP.SenderId,
    -- | sender can secure the queue
    sndSecure :: SenderCanSecure,
    -- | queue status
    status :: QueueStatus,
    -- | database queue ID (within connection)
    dbQueueId :: DBQueueId q,
    -- | True for a primary or a next primary queue of the connection (next if dbReplaceQueueId is set)
    primary :: Bool,
    -- | database queue ID to replace, Nothing if this queue is not replacing another, `Just Nothing` is used for replacing old queues
    dbReplaceQueueId :: Maybe Int64,
    rcvSwchStatus :: Maybe RcvSwitchStatus,
    -- | SMP client version
    smpClientVersion :: VersionSMPC,
    -- | credentials used in context of notifications
    clientNtfCreds :: Maybe ClientNtfCreds,
    deleteErrors :: Int
  }
  deriving (Show)

rcvQueueInfo :: RcvQueue -> RcvQueueInfo
rcvQueueInfo rq@RcvQueue {server, rcvSwchStatus} =
  RcvQueueInfo {rcvServer = server, rcvSwitchStatus = rcvSwchStatus, canAbortSwitch = canAbortRcvSwitch rq}

canAbortRcvSwitch :: RcvQueue -> Bool
canAbortRcvSwitch = maybe False canAbort . rcvSwchStatus
  where
    canAbort = \case
      RSSwitchStarted -> True
      RSSendingQADD -> True
      -- if switch is in RSSendingQUSE, a race condition with sender deleting the original queue is possible
      RSSendingQUSE -> False
      -- if switch is in RSReceivedMessage status, aborting switch (deleting new queue)
      -- will break the connection because the sender would have original queue deleted
      RSReceivedMessage -> False

data ClientNtfCreds = ClientNtfCreds
  { -- | key pair to be used by the notification server to authorize transmissions
    ntfPublicKey :: NtfPublicAuthKey,
    ntfPrivateKey :: NtfPrivateAuthKey,
    -- | queue ID to be used by the notification server for NSUB command
    notifierId :: NotifierId,
    -- | shared DH secret used to encrypt/decrypt notification metadata (NMsgMeta) from server to recipient
    rcvNtfDhSecret :: RcvNtfDhSecret
  }
  deriving (Show)

type SndQueue = StoredSndQueue 'QSStored

type NewSndQueue = StoredSndQueue 'QSNew

-- | A send queue. SMP queue through which the agent sends messages to a recipient.
data StoredSndQueue (q :: QueueStored) = SndQueue
  { userId :: UserId,
    connId :: ConnId,
    server :: SMPServer,
    -- | sender queue ID
    sndId :: SMP.SenderId,
    -- | sender can secure the queue
    sndSecure :: SenderCanSecure,
    -- | key pair used by the sender to authorize transmissions
    -- TODO combine keys to key pair so that types match
    sndPublicKey :: SndPublicAuthKey,
    sndPrivateKey :: SndPrivateAuthKey,
    -- | DH public key used to negotiate per-queue e2e encryption
    e2ePubKey :: Maybe C.PublicKeyX25519,
    -- | shared DH secret agreed for simple per-queue e2e encryption
    e2eDhSecret :: C.DhSecretX25519,
    -- | queue status
    status :: QueueStatus,
    -- | database queue ID (within connection)
    dbQueueId :: DBQueueId q,
    -- | True for a primary or a next primary queue of the connection (next if dbReplaceQueueId is set)
    primary :: Bool,
    -- | ID of the queue this one is replacing
    dbReplaceQueueId :: Maybe Int64,
    sndSwchStatus :: Maybe SndSwitchStatus,
    -- | SMP client version
    smpClientVersion :: VersionSMPC
  }
  deriving (Show)

sndQueueInfo :: SndQueue -> SndQueueInfo
sndQueueInfo SndQueue {server, sndSwchStatus} =
  SndQueueInfo {sndServer = server, sndSwitchStatus = sndSwchStatus}

instance SMPQueue RcvQueue where
  qServer RcvQueue {server} = server
  {-# INLINE qServer #-}
  queueId RcvQueue {rcvId} = rcvId
  {-# INLINE queueId #-}

instance SMPQueue NewRcvQueue where
  qServer RcvQueue {server} = server
  {-# INLINE qServer #-}
  queueId RcvQueue {rcvId} = rcvId
  {-# INLINE queueId #-}

instance SMPQueue SndQueue where
  qServer SndQueue {server} = server
  {-# INLINE qServer #-}
  queueId SndQueue {sndId} = sndId
  {-# INLINE queueId #-}

findQ :: SMPQueue q => (SMPServer, SMP.QueueId) -> NonEmpty q -> Maybe q
findQ = find . sameQueue
{-# INLINE findQ #-}

removeQ :: SMPQueue q => (SMPServer, SMP.QueueId) -> NonEmpty q -> Maybe (q, [q])
removeQ = removeQP . sameQueue
{-# INLINE removeQ #-}

removeQP :: (q -> Bool) -> NonEmpty q -> Maybe (q, [q])
removeQP p qs = case L.break p qs of
  (_, []) -> Nothing
  (qs1, q : qs2) -> Just (q, qs1 <> qs2)

sndAddress :: RcvQueue -> (SMPServer, SMP.SenderId)
sndAddress RcvQueue {server, sndId} = (server, sndId)
{-# INLINE sndAddress #-}

findRQ :: (SMPServer, SMP.SenderId) -> NonEmpty RcvQueue -> Maybe RcvQueue
findRQ sAddr = find $ sameQAddress sAddr . sndAddress
{-# INLINE findRQ #-}

switchingRQ :: NonEmpty RcvQueue -> Maybe RcvQueue
switchingRQ = find $ isJust . rcvSwchStatus
{-# INLINE switchingRQ #-}

updatedQs :: SMPQueueRec q => q -> NonEmpty q -> NonEmpty q
updatedQs q = L.map $ \q' -> if dbQId q == dbQId q' then q else q'
{-# INLINE updatedQs #-}

class SMPQueue q => SMPQueueRec q where
  qUserId :: q -> UserId
  qConnId :: q -> ConnId
  dbQId :: q -> Int64
  dbReplaceQId :: q -> Maybe Int64

instance SMPQueueRec RcvQueue where
  qUserId RcvQueue {userId} = userId
  {-# INLINE qUserId #-}
  qConnId RcvQueue {connId} = connId
  {-# INLINE qConnId #-}
  dbQId RcvQueue {dbQueueId = DBQueueId qId} = qId
  {-# INLINE dbQId #-}
  dbReplaceQId RcvQueue {dbReplaceQueueId} = dbReplaceQueueId
  {-# INLINE dbReplaceQId #-}

instance SMPQueueRec SndQueue where
  qUserId SndQueue {userId} = userId
  {-# INLINE qUserId #-}
  qConnId SndQueue {connId} = connId
  {-# INLINE qConnId #-}
  dbQId SndQueue {dbQueueId = DBQueueId qId} = qId
  {-# INLINE dbQId #-}
  dbReplaceQId SndQueue {dbReplaceQueueId} = dbReplaceQueueId
  {-# INLINE dbReplaceQId #-}

-- * Connection types

-- | Type of a connection.
data ConnType = CNew | CRcv | CSnd | CDuplex | CContact deriving (Eq, Show)

-- | Connection of a specific type.
--
-- - RcvConnection is a connection that only has a receive queue set up,
--   typically created by a recipient initiating a duplex connection.
--
-- - SndConnection is a connection that only has a send queue set up, typically
--   created by a sender joining a duplex connection through a recipient's invitation.
--
-- - DuplexConnection is a connection that has both receive and send queues set up,
--   typically created by upgrading a receive or a send connection with a missing queue.
data Connection (d :: ConnType) where
  NewConnection :: ConnData -> Connection CNew
  RcvConnection :: ConnData -> RcvQueue -> Connection CRcv
  SndConnection :: ConnData -> SndQueue -> Connection CSnd
  DuplexConnection :: ConnData -> NonEmpty RcvQueue -> NonEmpty SndQueue -> Connection CDuplex
  ContactConnection :: ConnData -> RcvQueue -> Connection CContact

deriving instance Show (Connection d)

toConnData :: Connection d -> ConnData
toConnData = \case
  NewConnection cData -> cData
  RcvConnection cData _ -> cData
  SndConnection cData _ -> cData
  DuplexConnection cData _ _ -> cData
  ContactConnection cData _ -> cData

updateConnection :: ConnData -> Connection d -> Connection d
updateConnection cData = \case
  NewConnection _ -> NewConnection cData
  RcvConnection _ rq -> RcvConnection cData rq
  SndConnection _ sq -> SndConnection cData sq
  DuplexConnection _ rqs sqs -> DuplexConnection cData rqs sqs
  ContactConnection _ rq -> ContactConnection cData rq

data SConnType :: ConnType -> Type where
  SCNew :: SConnType CNew
  SCRcv :: SConnType CRcv
  SCSnd :: SConnType CSnd
  SCDuplex :: SConnType CDuplex
  SCContact :: SConnType CContact

connType :: SConnType c -> ConnType
connType SCNew = CNew
connType SCRcv = CRcv
connType SCSnd = CSnd
connType SCDuplex = CDuplex
connType SCContact = CContact

deriving instance Show (SConnType d)

instance TestEquality SConnType where
  testEquality SCRcv SCRcv = Just Refl
  testEquality SCSnd SCSnd = Just Refl
  testEquality SCDuplex SCDuplex = Just Refl
  testEquality SCContact SCContact = Just Refl
  testEquality _ _ = Nothing

-- | Connection of an unknown type.
-- Used to refer to an arbitrary connection when retrieving from store.
data SomeConn = forall d. SomeConn (SConnType d) (Connection d)

deriving instance Show SomeConn

data ConnData = ConnData
  { connId :: ConnId,
    userId :: UserId,
    connAgentVersion :: VersionSMPA,
    enableNtfs :: Bool,
    lastExternalSndId :: PrevExternalSndId,
    deleted :: Bool,
    ratchetSyncState :: RatchetSyncState,
    pqSupport :: PQSupport
  }
  deriving (Eq, Show)

-- this function should be mirrored in the clients
ratchetSyncAllowed :: ConnData -> Bool
ratchetSyncAllowed ConnData {ratchetSyncState, connAgentVersion} =
  connAgentVersion >= ratchetSyncSMPAgentVersion && (ratchetSyncState `elem` ([RSAllowed, RSRequired] :: [RatchetSyncState]))

-- this function should be mirrored in the clients
ratchetSyncSendProhibited :: ConnData -> Bool
ratchetSyncSendProhibited ConnData {ratchetSyncState} =
  ratchetSyncState `elem` ([RSRequired, RSStarted, RSAgreed] :: [RatchetSyncState])

data PendingCommand = PendingCommand
  { cmdId :: AsyncCmdId,
    corrId :: ACorrId,
    userId :: UserId,
    connId :: ConnId,
    command :: AgentCommand
  }

data AgentCmdType = ACClient | ACInternal

instance StrEncoding AgentCmdType where
  strEncode = \case
    ACClient -> "CLIENT"
    ACInternal -> "INTERNAL"
  strP =
    A.takeTill (== ' ') >>= \case
      "CLIENT" -> pure ACClient
      "INTERNAL" -> pure ACInternal
      _ -> fail "bad AgentCmdType"

data AgentCommand
  = AClientCommand ACommand
  | AInternalCommand InternalCommand

instance StrEncoding AgentCommand where
  strEncode = \case
    AClientCommand cmd -> strEncode (ACClient, Str $ serializeCommand cmd)
    AInternalCommand cmd -> strEncode (ACInternal, cmd)
  strP =
    strP_ >>= \case
      ACClient -> AClientCommand <$> dbCommandP
      ACInternal -> AInternalCommand <$> strP

data AgentCommandTag
  = AClientCommandTag ACommandTag
  | AInternalCommandTag InternalCommandTag
  deriving (Show)

instance StrEncoding AgentCommandTag where
  strEncode = \case
    AClientCommandTag t -> strEncode (ACClient, t)
    AInternalCommandTag t -> strEncode (ACInternal, t)
  strP =
    strP_ >>= \case
      ACClient -> AClientCommandTag <$> strP
      ACInternal -> AInternalCommandTag <$> strP

data InternalCommand
  = ICAck SMP.RecipientId MsgId
  | ICAckDel SMP.RecipientId MsgId InternalId
  | ICAllowSecure SMP.RecipientId (Maybe SMP.SndPublicAuthKey)
  | ICDuplexSecure SMP.RecipientId SMP.SndPublicAuthKey
  | ICDeleteConn
  | ICDeleteRcvQueue SMP.RecipientId
  | ICQSecure SMP.RecipientId SMP.SndPublicAuthKey
  | ICQDelete SMP.RecipientId

data InternalCommandTag
  = ICAck_
  | ICAckDel_
  | ICAllowSecure_
  | ICDuplexSecure_
  | ICDeleteConn_
  | ICDeleteRcvQueue_
  | ICQSecure_
  | ICQDelete_
  deriving (Show)

instance StrEncoding InternalCommand where
  strEncode = \case
    ICAck rId srvMsgId -> strEncode (ICAck_, rId, srvMsgId)
    ICAckDel rId srvMsgId mId -> strEncode (ICAckDel_, rId, srvMsgId, mId)
    ICAllowSecure rId sndKey -> strEncode (ICAllowSecure_, rId, sndKey)
    ICDuplexSecure rId sndKey -> strEncode (ICDuplexSecure_, rId, sndKey)
    ICDeleteConn -> strEncode ICDeleteConn_
    ICDeleteRcvQueue rId -> strEncode (ICDeleteRcvQueue_, rId)
    ICQSecure rId senderKey -> strEncode (ICQSecure_, rId, senderKey)
    ICQDelete rId -> strEncode (ICQDelete_, rId)
  strP =
    strP >>= \case
      ICAck_ -> ICAck <$> _strP <*> _strP
      ICAckDel_ -> ICAckDel <$> _strP <*> _strP <*> _strP
      ICAllowSecure_ -> ICAllowSecure <$> _strP <*> _strP
      ICDuplexSecure_ -> ICDuplexSecure <$> _strP <*> _strP
      ICDeleteConn_ -> pure ICDeleteConn
      ICDeleteRcvQueue_ -> ICDeleteRcvQueue <$> _strP
      ICQSecure_ -> ICQSecure <$> _strP <*> _strP
      ICQDelete_ -> ICQDelete <$> _strP

instance StrEncoding InternalCommandTag where
  strEncode = \case
    ICAck_ -> "ACK"
    ICAckDel_ -> "ACK_DEL"
    ICAllowSecure_ -> "ALLOW_SECURE"
    ICDuplexSecure_ -> "DUPLEX_SECURE"
    ICDeleteConn_ -> "DELETE_CONN"
    ICDeleteRcvQueue_ -> "DELETE_RCV_QUEUE"
    ICQSecure_ -> "QSECURE"
    ICQDelete_ -> "QDELETE"
  strP =
    A.takeTill (== ' ') >>= \case
      "ACK" -> pure ICAck_
      "ACK_DEL" -> pure ICAckDel_
      "ALLOW_SECURE" -> pure ICAllowSecure_
      "DUPLEX_SECURE" -> pure ICDuplexSecure_
      "DELETE_CONN" -> pure ICDeleteConn_
      "DELETE_RCV_QUEUE" -> pure ICDeleteRcvQueue_
      "QSECURE" -> pure ICQSecure_
      "QDELETE" -> pure ICQDelete_
      _ -> fail "bad InternalCommandTag"

agentCommandTag :: AgentCommand -> AgentCommandTag
agentCommandTag = \case
  AClientCommand cmd -> AClientCommandTag $ aCommandTag cmd
  AInternalCommand cmd -> AInternalCommandTag $ internalCmdTag cmd

internalCmdTag :: InternalCommand -> InternalCommandTag
internalCmdTag = \case
  ICAck {} -> ICAck_
  ICAckDel {} -> ICAckDel_
  ICAllowSecure {} -> ICAllowSecure_
  ICDuplexSecure {} -> ICDuplexSecure_
  ICDeleteConn -> ICDeleteConn_
  ICDeleteRcvQueue {} -> ICDeleteRcvQueue_
  ICQSecure {} -> ICQSecure_
  ICQDelete _ -> ICQDelete_

-- * Confirmation types

data NewConfirmation = NewConfirmation
  { connId :: ConnId,
    senderConf :: SMPConfirmation,
    ratchetState :: RatchetX448
  }

data AcceptedConfirmation = AcceptedConfirmation
  { confirmationId :: ConfirmationId,
    connId :: ConnId,
    senderConf :: SMPConfirmation,
    ratchetState :: RatchetX448,
    ownConnInfo :: ConnInfo
  }

-- * Invitations

data NewInvitation = NewInvitation
  { contactConnId :: ConnId,
    connReq :: ConnectionRequestUri 'CMInvitation,
    recipientConnInfo :: ConnInfo
  }

data Invitation = Invitation
  { invitationId :: InvitationId,
    contactConnId :: ConnId,
    connReq :: ConnectionRequestUri 'CMInvitation,
    recipientConnInfo :: ConnInfo,
    ownConnInfo :: Maybe ConnInfo,
    accepted :: Bool
  }

-- * Message integrity validation types

-- | Corresponds to `last_external_snd_msg_id` in `connections` table
type PrevExternalSndId = Int64

-- | Corresponds to `last_rcv_msg_hash` in `connections` table
type PrevRcvMsgHash = MsgHash

-- | Corresponds to `last_snd_msg_hash` in `connections` table
type PrevSndMsgHash = MsgHash

-- * Message data containers

data RcvMsgData = RcvMsgData
  { msgMeta :: MsgMeta,
    msgType :: AgentMessageType,
    msgFlags :: MsgFlags,
    msgBody :: MsgBody,
    internalRcvId :: InternalRcvId,
    internalHash :: MsgHash,
    externalPrevSndHash :: MsgHash,
    encryptedMsgHash :: MsgHash
  }

data RcvMsg = RcvMsg
  { internalId :: InternalId,
    msgMeta :: MsgMeta,
    msgType :: AgentMessageType,
    msgBody :: MsgBody,
    internalHash :: MsgHash,
    msgReceipt :: Maybe MsgReceipt, -- if this message is a delivery receipt
    userAck :: Bool
  }

data SndMsgData = SndMsgData
  { internalId :: InternalId,
    internalSndId :: InternalSndId,
    internalTs :: InternalTs,
    msgType :: AgentMessageType,
    msgFlags :: MsgFlags,
    msgBody :: MsgBody,
    pqEncryption :: PQEncryption,
    internalHash :: MsgHash,
    prevMsgHash :: MsgHash,
    sndMsgPrepData_ :: Maybe SndMsgPrepData
  }

data SndMsgPrepData = SndMsgPrepData
  { encryptKey :: MsgEncryptKeyX448,
    paddedLen :: Int,
    sndMsgBodyId :: Int64
  }
  deriving (Show)

data SndMsg = SndMsg
  { internalId :: InternalId,
    internalSndId :: InternalSndId,
    msgType :: AgentMessageType,
    internalHash :: MsgHash,
    msgReceipt :: Maybe MsgReceipt
  }

data PendingMsgData = PendingMsgData
  { msgId :: InternalId,
    msgType :: AgentMessageType,
    msgFlags :: MsgFlags,
    msgBody :: MsgBody,
    pqEncryption :: PQEncryption,
    msgRetryState :: Maybe RI2State,
    internalTs :: InternalTs,
    internalSndId :: InternalSndId,
    prevMsgHash :: PrevSndMsgHash,
    pendingMsgPrepData_ :: Maybe PendingMsgPrepData
  }
  deriving (Show)

data PendingMsgPrepData = PendingMsgPrepData
  { encryptKey :: MsgEncryptKeyX448,
    paddedLen :: Int,
    sndMsgBody :: AMessage
  }
  deriving (Show)

-- internal Ids are newtypes to prevent mixing them up
newtype InternalRcvId = InternalRcvId {unRcvId :: Int64} deriving (Eq, Show)

type ExternalSndId = Int64

type ExternalSndTs = UTCTime

type BrokerId = MsgId

type BrokerTs = UTCTime

newtype InternalSndId = InternalSndId {unSndId :: Int64} deriving (Eq, Show)

-- | Base message data independent of direction.
data MsgBase = MsgBase
  { connId :: ConnId,
    -- | Monotonically increasing id of a message per connection, internal to the agent.
    -- Internal Id preserves ordering between both received and sent messages, and is needed
    -- to track the order of the conversation (which can be different for the sender / receiver)
    -- and address messages in commands. External [sender] Id cannot be used for this purpose
    -- due to a possibility of implementation errors in different agents.
    internalId :: InternalId,
    internalTs :: InternalTs,
    msgBody :: MsgBody,
    -- | Hash of the message as computed by agent.
    internalHash :: MsgHash
  }
  deriving (Eq, Show)

newtype InternalId = InternalId {unId :: Int64} deriving (Eq, Show)

instance StrEncoding InternalId where
  strEncode = strEncode . unId
  strP = InternalId <$> strP

type InternalTs = UTCTime

type AsyncCmdId = Int64

-- * Store errors

-- | Agent store error.
data StoreError
  = -- | IO exceptions in store actions.
    SEInternal ByteString
  | -- | Database busy
    SEDatabaseBusy ByteString
  | -- | Failed to generate unique random ID
    SEUniqueID
  | -- | User ID not found
    SEUserNotFound
  | -- | Connection not found (or both queues absent).
    SEConnNotFound
  | -- | Server not found.
    SEServerNotFound
  | -- | Connection already used.
    SEConnDuplicate
  | -- | Confirmed snd queue already exists.
    SESndQueueExists
  | -- | Wrong connection type, e.g. "send" connection when "receive" or "duplex" is expected, or vice versa.
    -- 'upgradeRcvConnToDuplex' and 'upgradeSndConnToDuplex' do not allow duplex connections - they would also return this error.
    SEBadConnType ConnType
  | -- | Confirmation not found.
    SEConfirmationNotFound
  | -- | Invitation not found
    SEInvitationNotFound String InvitationId
  | -- | Message not found
    SEMsgNotFound
  | -- | Command not found
    SECmdNotFound
  | -- | Currently not used. The intention was to pass current expected queue status in methods,
    -- as we always know what it should be at any stage of the protocol,
    -- and in case it does not match use this error.
    SEBadQueueStatus
  | -- | connection does not have associated double-ratchet state
    SERatchetNotFound
  | -- | connection does not have associated x3dh keys
    SEX3dhKeysNotFound
  | -- | Used to wrap agent errors inside store operations to avoid race conditions
    SEAgentError AgentErrorType
  | -- | XFTP Server not found.
    SEXFTPServerNotFound
  | -- | XFTP File not found.
    SEFileNotFound
  | -- | XFTP Deleted snd chunk replica not found.
    SEDeletedSndChunkReplicaNotFound
  | -- | Error when reading work item that suspends worker - do not use!
    SEWorkItemError ByteString
  | -- | Servers stats not found.
    SEServersStatsNotFound
  deriving (Eq, Show, Exception)
