{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Simplex.Messaging.Agent.Store where

import Control.Concurrent.STM (TVar)
import Control.Exception (Exception)
import Crypto.Random (ChaChaDRG)
import Data.ByteString.Char8 (ByteString)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Time (UTCTime)
import Data.Type.Equality
import Simplex.Messaging.Agent.Protocol
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet (RatchetX448, SkippedMsgDiff, SkippedMsgKeys)
import Simplex.Messaging.Notifications.Client
import Simplex.Messaging.Notifications.Protocol (DeviceToken, NtfTknStatus, NtfTokenId)
import Simplex.Messaging.Protocol
  ( MsgBody,
    MsgId,
    RcvDhSecret,
    RcvPrivateSignKey,
    SndPrivateSignKey,
  )
import qualified Simplex.Messaging.Protocol as SMP

-- * Store management

-- | Store class type. Defines store access methods for implementations.
class Monad m => MonadAgentStore s m where
  -- Queue and Connection management
  createRcvConn :: s -> TVar ChaChaDRG -> ConnData -> RcvQueue -> SConnectionMode c -> m ConnId
  createSndConn :: s -> TVar ChaChaDRG -> ConnData -> SndQueue -> m ConnId
  getConn :: s -> ConnId -> m SomeConn
  getRcvConn :: s -> SMPServer -> SMP.RecipientId -> m SomeConn
  deleteConn :: s -> ConnId -> m ()
  upgradeRcvConnToDuplex :: s -> ConnId -> SndQueue -> m ()
  upgradeSndConnToDuplex :: s -> ConnId -> RcvQueue -> m ()
  setRcvQueueStatus :: s -> RcvQueue -> QueueStatus -> m ()
  setRcvQueueConfirmedE2E :: s -> RcvQueue -> C.DhSecretX25519 -> m ()
  setSndQueueStatus :: s -> SndQueue -> QueueStatus -> m ()

  -- Confirmations
  createConfirmation :: s -> TVar ChaChaDRG -> NewConfirmation -> m ConfirmationId
  acceptConfirmation :: s -> ConfirmationId -> ConnInfo -> m AcceptedConfirmation
  getAcceptedConfirmation :: s -> ConnId -> m AcceptedConfirmation
  removeConfirmations :: s -> ConnId -> m ()

  -- Invitations - sent via Contact connections
  createInvitation :: s -> TVar ChaChaDRG -> NewInvitation -> m InvitationId
  getInvitation :: s -> InvitationId -> m Invitation
  acceptInvitation :: s -> InvitationId -> ConnInfo -> m ()
  deleteInvitation :: s -> ConnId -> InvitationId -> m ()

  -- Msg management
  updateRcvIds :: s -> ConnId -> m (InternalId, InternalRcvId, PrevExternalSndId, PrevRcvMsgHash)
  createRcvMsg :: s -> ConnId -> RcvMsgData -> m ()
  updateSndIds :: s -> ConnId -> m (InternalId, InternalSndId, PrevSndMsgHash)
  createSndMsg :: s -> ConnId -> SndMsgData -> m ()
  getPendingMsgData :: s -> ConnId -> InternalId -> m (Maybe RcvQueue, (AgentMessageType, MsgBody, InternalTs))
  getPendingMsgs :: s -> ConnId -> m [InternalId]
  checkRcvMsg :: s -> ConnId -> InternalId -> m ()
  deleteMsg :: s -> ConnId -> InternalId -> m ()

  -- Double ratchet persistence
  createRatchetX3dhKeys :: s -> ConnId -> C.PrivateKeyX448 -> C.PrivateKeyX448 -> m ()
  getRatchetX3dhKeys :: s -> ConnId -> m (C.PrivateKeyX448, C.PrivateKeyX448)
  createRatchet :: s -> ConnId -> RatchetX448 -> m ()
  getRatchet :: s -> ConnId -> m RatchetX448
  getSkippedMsgKeys :: s -> ConnId -> m SkippedMsgKeys
  updateRatchet :: s -> ConnId -> RatchetX448 -> SkippedMsgDiff -> m ()

  -- Notification device token persistence
  createNtfToken :: s -> NtfToken -> m ()

  -- TODO this should also return old tokens so that they are deleted from the server
  getDeviceNtfToken :: s -> DeviceToken -> m (Maybe NtfToken) -- return current token if it exists
  updateNtfTokenRegistration :: s -> NtfToken -> NtfTokenId -> C.DhSecretX25519 -> m ()
  updateNtfToken :: s -> NtfToken -> NtfTknStatus -> Maybe NtfTknAction -> m ()

-- * Queue types

-- | A receive queue. SMP queue through which the agent receives messages from a sender.
data RcvQueue = RcvQueue
  { server :: SMPServer,
    -- | recipient queue ID
    rcvId :: SMP.RecipientId,
    -- | key used by the recipient to sign transmissions
    rcvPrivateKey :: RcvPrivateSignKey,
    -- | shared DH secret used to encrypt/decrypt message bodies from server to recipient
    rcvDhSecret :: RcvDhSecret,
    -- | private DH key related to public sent to sender out-of-band (to agree simple per-queue e2e)
    e2ePrivKey :: C.PrivateKeyX25519,
    -- | public sender's DH key and agreed shared DH secret for simple per-queue e2e
    e2eDhSecret :: Maybe C.DhSecretX25519,
    -- | sender queue ID
    sndId :: Maybe SMP.SenderId,
    -- | queue status
    status :: QueueStatus
  }
  deriving (Eq, Show)

-- | A send queue. SMP queue through which the agent sends messages to a recipient.
data SndQueue = SndQueue
  { server :: SMPServer,
    -- | sender queue ID
    sndId :: SMP.SenderId,
    -- | key pair used by the sender to sign transmissions
    sndPublicKey :: Maybe C.APublicVerifyKey,
    sndPrivateKey :: SndPrivateSignKey,
    -- | DH public key used to negotiate per-queue e2e encryption
    e2ePubKey :: Maybe C.PublicKeyX25519,
    -- | shared DH secret agreed for simple per-queue e2e encryption
    e2eDhSecret :: C.DhSecretX25519,
    -- | queue status
    status :: QueueStatus
  }
  deriving (Eq, Show)

-- * Connection types

-- | Type of a connection.
data ConnType = CRcv | CSnd | CDuplex | CContact deriving (Eq, Show)

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
  RcvConnection :: ConnData -> RcvQueue -> Connection CRcv
  SndConnection :: ConnData -> SndQueue -> Connection CSnd
  DuplexConnection :: ConnData -> RcvQueue -> SndQueue -> Connection CDuplex
  ContactConnection :: ConnData -> RcvQueue -> Connection CContact

deriving instance Eq (Connection d)

deriving instance Show (Connection d)

data SConnType :: ConnType -> Type where
  SCRcv :: SConnType CRcv
  SCSnd :: SConnType CSnd
  SCDuplex :: SConnType CDuplex
  SCContact :: SConnType CContact

connType :: SConnType c -> ConnType
connType SCRcv = CRcv
connType SCSnd = CSnd
connType SCDuplex = CDuplex
connType SCContact = CContact

deriving instance Eq (SConnType d)

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

instance Eq SomeConn where
  SomeConn d c == SomeConn d' c' = case testEquality d d' of
    Just Refl -> c == c'
    _ -> False

deriving instance Show SomeConn

newtype ConnData = ConnData {connId :: ConnId}
  deriving (Eq, Show)

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

-- * Message data containers - used on Msg creation to reduce number of parameters

data RcvMsgData = RcvMsgData
  { msgMeta :: MsgMeta,
    msgType :: AgentMessageType,
    msgBody :: MsgBody,
    internalRcvId :: InternalRcvId,
    internalHash :: MsgHash,
    externalPrevSndHash :: MsgHash
  }

data SndMsgData = SndMsgData
  { internalId :: InternalId,
    internalSndId :: InternalSndId,
    internalTs :: InternalTs,
    msgType :: AgentMessageType,
    msgBody :: MsgBody,
    internalHash :: MsgHash,
    prevMsgHash :: MsgHash
  }

data PendingMsg = PendingMsg
  { connId :: ConnId,
    msgId :: InternalId
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

type InternalTs = UTCTime

-- * Store errors

-- | Agent store error.
data StoreError
  = -- | IO exceptions in store actions.
    SEInternal ByteString
  | -- | Failed to generate unique random ID
    SEUniqueID
  | -- | Connection not found (or both queues absent).
    SEConnNotFound
  | -- | Connection already used.
    SEConnDuplicate
  | -- | Wrong connection type, e.g. "send" connection when "receive" or "duplex" is expected, or vice versa.
    -- 'upgradeRcvConnToDuplex' and 'upgradeSndConnToDuplex' do not allow duplex connections - they would also return this error.
    SEBadConnType ConnType
  | -- | Confirmation not found.
    SEConfirmationNotFound
  | -- | Invitation not found
    SEInvitationNotFound
  | -- | Message not found
    SEMsgNotFound
  | -- | Currently not used. The intention was to pass current expected queue status in methods,
    -- as we always know what it should be at any stage of the protocol,
    -- and in case it does not match use this error.
    SEBadQueueStatus
  | -- | connection does not have associated double-ratchet state
    SERatchetNotFound
  | -- | connection does not have associated x3dh keys
    SEX3dhKeysNotFound
  | -- | Used in `getMsg` that is not implemented/used. TODO remove.
    SENotImplemented
  deriving (Eq, Show, Exception)
