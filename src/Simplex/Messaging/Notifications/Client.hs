{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Notifications.Client where

import Control.Monad.Except
import Control.Monad.Trans.Except
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Time (UTCTime)
import Data.Word (Word16)
import Database.SQLite.Simple.FromField (FromField (..))
import Database.SQLite.Simple.ToField (ToField (..))
import Simplex.Messaging.Client
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Notifications.Protocol
import Simplex.Messaging.Parsers (blobFieldDecoder)
import Simplex.Messaging.Protocol (NotifierId, NtfPrivateSignKey, ProtocolServer, RecipientId, SMPServer)

type NtfServer = ProtocolServer

type NtfClient = ProtocolClient NtfResponse

ntfRegisterToken :: NtfClient -> C.APrivateSignKey -> NewNtfEntity 'Token -> ExceptT ProtocolClientError IO (NtfTokenId, C.PublicKeyX25519)
ntfRegisterToken c pKey newTkn =
  sendNtfCommand c (Just pKey) "" (TNEW newTkn) >>= \case
    NRTknId tknId dhKey -> pure (tknId, dhKey)
    _ -> throwE PCEUnexpectedResponse

ntfVerifyToken :: NtfClient -> C.APrivateSignKey -> NtfTokenId -> NtfRegCode -> ExceptT ProtocolClientError IO ()
ntfVerifyToken c pKey tknId code = okNtfCommand (TVFY code) c pKey tknId

ntfCheckToken :: NtfClient -> C.APrivateSignKey -> NtfTokenId -> ExceptT ProtocolClientError IO NtfTknStatus
ntfCheckToken c pKey tknId =
  sendNtfCommand c (Just pKey) tknId TCHK >>= \case
    NRTkn stat -> pure stat
    _ -> throwE PCEUnexpectedResponse

ntfDeleteToken :: NtfClient -> C.APrivateSignKey -> NtfTokenId -> ExceptT ProtocolClientError IO ()
ntfDeleteToken = okNtfCommand TDEL

ntfEnableCron :: NtfClient -> C.APrivateSignKey -> NtfTokenId -> Word16 -> ExceptT ProtocolClientError IO ()
ntfEnableCron c pKey tknId int = okNtfCommand (TCRN int) c pKey tknId

ntfCreateSubscription :: NtfClient -> C.APrivateSignKey -> NewNtfEntity 'Subscription -> ExceptT ProtocolClientError IO NtfSubscriptionId
ntfCreateSubscription c pKey newSub =
  sendNtfCommand c (Just pKey) "" (SNEW newSub) >>= \case
    NRSubId subId -> pure subId
    _ -> throwE PCEUnexpectedResponse

ntfCheckSubscription :: NtfClient -> C.APrivateSignKey -> NtfSubscriptionId -> ExceptT ProtocolClientError IO NtfSubStatus
ntfCheckSubscription c pKey subId =
  sendNtfCommand c (Just pKey) subId SCHK >>= \case
    NRSub stat -> pure stat
    _ -> throwE PCEUnexpectedResponse

ntfDeleteSubscription :: NtfClient -> C.APrivateSignKey -> NtfSubscriptionId -> ExceptT ProtocolClientError IO ()
ntfDeleteSubscription = okNtfCommand SDEL

-- | Send notification server command
sendNtfCommand :: NtfEntityI e => NtfClient -> Maybe C.APrivateSignKey -> NtfEntityId -> NtfCommand e -> ExceptT ProtocolClientError IO NtfResponse
sendNtfCommand c pKey entId cmd = sendProtocolCommand c pKey entId (NtfCmd sNtfEntity cmd)

okNtfCommand :: NtfEntityI e => NtfCommand e -> NtfClient -> C.APrivateSignKey -> NtfEntityId -> ExceptT ProtocolClientError IO ()
okNtfCommand cmd c pKey entId =
  sendNtfCommand c (Just pKey) entId cmd >>= \case
    NROk -> return ()
    _ -> throwE PCEUnexpectedResponse

data NtfTknAction
  = NTARegister
  | NTAVerify NtfRegCode -- code to verify token
  | NTACheck
  | NTACron Word16
  | NTADelete
  deriving (Show)

instance Encoding NtfTknAction where
  smpEncode = \case
    NTARegister -> "R"
    NTAVerify code -> smpEncode ('V', code)
    NTACheck -> "C"
    NTACron interval -> smpEncode ('I', interval)
    NTADelete -> "D"
  smpP =
    A.anyChar >>= \case
      'R' -> pure NTARegister
      'V' -> NTAVerify <$> smpP
      'C' -> pure NTACheck
      'I' -> NTACron <$> smpP
      'D' -> pure NTADelete
      _ -> fail "bad NtfTknAction"

instance FromField NtfTknAction where fromField = blobFieldDecoder smpDecode

instance ToField NtfTknAction where toField = toField . smpEncode

data NtfToken = NtfToken
  { deviceToken :: DeviceToken,
    ntfServer :: NtfServer,
    ntfTokenId :: Maybe NtfTokenId,
    -- | key used by the ntf server to verify transmissions
    ntfPubKey :: C.APublicVerifyKey,
    -- | key used by the ntf client to sign transmissions
    ntfPrivKey :: C.APrivateSignKey,
    -- | client's DH keys (to repeat registration if necessary)
    ntfDhKeys :: C.KeyPair 'C.X25519,
    -- | shared DH secret used to encrypt/decrypt notifications e2e
    ntfDhSecret :: Maybe C.DhSecretX25519,
    -- | token status
    ntfTknStatus :: NtfTknStatus,
    -- | pending token action and the earliest time
    ntfTknAction :: Maybe NtfTknAction
  }
  deriving (Show)

newNtfToken :: DeviceToken -> NtfServer -> C.ASignatureKeyPair -> C.KeyPair 'C.X25519 -> NtfToken
newNtfToken deviceToken ntfServer (ntfPubKey, ntfPrivKey) ntfDhKeys =
  NtfToken
    { deviceToken,
      ntfServer,
      ntfTokenId = Nothing,
      ntfPubKey,
      ntfPrivKey,
      ntfDhKeys,
      ntfDhSecret = Nothing,
      ntfTknStatus = NTNew,
      ntfTknAction = Just NTARegister
    }

data NtfSubAction
  = NSANew NtfPrivateSignKey
  | NSACheck
  | NSADelete
  deriving (Show)

instance Encoding NtfSubAction where
  smpEncode = \case
    NSANew nKey -> smpEncode ('N', nKey)
    NSACheck -> "C"
    NSADelete -> "D"
  smpP =
    A.anyChar >>= \case
      'N' -> NSANew <$> smpP
      'C' -> pure NSACheck
      'D' -> pure NSADelete
      _ -> fail "bad NtfSubAction"

instance FromField NtfSubAction where fromField = blobFieldDecoder smpDecode

instance ToField NtfSubAction where toField = toField . smpEncode

data NtfSubSMPAction
  = NSAKey
  deriving (Show)

instance Encoding NtfSubSMPAction where
  smpEncode = \case
    NSAKey -> "K"
  smpP =
    A.anyChar >>= \case
      'K' -> pure NSAKey
      _ -> fail "bad NtfSubSMPAction"

instance FromField NtfSubSMPAction where fromField = blobFieldDecoder smpDecode

instance ToField NtfSubSMPAction where toField = toField . smpEncode

data NtfSubscription = NtfSubscription
  { ntfServer :: NtfServer,
    ntfSubId :: Maybe NtfSubscriptionId,
    ntfSubStatus :: NtfSubStatus,
    ntfSubActionTs :: UTCTime,
    ntfToken :: NtfToken, -- ?
    smpServer :: SMPServer, -- use SMPQueueNtf?
    rcvQueueId :: RecipientId,
    ntfQueueId :: Maybe NotifierId
  }
  deriving (Show)

newNtfSubscription :: NtfServer -> NtfToken -> SMPServer -> RecipientId -> UTCTime -> NtfSubscription
newNtfSubscription ntfServer ntfToken smpServer rcvQueueId ntfSubActionTs =
  NtfSubscription
    { ntfServer,
      ntfSubId = Nothing,
      ntfSubStatus = NSKey,
      ntfSubActionTs,
      ntfToken,
      smpServer,
      rcvQueueId,
      ntfQueueId = Nothing
    }
