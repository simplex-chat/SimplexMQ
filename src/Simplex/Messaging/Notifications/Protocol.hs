{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Simplex.Messaging.Notifications.Protocol where

import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Kind
import Data.Maybe (isNothing)
import Data.Type.Equality
import Data.Word (Word16)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Protocol
import Simplex.Messaging.Util ((<$?>))

data NtfEntity = Token | Subscription
  deriving (Show)

data SNtfEntity :: NtfEntity -> Type where
  SToken :: SNtfEntity 'Token
  SSubscription :: SNtfEntity 'Subscription

instance TestEquality SNtfEntity where
  testEquality SToken SToken = Just Refl
  testEquality SSubscription SSubscription = Just Refl
  testEquality _ _ = Nothing

deriving instance Show (SNtfEntity e)

class NtfEntityI (e :: NtfEntity) where sNtfEntity :: SNtfEntity e

instance NtfEntityI 'Token where sNtfEntity = SToken

instance NtfEntityI 'Subscription where sNtfEntity = SSubscription

data NtfCommandTag (e :: NtfEntity) where
  TNEW_ :: NtfCommandTag 'Token
  TVFY_ :: NtfCommandTag 'Token
  TDEL_ :: NtfCommandTag 'Token
  TCRN_ :: NtfCommandTag 'Token
  SNEW_ :: NtfCommandTag 'Subscription
  SCHK_ :: NtfCommandTag 'Subscription
  SDEL_ :: NtfCommandTag 'Subscription

deriving instance Show (NtfCommandTag e)

data NtfCmdTag = forall e. NtfEntityI e => NCT (SNtfEntity e) (NtfCommandTag e)

instance NtfEntityI e => Encoding (NtfCommandTag e) where
  smpEncode = \case
    TNEW_ -> "TNEW"
    TVFY_ -> "TVFY"
    TDEL_ -> "TDEL"
    TCRN_ -> "TCRN"
    SNEW_ -> "SNEW"
    SCHK_ -> "SCHK"
    SDEL_ -> "SDEL"
  smpP = messageTagP

instance Encoding NtfCmdTag where
  smpEncode (NCT _ t) = smpEncode t
  smpP = messageTagP

instance ProtocolMsgTag NtfCmdTag where
  decodeTag = \case
    "TNEW" -> Just $ NCT SToken TNEW_
    "TVFY" -> Just $ NCT SToken TVFY_
    "TDEL" -> Just $ NCT SToken TDEL_
    "TCRN" -> Just $ NCT SToken TCRN_
    "SNEW" -> Just $ NCT SSubscription SNEW_
    "SCHK" -> Just $ NCT SSubscription SCHK_
    "SDEL" -> Just $ NCT SSubscription SDEL_
    _ -> Nothing

instance NtfEntityI e => ProtocolMsgTag (NtfCommandTag e) where
  decodeTag s = decodeTag s >>= (\(NCT _ t) -> checkEntity' t)

type NtfRegistrationCode = ByteString

data NewNtfEntity (e :: NtfEntity) where
  NewNtfTkn :: DeviceToken -> C.APublicVerifyKey -> C.PublicKeyX25519 -> NewNtfEntity 'Token
  NewNtfSub :: NtfTokenId -> SMPQueueNtf -> NewNtfEntity 'Subscription

data ANewNtfEntity = forall e. NtfEntityI e => ANE (SNtfEntity e) (NewNtfEntity e)

instance NtfEntityI e => Encoding (NewNtfEntity e) where
  smpEncode = \case
    NewNtfTkn tkn verifyKey dhPubKey -> smpEncode ('T', tkn, verifyKey, dhPubKey)
    NewNtfSub tknId smpQueue -> smpEncode ('S', tknId, smpQueue)
  smpP = (\(ANE _ c) -> checkEntity c) <$?> smpP

instance Encoding ANewNtfEntity where
  smpEncode (ANE _ e) = smpEncode e
  smpP =
    A.anyChar >>= \case
      'T' -> ANE SToken <$> (NewNtfTkn <$> smpP <*> smpP <*> smpP)
      'S' -> ANE SSubscription <$> (NewNtfSub <$> smpP <*> smpP)
      _ -> fail "bad ANewNtfEntity"

instance Protocol NtfResponse where
  type ProtocolCommand NtfResponse = NtfCmd
  protocolError = \case
    NRErr e -> Just e
    _ -> Nothing

data NtfCommand (e :: NtfEntity) where
  -- | register new device token for notifications
  TNEW :: NewNtfEntity 'Token -> NtfCommand 'Token
  -- | verify token - uses e2e encrypted random string sent to the device via PN to confirm that the device has the token
  TVFY :: NtfRegistrationCode -> NtfCommand 'Token
  -- | delete token - all subscriptions will be removed and no more notifications will be sent
  TDEL :: NtfCommand 'Token
  -- | enable periodic background notification to fetch the new messages - interval is in minutes, minimum is 20, 0 to disable
  TCRN :: Word16 -> NtfCommand 'Token
  -- | create SMP subscription
  SNEW :: NewNtfEntity 'Subscription -> NtfCommand 'Subscription
  -- | check SMP subscription status (response is STAT)
  SCHK :: NtfCommand 'Subscription
  -- | delete SMP subscription
  SDEL :: NtfCommand 'Subscription

data NtfCmd = forall e. NtfEntityI e => NtfCmd (SNtfEntity e) (NtfCommand e)

instance NtfEntityI e => ProtocolEncoding (NtfCommand e) where
  type Tag (NtfCommand e) = NtfCommandTag e
  encodeProtocol = \case
    TNEW newTkn -> e (TNEW_, ' ', newTkn)
    TVFY code -> e (TVFY_, ' ', code)
    TDEL -> e TDEL_
    TCRN int -> e (TCRN_, ' ', int)
    SNEW newSub -> e (SNEW_, ' ', newSub)
    SCHK -> e SCHK_
    SDEL -> e SDEL_
    where
      e :: Encoding a => a -> ByteString
      e = smpEncode

  protocolP tag = (\(NtfCmd _ c) -> checkEntity c) <$?> protocolP (NCT (sNtfEntity @e) tag)

  checkCredentials (sig, _, entityId, _) cmd = case cmd of
    -- TNEW and SNEW must have signature but NOT token/subscription IDs
    TNEW {} -> sigNoEntity
    SNEW {} -> sigNoEntity
    -- other client commands must have both signature and entity ID
    _
      | isNothing sig || B.null entityId -> Left $ CMD NO_AUTH
      | otherwise -> Right cmd
    where
      sigNoEntity
        | isNothing sig = Left $ CMD NO_AUTH
        | not (B.null entityId) = Left $ CMD HAS_AUTH
        | otherwise = Right cmd

instance ProtocolEncoding NtfCmd where
  type Tag NtfCmd = NtfCmdTag
  encodeProtocol (NtfCmd _ c) = encodeProtocol c

  protocolP = \case
    NCT SToken tag ->
      NtfCmd SToken <$> case tag of
        TNEW_ -> TNEW <$> _smpP
        TVFY_ -> TVFY <$> _smpP
        TDEL_ -> pure TDEL
        TCRN_ -> TCRN <$> _smpP
    NCT SSubscription tag ->
      NtfCmd SSubscription <$> case tag of
        SNEW_ -> SNEW <$> _smpP
        SCHK_ -> pure SCHK
        SDEL_ -> pure SDEL

  checkCredentials t (NtfCmd e c) = NtfCmd e <$> checkCredentials t c

data NtfResponseTag
  = NRId_
  | NROk_
  | NRErr_
  | NRStat_
  deriving (Show)

instance Encoding NtfResponseTag where
  smpEncode = \case
    NRId_ -> "ID"
    NROk_ -> "OK"
    NRErr_ -> "ERR"
    NRStat_ -> "STAT"
  smpP = messageTagP

instance ProtocolMsgTag NtfResponseTag where
  decodeTag = \case
    "ID" -> Just NRId_
    "OK" -> Just NROk_
    "ERR" -> Just NRErr_
    "STAT" -> Just NRStat_
    _ -> Nothing

data NtfResponse
  = NRId C.PublicKeyX25519
  | NROk
  | NRErr ErrorType
  | NRStat NtfSubStatus

instance ProtocolEncoding NtfResponse where
  type Tag NtfResponse = NtfResponseTag
  encodeProtocol = \case
    NRId dhKey -> e (NRId_, ' ', dhKey)
    NROk -> e NROk_
    NRErr err -> e (NRErr_, ' ', err)
    NRStat stat -> e (NRStat_, ' ', stat)
    where
      e :: Encoding a => a -> ByteString
      e = smpEncode

  protocolP = \case
    NRId_ -> NRId <$> _smpP
    NROk_ -> pure NROk
    NRErr_ -> NRErr <$> _smpP
    NRStat_ -> NRStat <$> _smpP

  checkCredentials (_, _, subId, _) cmd = case cmd of
    -- ERR response does not always have entity ID
    NRErr _ -> Right cmd
    -- other server responses must have entity ID
    _
      | B.null subId -> Left $ CMD NO_ENTITY
      | otherwise -> Right cmd

data SMPQueueNtf = SMPQueueNtf
  { smpServer :: ProtocolServer,
    notifierId :: NotifierId,
    notifierKey :: NtfPrivateSignKey
  }

instance Encoding SMPQueueNtf where
  smpEncode SMPQueueNtf {smpServer, notifierId, notifierKey} = smpEncode (smpServer, notifierId, notifierKey)
  smpP = do
    (smpServer, notifierId, notifierKey) <- smpP
    pure $ SMPQueueNtf smpServer notifierId notifierKey

data PushPlatform = PPApple

instance Encoding PushPlatform where
  smpEncode = \case
    PPApple -> "A"
  smpP =
    A.anyChar >>= \case
      'A' -> pure PPApple
      _ -> fail "bad PushPlatform"

data DeviceToken = DeviceToken PushPlatform ByteString

instance Encoding DeviceToken where
  smpEncode (DeviceToken p t) = smpEncode (p, t)
  smpP = DeviceToken <$> smpP <*> smpP

type NtfEntityId = ByteString

type NtfSubsciptionId = NtfEntityId

type NtfTokenId = NtfEntityId

data NtfSubStatus
  = -- | state after SNEW
    NSNew
  | -- | pending connection/subscription to SMP server
    NSPending
  | -- | connected and subscribed to SMP server
    NSActive
  | -- | NEND received (we currently do not support it)
    NSEnd
  | -- | SMP AUTH error
    NSSMPAuth
  deriving (Eq)

instance Encoding NtfSubStatus where
  smpEncode = \case
    NSNew -> "NEW"
    NSPending -> "PENDING" -- e.g. after SMP server disconnect/timeout while ntf server is retrying to connect
    NSActive -> "ACTIVE"
    NSEnd -> "END"
    NSSMPAuth -> "SMP_AUTH"
  smpP =
    A.takeTill (== ' ') >>= \case
      "NEW" -> pure NSNew
      "PENDING" -> pure NSPending
      "ACTIVE" -> pure NSActive
      "END" -> pure NSEnd
      "SMP_AUTH" -> pure NSSMPAuth
      _ -> fail "bad NtfError"

data NtfTknStatus
  = -- | state after registration (TNEW)
    NTNew
  | -- | if initial notification or verification failed (push provider error)
    NTInvalid
  | -- | if initial notification succeeded
    NTConfirmed
  | -- | after successful verification (TVFY)
    NTActive
  | -- | after it is no longer valid (push provider error)
    NTExpired
  deriving (Eq)

checkEntity :: forall t e e'. (NtfEntityI e, NtfEntityI e') => t e' -> Either String (t e)
checkEntity c = case testEquality (sNtfEntity @e) (sNtfEntity @e') of
  Just Refl -> Right c
  Nothing -> Left "bad command party"

checkEntity' :: forall t p p'. (NtfEntityI p, NtfEntityI p') => t p' -> Maybe (t p)
checkEntity' c = case testEquality (sNtfEntity @p) (sNtfEntity @p') of
  Just Refl -> Just c
  _ -> Nothing
