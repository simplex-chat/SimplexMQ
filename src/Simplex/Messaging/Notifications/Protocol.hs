{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Simplex.Messaging.Notifications.Protocol where

import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Protocol

data RawNtfTransmission = RawNtfTransmission
  { signature :: ByteString,
    signed :: ByteString,
    sessId :: ByteString,
    corrId :: ByteString,
    subscriptionId :: ByteString,
    message :: ByteString
  }

-- | Parsed notifications server transmission without signature, size and session ID.
type NtfTransmission c = (CorrId, NtfSubsciptionId, c)

-- | signed parsed transmission, with original raw bytes and parsing error.
type SignedNtfTransmission c = (Maybe C.ASignature, Signed, Transmission (Either ErrorType c))

type Signed = ByteString

data NtfCommand
  = NCCreate DeviceToken SMPQueueNtfUri C.APublicVerifyKey C.PublicKeyX25519
  | NCCheck
  | NCToken DeviceToken
  | NCDelete

instance Encoding NtfCommand where
  smpEncode = \case
    NCCreate token smpQueue verifyKey dhKey -> "CREATE " <> smpEncode (token, smpQueue, verifyKey, dhKey)
    NCCheck -> "CHECK"
    NCToken token -> "TOKEN " <> smpEncode token
    NCDelete -> "DELETE"
  smpP =
    A.takeTill (== ' ') >>= \case
      "CREATE" -> do
        (token, smpQueue, verifyKey, dhKey) <- A.space *> smpP
        pure $ NCCreate token smpQueue verifyKey dhKey
      "CHECK" -> pure NCCheck
      "TOKEN" -> NCToken <$> (A.space *> smpP)
      "DELETE" -> pure NCDelete
      _ -> fail "bad NtfCommand"

data NtfResponse
  = NRSubId NtfSubsciptionId
  | NROk
  | NRErr NtfError
  | NRStat NtfStatus

instance Encoding NtfResponse where
  smpEncode = \case
    NRSubId subId -> "ID " <> smpEncode subId
    NROk -> "OK"
    NRErr err -> "ERR " <> smpEncode err
    NRStat stat -> "STAT " <> smpEncode stat
  smpP =
    A.takeTill (== ' ') >>= \case
      "ID" -> NRSubId <$> (A.space *> smpP)
      "OK" -> pure NROk
      "ERR" -> NRErr <$> (A.space *> smpP)
      "STAT" -> NRStat <$> (A.space *> smpP)
      _ -> fail "bad NtfResponse"

data SMPQueueNtfUri = SMPQueueNtfUri
  { smpServer :: SMPServer,
    notifierId :: NotifierId,
    notifierKey :: NtfPrivateSignKey
  }

instance Encoding SMPQueueNtfUri where
  smpEncode SMPQueueNtfUri {smpServer, notifierId, notifierKey} = smpEncode (smpServer, notifierId, notifierKey)
  smpP = do
    (smpServer, notifierId, notifierKey) <- smpP
    pure $ SMPQueueNtfUri smpServer notifierId notifierKey

newtype DeviceToken = DeviceToken ByteString

instance Encoding DeviceToken where
  smpEncode (DeviceToken t) = smpEncode t
  smpP = DeviceToken <$> smpP

newtype NtfSubsciptionId = NtfSubsciptionId ByteString
  deriving (Eq, Ord)

instance Encoding NtfSubsciptionId where
  smpEncode (NtfSubsciptionId t) = smpEncode t
  smpP = NtfSubsciptionId <$> smpP

data NtfError = NtfErrSyntax | NtfErrAuth

instance Encoding NtfError where
  smpEncode = \case
    NtfErrSyntax -> "SYNTAX"
    NtfErrAuth -> "AUTH"
  smpP =
    A.takeTill (== ' ') >>= \case
      "SYNTAX" -> pure NtfErrSyntax
      "AUTH" -> pure NtfErrAuth
      _ -> fail "bad NtfError"

data NtfStatus = NSPending | NSActive | NSEnd | NSSMPAuth

instance Encoding NtfStatus where
  smpEncode = \case
    NSPending -> "PENDING"
    NSActive -> "ACTIVE"
    NSEnd -> "END"
    NSSMPAuth -> "SMP_AUTH"
  smpP =
    A.takeTill (== ' ') >>= \case
      "PENDING" -> pure NSPending
      "ACTIVE" -> pure NSActive
      "END" -> pure NSEnd
      "SMP_AUTH" -> pure NSSMPAuth
      _ -> fail "bad NtfError"
