{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Simplex.RemoteControl.Types where

import Crypto.Random (ChaChaDRG)
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Time.Clock.System (SystemTime, getSystemTime)
import qualified Network.TLS as TLS
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.SNTRUP761.Bindings (KEMPublicKey, KEMSecretKey, sntrup761Keypair)
import Simplex.Messaging.Encoding (Encoding (..))
import Simplex.Messaging.Transport.Credentials (genCredentials, tlsCredentials)
import Simplex.Messaging.Version (VersionRange, mkVersionRange)
import UnliftIO

-- * Discovery

ipProbeVersionRange :: VersionRange
ipProbeVersionRange = mkVersionRange 1 1

data IpProbe = IpProbe
  { versionRange :: VersionRange,
    randomNonce :: ByteString
  }
  deriving (Show)

instance Encoding IpProbe where
  smpEncode IpProbe {versionRange, randomNonce} = smpEncode (versionRange, 'I', randomNonce)

  smpP = IpProbe <$> (smpP <* "I") *> smpP

-- * Controller

-- | A bunch of keys that should be generated by a controller to start a new remote session and produce invites
data CtrlSessionKeys = CtrlSessionKeys
  { ts :: SystemTime,
    ca :: C.KeyHash,
    credentials :: TLS.Credentials,
    sSigKey :: C.PrivateKeyEd25519,
    dhKey :: C.PrivateKeyX25519,
    kem :: (KEMPublicKey, KEMSecretKey)
  }

newCtrlSessionKeys :: TVar ChaChaDRG -> (C.APrivateSignKey, C.SignedCertificate) -> IO CtrlSessionKeys
newCtrlSessionKeys rng (caKey, caCert) = do
  ts <- getSystemTime
  (_, C.APrivateDhKey C.SX25519 dhKey) <- C.generateDhKeyPair C.SX25519
  (_, C.APrivateSignKey C.SEd25519 sSigKey) <- C.generateSignatureKeyPair C.SEd25519

  let parent = (C.signatureKeyPair caKey, caCert)
  sessionCreds <- genCredentials (Just parent) (0, 24) "Session"
  let (ca, credentials) = tlsCredentials $ sessionCreds :| [parent]
  kem <- sntrup761Keypair rng

  pure CtrlSessionKeys {ts, ca, credentials, sSigKey, dhKey, kem}

data CtrlCryptoHandle = CtrlCryptoHandle
  -- TODO

-- * Host

data HostSessionKeys = HostSessionKeys
  { ca :: C.KeyHash
    -- TODO
  }

data HostCryptoHandle = HostCryptoHandle
  -- TODO

-- * Utils

type Tasks = TVar [Async ()]

asyncRegistered :: MonadUnliftIO m => Tasks -> m () -> m ()
asyncRegistered tasks action = async action >>= registerAsync tasks

registerAsync :: MonadIO m => Tasks -> Async () -> m ()
registerAsync tasks = atomically . modifyTVar tasks . (:)

cancelTasks :: MonadIO m => Tasks -> m ()
cancelTasks tasks = readTVarIO tasks >>= mapM_ cancel
