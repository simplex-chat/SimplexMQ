{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-redundant-constraints #-}

module Simplex.Messaging.Crypto.Ratchet
  ( Ratchet (..),
    RatchetX448,
    MsgEncryptKey (..),
    MsgEncryptKeyX448,
    SkippedMsgDiff (..),
    SkippedMsgKeys,
    InitialKeys (..),
    pattern IKPQOn,
    pattern IKPQOff,
    PQEncryption (..),
    pattern PQEncOn,
    pattern PQEncOff,
    PQSupport (..),
    pattern PQSupportOn,
    pattern PQSupportOff,
    AUseKEM (..),
    RatchetKEMState (..),
    SRatchetKEMState (..),
    RcvPrivRKEMParams,
    APrivRKEMParams (..),
    RcvE2ERatchetParamsUri,
    RcvE2ERatchetParams,
    SndE2ERatchetParams,
    AE2ERatchetParams (..),
    E2ERatchetParamsUri (..),
    E2ERatchetParams (..),
    VersionE2E,
    VersionRangeE2E,
    pattern VersionE2E,
    RatchetVersions (..),
    kdfX3DHE2EEncryptVersion,
    pqRatchetE2EEncryptVersion,
    currentE2EEncryptVersion,
    supportedE2EEncryptVRange,
    generateRcvE2EParams,
    generateSndE2EParams,
    initialPQEncryption,
    connPQEncryption,
    replyKEM_,
    pqSupportToEnc,
    pqEncToSupport,
    pqSupportAnd,
    pqEnableSupport,
    pqX3dhSnd,
    pqX3dhRcv,
    initSndRatchet,
    initRcvRatchet,
    rcCheckCanPad,
    rcEncryptHeader,
    rcEncryptMsg,
    rcDecrypt,
    -- used in tests
    MsgHeader (..),
    RatchetInitParams (..),
    UseKEM (..),
    RKEMParams (..),
    ARKEMParams (..),
    SndRatchet (..),
    RcvRatchet (..),
    RatchetKEM (..),
    RatchetKEMAccepted (..),
    RatchetKey (..),
    fullHeaderLen,
    applySMDiff,
    encodeMsgHeader,
    msgHeaderP,
  )
where

import Control.Applicative ((<|>))
import Control.Monad (unless)
import Control.Monad.Except
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except
import Crypto.Cipher.AES (AES256)
import Crypto.Hash (SHA512)
import qualified Crypto.KDF.HKDF as H
import Crypto.Random (ChaChaDRG)
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Aeson as J
import qualified Data.Aeson.TH as JQ
import Data.Attoparsec.ByteString (Parser, peekWord8')
import qualified Data.Attoparsec.ByteString.Char8 as A
import qualified Data.ByteArray as BA
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as LB
import Data.Composition ((.:), (.:.))
import Data.Functor (($>))
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality
import Data.Typeable (Typeable)
import Data.Word (Word16, Word32)
import Simplex.Messaging.Agent.QueryString
import Simplex.Messaging.Agent.Store.DB (Binary (..), BoolInt (..), FromField (..), ToField (..), blobFieldDecoder)
import Simplex.Messaging.Crypto
import Simplex.Messaging.Crypto.SNTRUP761.Bindings
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (defaultJSON, parseE, parseE')
import Simplex.Messaging.Util (($>>=), (<$?>))
import Simplex.Messaging.Version
import Simplex.Messaging.Version.Internal
import UnliftIO.STM

-- e2e encryption headers version history:
-- 1 - binary protocol encoding (1/1/2022)
-- 2 - use KDF in x3dh (10/20/2022)

data E2EVersion

instance VersionScope E2EVersion

type VersionE2E = Version E2EVersion

type VersionRangeE2E = VersionRange E2EVersion

pattern VersionE2E :: Word16 -> VersionE2E
pattern VersionE2E v = Version v

kdfX3DHE2EEncryptVersion :: VersionE2E
kdfX3DHE2EEncryptVersion = VersionE2E 2

pqRatchetE2EEncryptVersion :: VersionE2E
pqRatchetE2EEncryptVersion = VersionE2E 3

currentE2EEncryptVersion :: VersionE2E
currentE2EEncryptVersion = VersionE2E 3

supportedE2EEncryptVRange :: VersionRangeE2E
supportedE2EEncryptVRange = mkVersionRange kdfX3DHE2EEncryptVersion currentE2EEncryptVersion

data RatchetKEMState
  = RKSProposed -- only KEM encapsulation key
  | RKSAccepted -- KEM ciphertext and the next encapsulation key

data SRatchetKEMState (s :: RatchetKEMState) where
  SRKSProposed :: SRatchetKEMState 'RKSProposed
  SRKSAccepted :: SRatchetKEMState 'RKSAccepted

deriving instance Show (SRatchetKEMState s)

instance TestEquality SRatchetKEMState where
  testEquality SRKSProposed SRKSProposed = Just Refl
  testEquality SRKSAccepted SRKSAccepted = Just Refl
  testEquality _ _ = Nothing

class RatchetKEMStateI (s :: RatchetKEMState) where sRatchetKEMState :: SRatchetKEMState s

instance RatchetKEMStateI 'RKSProposed where sRatchetKEMState = SRKSProposed

instance RatchetKEMStateI 'RKSAccepted where sRatchetKEMState = SRKSAccepted

checkRatchetKEMState :: forall t s s' a. (RatchetKEMStateI s, RatchetKEMStateI s') => t s' a -> Either String (t s a)
checkRatchetKEMState x = case testEquality (sRatchetKEMState @s) (sRatchetKEMState @s') of
  Just Refl -> Right x
  Nothing -> Left "bad ratchet KEM state"

checkRatchetKEMState' :: forall t s s'. (RatchetKEMStateI s, RatchetKEMStateI s') => t s' -> Either String (t s)
checkRatchetKEMState' x = case testEquality (sRatchetKEMState @s) (sRatchetKEMState @s') of
  Just Refl -> Right x
  Nothing -> Left "bad ratchet KEM state"

data RKEMParams (s :: RatchetKEMState) where
  RKParamsProposed :: KEMPublicKey -> RKEMParams 'RKSProposed
  RKParamsAccepted :: KEMCiphertext -> KEMPublicKey -> RKEMParams 'RKSAccepted

deriving instance Eq (RKEMParams s)

deriving instance Show (RKEMParams s)

data ARKEMParams = forall s. RatchetKEMStateI s => ARKP (SRatchetKEMState s) (RKEMParams s)

deriving instance Show ARKEMParams

instance RatchetKEMStateI s => Encoding (RKEMParams s) where
  smpEncode = \case
    RKParamsProposed k -> smpEncode ('P', k)
    RKParamsAccepted ct k -> smpEncode ('A', ct, k)
  smpP = (\(ARKP _ ps) -> checkRatchetKEMState' ps) <$?> smpP

instance Encoding ARKEMParams where
  smpEncode (ARKP _ ps) = smpEncode ps
  smpP =
    smpP >>= \case
      'P' -> ARKP SRKSProposed . RKParamsProposed <$> smpP
      'A' -> ARKP SRKSAccepted .: RKParamsAccepted <$> smpP <*> smpP
      _ -> fail "bad ratchet KEM params"

instance ToField ARKEMParams where toField = toField . Binary . smpEncode

instance FromField ARKEMParams where fromField = blobFieldDecoder smpDecode

data E2ERatchetParams (s :: RatchetKEMState) (a :: Algorithm)
  = E2ERatchetParams VersionE2E (PublicKey a) (PublicKey a) (Maybe (RKEMParams s))
  deriving (Show)

data AE2ERatchetParams (a :: Algorithm)
  = forall s.
    RatchetKEMStateI s =>
    AE2ERatchetParams (SRatchetKEMState s) (E2ERatchetParams s a)

deriving instance Show (AE2ERatchetParams a)

data AnyE2ERatchetParams
  = forall s a.
    (RatchetKEMStateI s, DhAlgorithm a, AlgorithmI a) =>
    AnyE2ERatchetParams (SRatchetKEMState s) (SAlgorithm a) (E2ERatchetParams s a)

deriving instance Show AnyE2ERatchetParams

instance (RatchetKEMStateI s, AlgorithmI a) => Encoding (E2ERatchetParams s a) where
  smpEncode (E2ERatchetParams v k1 k2 kem_)
    | v >= pqRatchetE2EEncryptVersion = smpEncode (v, k1, k2, kem_)
    | otherwise = smpEncode (v, k1, k2)
  smpP = toParams <$?> smpP
    where
      toParams :: AE2ERatchetParams a -> Either String (E2ERatchetParams s a)
      toParams = \case
        AE2ERatchetParams _ (E2ERatchetParams v k1 k2 Nothing) -> Right $ E2ERatchetParams v k1 k2 Nothing
        AE2ERatchetParams _ ps -> checkRatchetKEMState ps

instance AlgorithmI a => Encoding (AE2ERatchetParams a) where
  smpEncode (AE2ERatchetParams _ ps) = smpEncode ps
  smpP = (\(AnyE2ERatchetParams s _ ps) -> AE2ERatchetParams s <$> checkAlgorithm ps) <$?> smpP

instance Encoding AnyE2ERatchetParams where
  smpEncode (AnyE2ERatchetParams _ _ ps) = smpEncode ps
  smpP = do
    v :: VersionE2E <- smpP
    APublicDhKey a k1 <- smpP
    APublicDhKey a' k2 <- smpP
    case testEquality a a' of
      Nothing -> fail "bad e2e params: different key algorithms"
      Just Refl ->
        kemP v >>= \case
          Just (ARKP s kem) -> pure $ AnyE2ERatchetParams s a $ E2ERatchetParams v k1 k2 (Just kem)
          Nothing -> pure $ AnyE2ERatchetParams SRKSProposed a $ E2ERatchetParams v k1 k2 Nothing
    where
      kemP :: VersionE2E -> Parser (Maybe ARKEMParams)
      kemP v
        | v >= pqRatchetE2EEncryptVersion = smpP
        | otherwise = pure Nothing

instance VersionI E2EVersion (E2ERatchetParams s a) where
  type VersionRangeT E2EVersion (E2ERatchetParams s a) = E2ERatchetParamsUri s a
  version (E2ERatchetParams v _ _ _) = v
  toVersionRangeT (E2ERatchetParams _ k1 k2 kem_) vr = E2ERatchetParamsUri vr k1 k2 kem_

instance VersionRangeI E2EVersion (E2ERatchetParamsUri s a) where
  type VersionT E2EVersion (E2ERatchetParamsUri s a) = (E2ERatchetParams s a)
  versionRange (E2ERatchetParamsUri vr _ _ _) = vr
  toVersionT (E2ERatchetParamsUri _ k1 k2 kem_) v = E2ERatchetParams v k1 k2 kem_
  toVersionRange (E2ERatchetParamsUri _ k1 k2 kem_) vr = E2ERatchetParamsUri vr k1 k2 kem_

type RcvE2ERatchetParamsUri a = E2ERatchetParamsUri 'RKSProposed a

data E2ERatchetParamsUri (s :: RatchetKEMState) (a :: Algorithm)
  = E2ERatchetParamsUri VersionRangeE2E (PublicKey a) (PublicKey a) (Maybe (RKEMParams s))
  deriving (Eq, Show)

data AE2ERatchetParamsUri (a :: Algorithm)
  = forall s.
    RatchetKEMStateI s =>
    AE2ERatchetParamsUri (SRatchetKEMState s) (E2ERatchetParamsUri s a)

deriving instance Show (AE2ERatchetParamsUri a)

data AnyE2ERatchetParamsUri
  = forall s a.
    (RatchetKEMStateI s, DhAlgorithm a, AlgorithmI a) =>
    AnyE2ERatchetParamsUri (SRatchetKEMState s) (SAlgorithm a) (E2ERatchetParamsUri s a)

deriving instance Show AnyE2ERatchetParamsUri

instance (RatchetKEMStateI s, AlgorithmI a) => StrEncoding (E2ERatchetParamsUri s a) where
  strEncode (E2ERatchetParamsUri vs key1 key2 kem_) =
    strEncode . QSP QNoEscaping $
      [("v", strEncode vs), ("x3dh", strEncodeList [key1, key2])]
        <> maybe [] encodeKem kem_
    where
      encodeKem kem
        | maxVersion vs < pqRatchetE2EEncryptVersion = []
        | otherwise = case kem of
            RKParamsProposed k -> [("kem_key", strEncode k)]
            RKParamsAccepted ct k -> [("kem_ct", strEncode ct), ("kem_key", strEncode k)]
  strP = toParamsURI <$?> strP
    where
      toParamsURI = \case
        AE2ERatchetParamsUri _ (E2ERatchetParamsUri vr k1 k2 Nothing) -> Right $ E2ERatchetParamsUri vr k1 k2 Nothing
        AE2ERatchetParamsUri _ ps -> checkRatchetKEMState ps

instance AlgorithmI a => StrEncoding (AE2ERatchetParamsUri a) where
  strEncode (AE2ERatchetParamsUri _ ps) = strEncode ps
  strP = (\(AnyE2ERatchetParamsUri s _ ps) -> AE2ERatchetParamsUri s <$> checkAlgorithm ps) <$?> strP

instance StrEncoding AnyE2ERatchetParamsUri where
  strEncode (AnyE2ERatchetParamsUri _ _ ps) = strEncode ps
  strP = do
    query <- strP
    vr :: VersionRangeE2E <- queryParam "v" query
    keys <- L.toList <$> queryParam "x3dh" query
    case keys of
      [APublicDhKey a k1, APublicDhKey a' k2] -> case testEquality a a' of
        Nothing -> fail "bad e2e params: different key algorithms"
        Just Refl ->
          kemP vr query >>= \case
            Just (ARKP s kem) -> pure $ AnyE2ERatchetParamsUri s a $ E2ERatchetParamsUri vr k1 k2 (Just kem)
            Nothing -> pure $ AnyE2ERatchetParamsUri SRKSProposed a $ E2ERatchetParamsUri vr k1 k2 Nothing
      _ -> fail "bad e2e params"
    where
      kemP vr query
        | maxVersion vr >= pqRatchetE2EEncryptVersion =
            queryParam_ "kem_key" query
              $>>= \k -> Just . kemParams k <$> queryParam_ "kem_ct" query
        | otherwise = pure Nothing
      kemParams k = \case
        Nothing -> ARKP SRKSProposed $ RKParamsProposed k
        Just ct -> ARKP SRKSAccepted $ RKParamsAccepted ct k

type RcvE2ERatchetParams a = E2ERatchetParams 'RKSProposed a

type SndE2ERatchetParams a = AE2ERatchetParams a

data PrivRKEMParams (s :: RatchetKEMState) where
  PrivateRKParamsProposed :: KEMKeyPair -> PrivRKEMParams 'RKSProposed
  PrivateRKParamsAccepted :: KEMCiphertext -> KEMSharedKey -> KEMKeyPair -> PrivRKEMParams 'RKSAccepted

data APrivRKEMParams = forall s. RatchetKEMStateI s => APRKP (SRatchetKEMState s) (PrivRKEMParams s)

type RcvPrivRKEMParams = PrivRKEMParams 'RKSProposed

instance RatchetKEMStateI s => Encoding (PrivRKEMParams s) where
  smpEncode = \case
    PrivateRKParamsProposed k -> smpEncode ('P', k)
    PrivateRKParamsAccepted ct shared k -> smpEncode ('A', ct, shared, k)
  smpP = (\(APRKP _ ps) -> checkRatchetKEMState' ps) <$?> smpP

instance Encoding APrivRKEMParams where
  smpEncode (APRKP _ ps) = smpEncode ps
  smpP =
    smpP >>= \case
      'P' -> APRKP SRKSProposed . PrivateRKParamsProposed <$> smpP
      'A' -> APRKP SRKSAccepted .:. PrivateRKParamsAccepted <$> smpP <*> smpP <*> smpP
      _ -> fail "bad APrivRKEMParams"

instance RatchetKEMStateI s => ToField (PrivRKEMParams s) where toField = toField . Binary . smpEncode

instance (Typeable s, RatchetKEMStateI s) => FromField (PrivRKEMParams s) where fromField = blobFieldDecoder smpDecode

data UseKEM (s :: RatchetKEMState) where
  ProposeKEM :: UseKEM 'RKSProposed
  AcceptKEM :: KEMPublicKey -> UseKEM 'RKSAccepted

data AUseKEM = forall s. RatchetKEMStateI s => AUseKEM (SRatchetKEMState s) (UseKEM s)

generateE2EParams :: forall s a. (AlgorithmI a, DhAlgorithm a) => TVar ChaChaDRG -> VersionE2E -> Maybe (UseKEM s) -> IO (PrivateKey a, PrivateKey a, Maybe (PrivRKEMParams s), E2ERatchetParams s a)
generateE2EParams g v useKEM_ = do
  (k1, pk1) <- atomically $ generateKeyPair g
  (k2, pk2) <- atomically $ generateKeyPair g
  kems <- kemParams
  pure (pk1, pk2, snd <$> kems, E2ERatchetParams v k1 k2 (fst <$> kems))
  where
    kemParams :: IO (Maybe (RKEMParams s, PrivRKEMParams s))
    kemParams = case useKEM_ of
      Just useKem
        | v >= pqRatchetE2EEncryptVersion ->
            Just <$> do
              ks@(k, _) <- sntrup761Keypair g
              case useKem of
                ProposeKEM -> pure (RKParamsProposed k, PrivateRKParamsProposed ks)
                AcceptKEM k' -> do
                  (ct, shared) <- sntrup761Enc g k'
                  pure (RKParamsAccepted ct k, PrivateRKParamsAccepted ct shared ks)
      _ -> pure Nothing

-- used by party initiating connection, Bob in double-ratchet spec
generateRcvE2EParams :: (AlgorithmI a, DhAlgorithm a) => TVar ChaChaDRG -> VersionE2E -> PQSupport -> IO (PrivateKey a, PrivateKey a, Maybe (PrivRKEMParams 'RKSProposed), E2ERatchetParams 'RKSProposed a)
generateRcvE2EParams g v = generateE2EParams g v . proposeKEM_
  where
    proposeKEM_ :: PQSupport -> Maybe (UseKEM 'RKSProposed)
    proposeKEM_ = \case
      PQSupportOn -> Just ProposeKEM
      PQSupportOff -> Nothing

-- used by party accepting connection, Alice in double-ratchet spec
generateSndE2EParams :: forall a. (AlgorithmI a, DhAlgorithm a) => TVar ChaChaDRG -> VersionE2E -> Maybe AUseKEM -> IO (PrivateKey a, PrivateKey a, Maybe APrivRKEMParams, AE2ERatchetParams a)
generateSndE2EParams g v = \case
  Nothing -> do
    (pk1, pk2, _, e2eParams) <- generateE2EParams g v Nothing
    pure (pk1, pk2, Nothing, AE2ERatchetParams SRKSProposed e2eParams)
  Just (AUseKEM s useKEM) -> do
    (pk1, pk2, pKem, e2eParams) <- generateE2EParams g v (Just useKEM)
    pure (pk1, pk2, APRKP s <$> pKem, AE2ERatchetParams s e2eParams)

data RatchetInitParams = RatchetInitParams
  { assocData :: Str,
    ratchetKey :: RatchetKey,
    sndHK :: HeaderKey,
    rcvNextHK :: HeaderKey,
    kemAccepted :: Maybe RatchetKEMAccepted
  }
  deriving (Show)

-- this is used by the peer joining the connection
pqX3dhSnd :: DhAlgorithm a => PrivateKey a -> PrivateKey a -> Maybe APrivRKEMParams -> E2ERatchetParams 'RKSProposed a -> Either CryptoError (RatchetInitParams, Maybe KEMKeyPair)
--        3. replied       2. received
pqX3dhSnd spk1 spk2 spKem_ (E2ERatchetParams v rk1 rk2 rKem_) = do
  (ks_, kem_) <- sndPq
  let initParams = pqX3dh (publicKey spk1, rk1) (dh' rk1 spk2) (dh' rk2 spk1) (dh' rk2 spk2) kem_
  pure (initParams, ks_)
  where
    sndPq :: Either CryptoError (Maybe KEMKeyPair, Maybe RatchetKEMAccepted)
    sndPq = case spKem_ of
      Just (APRKP _ ps) | v >= pqRatchetE2EEncryptVersion -> case (ps, rKem_) of
        (PrivateRKParamsAccepted ct shared ks, Just (RKParamsProposed k)) -> Right (Just ks, Just $ RatchetKEMAccepted k shared ct)
        (PrivateRKParamsProposed ks, _) -> Right (Just ks, Nothing) -- both parties can send "proposal" in case of ratchet renegotiation
        _ -> Left CERatchetKEMState
      _ -> Right (Nothing, Nothing)

-- this is used by the peer that created new connection, after receiving the reply
pqX3dhRcv :: forall s a. (RatchetKEMStateI s, DhAlgorithm a) => PrivateKey a -> PrivateKey a -> Maybe (PrivRKEMParams 'RKSProposed) -> E2ERatchetParams s a -> ExceptT CryptoError IO (RatchetInitParams, Maybe KEMKeyPair)
--        1. sent          4. received in reply
pqX3dhRcv rpk1 rpk2 rpKem_ (E2ERatchetParams v sk1 sk2 sKem_) = do
  kem_ <- rcvPq
  let initParams = pqX3dh (sk1, publicKey rpk1) (dh' sk2 rpk1) (dh' sk1 rpk2) (dh' sk2 rpk2) (snd <$> kem_)
  pure (initParams, fst <$> kem_)
  where
    rcvPq :: ExceptT CryptoError IO (Maybe (KEMKeyPair, RatchetKEMAccepted))
    rcvPq = case sKem_ of
      Just (RKParamsAccepted ct k') | v >= pqRatchetE2EEncryptVersion -> case rpKem_ of
        Just (PrivateRKParamsProposed ks@(_, pk)) -> do
          shared <- liftIO $ sntrup761Dec ct pk
          pure $ Just (ks, RatchetKEMAccepted k' shared ct)
        Nothing -> throwE CERatchetKEMState
      _ -> pure Nothing -- both parties can send "proposal" in case of ratchet renegotiation

pqX3dh :: DhAlgorithm a => (PublicKey a, PublicKey a) -> DhSecret a -> DhSecret a -> DhSecret a -> Maybe RatchetKEMAccepted -> RatchetInitParams
pqX3dh (sk1, rk1) dh1 dh2 dh3 kemAccepted =
  RatchetInitParams {assocData, ratchetKey = RatchetKey sk, sndHK = Key hk, rcvNextHK = Key nhk, kemAccepted}
  where
    assocData = Str $ pubKeyBytes sk1 <> pubKeyBytes rk1
    dhs = dhBytes' dh1 <> dhBytes' dh2 <> dhBytes' dh3 <> pq
    pq = maybe "" (\RatchetKEMAccepted {rcPQRss = KEMSharedKey ss} -> BA.convert ss) kemAccepted
    (hk, nhk, sk) =
      let salt = B.replicate 64 '\0'
       in hkdf3 salt dhs "SimpleXX3DH"

type RatchetX448 = Ratchet 'X448

data Ratchet a = Ratchet
  { -- ratchet version range sent in messages (current .. max supported ratchet version)
    rcVersion :: RatchetVersions,
    -- associated data - must be the same in both parties ratchets
    rcAD :: Str,
    rcDHRs :: PrivateKey a,
    rcKEM :: Maybe RatchetKEM,
    rcSupportKEM :: PQSupport, -- defines header size, can only be enabled once
    rcEnableKEM :: PQEncryption, -- will enable KEM on the next ratchet step
    rcSndKEM :: PQEncryption, -- used KEM hybrid secret for sending ratchet
    rcRcvKEM :: PQEncryption, -- used KEM hybrid secret for receiving ratchet
    rcRK :: RatchetKey,
    rcSnd :: Maybe (SndRatchet a),
    rcRcv :: Maybe RcvRatchet,
    rcNs :: Word32,
    rcNr :: Word32,
    rcPN :: Word32,
    rcNHKs :: HeaderKey,
    rcNHKr :: HeaderKey
  }
  deriving (Show)

data RatchetVersions = RatchetVersions
  { current :: VersionE2E,
    maxSupported :: VersionE2E
  }
  deriving (Eq, Show)

instance ToJSON RatchetVersions where
  -- TODO v5.7 or v5.8 change to the default record encoding
  toJSON RatchetVersions {current, maxSupported} = toJSON (current, maxSupported)
  toEncoding RatchetVersions {current, maxSupported} = toEncoding (current, maxSupported)

instance FromJSON RatchetVersions where
  -- TODO v5.7 or v5.8 replace comment below with "tuple for backward"
  -- this parser supports JSON record encoding for forward compatibility
  parseJSON v = toRV <$> (tupleP <|> recordP v)
    where
      tupleP = parseJSON v
      recordP = J.withObject "RatchetVersions" $ \o -> (,) <$> o J..: "current" <*> o J..: "maxSupported"
      toRV (current, maxSupported) = RatchetVersions {current, maxSupported}

data SndRatchet a = SndRatchet
  { rcDHRr :: PublicKey a,
    rcCKs :: RatchetKey,
    rcHKs :: HeaderKey
  }
  deriving (Show)

data RcvRatchet = RcvRatchet
  { rcCKr :: RatchetKey,
    rcHKr :: HeaderKey
  }
  deriving (Show)

data RatchetKEM = RatchetKEM
  { rcPQRs :: KEMKeyPair,
    rcKEMs :: Maybe RatchetKEMAccepted
  }
  deriving (Show)

data RatchetKEMAccepted = RatchetKEMAccepted
  { rcPQRr :: KEMPublicKey, -- received key
    rcPQRss :: KEMSharedKey, -- computed shared secret
    rcPQRct :: KEMCiphertext -- sent encaps(rcPQRr, rcPQRss)
  }
  deriving (Show)

type SkippedMsgKeys = Map HeaderKey SkippedHdrMsgKeys

type SkippedHdrMsgKeys = Map Word32 MessageKey

data SkippedMsgDiff
  = SMDNoChange
  | SMDRemove HeaderKey Word32
  | SMDAdd SkippedMsgKeys

-- | this function is only used in tests to apply changes in skipped messages,
-- in the agent the diff is persisted, and the whole state is loaded for the next message.
applySMDiff :: SkippedMsgKeys -> SkippedMsgDiff -> SkippedMsgKeys
applySMDiff smks = \case
  SMDNoChange -> smks
  SMDRemove hk msgN -> fromMaybe smks $ do
    mks <- M.lookup hk smks
    _ <- M.lookup msgN mks
    let mks' = M.delete msgN mks
    pure $
      if M.null mks'
        then M.delete hk smks
        else M.insert hk mks' smks
  SMDAdd smks' ->
    let merge hk mks = M.alter (Just . maybe mks (M.union mks)) hk
     in M.foldrWithKey merge smks smks'

type HeaderKey = Key

data MessageKey = MessageKey Key IV
  deriving (Show)

instance Encoding MessageKey where
  smpEncode (MessageKey (Key key) (IV iv)) = smpEncode (key, iv)
  smpP = MessageKey <$> (Key <$> smpP) <*> (IV <$> smpP)

-- | Input key material for double ratchet HKDF functions
newtype RatchetKey = RatchetKey ByteString
  deriving (Show)

instance ToJSON RatchetKey where
  toJSON (RatchetKey k) = strToJSON k
  toEncoding (RatchetKey k) = strToJEncoding k

instance FromJSON RatchetKey where
  parseJSON = fmap RatchetKey . strParseJSON "Key"

instance ToField MessageKey where toField = toField . Binary . smpEncode

instance FromField MessageKey where fromField = blobFieldDecoder smpDecode

-- | Sending ratchet initialization
--
-- Please note that sPKey is not stored, and its public part together with random salt
-- is sent to the recipient.
-- @
-- RatchetInitAlicePQ2HE(state, SK, bob_dh_public_key, shared_hka, shared_nhkb, bob_pq_kem_encapsulation_key)
-- // below added for post-quantum KEM
-- state.PQRs = GENERATE_PQKEM()
-- state.PQRr = bob_pq_kem_encapsulation_key
-- state.PQRss = random // shared secret for KEM
-- state.PQRct = PQKEM-ENC(state.PQRr, state.PQRss) // encapsulated additional shared secret
-- // above added for KEM
-- @
initSndRatchet ::
  forall a. (AlgorithmI a, DhAlgorithm a) => RatchetVersions -> PublicKey a -> PrivateKey a -> (RatchetInitParams, Maybe KEMKeyPair) -> Ratchet a
initSndRatchet rcVersion rcDHRr rcDHRs (RatchetInitParams {assocData, ratchetKey, sndHK, rcvNextHK, kemAccepted}, rcPQRs_) = do
  -- state.RK, state.CKs, state.NHKs = KDF_RK_HE(SK, DH(state.DHRs, state.DHRr) || state.PQRss)
  let (rcRK, rcCKs, rcNHKs) = rootKdf ratchetKey rcDHRr rcDHRs (rcPQRss <$> kemAccepted)
      pqOn = isJust rcPQRs_
   in Ratchet
        { rcVersion,
          rcAD = assocData,
          rcDHRs,
          rcKEM = (`RatchetKEM` kemAccepted) <$> rcPQRs_,
          rcSupportKEM = PQSupport pqOn,
          rcEnableKEM = PQEncryption pqOn,
          rcSndKEM = PQEncryption $ isJust kemAccepted,
          rcRcvKEM = PQEncOff,
          rcRK,
          rcSnd = Just SndRatchet {rcDHRr, rcCKs, rcHKs = sndHK},
          rcRcv = Nothing,
          rcPN = 0,
          rcNs = 0,
          rcNr = 0,
          rcNHKs,
          rcNHKr = rcvNextHK
        }

-- | Receiving ratchet initialization, equivalent to RatchetInitBobPQ2HE in double ratchet spec
--
-- def RatchetInitBobPQ2HE(state, SK, bob_dh_key_pair, shared_hka, shared_nhkb, bob_pq_kem_key_pair)
--
-- Please note that the public part of rcDHRs was sent to the sender
-- as part of the connection request and random salt was received from the sender.
initRcvRatchet ::
  forall a. (AlgorithmI a, DhAlgorithm a) => RatchetVersions -> PrivateKey a -> (RatchetInitParams, Maybe KEMKeyPair) -> PQSupport -> Ratchet a
initRcvRatchet rcVersion rcDHRs (RatchetInitParams {assocData, ratchetKey, sndHK, rcvNextHK, kemAccepted}, rcPQRs_) pqSupport =
  Ratchet
    { rcVersion,
      rcAD = assocData,
      rcDHRs,
      -- rcKEM:
      -- state.PQRs = bob_pq_kem_key_pair
      -- state.PQRr = None
      -- state.PQRss = None
      -- state.PQRct = None
      rcKEM = (`RatchetKEM` kemAccepted) <$> rcPQRs_,
      rcSupportKEM = pqSupport,
      rcEnableKEM = pqSupportToEnc pqSupport,
      rcSndKEM = PQEncOff,
      rcRcvKEM = PQEncOff,
      rcRK = ratchetKey,
      rcSnd = Nothing,
      rcRcv = Nothing,
      rcPN = 0,
      rcNs = 0,
      rcNr = 0,
      rcNHKs = rcvNextHK,
      rcNHKr = sndHK
    }

-- encaps = state.PQRs.encaps, // added for KEM #2
-- ct = state.PQRct // added for KEM #1
data MsgHeader a = MsgHeader
  { -- | max supported ratchet version
    msgMaxVersion :: VersionE2E,
    msgDHRs :: PublicKey a,
    msgKEM :: Maybe ARKEMParams,
    msgPN :: Word32,
    msgNs :: Word32
  }
  deriving (Show)

-- to allow extension without increasing the size, the actual header length is:
-- 69 = 2 (original size) + 2 + 1+56 (Curve448) + 4 + 4
-- The exact size is 2288, added reserve
paddedHeaderLen :: VersionE2E -> PQSupport -> Int
paddedHeaderLen v = \case
  PQSupportOn | v >= pqRatchetE2EEncryptVersion -> 2310
  _ -> 88

-- only used in tests to validate correct padding
-- (2 bytes - version size, 1 byte - header size)
fullHeaderLen :: VersionE2E -> PQSupport -> Int
fullHeaderLen v pq = 2 + 1 + paddedHeaderLen v pq + authTagSize + ivSize @AES256

-- pass the current version, as MsgHeader only includes the max supported version that can be different from the current
encodeMsgHeader :: AlgorithmI a => VersionE2E -> MsgHeader a -> ByteString
encodeMsgHeader v MsgHeader {msgMaxVersion, msgDHRs, msgKEM, msgPN, msgNs}
  | v >= pqRatchetE2EEncryptVersion = smpEncode (msgMaxVersion, msgDHRs, msgKEM, msgPN, msgNs)
  | otherwise = smpEncode (msgMaxVersion, msgDHRs, msgPN, msgNs)

-- pass the current version, as MsgHeader only includes the max supported version that can be different from the current
msgHeaderP :: AlgorithmI a => VersionE2E -> Parser (MsgHeader a)
msgHeaderP v = do
  msgMaxVersion <- smpP
  msgDHRs <- smpP
  msgKEM <- if v >= pqRatchetE2EEncryptVersion then smpP else pure Nothing
  msgPN <- smpP
  msgNs <- smpP
  pure MsgHeader {msgMaxVersion, msgDHRs, msgKEM, msgPN, msgNs}

data EncMessageHeader = EncMessageHeader
  { ehVersion :: VersionE2E, -- this is current ratchet version
    ehIV :: IV,
    ehAuthTag :: AuthTag,
    ehBody :: ByteString
  }

-- this encoding depends on version in EncMessageHeader because it is "current" ratchet version
instance Encoding EncMessageHeader where
  smpEncode EncMessageHeader {ehVersion, ehIV, ehAuthTag, ehBody} =
    smpEncode (ehVersion, ehIV, ehAuthTag) <> encodeLarge ehVersion ehBody
  smpP = do
    (ehVersion, ehIV, ehAuthTag) <- smpP
    ehBody <- largeP
    pure EncMessageHeader {ehVersion, ehIV, ehAuthTag, ehBody}

-- the encoder always uses 2-byte lengths for the new version, even for short headers without PQ keys.
encodeLarge :: VersionE2E -> ByteString -> ByteString
encodeLarge v s
  | v >= pqRatchetE2EEncryptVersion = smpEncode $ Large s
  | otherwise = smpEncode s

-- This parser relies on the fact that header cannot be shorter than 32 bytes (it is ~69 bytes without PQ KEM),
-- therefore if the first byte is less or equal to 31 (x1F), then we have 2 byte-length limited to 8191.
-- This allows upgrading the current version in one message.
largeP :: Parser ByteString
largeP = do
  len1 <- peekWord8'
  if len1 < 32 then unLarge <$> smpP else smpP

-- the header is length-prefixed to parse it as string and use as part of associated data for authenticated encryption
data EncRatchetMessage = EncRatchetMessage
  { emHeader :: ByteString,
    emAuthTag :: AuthTag,
    emBody :: ByteString
  }

encodeEncRatchetMessage :: VersionE2E -> EncRatchetMessage -> ByteString
encodeEncRatchetMessage v EncRatchetMessage {emHeader, emBody, emAuthTag} =
  encodeLarge v emHeader <> smpEncode (emAuthTag, Tail emBody)

encRatchetMessageP :: Parser EncRatchetMessage
encRatchetMessageP = do
  emHeader <- largeP
  (emAuthTag, Tail emBody) <- smpP
  pure EncRatchetMessage {emHeader, emBody, emAuthTag}

newtype PQEncryption = PQEncryption {enablePQ :: Bool}
  deriving (Eq, Show)

pattern PQEncOn :: PQEncryption
pattern PQEncOn = PQEncryption True

pattern PQEncOff :: PQEncryption
pattern PQEncOff = PQEncryption False

{-# COMPLETE PQEncOn, PQEncOff #-}

instance ToJSON PQEncryption where
  toEncoding (PQEncryption pq) = toEncoding pq
  toJSON (PQEncryption pq) = toJSON pq

instance FromJSON PQEncryption where
  parseJSON v = PQEncryption <$> parseJSON v
  omittedField = Just PQEncOff

newtype PQSupport = PQSupport {supportPQ :: Bool}
  deriving (Eq, Show)

pattern PQSupportOn :: PQSupport
pattern PQSupportOn = PQSupport True

pattern PQSupportOff :: PQSupport
pattern PQSupportOff = PQSupport False

{-# COMPLETE PQSupportOn, PQSupportOff #-}

instance ToJSON PQSupport where
  toEncoding (PQSupport pq) = toEncoding pq
  toJSON (PQSupport pq) = toJSON pq

instance FromJSON PQSupport where
  parseJSON v = PQSupport <$> parseJSON v
  omittedField = Just PQSupportOff

pqSupportToEnc :: PQSupport -> PQEncryption
pqSupportToEnc (PQSupport pq) = PQEncryption pq

pqEncToSupport :: PQEncryption -> PQSupport
pqEncToSupport (PQEncryption pq) = PQSupport pq

pqSupportAnd :: PQSupport -> PQSupport -> PQSupport
pqSupportAnd (PQSupport s1) (PQSupport s2) = PQSupport $ s1 && s2

pqEnableSupport :: VersionE2E -> PQSupport -> PQEncryption -> PQSupport
pqEnableSupport v (PQSupport sup) (PQEncryption enc) = PQSupport $ sup || (v >= pqRatchetE2EEncryptVersion && enc)

replyKEM_ :: VersionE2E -> Maybe (RKEMParams 'RKSProposed) -> PQSupport -> Maybe AUseKEM
replyKEM_ v kem_ = \case
  PQSupportOn | v >= pqRatchetE2EEncryptVersion -> Just $ case kem_ of
    Just (RKParamsProposed k) -> AUseKEM SRKSAccepted $ AcceptKEM k
    Nothing -> AUseKEM SRKSProposed ProposeKEM
  _ -> Nothing

instance StrEncoding PQEncryption where
  strEncode pqMode
    | enablePQ pqMode = "pq=enable"
    | otherwise = "pq=disable"
  strP =
    A.takeTill (== ' ') >>= \case
      "pq=enable" -> pq True
      "pq=disable" -> pq False
      _ -> fail "bad PQEncryption"
    where
      pq = pure . PQEncryption

instance StrEncoding PQSupport where
  strEncode = strEncode . pqSupportToEnc
  {-# INLINE strEncode #-}
  strP = pqEncToSupport <$> strP
  {-# INLINE strP #-}

data InitialKeys = IKUsePQ | IKNoPQ PQSupport
  deriving (Eq, Show)

pattern IKPQOn :: InitialKeys
pattern IKPQOn = IKNoPQ PQSupportOn

pattern IKPQOff :: InitialKeys
pattern IKPQOff = IKNoPQ PQSupportOff

instance StrEncoding InitialKeys where
  strEncode = \case
    IKUsePQ -> "pq=invitation"
    IKNoPQ pq -> strEncode pq
  strP = IKNoPQ <$> strP <|> "pq=invitation" $> IKUsePQ

-- determines whether PQ key should be included in invitation link
initialPQEncryption :: InitialKeys -> PQSupport
initialPQEncryption = \case
  IKUsePQ -> PQSupportOn
  IKNoPQ _ -> PQSupportOff -- default

-- determines whether PQ encryption should be used in connection
connPQEncryption :: InitialKeys -> PQSupport
connPQEncryption = \case
  IKUsePQ -> PQSupportOn
  IKNoPQ pq -> pq -- default for creating connection is IKNoPQ PQEncOn

rcCheckCanPad :: Int -> ByteString -> ExceptT CryptoError IO ()
rcCheckCanPad paddedMsgLen msg =
  unless (canPad (B.length msg) paddedMsgLen) $ throwE CryptoLargeMsgError

rcEncryptHeader :: AlgorithmI a => Ratchet a -> Maybe PQEncryption -> VersionE2E -> ExceptT CryptoError IO (MsgEncryptKey a, Ratchet a)
rcEncryptHeader Ratchet {rcSnd = Nothing} _ _ = throwE CERatchetState
rcEncryptHeader rc@Ratchet {rcSnd = Just sr@SndRatchet {rcCKs, rcHKs}, rcDHRs, rcKEM, rcNs, rcPN, rcAD = Str rcAD, rcSupportKEM, rcEnableKEM, rcVersion} pqEnc_ supportedE2EVersion = do
  -- state.CKs, mk = KDF_CK(state.CKs)
  let (ck', mk, iv, ehIV) = chainKdf rcCKs
      v = current rcVersion
      -- PQ encryption can be enabled or disabled
      rcEnableKEM' = fromMaybe rcEnableKEM pqEnc_
      -- support for PQ encryption (and therefore large headers/small envelopes) can only be enabled, it cannot be disabled
      rcSupportKEM' = pqEnableSupport v rcSupportKEM rcEnableKEM'
      -- This sets max version to support PQ encryption.
      -- Current version upgrade happens when peer decrypts the message.
      -- TODO note that maxSupported will not downgrade here below current (v).
      maxSupported' = max supportedE2EVersion $ if pqEnc_ == Just PQEncOn then pqRatchetE2EEncryptVersion else v
      rcVersion' = rcVersion {maxSupported = maxSupported'}
  -- enc_header = HENCRYPT(state.HKs, header)
  (ehAuthTag, ehBody) <- encryptAEAD rcHKs ehIV (paddedHeaderLen v rcSupportKEM') rcAD (msgHeader v maxSupported')
  -- return enc_header
  let emHeader = smpEncode EncMessageHeader {ehVersion = v, ehBody, ehAuthTag, ehIV}
      msgEncryptKey =
        MsgEncryptKey
          { msgRcVersion = v,
            msgKey = MessageKey mk iv,
            msgRcAD = rcAD,
            msgEncHeader = emHeader
          }
      rc' =
        rc
          { rcSnd = Just sr {rcCKs = ck'},
            rcNs = rcNs + 1,
            rcSupportKEM = rcSupportKEM',
            rcEnableKEM = rcEnableKEM',
            rcVersion = rcVersion',
            rcKEM = if pqEnc_ == Just PQEncOff then (\rck -> rck {rcKEMs = Nothing}) <$> rcKEM else rcKEM
          }
  pure (msgEncryptKey, rc')
  where
    -- header = HEADER_PQ2(
    --   dh = state.DHRs.public,
    --   kem = state.PQRs.public, // added for KEM #2
    --   ct = state.PQRct, // added for KEM #1
    --   pn = state.PN,
    --   n = state.Ns
    -- )
    msgHeader v maxSupported' =
      encodeMsgHeader
        v
        MsgHeader
          { msgMaxVersion = maxSupported',
            msgDHRs = publicKey rcDHRs,
            msgKEM = msgKEMParams <$> rcKEM,
            msgPN = rcPN,
            msgNs = rcNs
          }
    msgKEMParams RatchetKEM {rcPQRs = (k, _), rcKEMs} = case rcKEMs of
      Nothing -> ARKP SRKSProposed $ RKParamsProposed k
      Just RatchetKEMAccepted {rcPQRct} -> ARKP SRKSAccepted $ RKParamsAccepted rcPQRct k

type MsgEncryptKeyX448 = MsgEncryptKey 'X448

data MsgEncryptKey a = MsgEncryptKey
  { msgRcVersion :: VersionE2E,
    msgKey :: MessageKey,
    msgRcAD :: ByteString,
    msgEncHeader :: ByteString
  }
  deriving (Show)

rcEncryptMsg :: AlgorithmI a => MsgEncryptKey a -> Int -> ByteString -> ExceptT CryptoError IO ByteString
rcEncryptMsg MsgEncryptKey {msgKey = MessageKey mk iv, msgRcAD, msgEncHeader, msgRcVersion = v} paddedMsgLen msg = do
  -- return ENCRYPT(mk, plaintext, CONCAT(AD, enc_header))
  (emAuthTag, emBody) <- encryptAEAD mk iv paddedMsgLen (msgRcAD <> msgEncHeader) msg
  let msg' = encodeEncRatchetMessage v EncRatchetMessage {emHeader = msgEncHeader, emBody, emAuthTag}
  pure msg'

data SkippedMessage a
  = SMMessage (DecryptResult a)
  | SMHeader (Maybe RatchetStep) (MsgHeader a)
  | SMNone

data RatchetStep = AdvanceRatchet | SameRatchet
  deriving (Eq, Show)

type DecryptResult a = (Either CryptoError ByteString, Ratchet a, SkippedMsgDiff)

maxSkip :: Word32
maxSkip = 512

rcDecrypt ::
  forall a.
  (AlgorithmI a, DhAlgorithm a) =>
  TVar ChaChaDRG ->
  Ratchet a ->
  SkippedMsgKeys ->
  ByteString ->
  ExceptT CryptoError IO (DecryptResult a)
rcDecrypt g rc@Ratchet {rcRcv, rcAD = Str rcAD, rcVersion} rcMKSkipped msg' = do
  encMsg@EncRatchetMessage {emHeader} <- parseE CryptoHeaderError encRatchetMessageP msg'
  encHdr <- parseE CryptoHeaderError smpP emHeader
  -- plaintext = TrySkippedMessageKeysHE(state, enc_header, cipher-text, AD)
  decryptSkipped encHdr encMsg >>= \case
    SMNone -> do
      (rcStep, hdr) <- decryptRcHeader rcRcv encHdr
      decryptRcMessage rcStep hdr encMsg
    SMHeader rcStep_ hdr ->
      case rcStep_ of
        Just rcStep -> decryptRcMessage rcStep hdr encMsg
        Nothing -> throwE CERatchetHeader
    SMMessage r -> pure r
  where
    decryptRcMessage :: RatchetStep -> MsgHeader a -> EncRatchetMessage -> ExceptT CryptoError IO (DecryptResult a)
    decryptRcMessage rcStep hdr@MsgHeader {msgMaxVersion, msgPN, msgNs} encMsg = do
      -- if dh_ratchet:
      (rc', smks1) <- case rcStep of
        SameRatchet -> pure (upgradedRatchet, M.empty)
        AdvanceRatchet -> do
          -- SkipMessageKeysHE(state, header.pn)
          (rc', hmks) <- liftEither $ skipMessageKeys msgPN upgradedRatchet
          -- DHRatchetPQ2HE(state, header)
          (,hmks) <$> ratchetStep rc' hdr
      -- SkipMessageKeysHE(state, header.n)
      case skipMessageKeys msgNs rc' of
        Left e -> pure (Left e, rc', smkDiff smks1)
        Right (rc''@Ratchet {rcRcv = Just rr@RcvRatchet {rcCKr}, rcNr}, smks2) -> do
          -- state.CKr, mk = KDF_CK(state.CKr)
          let (rcCKr', mk, iv, _) = chainKdf rcCKr
          -- return DECRYPT (mk, cipher-text, CONCAT (AD, enc_header))
          msg <- decryptMessage (MessageKey mk iv) encMsg
          -- state . Nr += 1
          pure (msg, rc'' {rcRcv = Just rr {rcCKr = rcCKr'}, rcNr = rcNr + 1}, smkDiff $ smks1 <> smks2)
        Right (rc'', smks2) ->
          pure (Left CERatchetState, rc'', smkDiff $ smks1 <> smks2)
      where
        upgradedRatchet :: Ratchet a
        upgradedRatchet
          | msgMaxVersion > current = rc {rcVersion = rcVersion {current = max current $ min msgMaxVersion maxSupported}}
          | otherwise = rc
          where
            RatchetVersions {current, maxSupported} = rcVersion
        smkDiff :: SkippedMsgKeys -> SkippedMsgDiff
        smkDiff smks = if M.null smks then SMDNoChange else SMDAdd smks
        ratchetStep :: Ratchet a -> MsgHeader a -> ExceptT CryptoError IO (Ratchet a)
        ratchetStep rc'@Ratchet {rcDHRs, rcRK, rcNHKs, rcNHKr, rcSupportKEM, rcVersion = rv} MsgHeader {msgDHRs, msgKEM} = do
          (kemSS, kemSS', rcKEM') <- pqRatchetStep rc' msgKEM
          -- state.DHRs = GENERATE_DH()
          (_, rcDHRs') <- atomically $ generateKeyPair @a g
          -- state.RK, state.CKr, state.NHKr = KDF_RK_HE(state.RK, DH(state.DHRs, state.DHRr) || ss)
          let (rcRK', rcCKr', rcNHKr') = rootKdf rcRK msgDHRs rcDHRs kemSS
              -- state.RK, state.CKs, state.NHKs = KDF_RK_HE(state.RK, DH(state.DHRs, state.DHRr) || state.PQRss)
              (rcRK'', rcCKs', rcNHKs') = rootKdf rcRK' msgDHRs rcDHRs' kemSS'
              sndKEM = isJust kemSS'
              rcvKEM = isJust kemSS
              rcEnableKEM' = PQEncryption $ sndKEM || rcvKEM || isJust rcKEM'
          pure
            rc'
              { rcDHRs = rcDHRs',
                rcKEM = rcKEM',
                rcSupportKEM = pqEnableSupport (current rv) rcSupportKEM rcEnableKEM',
                rcEnableKEM = rcEnableKEM',
                rcSndKEM = PQEncryption sndKEM,
                rcRcvKEM = PQEncryption rcvKEM,
                rcRK = rcRK'',
                rcSnd = Just SndRatchet {rcDHRr = msgDHRs, rcCKs = rcCKs', rcHKs = rcNHKs},
                rcRcv = Just RcvRatchet {rcCKr = rcCKr', rcHKr = rcNHKr},
                rcPN = rcNs rc,
                rcNs = 0,
                rcNr = 0,
                rcNHKs = rcNHKs',
                rcNHKr = rcNHKr'
              }
        pqRatchetStep :: Ratchet a -> Maybe ARKEMParams -> ExceptT CryptoError IO (Maybe KEMSharedKey, Maybe KEMSharedKey, Maybe RatchetKEM)
        pqRatchetStep Ratchet {rcKEM, rcEnableKEM = PQEncryption pqEnc, rcVersion = rv} = \case
          -- received message does not have KEM in header,
          -- but the user enabled KEM when sending previous message
          Nothing -> case rcKEM of
            Nothing | pqEnc && current rv >= pqRatchetE2EEncryptVersion -> do
              rcPQRs <- liftIO $ sntrup761Keypair g
              pure (Nothing, Nothing, Just RatchetKEM {rcPQRs, rcKEMs = Nothing})
            _ -> pure (Nothing, Nothing, Nothing)
          -- received message has KEM in header.
          Just (ARKP _ ps)
            | pqEnc && current rv >= pqRatchetE2EEncryptVersion -> do
                -- state.PQRr = header.kem
                (ss, rcPQRr) <- sharedSecret
                -- state.PQRct = PQKEM-ENC(state.PQRr, state.PQRss) // encapsulated additional shared secret KEM #1
                (rcPQRct, rcPQRss) <- liftIO $ sntrup761Enc g rcPQRr
                -- state.PQRs = GENERATE_PQKEM()
                rcPQRs <- liftIO $ sntrup761Keypair g
                let kem' = RatchetKEM {rcPQRs, rcKEMs = Just RatchetKEMAccepted {rcPQRr, rcPQRss, rcPQRct}}
                pure (ss, Just rcPQRss, Just kem')
            | otherwise -> do
                -- state.PQRr = header.kem
                (ss, _) <- sharedSecret
                pure (ss, Nothing, Nothing)
            where
              sharedSecret = case ps of
                RKParamsProposed k -> pure (Nothing, k)
                RKParamsAccepted ct k -> case rcKEM of
                  Nothing -> throwE CERatchetKEMState
                  -- ss = PQKEM-DEC(state.PQRs.private, header.ct)
                  Just RatchetKEM {rcPQRs} -> do
                    ss <- liftIO $ sntrup761Dec ct (snd rcPQRs)
                    pure (Just ss, k)
    skipMessageKeys :: Word32 -> Ratchet a -> Either CryptoError (Ratchet a, SkippedMsgKeys)
    skipMessageKeys _ r@Ratchet {rcRcv = Nothing} = Right (r, M.empty)
    skipMessageKeys untilN r@Ratchet {rcRcv = Just rr@RcvRatchet {rcCKr, rcHKr}, rcNr}
      | rcNr > untilN + 1 = Left $ CERatchetEarlierMessage (rcNr - untilN - 1)
      | rcNr == untilN + 1 = Left CERatchetDuplicateMessage
      | rcNr + maxSkip < untilN = Left $ CERatchetTooManySkipped (untilN + 1 - rcNr)
      | rcNr == untilN = Right (r, M.empty)
      | otherwise =
          let (rcCKr', rcNr', mks) = advanceRcvRatchet (untilN - rcNr) rcCKr rcNr M.empty
              r' = r {rcRcv = Just rr {rcCKr = rcCKr'}, rcNr = rcNr'}
           in Right (r', M.singleton rcHKr mks)
    advanceRcvRatchet :: Word32 -> RatchetKey -> Word32 -> SkippedHdrMsgKeys -> (RatchetKey, Word32, SkippedHdrMsgKeys)
    advanceRcvRatchet 0 ck msgNs mks = (ck, msgNs, mks)
    advanceRcvRatchet n ck msgNs mks =
      let (ck', mk, iv, _) = chainKdf ck
          mks' = M.insert msgNs (MessageKey mk iv) mks
       in advanceRcvRatchet (n - 1) ck' (msgNs + 1) mks'
    decryptSkipped :: EncMessageHeader -> EncRatchetMessage -> ExceptT CryptoError IO (SkippedMessage a)
    decryptSkipped encHdr encMsg = tryDecryptSkipped SMNone $ M.assocs rcMKSkipped
      where
        tryDecryptSkipped :: SkippedMessage a -> [(HeaderKey, SkippedHdrMsgKeys)] -> ExceptT CryptoError IO (SkippedMessage a)
        tryDecryptSkipped SMNone ((hk, mks) : hks) = do
          tryE (decryptHeader hk encHdr) >>= \case
            Left CERatchetHeader -> tryDecryptSkipped SMNone hks
            Left e -> throwE e
            Right hdr@MsgHeader {msgNs} ->
              case M.lookup msgNs mks of
                Nothing ->
                  let nextRc
                        | maybe False ((== hk) . rcHKr) rcRcv = Just SameRatchet
                        | hk == rcNHKr rc = Just AdvanceRatchet
                        | otherwise = Nothing
                   in pure $ SMHeader nextRc hdr
                Just mk -> do
                  msg <- decryptMessage mk encMsg
                  pure $ SMMessage (msg, rc, SMDRemove hk msgNs)
        tryDecryptSkipped r _ = pure r
    decryptRcHeader :: Maybe RcvRatchet -> EncMessageHeader -> ExceptT CryptoError IO (RatchetStep, MsgHeader a)
    decryptRcHeader Nothing hdr = decryptNextHeader hdr
    decryptRcHeader (Just RcvRatchet {rcHKr}) hdr =
      -- header = HDECRYPT(state.HKr, enc_header)
      ((SameRatchet,) <$> decryptHeader rcHKr hdr) `catchE` \case
        CERatchetHeader -> decryptNextHeader hdr
        e -> throwE e
    -- header = HDECRYPT(state.NHKr, enc_header)
    decryptNextHeader hdr = (AdvanceRatchet,) <$> decryptHeader (rcNHKr rc) hdr
    decryptHeader k EncMessageHeader {ehVersion, ehBody, ehAuthTag, ehIV} = do
      header <- decryptAEAD k ehIV rcAD ehBody ehAuthTag `catchE` \_ -> throwE CERatchetHeader
      parseE' CryptoHeaderError (msgHeaderP ehVersion) header
    decryptMessage :: MessageKey -> EncRatchetMessage -> ExceptT CryptoError IO (Either CryptoError ByteString)
    decryptMessage (MessageKey mk iv) EncRatchetMessage {emHeader, emBody, emAuthTag} =
      -- DECRYPT(mk, cipher-text, CONCAT(AD, enc_header))
      tryE $ decryptAEAD mk iv (rcAD <> emHeader) emBody emAuthTag

rootKdf :: (AlgorithmI a, DhAlgorithm a) => RatchetKey -> PublicKey a -> PrivateKey a -> Maybe KEMSharedKey -> (RatchetKey, RatchetKey, Key)
rootKdf (RatchetKey rk) k pk kemSecret_ =
  let dhOut = dhBytes' (dh' k pk)
      ss = case kemSecret_ of
        Just (KEMSharedKey s) -> dhOut <> BA.convert s
        Nothing -> dhOut
      (rk', ck, nhk) = hkdf3 rk ss "SimpleXRootRatchet"
   in (RatchetKey rk', RatchetKey ck, Key nhk)

chainKdf :: RatchetKey -> (RatchetKey, Key, IV, IV)
chainKdf (RatchetKey ck) =
  let (ck', mk, ivs) = hkdf3 "" ck "SimpleXChainRatchet"
      (iv1, iv2) = B.splitAt 16 ivs
   in (RatchetKey ck', Key mk, IV iv1, IV iv2)

hkdf3 :: ByteString -> ByteString -> ByteString -> (ByteString, ByteString, ByteString)
hkdf3 salt ikm info = (s1, s2, s3)
  where
    prk = H.extract salt ikm :: H.PRK SHA512
    out = H.expand prk info 96
    (s1, rest) = B.splitAt 32 out
    (s2, s3) = B.splitAt 32 rest

$(JQ.deriveJSON defaultJSON ''RcvRatchet)

$(JQ.deriveJSON defaultJSON ''RatchetKEMAccepted)

$(JQ.deriveJSON defaultJSON ''RatchetKEM)

instance AlgorithmI a => ToJSON (SndRatchet a) where
  toEncoding = $(JQ.mkToEncoding defaultJSON ''SndRatchet)
  toJSON = $(JQ.mkToJSON defaultJSON ''SndRatchet)

instance AlgorithmI a => FromJSON (SndRatchet a) where
  parseJSON = $(JQ.mkParseJSON defaultJSON ''SndRatchet)

instance AlgorithmI a => ToJSON (Ratchet a) where
  toEncoding = $(JQ.mkToEncoding defaultJSON ''Ratchet)
  toJSON = $(JQ.mkToJSON defaultJSON ''Ratchet)

instance AlgorithmI a => FromJSON (Ratchet a) where
  parseJSON = $(JQ.mkParseJSON defaultJSON ''Ratchet)

instance AlgorithmI a => ToField (Ratchet a) where toField = toField . Binary . LB.toStrict . J.encode

instance (AlgorithmI a, Typeable a) => FromField (Ratchet a) where fromField = blobFieldDecoder J.eitherDecodeStrict'

instance ToField PQEncryption where toField (PQEncryption pqEnc) = toField (BI pqEnc)

instance FromField PQEncryption where 
#if defined(dbPostgres)
  fromField f dat = PQEncryption . unBI <$> fromField f dat
#else
  fromField f = PQEncryption . unBI <$> fromField f
#endif

instance ToField PQSupport where toField (PQSupport pqEnc) = toField (BI pqEnc)

instance FromField PQSupport where
#if defined(dbPostgres)
  fromField f dat = PQSupport . unBI <$> fromField f dat
#else
  fromField f = PQSupport . unBI <$> fromField f
#endif

instance Encoding (MsgEncryptKey a) where
  smpEncode MsgEncryptKey {msgRcVersion = v, msgKey, msgRcAD, msgEncHeader} =
    smpEncode (v, msgRcAD, msgKey, Large msgEncHeader)
  smpP = do
    (v, msgRcAD, msgKey, Large msgEncHeader) <- smpP
    pure MsgEncryptKey {msgRcVersion = v, msgRcAD, msgKey, msgEncHeader}

instance AlgorithmI a => ToField (MsgEncryptKey a) where toField = toField . Binary . smpEncode

instance (AlgorithmI a, Typeable a) => FromField (MsgEncryptKey a) where fromField = blobFieldDecoder smpDecode
