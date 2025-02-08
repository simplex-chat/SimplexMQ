{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-redundant-constraints #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

-- |
-- Module      : Simplex.Messaging.Crypto
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- This module provides cryptography implementation for SMP protocols based on
-- <https://hackage.haskell.org/package/cryptonite cryptonite package>.
module Simplex.Messaging.Crypto
  ( -- * Cryptographic keys
    Algorithm (..),
    SAlgorithm (..),
    Alg (..),
    AuthAlg (..),
    DhAlg (..),
    DhAlgorithm,
    PrivateKey (..),
    PublicKey (..),
    PrivateKeyEd25519,
    PublicKeyEd25519,
    PrivateKeyX25519,
    PublicKeyX25519,
    PrivateKeyX448,
    PublicKeyX448,
    APrivateKey (..),
    APublicKey (..),
    APrivateSignKey (..),
    APublicVerifyKey (..),
    APrivateDhKey (..),
    APublicDhKey (..),
    APrivateAuthKey (..),
    APublicAuthKey (..),
    CryptoPublicKey (..),
    CryptoPrivateKey (..),
    AAuthKeyPair,
    KeyPair,
    KeyPairX25519,
    ASignatureKeyPair,
    DhSecret (..),
    DhSecretX25519,
    ADhSecret (..),
    KeyHash (..),
    newRandom,
    newRandomDRG,
    generateAKeyPair,
    generateKeyPair,
    generateSignatureKeyPair,
    generateAuthKeyPair,
    generateDhKeyPair,
    privateToX509,
    x509ToPublic,
    x509ToPrivate,
    publicKey,
    signatureKeyPair,
    publicToX509,
    encodeASNObj,

    -- * key encoding/decoding
    encodePubKey,
    decodePubKey,
    encodePrivKey,
    decodePrivKey,
    pubKeyBytes,

    -- * sign/verify
    Signature (..),
    ASignature (..),
    CryptoSignature (..),
    SignatureSize (..),
    SignatureAlgorithm,
    AuthAlgorithm,
    AlgorithmI (..),
    sign,
    sign',
    verify,
    verify',
    validSignatureSize,
    checkAlgorithm,

    -- * crypto_box authenticator, as discussed in https://groups.google.com/g/sci.crypt/c/73yb5a9pz2Y/m/LNgRO7IYXOwJ
    CbAuthenticator (..),
    cbAuthenticatorSize,
    cbAuthenticate,
    cbVerify,

    -- * DH derivation
    dh',
    dhBytes',

    -- * AES256 AEAD-GCM scheme
    Key (..),
    IV (..),
    GCMIV (unGCMIV), -- constructor is not exported
    AuthTag (..),
    encryptAEAD,
    decryptAEAD,
    encryptAESNoPad,
    decryptAESNoPad,
    authTagSize,
    randomAesKey,
    randomGCMIV,
    ivSize,
    gcmIVSize,
    gcmIV,

    -- * NaCl crypto_box
    CbNonce (unCbNonce),
    pattern CbNonce,
    cbEncrypt,
    cbEncryptNoPad,
    cbEncryptMaxLenBS,
    cbDecrypt,
    cbDecryptNoPad,
    sbDecrypt_,
    sbEncrypt_,
    cbNonce,
    randomCbNonce,
    reverseNonce,

    -- * NaCl crypto_secretbox
    SbKey (unSbKey),
    pattern SbKey,
    sbEncrypt,
    sbDecrypt,
    sbKey,
    unsafeSbKey,
    randomSbKey,

    -- * secret_box chains
    SbChainKey,
    SbKeyNonce,
    sbcInit,
    sbcHkdf,

    -- * pseudo-random bytes
    randomBytes,

    -- * digests
    sha256Hash,
    sha512Hash,

    -- * Message padding / un-padding
    pad,
    unPad,

    -- * X509 Certificates
    SignedCertificate,
    Certificate,
    signCertificate,
    signX509,
    verifyX509,
    certificateFingerprint,
    signedFingerprint,
    SignatureAlgorithmX509 (..),
    SignedObject (..),
    encodeCertChain,
    certChainP,

    -- * Cryptography error type
    CryptoError (..),

    -- * Limited size ByteStrings
    MaxLenBS,
    pattern MaxLenBS,
    maxLenBS,
    unsafeMaxLenBS,
    appendMaxLenBS,
  )
where

import Control.Concurrent.STM
import Control.Exception (Exception)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans.Except
import Crypto.Cipher.AES (AES256)
import qualified Crypto.Cipher.Types as AES
import qualified Crypto.Cipher.XSalsa as XSalsa
import qualified Crypto.Error as CE
import Crypto.Hash (Digest, SHA256 (..), SHA512 (..), hash, hashDigestSize)
import qualified Crypto.KDF.HKDF as H
import qualified Crypto.MAC.Poly1305 as Poly1305
import qualified Crypto.PubKey.Curve25519 as X25519
import qualified Crypto.PubKey.Curve448 as X448
import qualified Crypto.PubKey.Ed25519 as Ed25519
import qualified Crypto.PubKey.Ed448 as Ed448
import Crypto.Random (ChaChaDRG, MonadPseudoRandom, drgNew, randomBytesGenerate, withDRG)
import Data.ASN1.BinaryEncoding
import Data.ASN1.Encoding
import Data.ASN1.Types
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Bifunctor (bimap, first)
import Data.ByteArray (ByteArrayAccess)
import qualified Data.ByteArray as BA
import Data.ByteString.Base64 (decode, encode)
import qualified Data.ByteString.Base64.URL as U
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Constraint (Dict (..))
import Data.Kind (Constraint, Type)
import qualified Data.List.NonEmpty as L
import Data.String
import Data.Type.Equality
import Data.Typeable (Proxy (Proxy), Typeable)
import Data.Word (Word32)
import Data.X509
import Data.X509.Validation (Fingerprint (..), getFingerprint)
import GHC.TypeLits (ErrorMessage (..), KnownNat, Nat, TypeError, natVal, type (+))
import Network.Transport.Internal (decodeWord16, encodeWord16)
import Simplex.Messaging.Agent.Store.DB (Binary (..), FromField (..), ToField (..), blobFieldDecoder)
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (parseAll, parseString)
import Simplex.Messaging.Util ((<$?>))

-- | Cryptographic algorithms.
data Algorithm = Ed25519 | Ed448 | X25519 | X448

-- | Singleton types for 'Algorithm'.
data SAlgorithm :: Algorithm -> Type where
  SEd25519 :: SAlgorithm Ed25519
  SEd448 :: SAlgorithm Ed448
  SX25519 :: SAlgorithm X25519
  SX448 :: SAlgorithm X448

deriving instance Show (SAlgorithm a)

data Alg = forall a. AlgorithmI a => Alg (SAlgorithm a)

data AuthAlg
  = forall a.
    (AlgorithmI a, AuthAlgorithm a) =>
    AuthAlg (SAlgorithm a)

data DhAlg
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    DhAlg (SAlgorithm a)

class AlgorithmI (a :: Algorithm) where sAlgorithm :: SAlgorithm a

instance AlgorithmI Ed25519 where sAlgorithm = SEd25519

instance AlgorithmI Ed448 where sAlgorithm = SEd448

instance AlgorithmI X25519 where sAlgorithm = SX25519

instance AlgorithmI X448 where sAlgorithm = SX448

checkAlgorithm :: forall t a a'. (AlgorithmI a, AlgorithmI a') => t a' -> Either String (t a)
checkAlgorithm x = case testEquality (sAlgorithm @a) (sAlgorithm @a') of
  Just Refl -> Right x
  Nothing -> Left "bad algorithm"

instance TestEquality SAlgorithm where
  testEquality SEd25519 SEd25519 = Just Refl
  testEquality SEd448 SEd448 = Just Refl
  testEquality SX25519 SX25519 = Just Refl
  testEquality SX448 SX448 = Just Refl
  testEquality _ _ = Nothing

-- | GADT for public keys.
data PublicKey (a :: Algorithm) where
  PublicKeyEd25519 :: Ed25519.PublicKey -> PublicKey Ed25519
  PublicKeyEd448 :: Ed448.PublicKey -> PublicKey Ed448
  PublicKeyX25519 :: X25519.PublicKey -> PublicKey X25519
  PublicKeyX448 :: X448.PublicKey -> PublicKey X448

deriving instance Eq (PublicKey a)

deriving instance Show (PublicKey a)

data APublicKey
  = forall a.
    AlgorithmI a =>
    APublicKey (SAlgorithm a) (PublicKey a)

instance Encoding APublicKey where
  smpEncode = smpEncode . encodePubKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePubKey
  {-# INLINE smpDecode #-}

deriving instance Show APublicKey

type PublicKeyEd25519 = PublicKey Ed25519

type PublicKeyX25519 = PublicKey X25519

type PublicKeyX448 = PublicKey X448

-- | GADT for private keys.
data PrivateKey (a :: Algorithm) where
  PrivateKeyEd25519 :: Ed25519.SecretKey -> Ed25519.PublicKey -> PrivateKey Ed25519
  PrivateKeyEd448 :: Ed448.SecretKey -> Ed448.PublicKey -> PrivateKey Ed448
  PrivateKeyX25519 :: X25519.SecretKey -> X25519.PublicKey -> PrivateKey X25519
  PrivateKeyX448 :: X448.SecretKey -> X448.PublicKey -> PrivateKey X448

deriving instance Eq (PrivateKey a)

deriving instance Show (PrivateKey a)

-- Do not enable, to avoid leaking key data
-- instance StrEncoding (PrivateKey Ed25519) where

-- Used in notification store log
instance StrEncoding (PrivateKey X25519) where
  strEncode = strEncode . encodePrivKey
  {-# INLINE strEncode #-}
  strDecode = decodePrivKey
  {-# INLINE strDecode #-}

data APrivateKey
  = forall a.
    AlgorithmI a =>
    APrivateKey (SAlgorithm a) (PrivateKey a)

deriving instance Show APrivateKey

type PrivateKeyEd25519 = PrivateKey Ed25519

type PrivateKeyX25519 = PrivateKey X25519

type PrivateKeyX448 = PrivateKey X448

type family SignatureAlgorithm (a :: Algorithm) :: Constraint where
  SignatureAlgorithm Ed25519 = ()
  SignatureAlgorithm Ed448 = ()
  SignatureAlgorithm a =
    (Int ~ Bool, TypeError (Text "Algorithm " :<>: ShowType a :<>: Text " cannot be used to sign/verify"))

signatureAlgorithm :: SAlgorithm a -> Maybe (Dict (SignatureAlgorithm a))
signatureAlgorithm = \case
  SEd25519 -> Just Dict
  SEd448 -> Just Dict
  _ -> Nothing

data APrivateSignKey
  = forall a.
    (AlgorithmI a, SignatureAlgorithm a) =>
    APrivateSignKey (SAlgorithm a) (PrivateKey a)

deriving instance Show APrivateSignKey

instance Encoding APrivateSignKey where
  smpEncode = smpEncode . encodePrivKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePrivKey
  {-# INLINE smpDecode #-}

instance StrEncoding APrivateSignKey where
  strEncode = strEncode . encodePrivKey
  {-# INLINE strEncode #-}
  strDecode = decodePrivKey
  {-# INLINE strDecode #-}

data APublicVerifyKey
  = forall a.
    (AlgorithmI a, SignatureAlgorithm a) =>
    APublicVerifyKey (SAlgorithm a) (PublicKey a)

deriving instance Show APublicVerifyKey

data APrivateDhKey
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    APrivateDhKey (SAlgorithm a) (PrivateKey a)

deriving instance Show APrivateDhKey

data APublicDhKey
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    APublicDhKey (SAlgorithm a) (PublicKey a)

deriving instance Show APublicDhKey

data DhSecret (a :: Algorithm) where
  DhSecretX25519 :: X25519.DhSecret -> DhSecret X25519
  DhSecretX448 :: X448.DhSecret -> DhSecret X448

deriving instance Eq (DhSecret a)

deriving instance Show (DhSecret a)

data ADhSecret
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    ADhSecret (SAlgorithm a) (DhSecret a)

type DhSecretX25519 = DhSecret X25519

type family DhAlgorithm (a :: Algorithm) :: Constraint where
  DhAlgorithm X25519 = ()
  DhAlgorithm X448 = ()
  DhAlgorithm a =
    (Int ~ Bool, TypeError (Text "Algorithm " :<>: ShowType a :<>: Text " cannot be used for DH exchange"))

dhAlgorithm :: SAlgorithm a -> Maybe (Dict (DhAlgorithm a))
dhAlgorithm = \case
  SX25519 -> Just Dict
  SX448 -> Just Dict
  _ -> Nothing

data APrivateAuthKey
  = forall a.
    (AlgorithmI a, AuthAlgorithm a) =>
    APrivateAuthKey (SAlgorithm a) (PrivateKey a)

instance Eq APrivateAuthKey where
  APrivateAuthKey a k == APrivateAuthKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APrivateAuthKey

instance Encoding APrivateAuthKey where
  smpEncode = smpEncode . encodePrivKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePrivKey
  {-# INLINE smpDecode #-}

instance StrEncoding APrivateAuthKey where
  strEncode = strEncode . encodePrivKey
  {-# INLINE strEncode #-}
  strDecode = decodePrivKey
  {-# INLINE strDecode #-}

data APublicAuthKey
  = forall a.
    (AlgorithmI a, AuthAlgorithm a) =>
    APublicAuthKey (SAlgorithm a) (PublicKey a)

instance Eq APublicAuthKey where
  APublicAuthKey a k == APublicAuthKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APublicAuthKey

-- either X25519 or Ed algorithm that can be used to authorize commands to SMP server
type family AuthAlgorithm (a :: Algorithm) :: Constraint where
  AuthAlgorithm Ed25519 = ()
  AuthAlgorithm Ed448 = ()
  AuthAlgorithm X25519 = ()
  AuthAlgorithm a =
    (Int ~ Bool, TypeError (Text "Algorithm " :<>: ShowType a :<>: Text " cannot be used for authorization"))

authAlgorithm :: SAlgorithm a -> Maybe (Dict (AuthAlgorithm a))
authAlgorithm = \case
  SEd25519 -> Just Dict
  SEd448 -> Just Dict
  SX25519 -> Just Dict
  _ -> Nothing

dhBytes' :: DhSecret a -> ByteString
dhBytes' = \case
  DhSecretX25519 s -> BA.convert s
  DhSecretX448 s -> BA.convert s

instance AlgorithmI a => StrEncoding (DhSecret a) where
  strEncode = strEncode . dhBytes'
  strDecode = (\(ADhSecret _ s) -> checkAlgorithm s) <=< strDecode

instance StrEncoding ADhSecret where
  strEncode (ADhSecret _ s) = strEncode $ dhBytes' s
  strDecode = cryptoPassed . secret
    where
      secret bs
        | B.length bs == x25519_size = ADhSecret SX25519 . DhSecretX25519 <$> X25519.dhSecret bs
        | B.length bs == x448_size = ADhSecret SX448 . DhSecretX448 <$> X448.dhSecret bs
        | otherwise = CE.CryptoFailed CE.CryptoError_SharedSecretSizeInvalid
      cryptoPassed = \case
        CE.CryptoPassed s -> Right s
        CE.CryptoFailed e -> Left $ show e

instance AlgorithmI a => IsString (DhSecret a) where
  fromString = parseString strDecode

-- | Class for public key types
class CryptoPublicKey k where
  toPubKey :: (forall a. AlgorithmI a => PublicKey a -> b) -> k -> b
  pubKey :: APublicKey -> Either String k

instance CryptoPublicKey APublicKey where
  toPubKey f (APublicKey _ k) = f k
  pubKey = Right

instance CryptoPublicKey APublicVerifyKey where
  toPubKey f (APublicVerifyKey _ k) = f k
  pubKey (APublicKey a k) = case signatureAlgorithm a of
    Just Dict -> Right $ APublicVerifyKey a k
    _ -> Left "key does not support signature algorithms"

instance CryptoPublicKey APublicAuthKey where
  toPubKey f (APublicAuthKey _ k) = f k
  pubKey (APublicKey a k) = case authAlgorithm a of
    Just Dict -> Right $ APublicAuthKey a k
    _ -> Left "key does not support auth algorithms"

instance CryptoPublicKey APublicDhKey where
  toPubKey f (APublicDhKey _ k) = f k
  pubKey (APublicKey a k) = case dhAlgorithm a of
    Just Dict -> Right $ APublicDhKey a k
    _ -> Left "key does not support DH algorithms"

instance AlgorithmI a => CryptoPublicKey (PublicKey a) where
  toPubKey = id
  pubKey (APublicKey _ k) = checkAlgorithm k

instance Encoding APublicVerifyKey where
  smpEncode = smpEncode . encodePubKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePubKey
  {-# INLINE smpDecode #-}

instance Encoding APublicAuthKey where
  smpEncode = smpEncode . encodePubKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePubKey
  {-# INLINE smpDecode #-}

instance Encoding APublicDhKey where
  smpEncode = smpEncode . encodePubKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePubKey
  {-# INLINE smpDecode #-}

instance AlgorithmI a => Encoding (PublicKey a) where
  smpEncode = smpEncode . encodePubKey
  {-# INLINE smpEncode #-}
  smpDecode = decodePubKey
  {-# INLINE smpDecode #-}

instance StrEncoding APublicVerifyKey where
  strEncode = strEncode . encodePubKey
  {-# INLINE strEncode #-}
  strDecode = decodePubKey
  {-# INLINE strDecode #-}

instance StrEncoding APublicAuthKey where
  strEncode = strEncode . encodePubKey
  {-# INLINE strEncode #-}
  strDecode = decodePubKey
  {-# INLINE strDecode #-}

instance StrEncoding APublicDhKey where
  strEncode = strEncode . encodePubKey
  {-# INLINE strEncode #-}
  strDecode = decodePubKey
  {-# INLINE strDecode #-}

instance AlgorithmI a => StrEncoding (PublicKey a) where
  strEncode = strEncode . encodePubKey
  {-# INLINE strEncode #-}
  strDecode = decodePubKey
  {-# INLINE strDecode #-}

instance AlgorithmI a => ToJSON (PublicKey a) where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance AlgorithmI a => FromJSON (PublicKey a) where
  parseJSON = strParseJSON "PublicKey"

encodePubKey :: CryptoPublicKey k => k -> ByteString
encodePubKey = toPubKey $ encodeASNObj . publicToX509
{-# INLINE encodePubKey #-}

pubKeyBytes :: PublicKey a -> ByteString
pubKeyBytes = \case
  PublicKeyEd25519 k -> BA.convert k
  PublicKeyEd448 k -> BA.convert k
  PublicKeyX25519 k -> BA.convert k
  PublicKeyX448 k -> BA.convert k

class CryptoPrivateKey pk where
  type PublicKeyType pk
  toPrivKey :: (forall a. AlgorithmI a => PrivateKey a -> b) -> pk -> b
  privKey :: APrivateKey -> Either String pk

instance CryptoPrivateKey APrivateKey where
  type PublicKeyType APrivateKey = APublicKey
  toPrivKey f (APrivateKey _ k) = f k
  privKey = Right

instance CryptoPrivateKey APrivateSignKey where
  type PublicKeyType APrivateSignKey = APublicVerifyKey
  toPrivKey f (APrivateSignKey _ k) = f k
  privKey (APrivateKey a k) = case signatureAlgorithm a of
    Just Dict -> Right $ APrivateSignKey a k
    _ -> Left "key does not support signature algorithms"

instance CryptoPrivateKey APrivateAuthKey where
  type PublicKeyType APrivateAuthKey = APublicAuthKey
  toPrivKey f (APrivateAuthKey _ k) = f k
  privKey (APrivateKey a k) = case authAlgorithm a of
    Just Dict -> Right $ APrivateAuthKey a k
    _ -> Left "key does not support auth algorithms"

instance CryptoPrivateKey APrivateDhKey where
  type PublicKeyType APrivateDhKey = APublicDhKey
  toPrivKey f (APrivateDhKey _ k) = f k
  privKey (APrivateKey a k) = case dhAlgorithm a of
    Just Dict -> Right $ APrivateDhKey a k
    _ -> Left "key does not support DH algorithm"

instance AlgorithmI a => CryptoPrivateKey (PrivateKey a) where
  type PublicKeyType (PrivateKey a) = PublicKey a
  toPrivKey = id
  privKey (APrivateKey _ k) = checkAlgorithm k

publicKey :: PrivateKey a -> PublicKey a
publicKey = \case
  PrivateKeyEd25519 _ k -> PublicKeyEd25519 k
  PrivateKeyEd448 _ k -> PublicKeyEd448 k
  PrivateKeyX25519 _ k -> PublicKeyX25519 k
  PrivateKeyX448 _ k -> PublicKeyX448 k

-- | Expand signature private key to a key pair.
signatureKeyPair :: APrivateSignKey -> ASignatureKeyPair
signatureKeyPair ak@(APrivateSignKey a k) = (APublicVerifyKey a (publicKey k), ak)

encodePrivKey :: CryptoPrivateKey pk => pk -> ByteString
encodePrivKey = toPrivKey $ encodeASNObj . privateToX509

instance AlgorithmI a => IsString (PrivateKey a) where
  fromString = parseString $ decode >=> decodePrivKey

instance AlgorithmI a => IsString (PublicKey a) where
  fromString = parseString $ decode >=> decodePubKey

instance AlgorithmI a => ToJSON (PrivateKey a) where
  toJSON = strToJSON . strEncode . encodePrivKey
  toEncoding = strToJEncoding . strEncode . encodePrivKey

instance AlgorithmI a => FromJSON (PrivateKey a) where
  parseJSON v = (decodePrivKey <=< U.decode) <$?> strParseJSON "PrivateKey" v

type KeyPairType pk = (PublicKeyType pk, pk)

type KeyPair a = KeyPairType (PrivateKey a)

type KeyPairX25519 = KeyPair X25519

-- TODO narrow key pair types to have the same algorithm in both keys
type AKeyPair = KeyPairType APrivateKey

type ASignatureKeyPair = KeyPairType APrivateSignKey

type ADhKeyPair = KeyPairType APrivateDhKey

type AAuthKeyPair = KeyPairType APrivateAuthKey

newRandom :: IO (TVar ChaChaDRG)
newRandom = newTVarIO =<< drgNew

newRandomDRG :: TVar ChaChaDRG -> STM (TVar ChaChaDRG)
newRandomDRG g = newTVar =<< stateTVar g (`withDRG` drgNew)

generateAKeyPair :: AlgorithmI a => SAlgorithm a -> TVar ChaChaDRG -> STM AKeyPair
generateAKeyPair a g = bimap (APublicKey a) (APrivateKey a) <$> generateKeyPair g

generateSignatureKeyPair :: (AlgorithmI a, SignatureAlgorithm a) => SAlgorithm a -> TVar ChaChaDRG -> STM ASignatureKeyPair
generateSignatureKeyPair a g = bimap (APublicVerifyKey a) (APrivateSignKey a) <$> generateKeyPair g

generateAuthKeyPair :: (AlgorithmI a, AuthAlgorithm a) => SAlgorithm a -> TVar ChaChaDRG -> STM AAuthKeyPair
generateAuthKeyPair a g = bimap (APublicAuthKey a) (APrivateAuthKey a) <$> generateKeyPair g

generateDhKeyPair :: (AlgorithmI a, DhAlgorithm a) => SAlgorithm a -> TVar ChaChaDRG -> STM ADhKeyPair
generateDhKeyPair a g = bimap (APublicDhKey a) (APrivateDhKey a) <$> generateKeyPair g

generateKeyPair :: forall a. AlgorithmI a => TVar ChaChaDRG -> STM (KeyPair a)
generateKeyPair g = stateTVar g (`withDRG` generateKeyPair_)

generateKeyPair_ :: forall a. AlgorithmI a => MonadPseudoRandom ChaChaDRG (KeyPair a)
generateKeyPair_ = case sAlgorithm @a of
  SEd25519 ->
    Ed25519.generateSecretKey >>= \pk ->
      let k = Ed25519.toPublic pk
       in pure (PublicKeyEd25519 k, PrivateKeyEd25519 pk k)
  SEd448 ->
    Ed448.generateSecretKey >>= \pk ->
      let k = Ed448.toPublic pk
       in pure (PublicKeyEd448 k, PrivateKeyEd448 pk k)
  SX25519 ->
    X25519.generateSecretKey >>= \pk ->
      let k = X25519.toPublic pk
       in pure (PublicKeyX25519 k, PrivateKeyX25519 pk k)
  SX448 ->
    X448.generateSecretKey >>= \pk ->
      let k = X448.toPublic pk
       in pure (PublicKeyX448 k, PrivateKeyX448 pk k)

instance ToField APrivateSignKey where toField = toField . Binary . encodePrivKey

instance ToField APublicVerifyKey where toField = toField . Binary . encodePubKey

instance ToField APrivateAuthKey where toField = toField . Binary . encodePrivKey

instance ToField APublicAuthKey where toField = toField . Binary . encodePubKey

instance ToField APrivateDhKey where toField = toField . Binary . encodePrivKey

instance ToField APublicDhKey where toField = toField . Binary . encodePubKey

instance AlgorithmI a => ToField (PrivateKey a) where toField = toField . Binary . encodePrivKey

instance AlgorithmI a => ToField (PublicKey a) where toField = toField . Binary . encodePubKey

instance ToField (DhSecret a) where toField = toField . Binary . dhBytes'

instance FromField APrivateSignKey where fromField = blobFieldDecoder decodePrivKey

instance FromField APublicVerifyKey where fromField = blobFieldDecoder decodePubKey

instance FromField APrivateAuthKey where fromField = blobFieldDecoder decodePrivKey

instance FromField APublicAuthKey where fromField = blobFieldDecoder decodePubKey

instance FromField APrivateDhKey where fromField = blobFieldDecoder decodePrivKey

instance FromField APublicDhKey where fromField = blobFieldDecoder decodePubKey

instance (Typeable a, AlgorithmI a) => FromField (PrivateKey a) where fromField = blobFieldDecoder decodePrivKey

instance (Typeable a, AlgorithmI a) => FromField (PublicKey a) where fromField = blobFieldDecoder decodePubKey

instance (Typeable a, AlgorithmI a) => FromField (DhSecret a) where fromField = blobFieldDecoder strDecode

instance IsString ASignature where
  fromString = parseString $ decode >=> decodeSignature

data Signature (a :: Algorithm) where
  SignatureEd25519 :: Ed25519.Signature -> Signature Ed25519
  SignatureEd448 :: Ed448.Signature -> Signature Ed448

deriving instance Eq (Signature a)

deriving instance Show (Signature a)

data ASignature
  = forall a.
    (AlgorithmI a, SignatureAlgorithm a) =>
    ASignature (SAlgorithm a) (Signature a)

deriving instance Show ASignature

class CryptoSignature s where
  serializeSignature :: s -> ByteString
  serializeSignature = encode . signatureBytes
  signatureBytes :: s -> ByteString
  decodeSignature :: ByteString -> Either String s

instance CryptoSignature (Signature s) => StrEncoding (Signature s) where
  strEncode = serializeSignature
  strDecode = decodeSignature

instance CryptoSignature ASignature where
  signatureBytes (ASignature _ sig) = signatureBytes sig
  decodeSignature s
    | B.length s == Ed25519.signatureSize =
        ASignature SEd25519 . SignatureEd25519 <$> ed Ed25519.signature s
    | B.length s == Ed448.signatureSize =
        ASignature SEd448 . SignatureEd448 <$> ed Ed448.signature s
    | otherwise = Left "bad signature size"
    where
      ed alg = first show . CE.eitherCryptoError . alg

instance CryptoSignature (Maybe ASignature) where
  signatureBytes = maybe "" signatureBytes
  decodeSignature s
    | B.null s = Right Nothing
    | otherwise = Just <$> decodeSignature s

instance AlgorithmI a => CryptoSignature (Signature a) where
  signatureBytes = \case
    SignatureEd25519 s -> BA.convert s
    SignatureEd448 s -> BA.convert s
  decodeSignature s = do
    ASignature _ sig <- decodeSignature s
    checkAlgorithm sig

class SignatureSize s where signatureSize :: s -> Int

instance SignatureSize (Signature a) where
  signatureSize = \case
    SignatureEd25519 _ -> Ed25519.signatureSize
    SignatureEd448 _ -> Ed448.signatureSize

instance SignatureSize ASignature where
  signatureSize (ASignature _ s) = signatureSize s

instance SignatureSize APrivateSignKey where
  signatureSize (APrivateSignKey _ k) = signatureSize k

instance SignatureSize APublicVerifyKey where
  signatureSize (APublicVerifyKey _ k) = signatureSize k

instance SignatureAlgorithm a => SignatureSize (PrivateKey a) where
  signatureSize = \case
    PrivateKeyEd25519 _ _ -> Ed25519.signatureSize
    PrivateKeyEd448 _ _ -> Ed448.signatureSize

instance SignatureAlgorithm a => SignatureSize (PublicKey a) where
  signatureSize = \case
    PublicKeyEd25519 _ -> Ed25519.signatureSize
    PublicKeyEd448 _ -> Ed448.signatureSize

-- | Various cryptographic or related errors.
data CryptoError
  = -- | AES initialization error
    AESCipherError CE.CryptoError
  | -- | IV generation error
    CryptoIVError
  | -- | AES decryption error
    AESDecryptError
  | -- CryptoBox decryption error
    CBDecryptError
  | -- Poly1305 initialization error
    CryptoPoly1305Error CE.CryptoError
  | -- | message is larger that allowed padded length minus 2 (to prepend message length)
    -- (or required un-padded length is larger than the message length)
    CryptoLargeMsgError
  | -- | padded message is shorter than 2 bytes
    CryptoInvalidMsgError
  | -- | failure parsing message header
    CryptoHeaderError String
  | -- | no sending chain key in ratchet state
    CERatchetState
  | -- | no decapsulation key in ratchet state
    CERatchetKEMState
  | -- | header decryption error (could indicate that another key should be tried)
    CERatchetHeader
  | -- | too many skipped messages
    CERatchetTooManySkipped Word32
  | -- | earlier message number (or, possibly, skipped message that failed to decrypt?)
    CERatchetEarlierMessage Word32
  | -- | duplicate message number
    CERatchetDuplicateMessage
  deriving (Eq, Show, Exception)

aesKeySize :: Int
aesKeySize = 256 `div` 8

authTagSize :: Int
authTagSize = 128 `div` 8

x25519_size :: Int
x25519_size = 32

x448_size :: Int
x448_size = 448 `quot` 8

validSignatureSize :: Int -> Bool
validSignatureSize n =
  n == Ed25519.signatureSize || n == Ed448.signatureSize

-- | AES key newtype.
newtype Key = Key {unKey :: ByteString}
  deriving (Eq, Ord, Show)
  deriving newtype (FromField)

instance ToField Key where toField (Key s) = toField $ Binary s

instance ToJSON Key where
  toJSON = strToJSON . unKey
  toEncoding = strToJEncoding . unKey

instance FromJSON Key where
  parseJSON = fmap Key . strParseJSON "Key"

-- | IV bytes newtype.
newtype IV = IV {unIV :: ByteString}
  deriving (Eq, Show)

instance Encoding IV where
  smpEncode = unIV
  smpP = IV <$> A.take (ivSize @AES256)

instance ToJSON IV where
  toJSON = strToJSON . unIV
  toEncoding = strToJEncoding . unIV

instance FromJSON IV where
  parseJSON = fmap IV . strParseJSON "IV"

-- | GCMIV bytes newtype.
newtype GCMIV = GCMIV {unGCMIV :: ByteString}

gcmIV :: ByteString -> Either CryptoError GCMIV
gcmIV s
  | B.length s == gcmIVSize = Right $ GCMIV s
  | otherwise = Left CryptoIVError

newtype AuthTag = AuthTag {unAuthTag :: AES.AuthTag}

instance Encoding AuthTag where
  smpEncode = BA.convert . unAuthTag
  smpP = AuthTag . AES.AuthTag . BA.convert <$> A.take authTagSize

-- | Certificate fingerpint newtype.
--
-- Previously was used for server's public key hash in ad-hoc transport scheme, kept as is for compatibility.
newtype KeyHash = KeyHash {unKeyHash :: ByteString} deriving (Eq, Ord, Show)

instance Encoding KeyHash where
  smpEncode = smpEncode . unKeyHash
  smpP = KeyHash <$> smpP

instance StrEncoding KeyHash where
  strEncode = strEncode . unKeyHash
  strP = KeyHash <$> strP

instance ToJSON KeyHash where
  toEncoding = strToJEncoding
  toJSON = strToJSON

instance FromJSON KeyHash where
  parseJSON = strParseJSON "KeyHash"

instance IsString KeyHash where
  fromString = parseString $ parseAll strP

instance ToField KeyHash where toField = toField . Binary . strEncode

instance FromField KeyHash where fromField = blobFieldDecoder $ parseAll strP

-- | SHA256 digest.
sha256Hash :: ByteString -> ByteString
sha256Hash = BA.convert . (hash :: ByteString -> Digest SHA256)

-- | SHA512 digest.
sha512Hash :: ByteString -> ByteString
sha512Hash = BA.convert . (hash :: ByteString -> Digest SHA512)

-- | AEAD-GCM encryption with associated data.
--
-- Used as part of double ratchet encryption.
-- This function requires 16 bytes IV, it transforms IV in cryptonite_aes_gcm_init here:
-- https://github.com/haskell-crypto/cryptonite/blob/master/cbits/cryptonite_aes.c
encryptAEAD :: Key -> IV -> Int -> ByteString -> ByteString -> ExceptT CryptoError IO (AuthTag, ByteString)
encryptAEAD aesKey ivBytes paddedLen ad msg = do
  aead <- initAEAD @AES256 aesKey ivBytes
  msg' <- liftEither $ pad msg paddedLen
  pure . first AuthTag $ AES.aeadSimpleEncrypt aead ad msg' authTagSize

-- Used to encrypt WebRTC frames.
-- This function requires 12 bytes IV, it does not transform IV.
encryptAESNoPad :: Key -> GCMIV -> ByteString -> ExceptT CryptoError IO (AuthTag, ByteString)
encryptAESNoPad key iv = encryptAEADNoPad key iv ""

encryptAEADNoPad :: Key -> GCMIV -> ByteString -> ByteString -> ExceptT CryptoError IO (AuthTag, ByteString)
encryptAEADNoPad aesKey ivBytes ad msg = do
  aead <- initAEADGCM aesKey ivBytes
  pure . first AuthTag $ AES.aeadSimpleEncrypt aead ad msg authTagSize

-- | AEAD-GCM decryption with associated data.
--
-- Used as part of double ratchet encryption.
-- This function requires 16 bytes IV, it transforms IV in cryptonite_aes_gcm_init here:
-- https://github.com/haskell-crypto/cryptonite/blob/master/cbits/cryptonite_aes.c
-- To make it compatible with WebCrypto we will need to start using initAEADGCM.
decryptAEAD :: Key -> IV -> ByteString -> ByteString -> AuthTag -> ExceptT CryptoError IO ByteString
decryptAEAD aesKey ivBytes ad msg (AuthTag authTag) = do
  aead <- initAEAD @AES256 aesKey ivBytes
  liftEither . unPad =<< maybeError AESDecryptError (AES.aeadSimpleDecrypt aead ad msg authTag)

-- Used to decrypt WebRTC frames.
-- This function requires 12 bytes IV, it does not transform IV.
decryptAESNoPad :: Key -> GCMIV -> ByteString -> AuthTag -> ExceptT CryptoError IO ByteString
decryptAESNoPad key iv = decryptAEADNoPad key iv ""

decryptAEADNoPad :: Key -> GCMIV -> ByteString -> ByteString -> AuthTag -> ExceptT CryptoError IO ByteString
decryptAEADNoPad aesKey iv ad msg (AuthTag tag) = do
  aead <- initAEADGCM aesKey iv
  maybeError AESDecryptError (AES.aeadSimpleDecrypt aead ad msg tag)

maxMsgLen :: Int
maxMsgLen = 2 ^ (16 :: Int) - 3

pad :: ByteString -> Int -> Either CryptoError ByteString
pad msg paddedLen
  | len <= maxMsgLen && padLen >= 0 = Right $ encodeWord16 (fromIntegral len) <> msg <> B.replicate padLen '#'
  | otherwise = Left CryptoLargeMsgError
  where
    len = B.length msg
    padLen = paddedLen - len - 2

unPad :: ByteString -> Either CryptoError ByteString
unPad padded
  | B.length lenWrd == 2 && B.length rest >= len = Right $ B.take len rest
  | otherwise = Left CryptoInvalidMsgError
  where
    (lenWrd, rest) = B.splitAt 2 padded
    len = fromIntegral $ decodeWord16 lenWrd

newtype MaxLenBS (i :: Nat) = MLBS {unMaxLenBS :: ByteString}

pattern MaxLenBS :: ByteString -> MaxLenBS i
pattern MaxLenBS s <- MLBS s

{-# COMPLETE MaxLenBS #-}

instance KnownNat i => Encoding (MaxLenBS i) where
  smpEncode (MLBS s) = smpEncode s
  smpP = first show . maxLenBS <$?> smpP

instance KnownNat i => StrEncoding (MaxLenBS i) where
  strEncode (MLBS s) = strEncode s
  strP = first show . maxLenBS <$?> strP

maxLenBS :: forall i. KnownNat i => ByteString -> Either CryptoError (MaxLenBS i)
maxLenBS s
  | B.length s > maxLength @i = Left CryptoLargeMsgError
  | otherwise = Right $ MLBS s

unsafeMaxLenBS :: forall i. KnownNat i => ByteString -> MaxLenBS i
unsafeMaxLenBS = MLBS

padMaxLenBS :: forall i. KnownNat i => MaxLenBS i -> MaxLenBS (i + 2)
padMaxLenBS (MLBS msg) = MLBS $ encodeWord16 (fromIntegral len) <> msg <> B.replicate padLen '#'
  where
    len = B.length msg
    padLen = maxLength @i - len

appendMaxLenBS :: (KnownNat i, KnownNat j) => MaxLenBS i -> MaxLenBS j -> MaxLenBS (i + j)
appendMaxLenBS (MLBS s1) (MLBS s2) = MLBS $ s1 <> s2

maxLength :: forall i. KnownNat i => Int
maxLength = fromIntegral (natVal $ Proxy @i)

-- this function requires 16 bytes IV, it transforms IV in cryptonite_aes_gcm_init here:
-- https://github.com/haskell-crypto/cryptonite/blob/master/cbits/cryptonite_aes.c
-- This is used for double ratchet encryption, so to make it compatible with WebCrypto we will need to deprecate it and start using initAEADGCM
initAEAD :: forall c. AES.BlockCipher c => Key -> IV -> ExceptT CryptoError IO (AES.AEAD c)
initAEAD (Key aesKey) (IV ivBytes) = do
  iv <- makeIV @c ivBytes
  cryptoFailable $ do
    cipher <- AES.cipherInit aesKey
    AES.aeadInit AES.AEAD_GCM cipher iv

-- this function requires 12 bytes IV, it does not transforms IV.
initAEADGCM :: Key -> GCMIV -> ExceptT CryptoError IO (AES.AEAD AES256)
initAEADGCM (Key aesKey) (GCMIV ivBytes) = cryptoFailable $ do
  cipher <- AES.cipherInit aesKey
  AES.aeadInit AES.AEAD_GCM cipher ivBytes

-- | Random AES256 key.
randomAesKey :: TVar ChaChaDRG -> STM Key
randomAesKey = fmap Key . randomBytes aesKeySize

randomGCMIV :: TVar ChaChaDRG -> STM GCMIV
randomGCMIV = fmap GCMIV . randomBytes gcmIVSize

ivSize :: forall c. AES.BlockCipher c => Int
ivSize = AES.blockSize (undefined :: c)

gcmIVSize :: Int
gcmIVSize = 12

makeIV :: AES.BlockCipher c => ByteString -> ExceptT CryptoError IO (AES.IV c)
makeIV bs = maybeError CryptoIVError $ AES.makeIV bs

maybeError :: CryptoError -> Maybe a -> ExceptT CryptoError IO a
maybeError e = maybe (throwE e) return

cryptoFailable :: CE.CryptoFailable a -> ExceptT CryptoError IO a
cryptoFailable = liftEither . first AESCipherError . CE.eitherCryptoError

-- | Message signing.
--
-- Used by SMP clients to sign SMP commands and by SMP agents to sign messages.
sign' :: SignatureAlgorithm a => PrivateKey a -> ByteString -> Signature a
sign' (PrivateKeyEd25519 pk k) msg = SignatureEd25519 $ Ed25519.sign pk k msg
sign' (PrivateKeyEd448 pk k) msg = SignatureEd448 $ Ed448.sign pk k msg

sign :: APrivateSignKey -> ByteString -> ASignature
sign (APrivateSignKey a k) = ASignature a . sign' k

signCertificate :: APrivateSignKey -> Certificate -> SignedCertificate
signCertificate = signX509

signX509 :: (ASN1Object o, Eq o, Show o) => APrivateSignKey -> o -> SignedExact o
signX509 key = fst . objectToSignedExact f
  where
    f bytes =
      ( signatureBytes $ sign key bytes,
        signatureAlgorithmX509 key,
        ()
      )
{-# INLINE signX509 #-}

verifyX509 :: (ASN1Object o, Eq o, Show o) => APublicVerifyKey -> SignedExact o -> Either String o
verifyX509 key exact = do
  signature <- case signedAlg of
    SignatureALG_IntrinsicHash PubKeyALG_Ed25519 -> ASignature SEd25519 <$> decodeSignature signedSignature
    SignatureALG_IntrinsicHash PubKeyALG_Ed448 -> ASignature SEd448 <$> decodeSignature signedSignature
    _ -> Left "unknown x509 signature algorithm"
  if verify key signature $ getSignedData exact then Right signedObject else Left "bad signature"
  where
    Signed {signedObject, signedAlg, signedSignature} = getSigned exact
{-# INLINE verifyX509 #-}

certificateFingerprint :: SignedCertificate -> KeyHash
certificateFingerprint = signedFingerprint

signedFingerprint :: (ASN1Object o, Eq o, Show o) => SignedExact o -> KeyHash
signedFingerprint o = KeyHash fp
  where
    Fingerprint fp = getFingerprint o HashSHA256

class SignatureAlgorithmX509 a where
  signatureAlgorithmX509 :: a -> SignatureALG

instance SignatureAlgorithm a => SignatureAlgorithmX509 (SAlgorithm a) where
  signatureAlgorithmX509 = \case
    SEd25519 -> SignatureALG_IntrinsicHash PubKeyALG_Ed25519
    SEd448 -> SignatureALG_IntrinsicHash PubKeyALG_Ed448

instance SignatureAlgorithmX509 APrivateSignKey where
  signatureAlgorithmX509 (APrivateSignKey a _) = signatureAlgorithmX509 a

instance SignatureAlgorithmX509 APublicVerifyKey where
  signatureAlgorithmX509 (APublicVerifyKey a _) = signatureAlgorithmX509 a

-- | An instance for 'ASignatureKeyPair' / ('PublicKeyType' pk, pk), without touching its type family.
instance SignatureAlgorithmX509 pk => SignatureAlgorithmX509 (a, pk) where
  signatureAlgorithmX509 = signatureAlgorithmX509 . snd

-- | A wrapper to marshall signed ASN1 objects, like certificates.
newtype SignedObject a = SignedObject {getSignedExact :: SignedExact a}

instance (Typeable a, Eq a, Show a, ASN1Object a) => FromField (SignedObject a) where
#if defined(dbPostgres)
  fromField f dat = SignedObject <$> blobFieldDecoder decodeSignedObject f dat
#else
  fromField = fmap SignedObject . blobFieldDecoder decodeSignedObject
#endif

instance (Eq a, Show a, ASN1Object a) => ToField (SignedObject a) where
  toField (SignedObject s) = toField . Binary $ encodeSignedObject s

instance (Eq a, Show a, ASN1Object a) => Encoding (SignedObject a) where
  smpEncode (SignedObject exact) = smpEncode . Large $ encodeSignedObject exact
  smpP = fmap SignedObject . decodeSignedObject . unLarge <$?> smpP

encodeCertChain :: CertificateChain -> L.NonEmpty Large
encodeCertChain cc = L.fromList $ map Large blobs
  where
    CertificateChainRaw blobs = encodeCertificateChain cc

certChainP :: A.Parser CertificateChain
certChainP = do
  rawChain <- CertificateChainRaw . map unLarge . L.toList <$> smpP
  either (fail . show) pure $ decodeCertificateChain rawChain

-- | Signature verification.
--
-- Used by SMP servers to authorize SMP commands and by SMP agents to verify messages.
verify' :: SignatureAlgorithm a => PublicKey a -> Signature a -> ByteString -> Bool
verify' (PublicKeyEd25519 k) (SignatureEd25519 sig) msg = Ed25519.verify k msg sig
verify' (PublicKeyEd448 k) (SignatureEd448 sig) msg = Ed448.verify k msg sig

verify :: APublicVerifyKey -> ASignature -> ByteString -> Bool
verify (APublicVerifyKey a k) (ASignature a' sig) msg = case testEquality a a' of
  Just Refl -> verify' k sig msg
  _ -> False

dh' :: DhAlgorithm a => PublicKey a -> PrivateKey a -> DhSecret a
dh' (PublicKeyX25519 k) (PrivateKeyX25519 pk _) = DhSecretX25519 $ X25519.dh k pk
dh' (PublicKeyX448 k) (PrivateKeyX448 pk _) = DhSecretX448 $ X448.dh k pk

-- | NaCl @crypto_box@ encrypt with padding with a shared DH secret and 192-bit nonce.
cbEncrypt :: DhSecret X25519 -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
cbEncrypt (DhSecretX25519 secret) = sbEncrypt_ secret

-- | NaCl @crypto_box@ encrypt with a shared DH secret and 192-bit nonce (without padding).
cbEncryptNoPad :: DhSecret X25519 -> CbNonce -> ByteString -> ByteString
cbEncryptNoPad (DhSecretX25519 secret) (CbNonce nonce) = cryptoBox secret nonce

-- | NaCl @secret_box@ encrypt with a symmetric 256-bit key and 192-bit nonce.
sbEncrypt :: SbKey -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
sbEncrypt (SbKey key) = sbEncrypt_ key

sbEncrypt_ :: ByteArrayAccess key => key -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
sbEncrypt_ secret (CbNonce nonce) msg paddedLen = cryptoBox secret nonce <$> pad msg paddedLen

-- | NaCl @crypto_box@ encrypt with a shared DH secret and 192-bit nonce.
cbEncryptMaxLenBS :: KnownNat i => DhSecret X25519 -> CbNonce -> MaxLenBS i -> ByteString
cbEncryptMaxLenBS (DhSecretX25519 secret) (CbNonce nonce) = cryptoBox secret nonce . unMaxLenBS . padMaxLenBS

cryptoBox :: ByteArrayAccess key => key -> ByteString -> ByteString -> ByteString
cryptoBox secret nonce s = BA.convert tag <> c
  where
    (rs, c) = xSalsa20 secret nonce s
    tag = Poly1305.auth rs c

-- | NaCl @crypto_box@ decrypt with a shared DH secret and 192-bit nonce.
cbDecrypt :: DhSecret X25519 -> CbNonce -> ByteString -> Either CryptoError ByteString
cbDecrypt (DhSecretX25519 secret) = sbDecrypt_ secret

-- | NaCl @crypto_box@ decrypt with a shared DH secret and 192-bit nonce (without unpadding).
cbDecryptNoPad :: DhSecret X25519 -> CbNonce -> ByteString -> Either CryptoError ByteString
cbDecryptNoPad (DhSecretX25519 secret) = sbDecryptNoPad_ secret

-- | NaCl @secret_box@ decrypt with a symmetric 256-bit key and 192-bit nonce.
sbDecrypt :: SbKey -> CbNonce -> ByteString -> Either CryptoError ByteString
sbDecrypt (SbKey key) = sbDecrypt_ key

-- | NaCl @crypto_box@ decrypt with a shared DH secret and 192-bit nonce.
sbDecrypt_ :: ByteArrayAccess key => key -> CbNonce -> ByteString -> Either CryptoError ByteString
sbDecrypt_ secret nonce = unPad <=< sbDecryptNoPad_ secret nonce

-- | NaCl @crypto_box@ decrypt with a shared DH secret and 192-bit nonce (without unpadding).
sbDecryptNoPad_ :: ByteArrayAccess key => key -> CbNonce -> ByteString -> Either CryptoError ByteString
sbDecryptNoPad_ secret (CbNonce nonce) packet
  | B.length packet < 16 = Left CBDecryptError
  | BA.constEq tag' tag = Right msg
  | otherwise = Left CBDecryptError
  where
    (tag', c) = B.splitAt 16 packet
    (rs, msg) = xSalsa20 secret nonce c
    tag = Poly1305.auth rs c

-- type for authentication scheme using NaCl @crypto_box@ over the sha512 digest of the message.
newtype CbAuthenticator = CbAuthenticator ByteString deriving (Eq, Show)

cbAuthenticatorSize :: Int
cbAuthenticatorSize = hashDigestSize SHA512 + authTagSize -- 64 + 16 = 80 bytes

-- create crypto_box authenticator for a message.
cbAuthenticate :: PublicKeyX25519 -> PrivateKeyX25519 -> CbNonce -> ByteString -> CbAuthenticator
cbAuthenticate k pk nonce msg = CbAuthenticator $ cbEncryptNoPad (dh' k pk) nonce (sha512Hash msg)

-- verify crypto_box authenticator for a message.
cbVerify :: PublicKeyX25519 -> PrivateKeyX25519 -> CbNonce -> CbAuthenticator -> ByteString -> Bool
cbVerify k pk nonce (CbAuthenticator s) authorized = cbDecryptNoPad (dh' k pk) nonce s == Right (sha512Hash authorized)

newtype CbNonce = CryptoBoxNonce {unCbNonce :: ByteString}
  deriving (Eq, Show)
  deriving newtype (FromField)

instance ToField CbNonce where toField (CryptoBoxNonce s) = toField $ Binary s

pattern CbNonce :: ByteString -> CbNonce
pattern CbNonce s <- CryptoBoxNonce s

{-# COMPLETE CbNonce #-}

instance StrEncoding CbNonce where
  strEncode (CbNonce s) = strEncode s
  strP = cbNonce <$> strP

instance ToJSON CbNonce where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance FromJSON CbNonce where
  parseJSON = strParseJSON "CbNonce"

cbNonce :: ByteString -> CbNonce
cbNonce s
  | len == 24 = CryptoBoxNonce s
  | len > 24 = CryptoBoxNonce . fst $ B.splitAt 24 s
  | otherwise = CryptoBoxNonce $ s <> B.replicate (24 - len) (toEnum 0)
  where
    len = B.length s

randomCbNonce :: TVar ChaChaDRG -> STM CbNonce
randomCbNonce = fmap CryptoBoxNonce . randomBytes 24

randomBytes :: Int -> TVar ChaChaDRG -> STM ByteString
randomBytes n gVar = stateTVar gVar $ randomBytesGenerate n

reverseNonce :: CbNonce -> CbNonce
reverseNonce (CryptoBoxNonce s) = CryptoBoxNonce (B.reverse s)

instance Encoding CbNonce where
  smpEncode = unCbNonce
  smpP = CryptoBoxNonce <$> A.take 24

newtype SbKey = SecretBoxKey {unSbKey :: ByteString}
  deriving (Eq, Show)
  deriving newtype (FromField)

instance ToField SbKey where toField (SecretBoxKey s) = toField $ Binary s

pattern SbKey :: ByteString -> SbKey
pattern SbKey s <- SecretBoxKey s

{-# COMPLETE SbKey #-}

instance StrEncoding SbKey where
  strEncode (SbKey s) = strEncode s
  strP = sbKey <$?> strP

instance ToJSON SbKey where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance FromJSON SbKey where
  parseJSON = strParseJSON "SbKey"

sbKey :: ByteString -> Either String SbKey
sbKey s
  | B.length s == 32 = Right $ SecretBoxKey s
  | otherwise = Left "SbKey: invalid length"

unsafeSbKey :: ByteString -> SbKey
unsafeSbKey s = either error id $ sbKey s

randomSbKey :: TVar ChaChaDRG -> STM SbKey
randomSbKey gVar = SecretBoxKey <$> randomBytes 32 gVar

newtype SbChainKey = SecretBoxChainKey {unSbChainKey :: ByteString}
  deriving (Eq, Show)

sbcInit :: ByteArrayAccess secret => ByteString -> secret -> (SbChainKey, SbChainKey)
sbcInit salt secret = (SecretBoxChainKey ck1, SecretBoxChainKey ck2)
  where
    prk = H.extract salt secret :: H.PRK SHA512
    out = H.expand prk ("SimpleXSbChainInit" :: ByteString) 64
    (ck1, ck2) = B.splitAt 32 out

type SbKeyNonce = (SbKey, CbNonce)

sbcHkdf :: SbChainKey -> (SbKeyNonce, SbChainKey)
sbcHkdf (SecretBoxChainKey ck) = ((SecretBoxKey sk, CryptoBoxNonce nonce), SecretBoxChainKey ck')
  where
    prk = H.extract B.empty ck :: H.PRK SHA512
    out = H.expand prk ("SimpleXSbChain" :: ByteString) 88 -- = 32 (new chain key) + 32 (secret_box key) + 24 (nonce)
    (ck', rest) = B.splitAt 32 out
    (sk, nonce) = B.splitAt 32 rest

xSalsa20 :: ByteArrayAccess key => key -> ByteString -> ByteString -> (ByteString, ByteString)
xSalsa20 secret nonce msg = (rs, msg')
  where
    zero = B.replicate 16 $ toEnum 0
    (iv0, iv1) = B.splitAt 8 nonce
    state0 = XSalsa.initialize 20 secret (zero `B.append` iv0)
    state1 = XSalsa.derive state0 iv1
    (rs, state2) = XSalsa.generate state1 32
    (msg', _) = XSalsa.combine state2 msg

publicToX509 :: PublicKey a -> PubKey
publicToX509 = \case
  PublicKeyEd25519 k -> PubKeyEd25519 k
  PublicKeyEd448 k -> PubKeyEd448 k
  PublicKeyX25519 k -> PubKeyX25519 k
  PublicKeyX448 k -> PubKeyX448 k

privateToX509 :: PrivateKey a -> PrivKey
privateToX509 = \case
  PrivateKeyEd25519 k _ -> PrivKeyEd25519 k
  PrivateKeyEd448 k _ -> PrivKeyEd448 k
  PrivateKeyX25519 k _ -> PrivKeyX25519 k
  PrivateKeyX448 k _ -> PrivKeyX448 k

encodeASNObj :: ASN1Object a => a -> ByteString
encodeASNObj k = toStrict . encodeASN1 DER $ toASN1 k []

-- Decoding of binary X509 'CryptoPublicKey'.
decodePubKey :: CryptoPublicKey k => ByteString -> Either String k
decodePubKey = decodeKey >=> x509ToPublic >=> pubKey

-- Decoding of binary PKCS8 'PrivateKey'.
decodePrivKey :: CryptoPrivateKey k => ByteString -> Either String k
decodePrivKey = decodeKey >=> x509ToPrivate >=> privKey

x509ToPublic :: (PubKey, [ASN1]) -> Either String APublicKey
x509ToPublic = \case
  (PubKeyEd25519 k, []) -> Right . APublicKey SEd25519 $ PublicKeyEd25519 k
  (PubKeyEd448 k, []) -> Right . APublicKey SEd448 $ PublicKeyEd448 k
  (PubKeyX25519 k, []) -> Right . APublicKey SX25519 $ PublicKeyX25519 k
  (PubKeyX448 k, []) -> Right . APublicKey SX448 $ PublicKeyX448 k
  r -> keyError r

x509ToPrivate :: (PrivKey, [ASN1]) -> Either String APrivateKey
x509ToPrivate = \case
  (PrivKeyEd25519 k, []) -> Right . APrivateKey SEd25519 . PrivateKeyEd25519 k $ Ed25519.toPublic k
  (PrivKeyEd448 k, []) -> Right . APrivateKey SEd448 . PrivateKeyEd448 k $ Ed448.toPublic k
  (PrivKeyX25519 k, []) -> Right . APrivateKey SX25519 . PrivateKeyX25519 k $ X25519.toPublic k
  (PrivKeyX448 k, []) -> Right . APrivateKey SX448 . PrivateKeyX448 k $ X448.toPublic k
  r -> keyError r

decodeKey :: ASN1Object a => ByteString -> Either String (a, [ASN1])
decodeKey = fromASN1 <=< first show . decodeASN1 DER . fromStrict

keyError :: (a, [ASN1]) -> Either String b
keyError = \case
  (_, []) -> Left "unknown key algorithm"
  _ -> Left "more than one key"
