{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
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
    SignAlg (..),
    DhAlg (..),
    DhAlgorithm,
    PrivateKey (..),
    PublicKey (..),
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
    CryptoPublicKey (..),
    CryptoPrivateKey (..),
    KeyPair,
    ASignatureKeyPair,
    DhSecret (..),
    DhSecretX25519,
    ADhSecret (..),
    KeyHash (..),
    generateKeyPair,
    generateKeyPair',
    generateSignatureKeyPair,
    generateDhKeyPair,
    privateToX509,
    publicKey,

    -- * key encoding/decoding
    encodePubKey,
    encodePrivKey,
    pubKeyBytes,

    -- * sign/verify
    Signature (..),
    ASignature (..),
    CryptoSignature (..),
    SignatureSize (..),
    SignatureAlgorithm,
    AlgorithmI (..),
    sign,
    verify,
    verify',
    validSignatureSize,

    -- * DH derivation
    dh',
    dhBytes',

    -- * AES256 AEAD-GCM scheme
    Key (..),
    IV (..),
    AuthTag (..),
    encryptAES,
    decryptAES,
    encryptAEAD,
    decryptAEAD,
    authTagSize,
    randomAesKey,
    randomIV,
    ivSize,

    -- * NaCl crypto_box
    CbNonce (unCbNonce),
    cbEncrypt,
    cbEncryptMaxLenBS,
    cbDecrypt,
    cbNonce,
    randomCbNonce,
    pseudoRandomCbNonce,

    -- * pseudo-random bytes
    pseudoRandomBytes,

    -- * SHA256 hash
    sha256Hash,

    -- * Message padding / un-padding
    pad,
    unPad,

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
import Control.Monad.Except
import Control.Monad.Trans.Except
import Crypto.Cipher.AES (AES256)
import qualified Crypto.Cipher.Types as AES
import qualified Crypto.Cipher.XSalsa as XSalsa
import qualified Crypto.Error as CE
import Crypto.Hash (Digest, SHA256 (..), hash)
import qualified Crypto.MAC.Poly1305 as Poly1305
import qualified Crypto.PubKey.Curve25519 as X25519
import qualified Crypto.PubKey.Curve448 as X448
import qualified Crypto.PubKey.Ed25519 as Ed25519
import qualified Crypto.PubKey.Ed448 as Ed448
import Crypto.Random (ChaChaDRG, getRandomBytes, randomBytesGenerate)
import Data.ASN1.BinaryEncoding
import Data.ASN1.Encoding
import Data.ASN1.Types
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.Bifunctor (bimap, first)
import qualified Data.ByteArray as BA
import Data.ByteString.Base64 (decode, encode)
import qualified Data.ByteString.Base64.URL as U
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Internal (c2w, w2c)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Constraint (Dict (..))
import Data.Kind (Constraint, Type)
import Data.String
import Data.Type.Equality
import Data.Typeable (Proxy (Proxy), Typeable)
import Data.X509
import Database.SQLite.Simple.FromField (FromField (..))
import Database.SQLite.Simple.ToField (ToField (..))
import GHC.TypeLits (ErrorMessage (..), KnownNat, Nat, TypeError, natVal, type (+))
import Network.Transport.Internal (decodeWord16, encodeWord16)
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (blobFieldDecoder, parseAll, parseString)
import Simplex.Messaging.Util ((<$?>))

-- | Cryptographic algorithms.
data Algorithm = Ed25519 | Ed448 | X25519 | X448

-- | Singleton types for 'Algorithm'.
data SAlgorithm :: Algorithm -> Type where
  SEd25519 :: SAlgorithm Ed25519
  SEd448 :: SAlgorithm Ed448
  SX25519 :: SAlgorithm X25519
  SX448 :: SAlgorithm X448

deriving instance Eq (SAlgorithm a)

deriving instance Show (SAlgorithm a)

data Alg = forall a. AlgorithmI a => Alg (SAlgorithm a)

data SignAlg
  = forall a.
    (AlgorithmI a, SignatureAlgorithm a) =>
    SignAlg (SAlgorithm a)

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

instance Eq APublicKey where
  APublicKey a k == APublicKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APublicKey

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

instance StrEncoding (PrivateKey X25519) where
  strEncode = strEncode . encodePrivKey
  {-# INLINE strEncode #-}
  strDecode = decodePrivKey
  {-# INLINE strDecode #-}

data APrivateKey
  = forall a.
    AlgorithmI a =>
    APrivateKey (SAlgorithm a) (PrivateKey a)

instance Eq APrivateKey where
  APrivateKey a k == APrivateKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APrivateKey

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

instance Eq APrivateSignKey where
  APrivateSignKey a k == APrivateSignKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

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

instance Eq APublicVerifyKey where
  APublicVerifyKey a k == APublicVerifyKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APublicVerifyKey

data APrivateDhKey
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    APrivateDhKey (SAlgorithm a) (PrivateKey a)

instance Eq APrivateDhKey where
  APrivateDhKey a k == APrivateDhKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

deriving instance Show APrivateDhKey

data APublicDhKey
  = forall a.
    (AlgorithmI a, DhAlgorithm a) =>
    APublicDhKey (SAlgorithm a) (PublicKey a)

instance Eq APublicDhKey where
  APublicDhKey a k == APublicDhKey a' k' = case testEquality a a' of
    Just Refl -> k == k'
    Nothing -> False

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

instance Encoding DhSecretX25519 where
  smpEncode = smpEncode . strEncode
  {-# INLINE smpEncode #-}
  smpDecode = strDecode
  {-# INLINE smpDecode #-}


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

type AKeyPair = KeyPairType APrivateKey

type ASignatureKeyPair = KeyPairType APrivateSignKey

type ADhKeyPair = KeyPairType APrivateDhKey

generateKeyPair :: AlgorithmI a => SAlgorithm a -> IO AKeyPair
generateKeyPair a = bimap (APublicKey a) (APrivateKey a) <$> generateKeyPair'

generateSignatureKeyPair :: (AlgorithmI a, SignatureAlgorithm a) => SAlgorithm a -> IO ASignatureKeyPair
generateSignatureKeyPair a = bimap (APublicVerifyKey a) (APrivateSignKey a) <$> generateKeyPair'

generateDhKeyPair :: (AlgorithmI a, DhAlgorithm a) => SAlgorithm a -> IO ADhKeyPair
generateDhKeyPair a = bimap (APublicDhKey a) (APrivateDhKey a) <$> generateKeyPair'

generateKeyPair' :: forall a. AlgorithmI a => IO (KeyPair a)
generateKeyPair' = case sAlgorithm @a of
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

instance ToField APrivateSignKey where toField = toField . encodePrivKey

instance ToField APublicVerifyKey where toField = toField . encodePubKey

instance ToField APrivateDhKey where toField = toField . encodePrivKey

instance ToField APublicDhKey where toField = toField . encodePubKey

instance AlgorithmI a => ToField (PrivateKey a) where toField = toField . encodePrivKey

instance AlgorithmI a => ToField (PublicKey a) where toField = toField . encodePubKey

instance ToField (DhSecret a) where toField = toField . dhBytes'

instance FromField APrivateSignKey where fromField = blobFieldDecoder decodePrivKey

instance FromField APublicVerifyKey where fromField = blobFieldDecoder decodePubKey

instance FromField APrivateDhKey where fromField = blobFieldDecoder decodePrivKey

instance FromField APublicDhKey where fromField = blobFieldDecoder decodePubKey

instance (Typeable a, AlgorithmI a) => FromField (PrivateKey a) where fromField = blobFieldDecoder decodePrivKey

instance (Typeable a, AlgorithmI a) => FromField (PublicKey a) where fromField = blobFieldDecoder decodePubKey

instance (Typeable a, AlgorithmI a) => FromField (DhSecret a) where fromField = blobFieldDecoder strDecode

instance IsString (Maybe ASignature) where
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

instance Eq ASignature where
  ASignature a s == ASignature a' s' = case testEquality a a' of
    Just Refl -> s == s'
    _ -> False

deriving instance Show ASignature

class CryptoSignature s where
  serializeSignature :: s -> ByteString
  serializeSignature = encode . signatureBytes
  signatureBytes :: s -> ByteString
  decodeSignature :: ByteString -> Either String s

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
  | -- | message is larger that allowed padded length minus 2 (to prepend message length)
    -- (or required un-padded length is larger than the message length)
    CryptoLargeMsgError
  | -- | padded message is shorter than 2 bytes
    CryptoInvalidMsgError
  | -- | failure parsing message header
    CryptoHeaderError String
  | -- | no sending chain key in ratchet state
    CERatchetState
  | -- | header decryption error (could indicate that another key should be tried)
    CERatchetHeader
  | -- | too many skipped messages
    CERatchetTooManySkipped
  | -- | earlier message number (or, possibly, skipped message that failed to decrypt?)
    CERatchetEarlierMessage
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

instance ToField Key where toField = toField . unKey

instance FromField Key where fromField f = Key <$> fromField f

instance ToJSON Key where
  toJSON = strToJSON . unKey
  toEncoding = strToJEncoding . unKey

instance FromJSON Key where
  parseJSON = fmap Key . strParseJSON "Key"

-- | IV bytes newtype.
newtype IV = IV {unIV :: ByteString}

instance Encoding IV where
  smpEncode = unIV
  smpP = IV <$> A.take (ivSize @AES256)

newtype AuthTag = AuthTag {unAuthTag :: AES.AuthTag}

instance Encoding AuthTag where
  smpEncode = B.pack . map w2c . BA.unpack . AES.unAuthTag . unAuthTag
  smpP = AuthTag . AES.AuthTag . BA.pack . map c2w . B.unpack <$> A.take authTagSize

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

instance IsString KeyHash where
  fromString = parseString $ parseAll strP

instance ToField KeyHash where toField = toField . strEncode

instance FromField KeyHash where fromField = blobFieldDecoder $ parseAll strP

-- | SHA256 digest.
sha256Hash :: ByteString -> ByteString
sha256Hash = BA.convert . (hash :: ByteString -> Digest SHA256)

-- | AEAD-GCM encryption with empty associated data.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks encryption.
encryptAES :: Key -> IV -> Int -> ByteString -> ExceptT CryptoError IO (AuthTag, ByteString)
encryptAES key iv paddedLen = encryptAEAD key iv paddedLen ""

-- | AEAD-GCM encryption.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks encryption.
encryptAEAD :: Key -> IV -> Int -> ByteString -> ByteString -> ExceptT CryptoError IO (AuthTag, ByteString)
encryptAEAD aesKey ivBytes paddedLen ad msg = do
  aead <- initAEAD @AES256 aesKey ivBytes
  msg' <- liftEither $ pad msg paddedLen
  pure . first AuthTag $ AES.aeadSimpleEncrypt aead ad msg' authTagSize

-- | AEAD-GCM decryption with empty associated data.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks decryption.
decryptAES :: Key -> IV -> ByteString -> AuthTag -> ExceptT CryptoError IO ByteString
decryptAES key iv = decryptAEAD key iv ""

-- | AEAD-GCM decryption.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks decryption.
decryptAEAD :: Key -> IV -> ByteString -> ByteString -> AuthTag -> ExceptT CryptoError IO ByteString
decryptAEAD aesKey ivBytes ad msg (AuthTag authTag) = do
  aead <- initAEAD @AES256 aesKey ivBytes
  liftEither . unPad =<< maybeError AESDecryptError (AES.aeadSimpleDecrypt aead ad msg authTag)

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

initAEAD :: forall c. AES.BlockCipher c => Key -> IV -> ExceptT CryptoError IO (AES.AEAD c)
initAEAD (Key aesKey) (IV ivBytes) = do
  iv <- makeIV @c ivBytes
  cryptoFailable $ do
    cipher <- AES.cipherInit aesKey
    AES.aeadInit AES.AEAD_GCM cipher iv

-- | Random AES256 key.
randomAesKey :: IO Key
randomAesKey = Key <$> getRandomBytes aesKeySize

-- | Random IV bytes for AES256 encryption.
randomIV :: IO IV
randomIV = IV <$> getRandomBytes (ivSize @AES256)

ivSize :: forall c. AES.BlockCipher c => Int
ivSize = AES.blockSize (undefined :: c)

makeIV :: AES.BlockCipher c => ByteString -> ExceptT CryptoError IO (AES.IV c)
makeIV bs = maybeError CryptoIVError $ AES.makeIV bs

maybeError :: CryptoError -> Maybe a -> ExceptT CryptoError IO a
maybeError e = maybe (throwE e) return

cryptoFailable :: CE.CryptoFailable a -> ExceptT CryptoError IO a
cryptoFailable = liftEither . first AESCipherError . CE.eitherCryptoError

-- | Message signing.
--
-- Used by SMP clients to sign SMP commands and by SMP agents to sign messages.
sign' :: SignatureAlgorithm a => PrivateKey a -> ByteString -> ExceptT CryptoError IO (Signature a)
sign' (PrivateKeyEd25519 pk k) msg = pure . SignatureEd25519 $ Ed25519.sign pk k msg
sign' (PrivateKeyEd448 pk k) msg = pure . SignatureEd448 $ Ed448.sign pk k msg

sign :: APrivateSignKey -> ByteString -> ExceptT CryptoError IO ASignature
sign (APrivateSignKey a k) = fmap (ASignature a) . sign' k

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

-- | NaCl @crypto_box@ encrypt with a shared DH secret and 192-bit nonce.
cbEncrypt :: DhSecret X25519 -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
cbEncrypt secret (CbNonce nonce) msg paddedLen = cryptoBox secret nonce <$> pad msg paddedLen

-- | NaCl @crypto_box@ encrypt with a shared DH secret and 192-bit nonce.
cbEncryptMaxLenBS :: KnownNat i => DhSecret X25519 -> CbNonce -> MaxLenBS i -> ByteString
cbEncryptMaxLenBS secret (CbNonce nonce) = cryptoBox secret nonce . unMaxLenBS . padMaxLenBS

cryptoBox :: DhSecret 'X25519 -> ByteString -> ByteString -> ByteString
cryptoBox secret nonce s = BA.convert tag <> c
  where
    (rs, c) = xSalsa20 secret nonce s
    tag = Poly1305.auth rs c

-- | NaCl @crypto_box@ decrypt with a shared DH secret and 192-bit nonce.
cbDecrypt :: DhSecret X25519 -> CbNonce -> ByteString -> Either CryptoError ByteString
cbDecrypt secret (CbNonce nonce) packet
  | B.length packet < 16 = Left CBDecryptError
  | BA.constEq tag' tag = unPad msg
  | otherwise = Left CBDecryptError
  where
    (tag', c) = B.splitAt 16 packet
    (rs, msg) = xSalsa20 secret nonce c
    tag = Poly1305.auth rs c

newtype CbNonce = CbNonce {unCbNonce :: ByteString}
  deriving (Eq, Show)

instance StrEncoding CbNonce where
  strEncode (CbNonce s) = strEncode s
  strP = cbNonce <$> strP

instance ToJSON CbNonce where
  toJSON = strToJSON
  toEncoding = strToJEncoding

cbNonce :: ByteString -> CbNonce
cbNonce s
  | len == 24 = CbNonce s
  | len > 24 = CbNonce . fst $ B.splitAt 24 s
  | otherwise = CbNonce $ s <> B.replicate (24 - len) (toEnum 0)
  where
    len = B.length s

randomCbNonce :: IO CbNonce
randomCbNonce = CbNonce <$> getRandomBytes 24

pseudoRandomCbNonce :: TVar ChaChaDRG -> STM CbNonce
pseudoRandomCbNonce gVar = CbNonce <$> pseudoRandomBytes 24 gVar

pseudoRandomBytes :: Int -> TVar ChaChaDRG -> STM ByteString
pseudoRandomBytes n gVar = do
  g <- readTVar gVar
  let (bytes, g') = randomBytesGenerate n g
  writeTVar gVar g'
  return bytes

instance Encoding CbNonce where
  smpEncode = unCbNonce
  smpP = CbNonce <$> A.take 24

xSalsa20 :: DhSecret X25519 -> ByteString -> ByteString -> (ByteString, ByteString)
xSalsa20 (DhSecretX25519 shared) nonce msg = (rs, msg')
  where
    zero = B.replicate 16 $ toEnum 0
    (iv0, iv1) = B.splitAt 8 nonce
    state0 = XSalsa.initialize 20 shared (zero `B.append` iv0)
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
