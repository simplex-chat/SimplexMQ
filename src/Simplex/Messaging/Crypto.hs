{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
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
    APrivateKey (..),
    APublicKey (..),
    APrivateSignKey (..),
    APublicVerifyKey (..),
    APrivateDhKey (..),
    APublicDhKey (..),
    CryptoPublicKey (..),
    CryptoPrivateKey (..),
    KeyPair,
    DhSecret (..),
    DhSecretX25519,
    ADhSecret (..),
    CryptoDhSecret (..),
    KeyHash (..),
    generateKeyPair,
    generateKeyPair',
    generateSignatureKeyPair,
    generateDhKeyPair,
    privateToX509,

    -- * key encoding/decoding
    serializePubKey,
    serializePubKey',
    serializePubKeyUri,
    serializePubKeyUri',
    strPubKeyP,
    strPubKeyUriP,
    encodeLenKey',
    encodeLenKey,
    binaryLenKeyP,
    encodePubKey,
    encodePubKey',
    binaryPubKeyP,
    encodePrivKey,

    -- * E2E hybrid encryption scheme
    E2EEncryptionVersion,
    currentE2EVersion,

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
    dhSecret,
    dhSecret',

    -- * AES256 AEAD-GCM scheme
    Key (..),
    IV (..),
    encryptAES,
    decryptAES,
    encryptAEAD,
    decryptAEAD,
    authTagSize,
    authTagToBS,
    bsToAuthTag,
    randomAesKey,
    randomIV,
    ivP,
    ivSize,

    -- * NaCl crypto_box
    CbNonce (unCbNonce),
    cbEncrypt,
    cbDecrypt,
    cbNonce,
    randomCbNonce,
    cbNonceP,

    -- * SHA256 hash
    sha256Hash,

    -- * Message padding / un-padding
    pad,
    unPad,

    -- * Cryptography error type
    CryptoError (..),
  )
where

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
import Crypto.Random (getRandomBytes)
import Data.ASN1.BinaryEncoding
import Data.ASN1.Encoding
import Data.ASN1.Types
import Data.Attoparsec.ByteString.Char8 (Parser)
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
import Data.Typeable (Typeable)
import Data.Word (Word16)
import Data.X509
import Database.SQLite.Simple.FromField (FromField (..))
import Database.SQLite.Simple.ToField (ToField (..))
import GHC.TypeLits (ErrorMessage (..), TypeError)
import Network.Transport.Internal (decodeWord16, encodeWord16)
import Simplex.Messaging.Parsers (base64P, base64UriP, blobFieldParser, parseAll, parseString, word16P)
import Simplex.Messaging.Util ((<$?>))

type E2EEncryptionVersion = Word16

currentE2EVersion :: E2EEncryptionVersion
currentE2EVersion = 1

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

-- | GADT for private keys.
data PrivateKey (a :: Algorithm) where
  PrivateKeyEd25519 :: Ed25519.SecretKey -> Ed25519.PublicKey -> PrivateKey Ed25519
  PrivateKeyEd448 :: Ed448.SecretKey -> Ed448.PublicKey -> PrivateKey Ed448
  PrivateKeyX25519 :: X25519.SecretKey -> PrivateKey X25519
  PrivateKeyX448 :: X448.SecretKey -> PrivateKey X448

deriving instance Eq (PrivateKey a)

deriving instance Show (PrivateKey a)

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

class AlgorithmPrefix k where
  algorithmPrefix :: k -> ByteString

instance AlgorithmPrefix (SAlgorithm a) where
  algorithmPrefix = \case
    SEd25519 -> "ed25519"
    SEd448 -> "ed448"
    SX25519 -> "x25519"
    SX448 -> "x448"

instance AlgorithmI a => AlgorithmPrefix (PublicKey a) where
  algorithmPrefix _ = algorithmPrefix $ sAlgorithm @a

instance AlgorithmI a => AlgorithmPrefix (PrivateKey a) where
  algorithmPrefix _ = algorithmPrefix $ sAlgorithm @a

instance AlgorithmPrefix APublicKey where
  algorithmPrefix (APublicKey a _) = algorithmPrefix a

instance AlgorithmPrefix APrivateKey where
  algorithmPrefix (APrivateKey a _) = algorithmPrefix a

prefixAlgorithm :: ByteString -> Either String Alg
prefixAlgorithm = \case
  "ed25519" -> Right $ Alg SEd25519
  "ed448" -> Right $ Alg SEd448
  "x25519" -> Right $ Alg SX25519
  "x448" -> Right $ Alg SX448
  _ -> Left "unknown algorithm"

algP :: Parser Alg
algP = prefixAlgorithm <$?> A.takeTill (== ':')

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

class CryptoDhSecret s where
  serializeDhSecret :: s -> ByteString
  dhSecretBytes :: s -> ByteString
  strDhSecretP :: Parser s
  dhSecretP :: Parser s

instance AlgorithmI a => IsString (DhSecret a) where
  fromString = parseString $ dhSecret >=> dhSecret'

instance CryptoDhSecret ADhSecret where
  serializeDhSecret (ADhSecret _ s) = serializeDhSecret s
  dhSecretBytes (ADhSecret _ s) = dhSecretBytes s
  strDhSecretP = dhSecret <$?> base64P
  dhSecretP = dhSecret <$?> A.takeByteString

dhSecret :: ByteString -> Either String ADhSecret
dhSecret = cryptoPassed . secret
  where
    secret bs
      | B.length bs == x25519_size = ADhSecret SX25519 . DhSecretX25519 <$> X25519.dhSecret bs
      | B.length bs == x448_size = ADhSecret SX448 . DhSecretX448 <$> X448.dhSecret bs
      | otherwise = CE.CryptoFailed CE.CryptoError_SharedSecretSizeInvalid
    cryptoPassed = \case
      CE.CryptoPassed s -> Right s
      CE.CryptoFailed e -> Left $ show e

instance forall a. AlgorithmI a => CryptoDhSecret (DhSecret a) where
  serializeDhSecret = encode . dhSecretBytes
  dhSecretBytes = \case
    DhSecretX25519 s -> BA.convert s
    DhSecretX448 s -> BA.convert s
  strDhSecretP = dhSecret' <$?> strDhSecretP
  dhSecretP = dhSecret' <$?> dhSecretP

dhSecret' :: forall a. AlgorithmI a => ADhSecret -> Either String (DhSecret a)
dhSecret' (ADhSecret a s) = case testEquality a $ sAlgorithm @a of
  Just Refl -> Right s
  _ -> Left "bad DH secret algorithm"

-- | Class for all key types
class CryptoPublicKey k where
  toPubKey :: (forall a. AlgorithmI a => PublicKey a -> b) -> k -> b
  pubKey :: APublicKey -> Either String k

-- | X509 encoding of any public key.
instance CryptoPublicKey APublicKey where
  toPubKey f (APublicKey _ k) = f k
  pubKey = Right

-- | X509 encoding of signature public key.
instance CryptoPublicKey APublicVerifyKey where
  toPubKey f (APublicVerifyKey _ k) = f k
  pubKey (APublicKey a k) = case signatureAlgorithm a of
    Just Dict -> Right $ APublicVerifyKey a k
    _ -> Left "key does not support signature algorithms"

-- | X509 encoding of DH public key.
instance CryptoPublicKey APublicDhKey where
  toPubKey f (APublicDhKey _ k) = f k
  pubKey (APublicKey a k) = case dhAlgorithm a of
    Just Dict -> Right $ APublicDhKey a k
    _ -> Left "key does not support DH algorithms"

-- | X509 encoding of 'PublicKey'.
instance AlgorithmI a => CryptoPublicKey (PublicKey a) where
  toPubKey = id
  pubKey (APublicKey a k) = case testEquality a $ sAlgorithm @a of
    Just Refl -> Right k
    _ -> Left "bad key algorithm"

-- | base64 X509 key encoding with algorithm prefix
serializePubKey :: CryptoPublicKey k => k -> ByteString
serializePubKey = toPubKey serializePubKey'
{-# INLINE serializePubKey #-}

-- | base64url X509 key encoding with algorithm prefix
serializePubKeyUri :: CryptoPublicKey k => k -> ByteString
serializePubKeyUri = toPubKey serializePubKeyUri'
{-# INLINE serializePubKeyUri #-}

serializePubKey' :: AlgorithmI a => PublicKey a -> ByteString
serializePubKey' k = algorithmPrefix k <> ":" <> encode (encodePubKey' k)

serializePubKeyUri' :: AlgorithmI a => PublicKey a -> ByteString
serializePubKeyUri' k = algorithmPrefix k <> ":" <> U.encode (encodePubKey' k)

-- | base64 X509 (with algorithm prefix) key parser
strPubKeyP :: CryptoPublicKey k => Parser k
strPubKeyP = pubKey <$?> aStrPubKeyP
{-# INLINE strPubKeyP #-}

-- | base64url X509 (with algorithm prefix) key parser
strPubKeyUriP :: CryptoPublicKey k => Parser k
strPubKeyUriP = pubKey <$?> aStrPubKeyUriP
{-# INLINE strPubKeyUriP #-}

aStrPubKeyP :: Parser APublicKey
aStrPubKeyP = strPublicKeyP_ base64P

aStrPubKeyUriP :: Parser APublicKey
aStrPubKeyUriP = strPublicKeyP_ base64UriP

strPublicKeyP_ :: Parser ByteString -> Parser APublicKey
strPublicKeyP_ b64P = do
  Alg a <- algP <* A.char ':'
  k@(APublicKey a' _) <- decodePubKey <$?> b64P
  case testEquality a a' of
    Just Refl -> pure k
    _ -> fail $ "public key algorithm " <> show a <> " does not match prefix"

encodeLenKey :: CryptoPublicKey k => k -> ByteString
encodeLenKey = toPubKey encodeLenKey'
{-# INLINE encodeLenKey #-}

-- | binary X509 key encoding with 2-bytes length prefix
encodeLenKey' :: PublicKey a -> ByteString
encodeLenKey' k =
  let s = encodePubKey' k
      len = fromIntegral $ B.length s
   in encodeWord16 len <> s
{-# INLINE encodeLenKey' #-}

-- | binary X509 key parser with 2-bytes length prefix
binaryLenKeyP :: CryptoPublicKey k => Parser k
binaryLenKeyP = do
  len <- fromIntegral <$> word16P
  parseAll binaryPubKeyP <$?> A.take len

encodePubKey :: CryptoPublicKey pk => pk -> ByteString
encodePubKey = toPubKey encodePubKey'
{-# INLINE encodePubKey #-}

encodePubKey' :: PublicKey a -> ByteString
encodePubKey' = encodeASNObj . publicToX509

binaryPubKeyP :: CryptoPublicKey pk => Parser pk
binaryPubKeyP = pubKey <$?> aBinaryPubKeyP
{-# INLINE binaryPubKeyP #-}

aBinaryPubKeyP :: Parser APublicKey
aBinaryPubKeyP = decodePubKey <$?> A.takeByteString

class CryptoPrivateKey pk where
  toPrivKey :: (forall a. AlgorithmI a => PrivateKey a -> b) -> pk -> b
  privKey :: APrivateKey -> Either String pk

instance CryptoPrivateKey APrivateKey where
  toPrivKey f (APrivateKey _ k) = f k
  privKey = Right

instance CryptoPrivateKey APrivateSignKey where
  toPrivKey f (APrivateSignKey _ k) = f k
  privKey (APrivateKey a k) = case signatureAlgorithm a of
    Just Dict -> Right $ APrivateSignKey a k
    _ -> Left "key does not support signature algorithms"

instance CryptoPrivateKey APrivateDhKey where
  toPrivKey f (APrivateDhKey _ k) = f k
  privKey (APrivateKey a k) = case dhAlgorithm a of
    Just Dict -> Right $ APrivateDhKey a k
    _ -> Left "key does not support DH algorithm"

instance AlgorithmI a => CryptoPrivateKey (PrivateKey a) where
  toPrivKey = id
  privKey (APrivateKey a k) = case testEquality a $ sAlgorithm @a of
    Just Refl -> Right k
    _ -> Left "bad key algorithm"

encodePrivKey :: CryptoPrivateKey pk => pk -> ByteString
encodePrivKey = toPrivKey encodePrivKey'

encodePrivKey' :: PrivateKey a -> ByteString
encodePrivKey' = encodeASNObj . privateToX509

binaryPrivKeyP :: CryptoPrivateKey pk => Parser pk
binaryPrivKeyP = privKey <$?> aBinaryPrivKeyP

aBinaryPrivKeyP :: Parser APrivateKey
aBinaryPrivKeyP = decodePrivKey <$?> A.takeByteString

instance AlgorithmI a => IsString (PrivateKey a) where
  fromString = parseString $ decode >=> decodePrivKey >=> privKey

instance AlgorithmI a => IsString (PublicKey a) where
  fromString = parseString $ decode >=> decodePubKey >=> pubKey

-- | Tuple of RSA 'PublicKey' and 'PrivateKey'.
type KeyPair a = (PublicKey a, PrivateKey a)

type AKeyPair = (APublicKey, APrivateKey)

type ASignatureKeyPair = (APublicVerifyKey, APrivateSignKey)

type ADhKeyPair = (APublicDhKey, APrivateDhKey)

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
       in pure (PublicKeyX25519 k, PrivateKeyX25519 pk)
  SX448 ->
    X448.generateSecretKey >>= \pk ->
      let k = X448.toPublic pk
       in pure (PublicKeyX448 k, PrivateKeyX448 pk)

instance ToField APrivateSignKey where toField = toField . encodePrivKey

instance ToField APublicVerifyKey where toField = toField . encodePubKey

instance ToField APrivateDhKey where toField = toField . encodePrivKey

instance ToField APublicDhKey where toField = toField . encodePubKey

instance ToField (PrivateKey a) where toField = toField . encodePrivKey'

instance ToField (PublicKey a) where toField = toField . encodePubKey'

instance AlgorithmI a => ToField (DhSecret a) where toField = toField . dhSecretBytes

instance FromField APrivateSignKey where fromField = blobFieldParser binaryPrivKeyP

instance FromField APublicVerifyKey where fromField = blobFieldParser binaryPubKeyP

instance FromField APrivateDhKey where fromField = blobFieldParser binaryPrivKeyP

instance FromField APublicDhKey where fromField = blobFieldParser binaryPubKeyP

instance (Typeable a, AlgorithmI a) => FromField (PrivateKey a) where fromField = blobFieldParser binaryPrivKeyP

instance (Typeable a, AlgorithmI a) => FromField (PublicKey a) where fromField = blobFieldParser binaryPubKeyP

instance (Typeable a, AlgorithmI a) => FromField (DhSecret a) where fromField = blobFieldParser dhSecretP

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
    ASignature a sig <- decodeSignature s
    case testEquality a $ sAlgorithm @a of
      Just Refl -> Right sig
      _ -> Left "bad signature algorithm"

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
  | -- | failure parsing message header
    CryptoHeaderError String
  | -- | no sending chain key in ratchet state
    CERatchetState
  | -- | header decryption error (could indicate that another key should be tried)
    CERatchetHeader
  | -- | too many skipped messages
    CERatchetTooManySkipped
  | -- | duplicate message number (or, possibly, skipped message that failed to decrypt?)
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
  deriving (Eq, Ord)

-- | IV bytes newtype.
newtype IV = IV {unIV :: ByteString}

-- | Certificate fingerpint newtype.
--
-- Previously was used for server's public key hash in ad-hoc transport scheme, kept as is for compatibility.
newtype KeyHash = KeyHash {unKeyHash :: ByteString} deriving (Eq, Ord, Show)

instance IsString KeyHash where
  fromString = parseString . parseAll $ KeyHash <$> base64P

instance ToField KeyHash where toField = toField . encode . unKeyHash

instance FromField KeyHash where fromField = blobFieldParser $ KeyHash <$> base64P

-- | SHA256 digest.
sha256Hash :: ByteString -> ByteString
sha256Hash = BA.convert . (hash :: ByteString -> Digest SHA256)

-- | IV bytes parser.
ivP :: Parser IV
ivP = IV <$> A.take (ivSize @AES256)

-- | AEAD-GCM encryption with empty associated data.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks encryption.
encryptAES :: Key -> IV -> Int -> ByteString -> ExceptT CryptoError IO (AES.AuthTag, ByteString)
encryptAES key iv paddedLen = encryptAEAD key iv paddedLen ""

-- | AEAD-GCM encryption.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks encryption.
encryptAEAD :: Key -> IV -> Int -> ByteString -> ByteString -> ExceptT CryptoError IO (AES.AuthTag, ByteString)
encryptAEAD aesKey ivBytes paddedLen ad msg = do
  aead <- initAEAD @AES256 aesKey ivBytes
  msg' <- liftEither $ pad msg paddedLen
  return $ AES.aeadSimpleEncrypt aead ad msg' authTagSize

-- | AEAD-GCM decryption with empty associated data.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks decryption.
decryptAES :: Key -> IV -> ByteString -> AES.AuthTag -> ExceptT CryptoError IO ByteString
decryptAES key iv = decryptAEAD key iv ""

-- | AEAD-GCM decryption.
--
-- Used as part of hybrid E2E encryption scheme and for SMP transport blocks decryption.
decryptAEAD :: Key -> IV -> ByteString -> ByteString -> AES.AuthTag -> ExceptT CryptoError IO ByteString
decryptAEAD aesKey ivBytes ad msg authTag = do
  aead <- initAEAD @AES256 aesKey ivBytes
  liftEither . unPad =<< maybeError AESDecryptError (AES.aeadSimpleDecrypt aead ad msg authTag)

pad :: ByteString -> Int -> Either CryptoError ByteString
pad msg paddedLen
  | padLen >= 0 = Right $ encodeWord16 (fromIntegral len) <> msg <> B.replicate padLen '#'
  | otherwise = Left CryptoLargeMsgError
  where
    len = B.length msg
    padLen = paddedLen - len - 2

unPad :: ByteString -> Either CryptoError ByteString
unPad padded
  | B.length rest >= len = Right $ B.take len rest
  | otherwise = Left CryptoLargeMsgError
  where
    (lenWrd, rest) = B.splitAt 2 padded
    len = fromIntegral $ decodeWord16 lenWrd

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

-- | Convert AEAD 'AuthTag' to ByteString.
authTagToBS :: AES.AuthTag -> ByteString
authTagToBS = B.pack . map w2c . BA.unpack . AES.unAuthTag

-- | Convert ByteString to AEAD 'AuthTag'.
bsToAuthTag :: ByteString -> AES.AuthTag
bsToAuthTag = AES.AuthTag . BA.pack . map c2w . B.unpack

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
dh' (PublicKeyX25519 k) (PrivateKeyX25519 pk) = DhSecretX25519 $ X25519.dh k pk
dh' (PublicKeyX448 k) (PrivateKeyX448 pk) = DhSecretX448 $ X448.dh k pk

-- | NaCl @crypto_box@ encrypt with a shared DH secret and 192-bit nonce.
cbEncrypt :: DhSecret X25519 -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
cbEncrypt secret (CbNonce nonce) msg paddedLen = cryptoBox <$> pad msg paddedLen
  where
    cryptoBox s = BA.convert tag `B.append` c
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

cbNonce :: ByteString -> CbNonce
cbNonce s
  | len == 24 = CbNonce s
  | len > 24 = CbNonce . fst $ B.splitAt 24 s
  | otherwise = CbNonce $ s <> B.replicate (24 - len) (toEnum 0)
  where
    len = B.length s

randomCbNonce :: IO CbNonce
randomCbNonce = CbNonce <$> getRandomBytes 24

cbNonceP :: Parser CbNonce
cbNonceP = CbNonce <$> A.take 24

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
  PrivateKeyX25519 k -> PrivKeyX25519 k
  PrivateKeyX448 k -> PrivKeyX448 k

encodeASNObj :: ASN1Object a => a -> ByteString
encodeASNObj k = toStrict . encodeASN1 DER $ toASN1 k []

-- Decoding of binary X509 'PublicKey'.
decodePubKey :: ByteString -> Either String APublicKey
decodePubKey =
  decodeKey >=> \case
    (PubKeyEd25519 k, []) -> Right . APublicKey SEd25519 $ PublicKeyEd25519 k
    (PubKeyEd448 k, []) -> Right . APublicKey SEd448 $ PublicKeyEd448 k
    (PubKeyX25519 k, []) -> Right . APublicKey SX25519 $ PublicKeyX25519 k
    (PubKeyX448 k, []) -> Right . APublicKey SX448 $ PublicKeyX448 k
    r -> keyError r

-- Decoding of binary PKCS8 'PrivateKey'.
decodePrivKey :: ByteString -> Either String APrivateKey
decodePrivKey =
  decodeKey >=> \case
    (PrivKeyEd25519 k, []) -> Right . APrivateKey SEd25519 . PrivateKeyEd25519 k $ Ed25519.toPublic k
    (PrivKeyEd448 k, []) -> Right . APrivateKey SEd448 . PrivateKeyEd448 k $ Ed448.toPublic k
    (PrivKeyX25519 k, []) -> Right . APrivateKey SX25519 $ PrivateKeyX25519 k
    (PrivKeyX448 k, []) -> Right . APrivateKey SX448 $ PrivateKeyX448 k
    r -> keyError r

decodeKey :: ASN1Object a => ByteString -> Either String (a, [ASN1])
decodeKey = fromASN1 <=< first show . decodeASN1 DER . fromStrict

keyError :: (a, [ASN1]) -> Either String b
keyError = \case
  (_, []) -> Left "unknown key algorithm"
  _ -> Left "more than one key"
