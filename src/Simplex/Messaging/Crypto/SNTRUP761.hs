{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module Simplex.Messaging.Crypto.SNTRUP761 where

import Crypto.Hash (Digest, SHA256, hash)
import Data.ByteArray (ScrubbedBytes)
import qualified Data.ByteArray as BA
import Data.ByteString (ByteString)
import Simplex.Messaging.Crypto
import Simplex.Messaging.Crypto.SNTRUP761.Bindings

-- Hybrid shared secret for crypto_box is defined as SHA256(DHSecret || KEMSharedKey),
-- similar to https://datatracker.ietf.org/doc/draft-josefsson-ntruprime-hybrid/

newtype KEMHybridSecret = KEMHybridSecret ScrubbedBytes

-- | NaCl @crypto_box@ decrypt with a shared hybrid DH + KEM secret and 192-bit nonce.
kcbDecrypt :: KEMHybridSecret -> CbNonce -> ByteString -> Either CryptoError ByteString
kcbDecrypt (KEMHybridSecret secret) = sbDecrypt_ secret

-- | NaCl @crypto_box@ encrypt with a shared hybrid DH + KEM secret and 192-bit nonce.
kcbEncrypt :: KEMHybridSecret -> CbNonce -> ByteString -> Int -> Either CryptoError ByteString
kcbEncrypt (KEMHybridSecret secret) = sbEncrypt_ secret

kemHybridSecret :: DhSecret 'X25519 -> KEMSharedKey -> KEMHybridSecret
kemHybridSecret (DhSecretX25519 k1) (KEMSharedKey k2) =
  KEMHybridSecret $ BA.convert (hash $ BA.convert k1 <> k2 :: Digest SHA256)
