{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GADTs #-}

module Simplex.RemoteControl.Types where

import Control.Monad
import Crypto.Error (eitherCryptoError)
import qualified Crypto.PubKey.Ed25519 as Ed25519
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteArray (convert)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Foldable (toList)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8Lenient, encodeUtf8)
import Data.Time.Clock.System (SystemTime)
import Data.Word (Word16)
import Network.HTTP.Types (parseSimpleQuery)
import Network.HTTP.Types.URI (renderSimpleQuery, urlDecode, urlEncode)
import qualified Network.Socket as N
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding (Encoding (..))
import Simplex.Messaging.Encoding.String (StrEncoding (..))
import Simplex.Messaging.Version (VersionRange, mkVersionRange)
import UnliftIO

ipProbeVersionRange :: VersionRange
ipProbeVersionRange = mkVersionRange 1 1

data IpProbe = IpProbe
  { versionRange :: VersionRange,
    randomNonce :: ByteString
  } deriving (Show)

instance Encoding IpProbe where
  smpEncode IpProbe {versionRange, randomNonce} = smpEncode (versionRange, 'I', randomNonce)

  smpP = IpProbe <$> (smpP <* "I") *> smpP

announceVersionRange :: VersionRange
announceVersionRange = mkVersionRange  1 1

data Announce = Announce
  { versionRange :: VersionRange,
    sessionStart :: SystemTime,
    announceCounter :: Word16,
    serviceAddress :: (N.HostAddress, Word16),
    caFingerprint :: C.KeyHash,
    sessionDH :: C.PublicKeyX25519,
    announceKey :: C.PublicKeyEd25519
  } deriving (Show)

instance Encoding Announce where
  smpEncode Announce {versionRange, sessionStart, announceCounter, serviceAddress, caFingerprint, sessionDH, announceKey} =
    smpEncode (versionRange, 'A', sessionStart, announceCounter, serviceAddress)
      <> smpEncode (caFingerprint, sessionDH, announceKey)

  smpP = Announce <$> (smpP <* "A") <*> smpP <*> smpP <*> smpP <*> smpP <*> smpP <*> smpP

data SignedAnnounce = SignedAnnounce Announce (C.Signature 'C.Ed25519)

instance Encoding SignedAnnounce where
  smpEncode (SignedAnnounce ann (C.SignatureEd25519 sig)) = smpEncode (ann, convert sig :: ByteString)

  smpP = do
    sa <- SignedAnnounce <$> smpP <*> signatureP
    unless (verifySignedAnnounce sa) $ fail "bad announce signature"
    pure sa
    where
      signatureP = do
        bs <- smpP :: A.Parser ByteString
        case eitherCryptoError (Ed25519.signature bs) of
          Left ce -> fail $ show ce
          Right ok -> pure $ C.SignatureEd25519 ok

signAnnounce :: C.PrivateKey C.Ed25519 -> Announce -> SignedAnnounce
signAnnounce announceSecret ann = SignedAnnounce ann sig
  where
    sig =
      case C.sign (C.APrivateSignKey C.SEd25519 announceSecret) (smpEncode ann) of
        C.ASignature C.SEd25519 s -> s
        _ -> error "signing with ed25519"

verifySignedAnnounce :: SignedAnnounce -> Bool
verifySignedAnnounce (SignedAnnounce ann@Announce {announceKey} sig) = C.verify aKey aSig (smpEncode ann)
  where
    aKey = C.APublicVerifyKey C.SEd25519 announceKey
    aSig = C.ASignature C.SEd25519 sig

data OOB = OOB
  { -- authority part
    caFingerprint :: C.KeyHash,
    authToken :: Text,
    host :: Text,
    port :: Word16,
    -- query part
    version :: VersionRange, -- v=
    appName :: Text, -- app=
    sigPubKey :: C.PublicKeyEd25519, -- key=
    deviceName :: Maybe Text -- device=
  }
  deriving (Eq, Show)

instance StrEncoding OOB where
  strEncode OOB {caFingerprint, authToken, host, port, version, appName, sigPubKey, deviceName} =
    schema <> "://" <> authority <> "#/?" <> renderSimpleQuery False query
    where
      schema = "xrcp"
      authority =
        mconcat
          [ strEncode caFingerprint,
            ":",
            encodeUtf8 authToken,
            "@",
            encodeUtf8 host,
            ":",
            strEncode port
          ]
      query =
        [ ("v", strEncode version),
          ("app", encodeUtf8 appName),
          ("key", strEncode $ C.encodePubKey sigPubKey)
        ]
          ++ [("device", urlEncode True $ encodeUtf8 name) | name <- toList deviceName]

  strP = do
    _ <- A.string "xrcp://"
    caFingerprint <- strP
    _ <- A.char ':'
    authToken <- decodeUtf8Lenient <$> A.takeWhile (/= '@')
    _ <- A.char '@'
    host <- decodeUtf8Lenient <$> A.takeWhile (/= ':')
    _ <- A.char ':'
    port <- strP

    _ <- A.string "#/?"
    q <- parseSimpleQuery <$> A.takeByteString
    version <- maybe (fail "missing version") (either fail pure . strDecode) (lookup "v" q)
    appName <- maybe (fail "missing appName") (pure . decodeUtf8Lenient) (lookup "app" q)
    sigPubKeyB64 <- maybe (fail "missing key") pure (lookup "key" q)
    sigPubKey <- either fail pure $ strDecode sigPubKeyB64 >>= C.decodePubKey
    let deviceName = fmap (decodeUtf8Lenient . urlDecode True) (lookup "device" q)
    pure OOB {caFingerprint, authToken, host, port, version, appName, sigPubKey, deviceName}

data SignedOOB = SignedOOB OOB (C.Signature 'C.Ed25519)
  deriving (Eq, Show)

instance StrEncoding SignedOOB where
  strEncode (SignedOOB oob sig) = strEncode oob <> "&sig=" <> strEncode (C.signatureBytes sig)

  strDecode s = do
    unless (B.length sig == sigLen) $ Left "bad size"
    unless ("&sig=" `B.isPrefixOf` sig) $ Left "bad signature prefix"
    signedOOB <- SignedOOB <$> strDecode oob <*> (strDecode (B.drop 5 sig) >>= C.decodeSignature)
    unless (verifySignedOOB signedOOB) $ Left "bad signature"
    pure signedOOB
    where
      l = B.length s
      (oob, sig) = B.splitAt (l - sigLen) s
      sigLen = 93 -- &sig= + ed25519 sig size in base64 (88)

  -- XXX: strP is used in chat command parser, but default strP assumes bas64url-encoded bytestring, where OOB is an URL-like
  strP = A.takeWhile (/= ' ') >>= either fail pure . strDecode

signOOB :: C.PrivateKey C.Ed25519 -> OOB -> SignedOOB
signOOB key oob = SignedOOB oob sig
  where
    sig =
      case C.sign (C.APrivateSignKey C.SEd25519 key) (strEncode oob) of
        C.ASignature C.SEd25519 s -> s
        _ -> error "signing with ed25519"

verifySignedOOB :: SignedOOB -> Bool
verifySignedOOB (SignedOOB oob@OOB {sigPubKey} sig) = C.verify aKey aSig (strEncode oob)
  where
    aKey = C.APublicVerifyKey C.SEd25519 sigPubKey
    aSig = C.ASignature C.SEd25519 sig

decodeOOBLink :: Text -> Either String OOB
decodeOOBLink = fmap (\(SignedOOB oob _verified) -> oob) . strDecode . encodeUtf8

-- XXX: Move to utils?
type Tasks = TVar [Async ()]

asyncRegistered :: MonadUnliftIO m => Tasks -> m () -> m ()
asyncRegistered tasks action = async action >>= registerAsync tasks

registerAsync :: MonadIO m => Tasks -> Async () -> m ()
registerAsync tasks = atomically . modifyTVar tasks . (:)

cancelTasks :: (MonadIO m) => Tasks -> m ()
cancelTasks tasks = readTVarIO tasks >>= mapM_ cancel
