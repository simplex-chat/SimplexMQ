{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module Simplex.RemoteControl.Invite where

import Control.Concurrent.STM (TVar)
import Control.Monad (unless)
import Crypto.Random (ChaChaDRG)
import Data.Aeson.TH (deriveJSON)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8Lenient, encodeUtf8)
import Data.Time.Clock.System (SystemTime, getSystemTime)
import Data.Word (Word16, Word32)
import Network.HTTP.Types (parseSimpleQuery)
import Network.HTTP.Types.URI (SimpleQuery, renderQuery, urlDecode)
import qualified Network.TLS as TLS
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.SNTRUP761.Bindings (KEMPublicKey, KEMSecretKey, sntrup761Keypair)
import Simplex.Messaging.Encoding (Encoding (..))
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (defaultJSON)
import Simplex.Messaging.Transport.Client (TransportHost)
import Simplex.Messaging.Transport.Credentials (genCredentials, tlsCredentials)
import Simplex.Messaging.Version (VersionRange, mkVersionRange)

data Invite = Invite
  { -- | CA TLS certificate fingerprint of the controller.
    --
    -- This is part of long term identity of the controller established during the first session, and repeated in the subsequent session announcements.
    ca :: C.KeyHash,
    host :: TransportHost,
    port :: Word16,
    -- | Supported version range for remote control protocol
    v :: VersionRange,
    -- | Application name
    app :: Maybe Text,
    -- | App version
    appv :: Maybe VersionRange,
    -- | Device name
    device :: Maybe Text,
    -- | Session start time in seconds since epoch
    ts :: SystemTime,
    -- | Session Ed25519 public key used to verify the announcement and commands
    --
    -- This mitigates the compromise of the long term signature key, as the controller will have to sign each command with this key first.
    skey :: C.PublicKeyEd25519,
    -- | Long-term Ed25519 public key used to verify the announcement and commands.
    --
    -- Is apart of the long term controller identity.
    idkey :: C.PublicKeyEd25519,
    -- | SNTRUP761 encapsulation key
    kem :: KEMPublicKey,
    -- | Session X25519 DH key
    dh :: C.PublicKeyX25519
  }

instance StrEncoding Invite where
  strEncode Invite {ca, host, port, v, app, appv, device, ts, skey, idkey, kem, dh} =
    mconcat
      [ "xrcp://",
        strEncode ca,
        "@",
        strEncode host,
        ":",
        strEncode port,
        "#/?",
        renderQuery False $ filter (isJust . snd) query
      ]
    where
      query =
        [ ("ca", Just $ strEncode ca),
          ("host", Just $ strEncode host),
          ("port", Just $ strEncode port),
          ("v", Just $ strEncode v),
          ("app", fmap encodeUtf8 app),
          ("appv", fmap strEncode appv),
          ("device", fmap encodeUtf8 device),
          ("ts", Just $ strEncode ts),
          ("skey", Just $ strEncode skey),
          ("idkey", Just $ strEncode idkey),
          ("kem", Just $ strEncode kem),
          ("dh", Just $ strEncode dh)
        ]

  strP = do
    _ <- A.string "xrcp://"
    ca <- strP
    _ <- A.char '@'
    host <- A.takeWhile (/= ':') >>= either fail pure . strDecode . urlDecode True
    _ <- A.char ':'
    port <- strP
    _ <- A.string "#/?"

    q <- parseSimpleQuery <$> A.takeWhile (/= ' ')
    v <- requiredP q "v" strDecode
    app <- optionalP q "app" $ pure . decodeUtf8Lenient . urlDecode True
    appv <- optionalP q "appv" strDecode
    device <- optionalP q "device" $ pure . decodeUtf8Lenient . urlDecode True
    ts <- requiredP q "ts" $ strDecode . urlDecode True
    skey <- requiredP q "skey" strDecode
    idkey <- requiredP q "idkey" strDecode
    kem <- requiredP q "kem" strDecode
    dh <- requiredP q "dh" strDecode
    pure Invite {ca, host, port, v, app, appv, device, ts, skey, idkey, kem, dh}

data SignedInvite = SignedInvite
  { invite :: Invite,
    ssig :: C.Signature 'C.Ed25519,
    idsig :: C.Signature 'C.Ed25519
  }

instance StrEncoding SignedInvite where
  strEncode SignedInvite {invite, ssig, idsig} =
    mconcat
      [ strEncode invite,
        "&ssig=",
        strEncode ssig,
        "&idsig=",
        strEncode idsig
      ]

  strP = do
    (xrcpURL, invite) <- A.match strP
    sigs <- case B.breakSubstring "&ssig=" xrcpURL of
      (_invite, sigs) | B.null sigs -> fail "missing signatures"
      (_invite, sigs) -> pure $ parseSimpleQuery $ B.drop 1 sigs
    ssig <- requiredP sigs "ssig" strDecode
    idsig <- requiredP sigs "idsig" strDecode
    pure SignedInvite {invite, ssig, idsig}

signInviteURL :: C.PrivateKey C.Ed25519 -> C.PrivateKey C.Ed25519 -> Invite -> SignedInvite
signInviteURL sKey idKey invite = SignedInvite {invite, ssig, idsig}
  where
    inviteUrl = strEncode invite
    ssig =
      case C.sign (C.APrivateSignKey C.SEd25519 sKey) inviteUrl of
        C.ASignature C.SEd25519 s -> s
        _ -> error "signing with ed25519"
    inviteUrlSigned = mconcat [inviteUrl, "&ssig=", strEncode ssig]
    idsig =
      case C.sign (C.APrivateSignKey C.SEd25519 idKey) inviteUrlSigned of
        C.ASignature C.SEd25519 s -> s
        _ -> error "signing with ed25519"

verifySignedInviteURL :: SignedInvite -> Either SignatureError ()
verifySignedInviteURL SignedInvite {invite, ssig, idsig} = do
  unless (C.verify aSKey aSSig inviteURL) $ Left BadSessionSignature
  unless (C.verify aIdKey aIdSig inviteURLS) $ Left BadIdentitySignature
  where
    Invite {skey, idkey} = invite
    inviteURL = strEncode invite
    inviteURLS = mconcat [inviteURL, "&ssig=", strEncode ssig]
    aSKey = C.APublicVerifyKey C.SEd25519 skey
    aSSig = C.ASignature C.SEd25519 ssig
    aIdKey = C.APublicVerifyKey C.SEd25519 idkey
    aIdSig = C.ASignature C.SEd25519 idsig

data EncryptedAnnounce = EncryptedAnnounce
  { dhPubKey :: C.PublicKeyX25519,
    encrypted :: ByteString
  }

instance Encoding EncryptedAnnounce where
  smpEncode EncryptedAnnounce {dhPubKey, encrypted} =
    mconcat
      [ smpEncode dhPubKey,
        smpEncode @Word32 $ fromIntegral (B.length encrypted),
        encrypted
      ]
  smpP = do
    dhPubKey <- smpP
    len <- smpP
    encrypted <- error "take encrypted"
    pure EncryptedAnnounce {dhPubKey, encrypted}

-- * Utils

-- | A bunch of keys that should be generated by a controller to start a new remote session and produce invites
data SessionKeys = SessionKeys
  { ts :: SystemTime,
    ca :: C.KeyHash,
    tls :: TLS.Credentials,
    sig :: C.PrivateKeyEd25519,
    dh :: C.PrivateKeyX25519,
    kem :: (KEMPublicKey, KEMSecretKey)
  }

newSessionKeys :: TVar ChaChaDRG -> (C.APrivateSignKey, C.SignedCertificate) -> IO SessionKeys
newSessionKeys rng (caKey, caCert) = do
  ts <- getSystemTime
  (_, C.APrivateDhKey C.SX25519 dh) <- C.generateDhKeyPair C.SX25519
  (_, C.APrivateSignKey C.SEd25519 sig) <- C.generateSignatureKeyPair C.SEd25519

  let parent = (C.signatureKeyPair caKey, caCert)
  sessionCreds <- genCredentials (Just parent) (0, 24) "Session"
  let (ca, tls) = tlsCredentials $ sessionCreds :| [parent]
  kem <- sntrup761Keypair rng

  pure SessionKeys {ts, ca, tls, sig, dh, kem}

sessionInvite ::
  -- | App information
  Maybe (Text, VersionRange) ->
  -- | Device name
  Maybe Text ->
  -- | Long-term identity key
  C.PublicKeyEd25519 ->
  SessionKeys ->
  -- | Service address
  (TransportHost, Word16) ->
  Invite
sessionInvite app_ device idkey SessionKeys {ts, ca, sig, dh, kem} (host, port) =
  Invite
    { ca,
      host,
      port,
      v = mkVersionRange 1 1,
      app,
      appv,
      device,
      ts,
      skey = C.publicKey sig,
      idkey,
      kem = fst kem,
      dh = C.publicKey dh
    }
  where
    (app, appv) = (fmap fst app_, fmap snd app_)

requiredP :: MonadFail m => SimpleQuery -> ByteString -> (ByteString -> Either String a) -> m a
requiredP q k f = maybe (fail $ "missing " <> show k) (either fail pure . f) $ lookup k q

optionalP :: MonadFail m => SimpleQuery -> ByteString -> (ByteString -> Either String a) -> m (Maybe a)
optionalP q k f = maybe (pure Nothing) (either fail (pure . Just) . f) $ lookup k q

data SignatureError
  = BadSessionSignature
  | BadIdentitySignature
  deriving (Eq, Show)

$(deriveJSON defaultJSON ''Invite)
