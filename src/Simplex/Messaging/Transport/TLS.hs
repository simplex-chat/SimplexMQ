{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Transport.TLS (TLS) where

import Control.Concurrent.STM
import qualified Control.Exception as E
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import Data.Default (def)
import Data.Either (fromRight)
import Network.Socket (Socket)
import qualified Network.TLS as T
import qualified Network.TLS.Extra as TE
import Simplex.Messaging.Transport (Transport (..), TransportError (..))

data TLS = TLS {tlsContext :: T.Context, buffer :: TVar ByteString, getLock :: TMVar ()}

connect :: T.TLSParams p => String -> p -> Socket -> IO TLS
connect party params sock = E.bracketOnError (T.contextNew sock params) closeTLS $ \tlsContext -> do
  T.handshake tlsContext
    `E.catch` \(e :: E.SomeException) -> putStrLn (party <> " exception: " <> show e) >> E.throwIO e
  buffer <- newTVarIO ""
  getLock <- newTMVarIO ()
  pure TLS {tlsContext, buffer, getLock}

closeTLS :: T.Context -> IO ()
closeTLS ctx =
  (T.bye ctx >> T.contextClose ctx) -- sometimes socket was closed before 'TLS.bye'
    `E.catch` (\(_ :: E.SomeException) -> pure ()) -- so we catch the 'Broken pipe' error here

serverCredential :: T.Credential
serverCredential =
  fromRight (error "invalid credential") $
    T.credentialLoadX509FromMemory serverCert serverPrivateKey

-- To generate self-signed certificate:
-- https://blog.pinterjann.is/ed25519-certificates.html

serverCert :: ByteString
serverCert =
  "-----BEGIN CERTIFICATE-----\n\
  \MIIBSTCBygIUG8XHI4lGld/8Tb824iWF390hsOwwBQYDK2VxMCExCzAJBgNVBAYT\n\
  \AkRFMRIwEAYDVQQDDAlsb2NhbGhvc3QwIBcNMjExMTIwMTAwMTU5WhgPOTk5OTEy\n\
  \MzExMDAxNTlaMCExCzAJBgNVBAYTAkRFMRIwEAYDVQQDDAlsb2NhbGhvc3QwQzAF\n\
  \BgMrZXEDOgDVgTUc+4Ur9or2N0IKjNgBx649yzAoM5cJKE90OklUKYKdnk4V7kap\n\
  \oHX/d/OUgCdCYa6geMj69QAwBQYDK2VxA3MAlv8lp6xm+KgsGpE+5QLuNT0xdf/i\n\
  \jjJfqLzXzuBwE0+5HwxnrOZ1xMPs30aubiChbpJaJx2xPXkAmnR5Z4p7HcmnPm1P\n\
  \B1SKVlxfqTGWTsJI8E6rNjSsoTZR48DuDZuQthr4SWdcz+jkdFmprTrWHgMA\n\
  \-----END CERTIFICATE-----"

serverPrivateKey :: ByteString
serverPrivateKey =
  "-----BEGIN PRIVATE KEY-----\n\
  \MEcCAQAwBQYDK2VxBDsEOWbzhmByxZ0z656MQV0Vtbkv0VfMpwpvdal8W4Vu9gXu\n\
  \uT7CCDxjBTQQZ8yPnuUNY75jwlyEwbkM1g==\n\
  \-----END PRIVATE KEY-----"

serverParams :: T.ServerParams
serverParams =
  def
    { T.serverWantClientCert = False,
      T.serverShared = def {T.sharedCredentials = T.Credentials [serverCredential]},
      T.serverHooks = def,
      T.serverSupported = supportedParameters
    }

clientParams :: T.ClientParams
clientParams =
  (T.defaultParamsClient "localhost" "5223")
    { T.clientShared = def,
      T.clientHooks = def {T.onServerCertificate = \_ _ _ _ -> pure []},
      T.clientSupported = supportedParameters
    }

supportedParameters :: T.Supported
supportedParameters =
  def
    { T.supportedVersions = [T.TLS13],
      T.supportedCiphers = [TE.cipher_TLS13_AES256GCM_SHA384],
      T.supportedHashSignatures = [(T.HashIntrinsic, T.SignatureEd448)],
      T.supportedSecureRenegotiation = False,
      T.supportedGroups = [T.X448]
    }

instance Transport TLS where
  transportName _ = "TLS 1.3"
  getServerConnection = connect "server" serverParams
  getClientConnection = connect "client" clientParams
  closeConnection = closeTLS . tlsContext

  cGet :: TLS -> Int -> IO ByteString
  cGet TLS {tlsContext, buffer, getLock} n =
    E.bracket_
      (atomically $ takeTMVar getLock)
      (atomically $ putTMVar getLock ())
      $ do
        b <- readChunks =<< readTVarIO buffer
        let (s, b') = B.splitAt n b
        atomically $ writeTVar buffer b'
        pure s
    where
      readChunks :: ByteString -> IO ByteString
      readChunks b
        | B.length b >= n = pure b
        | otherwise = readChunks . (b <>) =<< T.recvData tlsContext `E.catch` handleEOF
      handleEOF = \case
        T.Error_EOF -> E.throwIO TEBadBlock
        e -> E.throwIO e

  cPut :: TLS -> ByteString -> IO ()
  cPut c = T.sendData (tlsContext c) . BL.fromStrict
