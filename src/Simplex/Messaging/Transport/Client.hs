{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Transport.Client
  ( runTransportClient,
    runTLSTransportClient,
    smpClientHandshake,
    defaultSMPPort,
    defaultSocksProxy,
    SocksProxy,
  )
where

import Control.Applicative (optional)
import Control.Monad.Except
import Control.Monad.IO.Unlift
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Default (def)
import Data.Maybe (fromMaybe)
import qualified Data.X509 as X
import qualified Data.X509.CertificateStore as XS
import Data.X509.Validation (Fingerprint (..))
import qualified Data.X509.Validation as XV
import GHC.IO.Exception (IOErrorType (..))
import Network.Socket
import Network.Socks5
import qualified Network.TLS as T
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Transport
import Simplex.Messaging.Transport.KeepAlive
import System.IO.Error
import Text.Read (readMaybe)
import UnliftIO.Exception (IOException)
import qualified UnliftIO.Exception as E

-- | Connect to passed TCP host:port and pass handle to the client.
runTransportClient :: (Transport c, MonadUnliftIO m) => Maybe SocksProxy -> HostName -> ServiceName -> Maybe C.KeyHash -> Maybe KeepAliveOpts -> (c -> m a) -> m a
runTransportClient = runTLSTransportClient supportedParameters Nothing

runTLSTransportClient :: (Transport c, MonadUnliftIO m) => T.Supported -> Maybe XS.CertificateStore -> Maybe SocksProxy -> HostName -> ServiceName -> Maybe C.KeyHash -> Maybe KeepAliveOpts -> (c -> m a) -> m a
runTLSTransportClient tlsParams caStore_ socksProxy_ host port keyHash keepAliveOpts client = do
  let clientParams = mkTLSClientParams tlsParams caStore_ host port keyHash
      connectTCP = maybe connectTCPClient connectSocksClient socksProxy_
  c <- liftIO $ connectTLSClient connectTCP host port clientParams keepAliveOpts
  client c `E.finally` liftIO (closeConnection c)

connectTLSClient :: forall c. Transport c => (HostName -> ServiceName -> IO Socket) -> HostName -> ServiceName -> T.ClientParams -> Maybe KeepAliveOpts -> IO c
connectTLSClient tcpClient host port clientParams keepAliveOpts = do
  sock <- tcpClient host port
  mapM_ (setSocketKeepAlive sock) keepAliveOpts
  ctx <- connectTLS clientParams sock
  getClientConnection ctx

connectTCPClient :: HostName -> ServiceName -> IO Socket
connectTCPClient host port = withSocketsDo $ resolve >>= tryOpen err
  where
    err :: IOException
    err = mkIOError NoSuchThing "no address" Nothing Nothing

    resolve :: IO [AddrInfo]
    resolve =
      let hints = defaultHints {addrSocketType = Stream}
       in getAddrInfo (Just hints) (Just host) (Just port)

    tryOpen :: IOException -> [AddrInfo] -> IO Socket
    tryOpen e [] = E.throwIO e
    tryOpen _ (addr : as) =
      E.try (open addr) >>= either (`tryOpen` as) pure

    open :: AddrInfo -> IO Socket
    open addr = do
      sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
      connect sock $ addrAddress addr
      pure sock

defaultSMPPort :: PortNumber
defaultSMPPort = 5223

connectSocksClient :: SocksProxy -> HostName -> ServiceName -> IO Socket
connectSocksClient (SocksProxy addr) host _port = do
  let port = if null _port then defaultSMPPort else fromMaybe defaultSMPPort $ readMaybe _port
  fst <$> socksConnect (defaultSocksConf addr) (SocksAddress (SocksAddrDomainName $ B.pack host) port)

defaultSocksHost :: HostAddress
defaultSocksHost = tupleToHostAddress (127, 0, 0, 1)

defaultSocksProxy :: SocksProxy
defaultSocksProxy = SocksProxy $ SockAddrInet 9050 defaultSocksHost

newtype SocksProxy = SocksProxy SockAddr
  deriving (Eq)

instance Show SocksProxy where show (SocksProxy addr) = show addr

instance StrEncoding SocksProxy where
  strEncode = B.pack . show
  strP = do
    host <- maybe defaultSocksHost tupleToHostAddress <$> optional ipv4P
    port <- fromMaybe 9050 <$> optional (A.char ':' *> (fromInteger <$> A.decimal))
    pure . SocksProxy $ SockAddrInet port host
    where
      ipv4P = (,,,) <$> ipNum <*> ipNum <*> ipNum <*> A.decimal
      ipNum = A.decimal <* A.char '.'

instance ToJSON SocksProxy where
  toJSON = strToJSON
  toEncoding = strToJEncoding

instance FromJSON SocksProxy where
  parseJSON = strParseJSON "SocksProxy"

mkTLSClientParams :: T.Supported -> Maybe XS.CertificateStore -> HostName -> ServiceName -> Maybe C.KeyHash -> T.ClientParams
mkTLSClientParams supported caStore_ host port keyHash_ = do
  let p = B.pack port
  (T.defaultParamsClient host p)
    { T.clientShared = maybe def (\caStore -> def {T.sharedCAStore = caStore}) caStore_,
      T.clientHooks = maybe def (\keyHash -> def {T.onServerCertificate = \_ _ _ -> validateCertificateChain keyHash host p}) keyHash_,
      T.clientSupported = supported
    }

validateCertificateChain :: C.KeyHash -> HostName -> ByteString -> X.CertificateChain -> IO [XV.FailedReason]
validateCertificateChain _ _ _ (X.CertificateChain []) = pure [XV.EmptyChain]
validateCertificateChain _ _ _ (X.CertificateChain [_]) = pure [XV.EmptyChain]
validateCertificateChain (C.KeyHash kh) host port cc@(X.CertificateChain sc@[_, caCert]) =
  if Fingerprint kh == XV.getFingerprint caCert X.HashSHA256
    then x509validate
    else pure [XV.UnknownCA]
  where
    x509validate :: IO [XV.FailedReason]
    x509validate = XV.validate X.HashSHA256 hooks checks certStore cache serviceID cc
      where
        hooks = XV.defaultHooks
        checks = XV.defaultChecks {XV.checkFQHN = False}
        certStore = XS.makeCertificateStore sc
        cache = XV.exceptionValidationCache [] -- we manually check fingerprint only of the identity certificate (ca.crt)
        serviceID = (host, port)
validateCertificateChain _ _ _ _ = pure [XV.AuthorityTooDeep]
