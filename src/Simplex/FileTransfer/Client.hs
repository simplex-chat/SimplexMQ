{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Simplex.FileTransfer.Client where

import Control.Monad
import Control.Monad.Except
import Crypto.Random (ChaChaDRG)
import Data.Bifunctor (first)
import Data.ByteString.Builder (Builder, byteString)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Time (UTCTime)
import Data.Word (Word32)
import qualified Network.HTTP.Types as N
import qualified Network.HTTP2.Client as H
import Simplex.FileTransfer.Description (mb)
import Simplex.FileTransfer.Protocol
import Simplex.FileTransfer.Transport
import Simplex.Messaging.Client
  ( NetworkConfig (..),
    ProtocolClientError (..),
    TransportSession,
    chooseTransportHost,
    defaultNetworkConfig,
    proxyUsername,
    transportClientConfig,
  )
import Simplex.Messaging.Client.Agent ()
import qualified Simplex.Messaging.Crypto as C
import qualified Simplex.Messaging.Crypto.Lazy as LC
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
  ( BasicAuth,
    Protocol (..),
    ProtocolServer (..),
    RecipientId,
    SenderId,
  )
import Simplex.Messaging.Transport (THandleParams (..), supportedParameters)
import Simplex.Messaging.Transport.Client (TransportClientConfig, TransportHost)
import Simplex.Messaging.Transport.HTTP2
import Simplex.Messaging.Transport.HTTP2.Client
import Simplex.Messaging.Transport.HTTP2.File
import Simplex.Messaging.Util (bshow, whenM)
import UnliftIO
import UnliftIO.Directory

data XFTPClient = XFTPClient
  { http2Client :: HTTP2Client,
    transportSession :: TransportSession FileResponse,
    thParams :: THandleParams XFTPVersion,
    config :: XFTPClientConfig
  }

data XFTPClientConfig = XFTPClientConfig
  { xftpNetworkConfig :: NetworkConfig,
    uploadTimeoutPerMb :: Int64
  }

data XFTPChunkBody = XFTPChunkBody
  { chunkSize :: Int,
    chunkPart :: Int -> IO ByteString,
    http2Body :: HTTP2Body
  }

data XFTPChunkSpec = XFTPChunkSpec
  { filePath :: FilePath,
    chunkOffset :: Int64,
    chunkSize :: Word32
  }
  deriving (Eq, Show)

type XFTPClientError = ProtocolClientError XFTPErrorType

defaultXFTPClientConfig :: XFTPClientConfig
defaultXFTPClientConfig =
  XFTPClientConfig
    { xftpNetworkConfig = defaultNetworkConfig,
      uploadTimeoutPerMb = 10000000 -- 10 seconds
    }

getXFTPClient :: TransportSession FileResponse -> XFTPClientConfig -> (XFTPClient -> IO ()) -> IO (Either XFTPClientError XFTPClient)
getXFTPClient transportSession@(_, srv, _) config@XFTPClientConfig {xftpNetworkConfig} disconnected = runExceptT $ do
  let tcConfig = transportClientConfig xftpNetworkConfig
      http2Config = xftpHTTP2Config tcConfig config
      username = proxyUsername transportSession
      ProtocolServer _ host port keyHash = srv
  useHost <- liftEither $ chooseTransportHost xftpNetworkConfig host
  clientVar <- newTVarIO Nothing
  let usePort = if null port then "443" else port
      clientDisconnected = readTVarIO clientVar >>= mapM_ disconnected
  http2Client <- withExceptT xftpClientError . ExceptT $ getVerifiedHTTP2Client (Just username) useHost usePort (Just keyHash) Nothing http2Config clientDisconnected
  let HTTP2Client {sessionId} = http2Client
      thParams = THandleParams {sessionId, blockSize = xftpBlockSize, thVersion = currentXFTPVersion, thAuth = Nothing, implySessId = False, batch = True}
      c = XFTPClient {http2Client, thParams, transportSession, config}
  atomically $ writeTVar clientVar $ Just c
  pure c

closeXFTPClient :: XFTPClient -> IO ()
closeXFTPClient XFTPClient {http2Client} = closeHTTP2Client http2Client

xftpClientServer :: XFTPClient -> String
xftpClientServer = B.unpack . strEncode . snd3 . transportSession
  where
    snd3 (_, s, _) = s

xftpTransportHost :: XFTPClient -> TransportHost
xftpTransportHost XFTPClient {http2Client = HTTP2Client {client_ = HClient {host}}} = host

xftpSessionTs :: XFTPClient -> UTCTime
xftpSessionTs = sessionTs . http2Client

xftpHTTP2Config :: TransportClientConfig -> XFTPClientConfig -> HTTP2ClientConfig
xftpHTTP2Config transportConfig XFTPClientConfig {xftpNetworkConfig = NetworkConfig {tcpConnectTimeout}} =
  defaultHTTP2ClientConfig
    { bodyHeadSize = xftpBlockSize,
      suportedTLSParams = supportedParameters,
      connTimeout = tcpConnectTimeout,
      transportConfig
    }

xftpClientError :: HTTP2ClientError -> XFTPClientError
xftpClientError = \case
  HCResponseTimeout -> PCEResponseTimeout
  HCNetworkError -> PCENetworkError
  HCIOError e -> PCEIOError e

sendXFTPCommand :: forall p. FilePartyI p => XFTPClient -> C.APrivateAuthKey -> XFTPFileId -> FileCommand p -> Maybe XFTPChunkSpec -> ExceptT XFTPClientError IO (FileResponse, HTTP2Body)
sendXFTPCommand c@XFTPClient {thParams} pKey fId cmd chunkSpec_ = do
  t <-
    liftEither . first PCETransportError $
      xftpEncodeAuthTransmission thParams pKey ("", fId, FileCmd (sFileParty @p) cmd)
  sendXFTPTransmission c t chunkSpec_

sendXFTPTransmission :: XFTPClient -> ByteString -> Maybe XFTPChunkSpec -> ExceptT XFTPClientError IO (FileResponse, HTTP2Body)
sendXFTPTransmission XFTPClient {config, thParams, http2Client} t chunkSpec_ = do
  let req = H.requestStreaming N.methodPost "/" [] streamBody
      reqTimeout = (\XFTPChunkSpec {chunkSize} -> chunkTimeout config chunkSize) <$> chunkSpec_
  HTTP2Response {respBody = body@HTTP2Body {bodyHead}} <- withExceptT xftpClientError . ExceptT $ sendRequest http2Client req reqTimeout
  when (B.length bodyHead /= xftpBlockSize) $ throwError $ PCEResponseError BLOCK
  -- TODO validate that the file ID is the same as in the request?
  (_, _, (_, _fId, respOrErr)) <- liftEither . first PCEResponseError $ xftpDecodeTransmission thParams bodyHead
  case respOrErr of
    Right r -> case protocolError r of
      Just e -> throwError $ PCEProtocolError e
      _ -> pure (r, body)
    Left e -> throwError $ PCEResponseError e
  where
    streamBody :: (Builder -> IO ()) -> IO () -> IO ()
    streamBody send done = do
      send $ byteString t
      forM_ chunkSpec_ $ \XFTPChunkSpec {filePath, chunkOffset, chunkSize} ->
        withFile filePath ReadMode $ \h -> do
          hSeek h AbsoluteSeek $ fromIntegral chunkOffset
          hSendFile h send $ fromIntegral chunkSize
      done

createXFTPChunk ::
  XFTPClient ->
  C.APrivateAuthKey ->
  FileInfo ->
  NonEmpty C.APublicAuthKey ->
  Maybe BasicAuth ->
  ExceptT XFTPClientError IO (SenderId, NonEmpty RecipientId)
createXFTPChunk c spKey file rcps auth_ =
  sendXFTPCommand c spKey "" (FNEW file rcps auth_) Nothing >>= \case
    (FRSndIds sId rIds, body) -> noFile body (sId, rIds)
    (r, _) -> throwError . PCEUnexpectedResponse $ bshow r

addXFTPRecipients :: XFTPClient -> C.APrivateAuthKey -> XFTPFileId -> NonEmpty C.APublicAuthKey -> ExceptT XFTPClientError IO (NonEmpty RecipientId)
addXFTPRecipients c spKey fId rcps =
  sendXFTPCommand c spKey fId (FADD rcps) Nothing >>= \case
    (FRRcvIds rIds, body) -> noFile body rIds
    (r, _) -> throwError . PCEUnexpectedResponse $ bshow r

uploadXFTPChunk :: XFTPClient -> C.APrivateAuthKey -> XFTPFileId -> XFTPChunkSpec -> ExceptT XFTPClientError IO ()
uploadXFTPChunk c spKey fId chunkSpec =
  sendXFTPCommand c spKey fId FPUT (Just chunkSpec) >>= okResponse

downloadXFTPChunk :: TVar ChaChaDRG -> XFTPClient -> C.APrivateAuthKey -> XFTPFileId -> XFTPRcvChunkSpec -> ExceptT XFTPClientError IO ()
downloadXFTPChunk g c@XFTPClient {config} rpKey fId chunkSpec@XFTPRcvChunkSpec {filePath, chunkSize} = do
  (rDhKey, rpDhKey) <- atomically $ C.generateKeyPair g
  sendXFTPCommand c rpKey fId (FGET rDhKey) Nothing >>= \case
    (FRFile sDhKey cbNonce, HTTP2Body {bodyHead = _bg, bodySize = _bs, bodyPart}) -> case bodyPart of
      -- TODO atm bodySize is set to 0, so chunkSize will be incorrect - validate once set
      Just chunkPart -> do
        let dhSecret = C.dh' sDhKey rpDhKey
        cbState <- liftEither . first PCECryptoError $ LC.cbInit dhSecret cbNonce
        let t = chunkTimeout config chunkSize
        ExceptT (fmap sequence . timeout t $ download cbState) >>= maybe (throwError PCEResponseTimeout) pure
        where
          download cbState = runExceptT $
            withExceptT PCEResponseError $
              receiveEncFile chunkPart cbState chunkSpec `catchError` \e ->
                whenM (doesFileExist filePath) (removeFile filePath) >> throwError e
      _ -> throwError $ PCEResponseError NO_FILE
    (r, _) -> throwError . PCEUnexpectedResponse $ bshow r

chunkTimeout :: XFTPClientConfig -> Word32 -> Int
chunkTimeout config chunkSize = fromIntegral $ (fromIntegral chunkSize * uploadTimeoutPerMb config) `div` mb 1

deleteXFTPChunk :: XFTPClient -> C.APrivateAuthKey -> SenderId -> ExceptT XFTPClientError IO ()
deleteXFTPChunk c spKey sId = sendXFTPCommand c spKey sId FDEL Nothing >>= okResponse

ackXFTPChunk :: XFTPClient -> C.APrivateAuthKey -> RecipientId -> ExceptT XFTPClientError IO ()
ackXFTPChunk c rpKey rId = sendXFTPCommand c rpKey rId FACK Nothing >>= okResponse

pingXFTP :: XFTPClient -> ExceptT XFTPClientError IO ()
pingXFTP c@XFTPClient {thParams} = do
  t <-
    liftEither . first PCETransportError $
      xftpEncodeTransmission thParams ("", "", FileCmd SFRecipient PING)
  (r, _) <- sendXFTPTransmission c t Nothing
  case r of
    FRPong -> pure ()
    _ -> throwError $ PCEUnexpectedResponse $ bshow r

okResponse :: (FileResponse, HTTP2Body) -> ExceptT XFTPClientError IO ()
okResponse = \case
  (FROk, body) -> noFile body ()
  (r, _) -> throwError . PCEUnexpectedResponse $ bshow r

-- TODO this currently does not check anything because response size is not set and bodyPart is always Just
noFile :: HTTP2Body -> a -> ExceptT XFTPClientError IO a
noFile HTTP2Body {bodyPart} a = case bodyPart of
  Just _ -> pure a -- throwError $ PCEResponseError HAS_FILE
  _ -> pure a

-- FACK :: FileCommand Recipient
-- PING :: FileCommand Recipient
