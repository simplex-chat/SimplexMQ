{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Simplex.Messaging.Client
-- Copyright   : (c) simplex.chat
-- License     : AGPL-3
--
-- Maintainer  : chat@simplex.chat
-- Stability   : experimental
-- Portability : non-portable
--
-- This module provides a functional client API for SMP protocol.
--
-- See https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md
module Simplex.Messaging.Client
  ( -- * Connect (disconnect) client to (from) SMP server
    ProtocolClient,
    SMPClient,
    getProtocolClient,
    closeProtocolClient,

    -- * SMP protocol command functions
    createSMPQueue,
    subscribeSMPQueue,
    subscribeSMPQueueNotifications,
    secureSMPQueue,
    enableSMPQueueNotifications,
    sendSMPMessage,
    ackSMPMessage,
    suspendSMPQueue,
    deleteSMPQueue,
    sendProtocolCommand,

    -- * Supporting types and client configuration
    ProtocolClientError (..),
    ProtocolClientConfig (..),
    defaultClientConfig,
    ServerTransmission,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Except
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Maybe (fromMaybe)
import Network.Socket (ServiceName)
import Numeric.Natural
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport (ATransport (..), THandle (..), TLS, TProxy, Transport (..), TransportError)
import Simplex.Messaging.Transport.Client (runTransportClient, smpClientHandshake)
import Simplex.Messaging.Transport.KeepAlive
import Simplex.Messaging.Transport.WebSockets (WS)
import Simplex.Messaging.Util (bshow, liftError, raceAny_)
import System.Timeout (timeout)

-- | 'SMPClient' is a handle used to send commands to a specific SMP server.
--
-- Use 'getSMPClient' to connect to an SMP server and create a client handle.
data ProtocolClient msg = ProtocolClient
  { action :: Async (),
    connected :: TVar Bool,
    sessionId :: ByteString,
    protocolServer :: ProtocolServer,
    tcpTimeout :: Int,
    clientCorrId :: TVar Natural,
    sentCommands :: TMap CorrId (Request msg),
    sndQ :: TBQueue SentRawTransmission,
    rcvQ :: TBQueue (SignedTransmission msg),
    msgQ :: TBQueue (ServerTransmission msg)
  }

type SMPClient = ProtocolClient SMP.BrokerMsg

-- | Type synonym for transmission from some SPM server queue.
type ServerTransmission msg = (ProtocolServer, QueueId, msg)

-- | protocol client configuration.
data ProtocolClientConfig = ProtocolClientConfig
  { -- | size of TBQueue to use for server commands and responses
    qSize :: Natural,
    -- | default server port if port is not specified in ProtocolServer
    defaultTransport :: (ServiceName, ATransport),
    -- | timeout of TCP commands (microseconds)
    tcpTimeout :: Int,
    -- | TCP keep-alive options, Nothing to skip enabling keep-alive
    tcpKeepAlive :: Maybe KeepAliveOpts,
    -- | period for SMP ping commands (microseconds)
    smpPing :: Int
  }

-- | Default protocol client configuration.
defaultClientConfig :: ProtocolClientConfig
defaultClientConfig =
  ProtocolClientConfig
    { qSize = 64,
      defaultTransport = ("5223", transport @TLS),
      tcpTimeout = 5_000_000,
      tcpKeepAlive = Just defaultKeepAliveOpts,
      smpPing = 300_000_000 -- 5 min
    }

data Request msg = Request
  { queueId :: QueueId,
    responseVar :: TMVar (Response msg)
  }

type Response msg = Either ProtocolClientError msg

-- | Connects to 'ProtocolServer' using passed client configuration
-- and queue for messages and notifications.
--
-- A single queue can be used for multiple 'SMPClient' instances,
-- as 'SMPServerTransmission' includes server information.
getProtocolClient :: forall msg. Protocol msg => ProtocolServer -> ProtocolClientConfig -> TBQueue (ServerTransmission msg) -> IO () -> IO (Either ProtocolClientError (ProtocolClient msg))
getProtocolClient protocolServer cfg@ProtocolClientConfig {qSize, tcpTimeout, tcpKeepAlive, smpPing} msgQ disconnected =
  (atomically mkSMPClient >>= runClient useTransport)
    `catch` \(e :: IOException) -> pure . Left $ PCEIOError e
  where
    mkSMPClient :: STM (ProtocolClient msg)
    mkSMPClient = do
      connected <- newTVar False
      clientCorrId <- newTVar 0
      sentCommands <- TM.empty
      sndQ <- newTBQueue qSize
      rcvQ <- newTBQueue qSize
      return
        ProtocolClient
          { action = undefined,
            sessionId = undefined,
            connected,
            protocolServer,
            tcpTimeout,
            clientCorrId,
            sentCommands,
            sndQ,
            rcvQ,
            msgQ
          }

    runClient :: (ServiceName, ATransport) -> ProtocolClient msg -> IO (Either ProtocolClientError (ProtocolClient msg))
    runClient (port', ATransport t) c = do
      thVar <- newEmptyTMVarIO
      action <-
        async $
          runTransportClient (host protocolServer) port' (keyHash protocolServer) tcpKeepAlive (client t c thVar)
            `finally` atomically (putTMVar thVar $ Left PCENetworkError)
      th_ <- tcpTimeout `timeout` atomically (takeTMVar thVar)
      pure $ case th_ of
        Just (Right THandle {sessionId}) -> Right c {action, sessionId}
        Just (Left e) -> Left e
        Nothing -> Left PCENetworkError

    useTransport :: (ServiceName, ATransport)
    useTransport = case port protocolServer of
      "" -> defaultTransport cfg
      "80" -> ("80", transport @WS)
      p -> (p, transport @TLS)

    client :: forall c. Transport c => TProxy c -> ProtocolClient msg -> TMVar (Either ProtocolClientError (THandle c)) -> c -> IO ()
    client _ c thVar h =
      runExceptT (smpClientHandshake h $ keyHash protocolServer) >>= \case
        Left e -> atomically . putTMVar thVar . Left $ PCETransportError e
        Right th@THandle {sessionId} -> do
          atomically $ do
            writeTVar (connected c) True
            putTMVar thVar $ Right th
          let c' = c {sessionId} :: ProtocolClient msg
          raceAny_ [send c' th, process c', receive c' th, ping c']
            `finally` disconnected

    send :: Transport c => ProtocolClient msg -> THandle c -> IO ()
    send ProtocolClient {sndQ} h = forever $ atomically (readTBQueue sndQ) >>= tPut h

    receive :: Transport c => ProtocolClient msg -> THandle c -> IO ()
    receive ProtocolClient {rcvQ} h = forever $ tGet h >>= atomically . writeTBQueue rcvQ

    ping :: ProtocolClient msg -> IO ()
    ping c = forever $ do
      threadDelay smpPing
      void . either throwIO pure =<< runExceptT (sendProtocolCommand c Nothing "" PING)

    process :: ProtocolClient msg -> IO ()
    process ProtocolClient {rcvQ, sentCommands} = forever $ do
      (_, _, (corrId, qId, respOrErr)) <- atomically $ readTBQueue rcvQ
      if B.null $ bs corrId
        then sendMsg qId respOrErr
        else do
          atomically (TM.lookup corrId sentCommands) >>= \case
            Nothing -> sendMsg qId respOrErr
            Just Request {queueId, responseVar} -> atomically $ do
              TM.delete corrId sentCommands
              putTMVar responseVar $
                if queueId == qId
                  then case respOrErr of
                    Left e -> Left $ PCEResponseError e
                    Right r -> case protocolError r of
                      Just e -> Left $ PCEProtocolError e
                      _ -> Right r
                  else Left PCEUnexpectedResponse

    sendMsg :: QueueId -> Either ErrorType msg -> IO ()
    sendMsg qId = \case
      Right cmd -> atomically $ writeTBQueue msgQ (protocolServer, qId, cmd)
      -- TODO send everything else to errQ and log in agent
      _ -> return ()

-- | Disconnects client from the server and terminates client threads.
closeProtocolClient :: ProtocolClient msg -> IO ()
closeProtocolClient = uninterruptibleCancel . action

-- | SMP client error type.
data ProtocolClientError
  = -- | Correctly parsed SMP server ERR response.
    -- This error is forwarded to the agent client as `ERR SMP err`.
    PCEProtocolError ErrorType
  | -- | Invalid server response that failed to parse.
    -- Forwarded to the agent client as `ERR BROKER RESPONSE`.
    PCEResponseError ErrorType
  | -- | Different response from what is expected to a certain SMP command,
    -- e.g. server should respond `IDS` or `ERR` to `NEW` command,
    -- other responses would result in this error.
    -- Forwarded to the agent client as `ERR BROKER UNEXPECTED`.
    PCEUnexpectedResponse
  | -- | Used for TCP connection and command response timeouts.
    -- Forwarded to the agent client as `ERR BROKER TIMEOUT`.
    PCEResponseTimeout
  | -- | Failure to establish TCP connection.
    -- Forwarded to the agent client as `ERR BROKER NETWORK`.
    PCENetworkError
  | -- | TCP transport handshake or some other transport error.
    -- Forwarded to the agent client as `ERR BROKER TRANSPORT e`.
    PCETransportError TransportError
  | -- | Error when cryptographically "signing" the command.
    PCESignatureError C.CryptoError
  | -- | IO Error
    PCEIOError IOException
  deriving (Eq, Show, Exception)

-- | Create a new SMP queue.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#create-queue-command
createSMPQueue ::
  SMPClient ->
  RcvPrivateSignKey ->
  RcvPublicVerifyKey ->
  RcvPublicDhKey ->
  ExceptT ProtocolClientError IO QueueIdsKeys
createSMPQueue c rpKey rKey dhKey =
  sendProtocolCommand c (Just rpKey) "" (NEW rKey dhKey) >>= \case
    IDS qik -> pure qik
    _ -> throwE PCEUnexpectedResponse

-- | Subscribe to the SMP queue.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#subscribe-to-queue
subscribeSMPQueue :: SMPClient -> RcvPrivateSignKey -> RecipientId -> ExceptT ProtocolClientError IO ()
subscribeSMPQueue c@ProtocolClient {protocolServer, msgQ} rpKey rId =
  sendProtocolCommand c (Just rpKey) rId SUB >>= \case
    OK -> return ()
    cmd@MSG {} ->
      lift . atomically $ writeTBQueue msgQ (protocolServer, rId, cmd)
    _ -> throwE PCEUnexpectedResponse

-- | Subscribe to the SMP queue notifications.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#subscribe-to-queue-notifications
subscribeSMPQueueNotifications :: SMPClient -> NtfPrivateSignKey -> NotifierId -> ExceptT ProtocolClientError IO ()
subscribeSMPQueueNotifications = okSMPCommand NSUB

-- | Secure the SMP queue by adding a sender public key.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#secure-queue-command
secureSMPQueue :: SMPClient -> RcvPrivateSignKey -> RecipientId -> SndPublicVerifyKey -> ExceptT ProtocolClientError IO ()
secureSMPQueue c rpKey rId senderKey = okSMPCommand (KEY senderKey) c rpKey rId

-- | Enable notifications for the queue for push notifications server.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#enable-notifications-command
enableSMPQueueNotifications :: SMPClient -> RcvPrivateSignKey -> RecipientId -> NtfPublicVerifyKey -> ExceptT ProtocolClientError IO NotifierId
enableSMPQueueNotifications c rpKey rId notifierKey =
  sendProtocolCommand c (Just rpKey) rId (NKEY notifierKey) >>= \case
    NID nId -> pure nId
    _ -> throwE PCEUnexpectedResponse

-- | Send SMP message.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#send-message
sendSMPMessage :: SMPClient -> Maybe SndPrivateSignKey -> SenderId -> MsgBody -> ExceptT ProtocolClientError IO ()
sendSMPMessage c spKey sId msg =
  sendProtocolCommand c spKey sId (SEND msg) >>= \case
    OK -> pure ()
    _ -> throwE PCEUnexpectedResponse

-- | Acknowledge message delivery (server deletes the message).
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#acknowledge-message-delivery
ackSMPMessage :: SMPClient -> RcvPrivateSignKey -> QueueId -> ExceptT ProtocolClientError IO ()
ackSMPMessage c@ProtocolClient {protocolServer, msgQ} rpKey rId =
  sendProtocolCommand c (Just rpKey) rId ACK >>= \case
    OK -> return ()
    cmd@MSG {} ->
      lift . atomically $ writeTBQueue msgQ (protocolServer, rId, cmd)
    _ -> throwE PCEUnexpectedResponse

-- | Irreversibly suspend SMP queue.
-- The existing messages from the queue will still be delivered.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#suspend-queue
suspendSMPQueue :: SMPClient -> RcvPrivateSignKey -> QueueId -> ExceptT ProtocolClientError IO ()
suspendSMPQueue = okSMPCommand OFF

-- | Irreversibly delete SMP queue and all messages in it.
--
-- https://github.com/simplex-chat/simplexmq/blob/master/protocol/simplex-messaging.md#delete-queue
deleteSMPQueue :: SMPClient -> RcvPrivateSignKey -> QueueId -> ExceptT ProtocolClientError IO ()
deleteSMPQueue = okSMPCommand DEL

okSMPCommand :: PartyI p => Command p -> SMPClient -> C.APrivateSignKey -> QueueId -> ExceptT ProtocolClientError IO ()
okSMPCommand cmd c pKey qId =
  sendProtocolCommand c (Just pKey) qId cmd >>= \case
    OK -> return ()
    _ -> throwE PCEUnexpectedResponse

-- | Send SMP command
-- TODO sign all requests (SEND of SMP confirmation would be signed with the same key that is passed to the recipient)
sendProtocolCommand :: forall p msg. PartyI p => ProtocolClient msg -> Maybe C.APrivateSignKey -> QueueId -> Command p -> ExceptT ProtocolClientError IO msg
sendProtocolCommand ProtocolClient {sndQ, sentCommands, clientCorrId, sessionId, tcpTimeout} pKey qId cmd = do
  corrId <- lift_ getNextCorrId
  t <- signTransmission $ encodeTransmission sessionId (corrId, qId, cmd)
  ExceptT $ sendRecv corrId t
  where
    lift_ :: STM a -> ExceptT ProtocolClientError IO a
    lift_ action = ExceptT $ Right <$> atomically action

    getNextCorrId :: STM CorrId
    getNextCorrId = do
      i <- stateTVar clientCorrId $ \i -> (i, i + 1)
      pure . CorrId $ bshow i

    signTransmission :: ByteString -> ExceptT ProtocolClientError IO SentRawTransmission
    signTransmission t = case pKey of
      Nothing -> return (Nothing, t)
      Just pk -> do
        sig <- liftError PCESignatureError $ C.sign pk t
        return (Just sig, t)

    -- two separate "atomically" needed to avoid blocking
    sendRecv :: CorrId -> SentRawTransmission -> IO (Response msg)
    sendRecv corrId t = atomically (send corrId t) >>= withTimeout . atomically . takeTMVar
      where
        withTimeout a = fromMaybe (Left PCEResponseTimeout) <$> timeout tcpTimeout a

    send :: CorrId -> SentRawTransmission -> STM (TMVar (Response msg))
    send corrId t = do
      r <- newEmptyTMVar
      TM.insert corrId (Request qId r) sentCommands
      writeTBQueue sndQ t
      return r
