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
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Simplex.Messaging.Agent.Store.AgentStore
  ( -- * Users
    createUserRecord,
    deleteUserRecord,
    setUserDeleted,
    deleteUserWithoutConns,
    deleteUsersWithoutConns,
    checkUser,

    -- * Queues and connections
    createNewConn,
    updateNewConnRcv,
    updateNewConnSnd,
    createSndConn,
    getConn,
    getDeletedConn,
    getConns,
    getDeletedConns,
    getConnData,
    setConnDeleted,
    setConnUserId,
    setConnAgentVersion,
    setConnPQSupport,
    getDeletedConnIds,
    getDeletedWaitingDeliveryConnIds,
    setConnRatchetSync,
    addProcessedRatchetKeyHash,
    checkRatchetKeyHashExists,
    deleteRatchetKeyHashesExpired,
    getRcvConn,
    getRcvQueueById,
    getSndQueueById,
    deleteConn,
    upgradeRcvConnToDuplex,
    upgradeSndConnToDuplex,
    addConnRcvQueue,
    addConnSndQueue,
    setRcvQueueStatus,
    setRcvSwitchStatus,
    setRcvQueueDeleted,
    setRcvQueueConfirmedE2E,
    setSndQueueStatus,
    setSndSwitchStatus,
    setRcvQueuePrimary,
    setSndQueuePrimary,
    deleteConnRcvQueue,
    incRcvDeleteErrors,
    deleteConnSndQueue,
    getPrimaryRcvQueue,
    getRcvQueue,
    getDeletedRcvQueue,
    setRcvQueueNtfCreds,
    -- Confirmations
    createConfirmation,
    acceptConfirmation,
    getAcceptedConfirmation,
    removeConfirmations,
    -- Invitations - sent via Contact connections
    createInvitation,
    getInvitation,
    acceptInvitation,
    unacceptInvitation,
    deleteInvitation,
    -- Messages
    updateRcvIds,
    createRcvMsg,
    updateRcvMsgHash,
    createSndMsgBody,
    updateSndIds,
    createSndMsg,
    updateSndMsgHash,
    createSndMsgDelivery,
    getSndMsgViaRcpt,
    updateSndMsgRcpt,
    getPendingQueueMsg,
    getConnectionsForDelivery,
    updatePendingMsgRIState,
    deletePendingMsgs,
    getExpiredSndMessages,
    setMsgUserAck,
    getRcvMsg,
    getLastMsg,
    checkRcvMsgHashExists,
    getRcvMsgBrokerTs,
    deleteMsg,
    deleteDeliveredSndMsg,
    deleteSndMsgDelivery,
    deleteRcvMsgHashesExpired,
    deleteSndMsgsExpired,
    -- Double ratchet persistence
    createRatchetX3dhKeys,
    getRatchetX3dhKeys,
    setRatchetX3dhKeys,
    createSndRatchet,
    getSndRatchet,
    createRatchet,
    deleteRatchet,
    getRatchet,
    getSkippedMsgKeys,
    updateRatchet,
    -- Async commands
    createCommand,
    getPendingCommandServers,
    getPendingServerCommand,
    updateCommandServer,
    deleteCommand,
    -- Notification device token persistence
    createNtfToken,
    getSavedNtfToken,
    updateNtfTokenRegistration,
    updateDeviceToken,
    updateNtfMode,
    updateNtfToken,
    removeNtfToken,
    addNtfTokenToDelete,
    deleteExpiredNtfTokensToDelete,
    NtfTokenToDelete,
    getNextNtfTokenToDelete,
    markNtfTokenToDeleteFailed_, -- exported for tests
    getPendingDelTknServers,
    deleteNtfTokenToDelete,
    -- Notification subscription persistence
    NtfSupervisorSub,
    getNtfSubscription,
    createNtfSubscription,
    supervisorUpdateNtfSub,
    supervisorUpdateNtfAction,
    updateNtfSubscription,
    setNullNtfSubscriptionAction,
    deleteNtfSubscription,
    deleteNtfSubscription',
    getNextNtfSubNTFActions,
    markNtfSubActionNtfFailed_, -- exported for tests
    getNextNtfSubSMPActions,
    markNtfSubActionSMPFailed_, -- exported for tests
    getActiveNtfToken,
    getNtfRcvQueue,
    setConnectionNtfs,

    -- * File transfer

    -- Rcv files
    createRcvFile,
    createRcvFileRedirect,
    getRcvFile,
    getRcvFileByEntityId,
    getRcvFileRedirects,
    updateRcvChunkReplicaDelay,
    updateRcvFileChunkReceived,
    updateRcvFileStatus,
    updateRcvFileError,
    updateRcvFileComplete,
    updateRcvFileRedirect,
    updateRcvFileNoTmpPath,
    updateRcvFileDeleted,
    deleteRcvFile',
    getNextRcvChunkToDownload,
    getNextRcvFileToDecrypt,
    getPendingRcvFilesServers,
    getCleanupRcvFilesTmpPaths,
    getCleanupRcvFilesDeleted,
    getRcvFilesExpired,
    -- Snd files
    createSndFile,
    getSndFile,
    getSndFileByEntityId,
    getNextSndFileToPrepare,
    updateSndFileError,
    updateSndFileStatus,
    updateSndFileEncrypted,
    updateSndFileComplete,
    updateSndFileNoPrefixPath,
    updateSndFileDeleted,
    deleteSndFile',
    getSndFileDeleted,
    createSndFileReplica,
    createSndFileReplica_, -- exported for tests
    getNextSndChunkToUpload,
    updateSndChunkReplicaDelay,
    addSndChunkReplicaRecipients,
    updateSndChunkReplicaStatus,
    getPendingSndFilesServers,
    getCleanupSndFilesPrefixPaths,
    getCleanupSndFilesDeleted,
    getSndFilesExpired,
    createDeletedSndChunkReplica,
    getNextDeletedSndChunkReplica,
    updateDeletedSndChunkReplicaDelay,
    deleteDeletedSndChunkReplica,
    getPendingDelFilesServers,
    deleteDeletedSndChunkReplicasExpired,
    -- Stats
    updateServersStats,
    getServersStats,
    resetServersStats,

    -- * utilities
    withConnection,
    withTransaction,
    withTransactionPriority,
    firstRow,
    firstRow',
    maybeFirstRow,
    fromOnlyBI,
  )
where

import Control.Logger.Simple
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Crypto.Random (ChaChaDRG)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Base64.URL as U
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import Data.Int (Int64)
import Data.List (foldl', sortBy)
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as L
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
import Data.Ord (Down (..))
import Data.Text.Encoding (decodeLatin1, encodeUtf8)
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Data.Word (Word32)
import Network.Socket (ServiceName)
import Simplex.FileTransfer.Client (XFTPChunkSpec (..))
import Simplex.FileTransfer.Description
import Simplex.FileTransfer.Protocol (FileParty (..), SFileParty (..))
import Simplex.FileTransfer.Types
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval (RI2State (..))
import Simplex.Messaging.Agent.Stats
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.Common
import qualified Simplex.Messaging.Agent.Store.DB as DB
import Simplex.Messaging.Agent.Store.DB (Binary (..), BoolInt (..), FromField (..), ToField (..), blobFieldDecoder, fromTextField_)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.File (CryptoFile (..), CryptoFileArgs (..))
import Simplex.Messaging.Crypto.Ratchet (PQEncryption (..), PQSupport (..), RatchetX448, SkippedMsgDiff (..), SkippedMsgKeys)
import qualified Simplex.Messaging.Crypto.Ratchet as CR
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Notifications.Protocol (DeviceToken (..), NtfSubscriptionId, NtfTknStatus (..), NtfTokenId, SMPQueueNtf (..))
import Simplex.Messaging.Notifications.Types
import Simplex.Messaging.Parsers (parseAll)
import Simplex.Messaging.Protocol
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Transport.Client (TransportHost)
import Simplex.Messaging.Util (bshow, catchAllErrors, eitherToMaybe, firstRow, firstRow', ifM, maybeFirstRow, tshow, ($>>=), (<$$>))
import Simplex.Messaging.Version.Internal
import qualified UnliftIO.Exception as E
import UnliftIO.STM
#if defined(dbPostgres)
import Database.PostgreSQL.Simple (Only (..), Query, SqlError, (:.) (..))
import Database.PostgreSQL.Simple.Errors (constraintViolation)
import Database.PostgreSQL.Simple.SqlQQ (sql)
#else
import Database.SQLite.Simple (FromRow (..), Only (..), Query (..), SQLError, ToRow (..), field, (:.) (..))
import qualified Database.SQLite.Simple as SQL
import Database.SQLite.Simple.QQ (sql)
#endif

checkConstraint :: StoreError -> IO (Either StoreError a) -> IO (Either StoreError a)
checkConstraint err action = action `E.catch` (pure . Left . handleSQLError err)

#if defined(dbPostgres)
handleSQLError :: StoreError -> SqlError -> StoreError
handleSQLError err e = case constraintViolation e of
  Just _ -> err
  Nothing -> SEInternal $ bshow e
#else
handleSQLError :: StoreError -> SQLError -> StoreError
handleSQLError err e
  | SQL.sqlError e == SQL.ErrorConstraint = err
  | otherwise = SEInternal $ bshow e
#endif

createUserRecord :: DB.Connection -> IO UserId
createUserRecord db = do
  DB.execute_ db "INSERT INTO users DEFAULT VALUES"
  insertedRowId db

checkUser :: DB.Connection -> UserId -> IO (Either StoreError ())
checkUser db userId =
  firstRow (\(_ :: Only Int64) -> ()) SEUserNotFound $
    DB.query db "SELECT user_id FROM users WHERE user_id = ? AND deleted = ?" (userId, BI False)

deleteUserRecord :: DB.Connection -> UserId -> IO (Either StoreError ())
deleteUserRecord db userId = runExceptT $ do
  ExceptT $ checkUser db userId
  liftIO $ DB.execute db "DELETE FROM users WHERE user_id = ?" (Only userId)

setUserDeleted :: DB.Connection -> UserId -> IO (Either StoreError [ConnId])
setUserDeleted db userId = runExceptT $ do
  ExceptT $ checkUser db userId
  liftIO $ do
    DB.execute db "UPDATE users SET deleted = ? WHERE user_id = ?" (BI True, userId)
    map fromOnly <$> DB.query db "SELECT conn_id FROM connections WHERE user_id = ?" (Only userId)

deleteUserWithoutConns :: DB.Connection -> UserId -> IO Bool
deleteUserWithoutConns db userId = do
  userId_ :: Maybe Int64 <-
    maybeFirstRow fromOnly $
      DB.query
        db
        [sql|
          SELECT user_id FROM users u
          WHERE u.user_id = ?
            AND u.deleted = ?
            AND NOT EXISTS (SELECT c.conn_id FROM connections c WHERE c.user_id = u.user_id)
        |]
        (userId, BI True)
  case userId_ of
    Just _ -> DB.execute db "DELETE FROM users WHERE user_id = ?" (Only userId) $> True
    _ -> pure False

deleteUsersWithoutConns :: DB.Connection -> IO [Int64]
deleteUsersWithoutConns db = do
  userIds <-
    map fromOnly
      <$> DB.query
        db
        [sql|
          SELECT user_id FROM users u
          WHERE u.deleted = ?
            AND NOT EXISTS (SELECT c.conn_id FROM connections c WHERE c.user_id = u.user_id)
        |]
        (Only (BI True))
  forM_ userIds $ DB.execute db "DELETE FROM users WHERE user_id = ?" . Only
  pure userIds

createConn_ ::
  TVar ChaChaDRG ->
  ConnData ->
  (ConnId -> IO a) ->
  IO (Either StoreError (ConnId, a))
createConn_ gVar cData create = checkConstraint SEConnDuplicate $ case cData of
  ConnData {connId = ""} -> createWithRandomId' gVar create
  ConnData {connId} -> Right . (connId,) <$> create connId

createNewConn :: DB.Connection -> TVar ChaChaDRG -> ConnData -> SConnectionMode c -> IO (Either StoreError ConnId)
createNewConn db gVar cData cMode = do
  fst <$$> createConn_ gVar cData (\connId -> createConnRecord db connId cData cMode)

updateNewConnRcv :: DB.Connection -> ConnId -> NewRcvQueue -> IO (Either StoreError RcvQueue)
updateNewConnRcv db connId rq =
  getConn db connId $>>= \case
    (SomeConn _ NewConnection {}) -> updateConn
    (SomeConn _ RcvConnection {}) -> updateConn -- to allow retries
    (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
  where
    updateConn :: IO (Either StoreError RcvQueue)
    updateConn = Right <$> addConnRcvQueue_ db connId rq

updateNewConnSnd :: DB.Connection -> ConnId -> NewSndQueue -> IO (Either StoreError SndQueue)
updateNewConnSnd db connId sq =
  getConn db connId $>>= \case
    (SomeConn _ NewConnection {}) -> updateConn
    (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
  where
    updateConn :: IO (Either StoreError SndQueue)
    updateConn = Right <$> addConnSndQueue_ db connId sq

createSndConn :: DB.Connection -> TVar ChaChaDRG -> ConnData -> NewSndQueue -> IO (Either StoreError (ConnId, SndQueue))
createSndConn db gVar cData q@SndQueue {server} =
  -- check confirmed snd queue doesn't already exist, to prevent it being deleted by REPLACE in insertSndQueue_
  ifM (liftIO $ checkConfirmedSndQueueExists_ db q) (pure $ Left SESndQueueExists) $
    createConn_ gVar cData $ \connId -> do
      serverKeyHash_ <- createServer_ db server
      createConnRecord db connId cData SCMInvitation
      insertSndQueue_ db connId q serverKeyHash_

createConnRecord :: DB.Connection -> ConnId -> ConnData -> SConnectionMode c -> IO ()
createConnRecord db connId ConnData {userId, connAgentVersion, enableNtfs, pqSupport} cMode =
  DB.execute
    db
    [sql|
      INSERT INTO connections
        (user_id, conn_id, conn_mode, smp_agent_version, enable_ntfs, pq_support, duplex_handshake) VALUES (?,?,?,?,?,?,?)
    |]
    (userId, connId, cMode, connAgentVersion, BI enableNtfs, pqSupport, BI True)

checkConfirmedSndQueueExists_ :: DB.Connection -> NewSndQueue -> IO Bool
checkConfirmedSndQueueExists_ db SndQueue {server, sndId} = do
  fromMaybe False
    <$> maybeFirstRow
      fromOnly
      ( DB.query
          db
          "SELECT 1 FROM snd_queues WHERE host = ? AND port = ? AND snd_id = ? AND status != ? LIMIT 1"
          (host server, port server, sndId, New)
      )

getRcvConn :: DB.Connection -> SMPServer -> SMP.RecipientId -> IO (Either StoreError (RcvQueue, SomeConn))
getRcvConn db ProtocolServer {host, port} rcvId = runExceptT $ do
  rq@RcvQueue {connId} <-
    ExceptT . firstRow toRcvQueue SEConnNotFound $
      DB.query db (rcvQueueQuery <> " WHERE q.host = ? AND q.port = ? AND q.rcv_id = ? AND q.deleted = 0") (host, port, rcvId)
  (rq,) <$> ExceptT (getConn db connId)

-- | Deletes connection, optionally checking for pending snd message deliveries; returns connection id if it was deleted
deleteConn :: DB.Connection -> Maybe NominalDiffTime -> ConnId -> IO (Maybe ConnId)
deleteConn db waitDeliveryTimeout_ connId = case waitDeliveryTimeout_ of
  Nothing -> delete
  Just timeout ->
    ifM
      checkNoPendingDeliveries_
      delete
      ( ifM
          (checkWaitDeliveryTimeout_ timeout)
          delete
          (pure Nothing)
      )
  where
    delete = DB.execute db "DELETE FROM connections WHERE conn_id = ?" (Only connId) $> Just connId
    checkNoPendingDeliveries_ = do
      r :: (Maybe Int64) <-
        maybeFirstRow fromOnly $
          DB.query db "SELECT 1 FROM snd_message_deliveries WHERE conn_id = ? AND failed = 0 LIMIT 1" (Only connId)
      pure $ isNothing r
    checkWaitDeliveryTimeout_ timeout = do
      cutoffTs <- addUTCTime (-timeout) <$> getCurrentTime
      r :: (Maybe Int64) <-
        maybeFirstRow fromOnly $
          DB.query db "SELECT 1 FROM connections WHERE conn_id = ? AND deleted_at_wait_delivery < ? LIMIT 1" (connId, cutoffTs)
      pure $ isJust r

upgradeRcvConnToDuplex :: DB.Connection -> ConnId -> NewSndQueue -> IO (Either StoreError SndQueue)
upgradeRcvConnToDuplex db connId sq =
  getConn db connId $>>= \case
    (SomeConn _ RcvConnection {}) -> Right <$> addConnSndQueue_ db connId sq
    (SomeConn c _) -> pure . Left . SEBadConnType $ connType c

upgradeSndConnToDuplex :: DB.Connection -> ConnId -> NewRcvQueue -> IO (Either StoreError RcvQueue)
upgradeSndConnToDuplex db connId rq =
  getConn db connId >>= \case
    Right (SomeConn _ SndConnection {}) -> Right <$> addConnRcvQueue_ db connId rq
    Right (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
    _ -> pure $ Left SEConnNotFound

addConnRcvQueue :: DB.Connection -> ConnId -> NewRcvQueue -> IO (Either StoreError RcvQueue)
addConnRcvQueue db connId rq =
  getConn db connId >>= \case
    Right (SomeConn _ DuplexConnection {}) -> Right <$> addConnRcvQueue_ db connId rq
    Right (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
    _ -> pure $ Left SEConnNotFound

addConnRcvQueue_ :: DB.Connection -> ConnId -> NewRcvQueue -> IO RcvQueue
addConnRcvQueue_ db connId rq@RcvQueue {server} = do
  serverKeyHash_ <- createServer_ db server
  insertRcvQueue_ db connId rq serverKeyHash_

addConnSndQueue :: DB.Connection -> ConnId -> NewSndQueue -> IO (Either StoreError SndQueue)
addConnSndQueue db connId sq =
  getConn db connId >>= \case
    Right (SomeConn _ DuplexConnection {}) -> Right <$> addConnSndQueue_ db connId sq
    Right (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
    _ -> pure $ Left SEConnNotFound

addConnSndQueue_ :: DB.Connection -> ConnId -> NewSndQueue -> IO SndQueue
addConnSndQueue_ db connId sq@SndQueue {server} = do
  serverKeyHash_ <- createServer_ db server
  insertSndQueue_ db connId sq serverKeyHash_

setRcvQueueStatus :: DB.Connection -> RcvQueue -> QueueStatus -> IO ()
setRcvQueueStatus db RcvQueue {rcvId, server = ProtocolServer {host, port}} status =
  -- ? return error if queue does not exist?
  DB.execute
    db
    [sql|
      UPDATE rcv_queues
      SET status = ?
      WHERE host = ? AND port = ? AND rcv_id = ?
    |]
    (status, host, port, rcvId)

setRcvSwitchStatus :: DB.Connection -> RcvQueue -> Maybe RcvSwitchStatus -> IO RcvQueue
setRcvSwitchStatus db rq@RcvQueue {rcvId, server = ProtocolServer {host, port}} rcvSwchStatus = do
  DB.execute
    db
    [sql|
      UPDATE rcv_queues
      SET switch_status = ?
      WHERE host = ? AND port = ? AND rcv_id = ?
    |]
    (rcvSwchStatus, host, port, rcvId)
  pure rq {rcvSwchStatus}

setRcvQueueDeleted :: DB.Connection -> RcvQueue -> IO ()
setRcvQueueDeleted db RcvQueue {rcvId, server = ProtocolServer {host, port}} = do
  DB.execute
    db
    [sql|
      UPDATE rcv_queues
      SET deleted = 1
      WHERE host = ? AND port = ? AND rcv_id = ?
    |]
    (host, port, rcvId)

setRcvQueueConfirmedE2E :: DB.Connection -> RcvQueue -> C.DhSecretX25519 -> VersionSMPC -> IO ()
setRcvQueueConfirmedE2E db RcvQueue {rcvId, server = ProtocolServer {host, port}} e2eDhSecret smpClientVersion =
  DB.execute
    db
    [sql|
      UPDATE rcv_queues
      SET e2e_dh_secret = ?,
          status = ?,
          smp_client_version = ?
      WHERE host = ? AND port = ? AND rcv_id = ?
    |]
    (e2eDhSecret, Confirmed, smpClientVersion, host, port, rcvId)

setSndQueueStatus :: DB.Connection -> SndQueue -> QueueStatus -> IO ()
setSndQueueStatus db SndQueue {sndId, server = ProtocolServer {host, port}} status =
  -- ? return error if queue does not exist?
  DB.execute
    db
    [sql|
      UPDATE snd_queues
      SET status = ?
      WHERE host = ? AND port = ? AND snd_id = ?
    |]
    (status, host, port, sndId)

setSndSwitchStatus :: DB.Connection -> SndQueue -> Maybe SndSwitchStatus -> IO SndQueue
setSndSwitchStatus db sq@SndQueue {sndId, server = ProtocolServer {host, port}} sndSwchStatus = do
  DB.execute
    db
    [sql|
      UPDATE snd_queues
      SET switch_status = ?
      WHERE host = ? AND port = ? AND snd_id = ?
    |]
    (sndSwchStatus, host, port, sndId)
  pure sq {sndSwchStatus}

setRcvQueuePrimary :: DB.Connection -> ConnId -> RcvQueue -> IO ()
setRcvQueuePrimary db connId RcvQueue {dbQueueId} = do
  DB.execute db "UPDATE rcv_queues SET rcv_primary = ? WHERE conn_id = ?" (BI False, connId)
  DB.execute
    db
    "UPDATE rcv_queues SET rcv_primary = ?, replace_rcv_queue_id = ? WHERE conn_id = ? AND rcv_queue_id = ?"
    (BI True, Nothing :: Maybe Int64, connId, dbQueueId)

setSndQueuePrimary :: DB.Connection -> ConnId -> SndQueue -> IO ()
setSndQueuePrimary db connId SndQueue {dbQueueId} = do
  DB.execute db "UPDATE snd_queues SET snd_primary = ? WHERE conn_id = ?" (BI False, connId)
  DB.execute
    db
    "UPDATE snd_queues SET snd_primary = ?, replace_snd_queue_id = ? WHERE conn_id = ? AND snd_queue_id = ?"
    (BI True, Nothing :: Maybe Int64, connId, dbQueueId)

incRcvDeleteErrors :: DB.Connection -> RcvQueue -> IO ()
incRcvDeleteErrors db RcvQueue {connId, dbQueueId} =
  DB.execute db "UPDATE rcv_queues SET delete_errors = delete_errors + 1 WHERE conn_id = ? AND rcv_queue_id = ?" (connId, dbQueueId)

deleteConnRcvQueue :: DB.Connection -> RcvQueue -> IO ()
deleteConnRcvQueue db RcvQueue {connId, dbQueueId} =
  DB.execute db "DELETE FROM rcv_queues WHERE conn_id = ? AND rcv_queue_id = ?" (connId, dbQueueId)

deleteConnSndQueue :: DB.Connection -> ConnId -> SndQueue -> IO ()
deleteConnSndQueue db connId SndQueue {dbQueueId} = do
  DB.execute db "DELETE FROM snd_queues WHERE conn_id = ? AND snd_queue_id = ?" (connId, dbQueueId)
  DB.execute db "DELETE FROM snd_message_deliveries WHERE conn_id = ? AND snd_queue_id = ?" (connId, dbQueueId)

getPrimaryRcvQueue :: DB.Connection -> ConnId -> IO (Either StoreError RcvQueue)
getPrimaryRcvQueue db connId =
  maybe (Left SEConnNotFound) (Right . L.head) <$> getRcvQueuesByConnId_ db connId

getRcvQueue :: DB.Connection -> ConnId -> SMPServer -> SMP.RecipientId -> IO (Either StoreError RcvQueue)
getRcvQueue db connId (SMPServer host port _) rcvId =
  firstRow toRcvQueue SEConnNotFound $
    DB.query db (rcvQueueQuery <> " WHERE q.conn_id = ? AND q.host = ? AND q.port = ? AND q.rcv_id = ? AND q.deleted = 0") (connId, host, port, rcvId)

getDeletedRcvQueue :: DB.Connection -> ConnId -> SMPServer -> SMP.RecipientId -> IO (Either StoreError RcvQueue)
getDeletedRcvQueue db connId (SMPServer host port _) rcvId =
  firstRow toRcvQueue SEConnNotFound $
    DB.query db (rcvQueueQuery <> " WHERE q.conn_id = ? AND q.host = ? AND q.port = ? AND q.rcv_id = ? AND q.deleted = 1") (connId, host, port, rcvId)

setRcvQueueNtfCreds :: DB.Connection -> ConnId -> Maybe ClientNtfCreds -> IO ()
setRcvQueueNtfCreds db connId clientNtfCreds =
  DB.execute
    db
    [sql|
      UPDATE rcv_queues
      SET ntf_public_key = ?, ntf_private_key = ?, ntf_id = ?, rcv_ntf_dh_secret = ?
      WHERE conn_id = ?
    |]
    (ntfPublicKey_, ntfPrivateKey_, notifierId_, rcvNtfDhSecret_, connId)
  where
    (ntfPublicKey_, ntfPrivateKey_, notifierId_, rcvNtfDhSecret_) = case clientNtfCreds of
      Just ClientNtfCreds {ntfPublicKey, ntfPrivateKey, notifierId, rcvNtfDhSecret} -> (Just ntfPublicKey, Just ntfPrivateKey, Just notifierId, Just rcvNtfDhSecret)
      Nothing -> (Nothing, Nothing, Nothing, Nothing)

type SMPConfirmationRow = (Maybe SndPublicAuthKey, C.PublicKeyX25519, ConnInfo, Maybe [SMPQueueInfo], Maybe VersionSMPC)

smpConfirmation :: SMPConfirmationRow -> SMPConfirmation
smpConfirmation (senderKey, e2ePubKey, connInfo, smpReplyQueues_, smpClientVersion_) =
  SMPConfirmation
    { senderKey,
      e2ePubKey,
      connInfo,
      smpReplyQueues = fromMaybe [] smpReplyQueues_,
      smpClientVersion = fromMaybe initialSMPClientVersion smpClientVersion_
    }

createConfirmation :: DB.Connection -> TVar ChaChaDRG -> NewConfirmation -> IO (Either StoreError ConfirmationId)
createConfirmation db gVar NewConfirmation {connId, senderConf = SMPConfirmation {senderKey, e2ePubKey, connInfo, smpReplyQueues, smpClientVersion}, ratchetState} =
  createWithRandomId gVar $ \confirmationId ->
    DB.execute
      db
      [sql|
        INSERT INTO conn_confirmations
        (confirmation_id, conn_id, sender_key, e2e_snd_pub_key, ratchet_state, sender_conn_info, smp_reply_queues, smp_client_version, accepted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0);
      |]
      (Binary confirmationId, connId, senderKey, e2ePubKey, ratchetState, Binary connInfo, smpReplyQueues, smpClientVersion)

acceptConfirmation :: DB.Connection -> ConfirmationId -> ConnInfo -> IO (Either StoreError AcceptedConfirmation)
acceptConfirmation db confirmationId ownConnInfo = do
  DB.execute
    db
    [sql|
      UPDATE conn_confirmations
      SET accepted = 1,
          own_conn_info = ?
      WHERE confirmation_id = ?
    |]
    (Binary ownConnInfo, Binary confirmationId)
  firstRow confirmation SEConfirmationNotFound $
    DB.query
      db
      [sql|
        SELECT conn_id, ratchet_state, sender_key, e2e_snd_pub_key, sender_conn_info, smp_reply_queues, smp_client_version
        FROM conn_confirmations
        WHERE confirmation_id = ?;
      |]
      (Only (Binary confirmationId))
  where
    confirmation ((connId, ratchetState) :. confRow) =
      AcceptedConfirmation
        { confirmationId,
          connId,
          senderConf = smpConfirmation confRow,
          ratchetState,
          ownConnInfo
        }

getAcceptedConfirmation :: DB.Connection -> ConnId -> IO (Either StoreError AcceptedConfirmation)
getAcceptedConfirmation db connId =
  firstRow confirmation SEConfirmationNotFound $
    DB.query
      db
      [sql|
        SELECT confirmation_id, ratchet_state, own_conn_info, sender_key, e2e_snd_pub_key, sender_conn_info, smp_reply_queues, smp_client_version
        FROM conn_confirmations
        WHERE conn_id = ? AND accepted = 1;
      |]
      (Only connId)
  where
    confirmation ((confirmationId, ratchetState, ownConnInfo) :. confRow) =
      AcceptedConfirmation
        { confirmationId,
          connId,
          senderConf = smpConfirmation confRow,
          ratchetState,
          ownConnInfo
        }

removeConfirmations :: DB.Connection -> ConnId -> IO ()
removeConfirmations db connId =
  DB.execute
    db
    [sql|
      DELETE FROM conn_confirmations
      WHERE conn_id = ?
    |]
    (Only connId)

createInvitation :: DB.Connection -> TVar ChaChaDRG -> NewInvitation -> IO (Either StoreError InvitationId)
createInvitation db gVar NewInvitation {contactConnId, connReq, recipientConnInfo} =
  createWithRandomId gVar $ \invitationId ->
    DB.execute
      db
      [sql|
        INSERT INTO conn_invitations
        (invitation_id,  contact_conn_id, cr_invitation, recipient_conn_info, accepted) VALUES (?, ?, ?, ?, 0);
      |]
      (Binary invitationId, contactConnId, connReq, Binary recipientConnInfo)

getInvitation :: DB.Connection -> String -> InvitationId -> IO (Either StoreError Invitation)
getInvitation db cxt invitationId =
  firstRow invitation (SEInvitationNotFound cxt invitationId) $
    DB.query
      db
      [sql|
        SELECT contact_conn_id, cr_invitation, recipient_conn_info, own_conn_info, accepted
        FROM conn_invitations
        WHERE invitation_id = ?
          AND accepted = 0
      |]
      (Only (Binary invitationId))
  where
    invitation (contactConnId, connReq, recipientConnInfo, ownConnInfo, BI accepted) =
      Invitation {invitationId, contactConnId, connReq, recipientConnInfo, ownConnInfo, accepted}

acceptInvitation :: DB.Connection -> InvitationId -> ConnInfo -> IO ()
acceptInvitation db invitationId ownConnInfo =
  DB.execute
    db
    [sql|
      UPDATE conn_invitations
      SET accepted = 1,
          own_conn_info = ?
      WHERE invitation_id = ?
    |]
    (Binary ownConnInfo, Binary invitationId)

unacceptInvitation :: DB.Connection -> InvitationId -> IO ()
unacceptInvitation db invitationId =
  DB.execute db "UPDATE conn_invitations SET accepted = 0, own_conn_info = NULL WHERE invitation_id = ?" (Only (Binary invitationId))

deleteInvitation :: DB.Connection -> ConnId -> InvitationId -> IO (Either StoreError ())
deleteInvitation db contactConnId invId =
  getConn db contactConnId $>>= \case
    SomeConn SCContact _ ->
      Right <$> DB.execute db "DELETE FROM conn_invitations WHERE contact_conn_id = ? AND invitation_id = ?" (contactConnId, Binary invId)
    _ -> pure $ Left SEConnNotFound

updateRcvIds :: DB.Connection -> ConnId -> IO (InternalId, InternalRcvId, PrevExternalSndId, PrevRcvMsgHash)
updateRcvIds db connId = do
  (lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash) <- retrieveLastIdsAndHashRcv_ db connId
  let internalId = InternalId $ unId lastInternalId + 1
      internalRcvId = InternalRcvId $ unRcvId lastInternalRcvId + 1
  updateLastIdsRcv_ db connId internalId internalRcvId
  pure (internalId, internalRcvId, lastExternalSndId, lastRcvHash)

createRcvMsg :: DB.Connection -> ConnId -> RcvQueue -> RcvMsgData -> IO ()
createRcvMsg db connId rq@RcvQueue {dbQueueId} rcvMsgData@RcvMsgData {msgMeta = MsgMeta {sndMsgId, broker = (_, brokerTs)}, internalRcvId, internalHash} = do
  insertRcvMsgBase_ db connId rcvMsgData
  insertRcvMsgDetails_ db connId rq rcvMsgData
  updateRcvMsgHash db connId sndMsgId internalRcvId internalHash
  DB.execute db "UPDATE rcv_queues SET last_broker_ts = ? WHERE conn_id = ? AND rcv_queue_id = ?" (brokerTs, connId, dbQueueId)

createSndMsgBody :: DB.Connection -> AMessage -> IO Int64
createSndMsgBody db aMessage =
  fromOnly . head <$>
    DB.query
      db
      "INSERT INTO snd_message_bodies (agent_msg) VALUES (?) RETURNING snd_message_body_id"
      (Only aMessage)

updateSndIds :: DB.Connection -> ConnId -> IO (Either StoreError (InternalId, InternalSndId, PrevSndMsgHash))
updateSndIds db connId = runExceptT $ do
  (lastInternalId, lastInternalSndId, prevSndHash) <- ExceptT $ retrieveLastIdsAndHashSnd_ db connId
  let internalId = InternalId $ unId lastInternalId + 1
      internalSndId = InternalSndId $ unSndId lastInternalSndId + 1
  liftIO $ updateLastIdsSnd_ db connId internalId internalSndId
  pure (internalId, internalSndId, prevSndHash)

createSndMsg :: DB.Connection -> ConnId -> SndMsgData -> IO ()
createSndMsg db connId sndMsgData@SndMsgData {internalSndId, internalHash} = do
  insertSndMsgBase_ db connId sndMsgData
  insertSndMsgDetails_ db connId sndMsgData
  updateSndMsgHash db connId internalSndId internalHash

createSndMsgDelivery :: DB.Connection -> ConnId -> SndQueue -> InternalId -> IO ()
createSndMsgDelivery db connId SndQueue {dbQueueId} msgId =
  DB.execute db "INSERT INTO snd_message_deliveries (conn_id, snd_queue_id, internal_id) VALUES (?, ?, ?)" (connId, dbQueueId, msgId)

getSndMsgViaRcpt :: DB.Connection -> ConnId -> InternalSndId -> IO (Either StoreError SndMsg)
getSndMsgViaRcpt db connId sndMsgId =
  firstRow toSndMsg SEMsgNotFound $
    DB.query
      db
      [sql|
        SELECT s.internal_id, m.msg_type, s.internal_hash, s.rcpt_internal_id, s.rcpt_status
        FROM snd_messages s
        JOIN messages m ON s.conn_id = m.conn_id AND s.internal_id = m.internal_id
        WHERE s.conn_id = ? AND s.internal_snd_id = ?
      |]
      (connId, sndMsgId)
  where
    toSndMsg :: (InternalId, AgentMessageType, MsgHash, Maybe AgentMsgId, Maybe MsgReceiptStatus) -> SndMsg
    toSndMsg (internalId, msgType, internalHash, rcptInternalId_, rcptStatus_) =
      let msgReceipt = MsgReceipt <$> rcptInternalId_ <*> rcptStatus_
       in SndMsg {internalId, internalSndId = sndMsgId, msgType, internalHash, msgReceipt}

updateSndMsgRcpt :: DB.Connection -> ConnId -> InternalSndId -> MsgReceipt -> IO ()
updateSndMsgRcpt db connId sndMsgId MsgReceipt {agentMsgId, msgRcptStatus} =
  DB.execute
    db
    "UPDATE snd_messages SET rcpt_internal_id = ?, rcpt_status = ? WHERE conn_id = ? AND internal_snd_id = ?"
    (agentMsgId, msgRcptStatus, connId, sndMsgId)

getConnectionsForDelivery :: DB.Connection -> IO [ConnId]
getConnectionsForDelivery db =
  map fromOnly <$> DB.query_ db "SELECT DISTINCT conn_id FROM snd_message_deliveries WHERE failed = 0"

getPendingQueueMsg :: DB.Connection -> ConnId -> SndQueue -> IO (Either StoreError (Maybe (Maybe RcvQueue, PendingMsgData)))
getPendingQueueMsg db connId SndQueue {dbQueueId} =
  getWorkItem "message" getMsgId getMsgData markMsgFailed
  where
    getMsgId :: IO (Maybe InternalId)
    getMsgId =
      maybeFirstRow fromOnly $
        DB.query
          db
          [sql|
            SELECT internal_id
            FROM snd_message_deliveries d
            WHERE conn_id = ? AND snd_queue_id = ? AND failed = 0
            ORDER BY internal_id ASC
            LIMIT 1
          |]
          (connId, dbQueueId)
    getMsgData :: InternalId -> IO (Either StoreError (Maybe RcvQueue, PendingMsgData))
    getMsgData msgId = runExceptT $ do
      msg <- ExceptT $ firstRow' pendingMsgData err getMsgData_
      rq_ <- liftIO $ L.head <$$> getRcvQueuesByConnId_ db connId
      pure (rq_, msg)
      where
        getMsgData_ =
          DB.query
            db
            [sql|
              SELECT
                m.msg_type, m.msg_flags, m.msg_body, m.pq_encryption, m.internal_ts, m.internal_snd_id, s.previous_msg_hash,
                s.retry_int_slow, s.retry_int_fast, s.msg_encrypt_key, s.padded_msg_len, sb.agent_msg
              FROM messages m
              JOIN snd_messages s ON s.conn_id = m.conn_id AND s.internal_id = m.internal_id
              LEFT JOIN snd_message_bodies sb ON sb.snd_message_body_id = s.snd_message_body_id
              WHERE m.conn_id = ? AND m.internal_id = ?
            |]
            (connId, msgId)
        err = SEInternal $ "msg delivery " <> bshow msgId <> " returned []"
        pendingMsgData :: (AgentMessageType, Maybe MsgFlags, MsgBody, PQEncryption, InternalTs, InternalSndId, PrevSndMsgHash, Maybe Int64, Maybe Int64, Maybe CR.MsgEncryptKeyX448, Maybe Int, Maybe AMessage) -> Either StoreError PendingMsgData
        pendingMsgData (msgType, msgFlags_, msgBody, pqEncryption, internalTs, internalSndId, prevMsgHash, riSlow_, riFast_, encryptKey_, paddedLen_, sndMsgBody_) = do
          let msgFlags = fromMaybe SMP.noMsgFlags msgFlags_
              msgRetryState = RI2State <$> riSlow_ <*> riFast_
              result pendingMsgPrepData_ = PendingMsgData {msgId, msgType, msgFlags, msgBody, pqEncryption, msgRetryState, internalTs, internalSndId, prevMsgHash, pendingMsgPrepData_}
           in result <$> case (encryptKey_, paddedLen_, sndMsgBody_) of
                (Nothing, Nothing, Nothing) -> Right Nothing
                (Just encryptKey, Just paddedLen, Just sndMsgBody) -> Right $ Just PendingMsgPrepData {encryptKey, paddedLen, sndMsgBody}
                _ -> Left $ SEInternal "unexpected snd msg data"
    markMsgFailed msgId = DB.execute db "UPDATE snd_message_deliveries SET failed = 1 WHERE conn_id = ? AND internal_id = ?" (connId, msgId)

getWorkItem :: Show i => ByteString -> IO (Maybe i) -> (i -> IO (Either StoreError a)) -> (i -> IO ()) -> IO (Either StoreError (Maybe a))
getWorkItem itemName getId getItem markFailed =
  runExceptT $ handleWrkErr itemName "getId" getId >>= mapM (tryGetItem itemName getItem markFailed)

getWorkItems :: Show i => ByteString -> IO [i] -> (i -> IO (Either StoreError a)) -> (i -> IO ()) -> IO (Either StoreError [Either StoreError a])
getWorkItems itemName getIds getItem markFailed =
  runExceptT $ handleWrkErr itemName "getIds" getIds >>= mapM (tryE . tryGetItem itemName getItem markFailed)

tryGetItem :: Show i => ByteString -> (i -> IO (Either StoreError a)) -> (i -> IO ()) -> i -> ExceptT StoreError IO a
tryGetItem itemName getItem markFailed itemId = ExceptT (getItem itemId) `catchStoreError` \e -> mark >> throwE e
  where
    mark = handleWrkErr itemName ("markFailed ID " <> bshow itemId) $ markFailed itemId

catchStoreError :: ExceptT StoreError IO a -> (StoreError -> ExceptT StoreError IO a) -> ExceptT StoreError IO a
catchStoreError = catchAllErrors (SEInternal . bshow)

-- Errors caught by this function will suspend worker as if there is no more work,
handleWrkErr :: ByteString -> ByteString -> IO a -> ExceptT StoreError IO a
handleWrkErr itemName opName action = ExceptT $ first mkError <$> E.try action
  where
    mkError :: E.SomeException -> StoreError
    mkError e = SEWorkItemError $ itemName <> " " <> opName <> " error: " <> bshow e

updatePendingMsgRIState :: DB.Connection -> ConnId -> InternalId -> RI2State -> IO ()
updatePendingMsgRIState db connId msgId RI2State {slowInterval, fastInterval} =
  DB.execute db "UPDATE snd_messages SET retry_int_slow = ?, retry_int_fast = ? WHERE conn_id = ? AND internal_id = ?" (slowInterval, fastInterval, connId, msgId)

deletePendingMsgs :: DB.Connection -> ConnId -> SndQueue -> IO ()
deletePendingMsgs db connId SndQueue {dbQueueId} =
  DB.execute db "DELETE FROM snd_message_deliveries WHERE conn_id = ? AND snd_queue_id = ?" (connId, dbQueueId)

getExpiredSndMessages :: DB.Connection -> ConnId -> SndQueue -> UTCTime -> IO [InternalId]
getExpiredSndMessages db connId SndQueue {dbQueueId} expireTs = do
  -- type is Maybe InternalId because MAX always returns one row, possibly with NULL value
  maxId :: [Maybe InternalId] <-
    map fromOnly
      <$> DB.query
        db
        [sql|
          SELECT MAX(internal_id)
          FROM messages
          WHERE conn_id = ? AND internal_snd_id IS NOT NULL AND internal_ts < ?
        |]
        (connId, expireTs)
  case maxId of
    Just msgId : _ ->
      map fromOnly
        <$> DB.query
          db
          [sql|
            SELECT internal_id
            FROM snd_message_deliveries
            WHERE conn_id = ? AND snd_queue_id = ? AND failed = 0 AND internal_id <= ?
            ORDER BY internal_id ASC
          |]
          (connId, dbQueueId, msgId)
    _ -> pure []

setMsgUserAck :: DB.Connection -> ConnId -> InternalId -> IO (Either StoreError (RcvQueue, SMP.MsgId))
setMsgUserAck db connId agentMsgId = runExceptT $ do
  (dbRcvId, srvMsgId) <-
    ExceptT . firstRow id SEMsgNotFound $
      DB.query db "SELECT rcv_queue_id, broker_id FROM rcv_messages WHERE conn_id = ? AND internal_id = ?" (connId, agentMsgId)
  rq <- ExceptT $ getRcvQueueById db connId dbRcvId
  liftIO $ DB.execute db "UPDATE rcv_messages SET user_ack = ? WHERE conn_id = ? AND internal_id = ?" (BI True, connId, agentMsgId)
  pure (rq, srvMsgId)

getRcvMsg :: DB.Connection -> ConnId -> InternalId -> IO (Either StoreError RcvMsg)
getRcvMsg db connId agentMsgId =
  firstRow toRcvMsg SEMsgNotFound $
    DB.query
      db
      [sql|
        SELECT
          r.internal_id, m.internal_ts, r.broker_id, r.broker_ts, r.external_snd_id, r.integrity, r.internal_hash,
          m.msg_type, m.msg_body, m.pq_encryption, s.internal_id, s.rcpt_status, r.user_ack
        FROM rcv_messages r
        JOIN messages m ON r.conn_id = m.conn_id AND r.internal_id = m.internal_id
        LEFT JOIN snd_messages s ON s.conn_id = r.conn_id AND s.rcpt_internal_id = r.internal_id
        WHERE r.conn_id = ? AND r.internal_id = ?
      |]
      (connId, agentMsgId)

getLastMsg :: DB.Connection -> ConnId -> SMP.MsgId -> IO (Maybe RcvMsg)
getLastMsg db connId msgId =
  maybeFirstRow toRcvMsg $
    DB.query
      db
      [sql|
        SELECT
          r.internal_id, m.internal_ts, r.broker_id, r.broker_ts, r.external_snd_id, r.integrity, r.internal_hash,
          m.msg_type, m.msg_body, m.pq_encryption, s.internal_id, s.rcpt_status, r.user_ack
        FROM rcv_messages r
        JOIN messages m ON r.conn_id = m.conn_id AND r.internal_id = m.internal_id
        JOIN connections c ON r.conn_id = c.conn_id AND c.last_internal_msg_id = r.internal_id
        LEFT JOIN snd_messages s ON s.conn_id = r.conn_id AND s.rcpt_internal_id = r.internal_id
        WHERE r.conn_id = ? AND r.broker_id = ?
      |]
      (connId, Binary msgId)

toRcvMsg :: (Int64, InternalTs, BrokerId, BrokerTs) :. (AgentMsgId, MsgIntegrity, MsgHash, AgentMessageType, MsgBody, PQEncryption, Maybe AgentMsgId, Maybe MsgReceiptStatus, BoolInt) -> RcvMsg
toRcvMsg ((agentMsgId, internalTs, brokerId, brokerTs) :. (sndMsgId, integrity, internalHash, msgType, msgBody, pqEncryption, rcptInternalId_, rcptStatus_, BI userAck)) =
  let msgMeta = MsgMeta {recipient = (agentMsgId, internalTs), broker = (brokerId, brokerTs), sndMsgId, integrity, pqEncryption}
      msgReceipt = MsgReceipt <$> rcptInternalId_ <*> rcptStatus_
   in RcvMsg {internalId = InternalId agentMsgId, msgMeta, msgType, msgBody, internalHash, msgReceipt, userAck}

checkRcvMsgHashExists :: DB.Connection -> ConnId -> ByteString -> IO Bool
checkRcvMsgHashExists db connId hash = do
  fromMaybe False
    <$> maybeFirstRow
      fromOnly
      ( DB.query
          db
          "SELECT 1 FROM encrypted_rcv_message_hashes WHERE conn_id = ? AND hash = ? LIMIT 1"
          (connId, Binary hash)
      )

getRcvMsgBrokerTs :: DB.Connection -> ConnId -> SMP.MsgId -> IO (Either StoreError BrokerTs)
getRcvMsgBrokerTs db connId msgId =
  firstRow fromOnly SEMsgNotFound $
    DB.query db "SELECT broker_ts FROM rcv_messages WHERE conn_id = ? AND broker_id = ?" (connId, Binary msgId)

deleteMsg :: DB.Connection -> ConnId -> InternalId -> IO ()
deleteMsg db connId msgId =
  DB.execute db "DELETE FROM messages WHERE conn_id = ? AND internal_id = ?;" (connId, msgId)

deleteMsgContent :: DB.Connection -> ConnId -> InternalId -> IO ()
deleteMsgContent db connId msgId = do
#if defined(dbPostgres)
  DB.execute db "UPDATE messages SET msg_body = ''::BYTEA WHERE conn_id = ? AND internal_id = ?" (connId, msgId)
#else
  DB.execute db "UPDATE messages SET msg_body = x'' WHERE conn_id = ? AND internal_id = ?" (connId, msgId)
#endif
  DB.execute db "UPDATE snd_messages SET snd_message_body_id = NULL WHERE conn_id = ? AND internal_id = ?" (connId, msgId)

deleteDeliveredSndMsg :: DB.Connection -> ConnId -> InternalId -> IO ()
deleteDeliveredSndMsg db connId msgId = do
  cnt <- countPendingSndDeliveries_ db connId msgId
  when (cnt == 0) $ deleteMsg db connId msgId

deleteSndMsgDelivery :: DB.Connection -> ConnId -> SndQueue -> InternalId -> Bool -> IO ()
deleteSndMsgDelivery db connId SndQueue {dbQueueId} msgId keepForReceipt = do
  DB.execute
    db
    "DELETE FROM snd_message_deliveries WHERE conn_id = ? AND snd_queue_id = ? AND internal_id = ?"
    (connId, dbQueueId, msgId)
  getRcptAndBodyId >>= mapM_ deleteMsgAndBody
  where
    getRcptAndBodyId :: IO (Maybe (Maybe MsgReceiptStatus, Maybe Int64))
    getRcptAndBodyId =
      -- Get receipt status and message body ID if there are no pending deliveries.
      -- The current delivery is deleted above.
      maybeFirstRow id $
        DB.query
          db
          [sql|
            SELECT rcpt_status, snd_message_body_id FROM snd_messages
            WHERE NOT EXISTS (SELECT 1 FROM snd_message_deliveries WHERE conn_id = ? AND internal_id = ? AND failed = 0)
              AND conn_id = ? AND internal_id = ?
          |]
          (connId, msgId, connId, msgId)
    deleteMsgAndBody :: (Maybe MsgReceiptStatus, Maybe Int64) -> IO ()
    deleteMsgAndBody (rcptStatus_, sndMsgBodyId_) = do
      let del = case rcptStatus_ of
            -- we are not deleting message if receipt is not received or had incorrect hash (for debugging).
            Just MROk -> deleteMsg
            _ -> if keepForReceipt then deleteMsgContent else deleteMsg
      del db connId msgId
      forM_ sndMsgBodyId_ $ \bodyId ->
        -- Delete message body if it is not used by any snd message.
        -- The current snd message is already deleted by deleteMsg or cleared by deleteMsgContent.
        DB.execute
          db
          [sql|
            DELETE FROM snd_message_bodies
            WHERE NOT EXISTS (SELECT 1 FROM snd_messages WHERE snd_message_body_id = ?)
              AND snd_message_body_id = ?
          |]
          (bodyId, bodyId)

countPendingSndDeliveries_ :: DB.Connection -> ConnId -> InternalId -> IO Int
countPendingSndDeliveries_ db connId msgId = do
  (Only cnt : _) <- DB.query db "SELECT count(*) FROM snd_message_deliveries WHERE conn_id = ? AND internal_id = ? AND failed = 0" (connId, msgId)
  pure cnt

deleteRcvMsgHashesExpired :: DB.Connection -> NominalDiffTime -> IO ()
deleteRcvMsgHashesExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.execute db "DELETE FROM encrypted_rcv_message_hashes WHERE created_at < ?" (Only cutoffTs)

deleteSndMsgsExpired :: DB.Connection -> NominalDiffTime -> IO ()
deleteSndMsgsExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.execute
    db
    "DELETE FROM messages WHERE internal_ts < ? AND internal_snd_id IS NOT NULL"
    (Only cutoffTs)

createRatchetX3dhKeys :: DB.Connection -> ConnId -> C.PrivateKeyX448 -> C.PrivateKeyX448 -> Maybe CR.RcvPrivRKEMParams -> IO ()
createRatchetX3dhKeys db connId x3dhPrivKey1 x3dhPrivKey2 pqPrivKem =
  DB.execute db "INSERT INTO ratchets (conn_id, x3dh_priv_key_1, x3dh_priv_key_2, pq_priv_kem) VALUES (?, ?, ?, ?)" (connId, x3dhPrivKey1, x3dhPrivKey2, pqPrivKem)

getRatchetX3dhKeys :: DB.Connection -> ConnId -> IO (Either StoreError (C.PrivateKeyX448, C.PrivateKeyX448, Maybe CR.RcvPrivRKEMParams))
getRatchetX3dhKeys db connId =
  firstRow' keys SEX3dhKeysNotFound $
    DB.query db "SELECT x3dh_priv_key_1, x3dh_priv_key_2, pq_priv_kem FROM ratchets WHERE conn_id = ?" (Only connId)
  where
    keys = \case
      (Just k1, Just k2, pKem) -> Right (k1, k2, pKem)
      _ -> Left SEX3dhKeysNotFound

-- used to remember new keys when starting ratchet re-synchronization
setRatchetX3dhKeys :: DB.Connection -> ConnId -> C.PrivateKeyX448 -> C.PrivateKeyX448 -> Maybe CR.RcvPrivRKEMParams -> IO ()
setRatchetX3dhKeys db connId x3dhPrivKey1 x3dhPrivKey2 pqPrivKem =
  DB.execute
    db
    [sql|
      UPDATE ratchets
      SET x3dh_priv_key_1 = ?, x3dh_priv_key_2 = ?, pq_priv_kem = ?
      WHERE conn_id = ?
    |]
    (x3dhPrivKey1, x3dhPrivKey2, pqPrivKem, connId)

createSndRatchet :: DB.Connection -> ConnId -> RatchetX448 -> CR.AE2ERatchetParams 'C.X448 -> IO ()
createSndRatchet db connId ratchetState (CR.AE2ERatchetParams s (CR.E2ERatchetParams _ x3dhPubKey1 x3dhPubKey2 pqPubKem)) =
  DB.execute
    db
    [sql|
      INSERT INTO ratchets
        (conn_id, ratchet_state, x3dh_pub_key_1, x3dh_pub_key_2, pq_pub_kem) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (conn_id) DO UPDATE SET
        ratchet_state = EXCLUDED.ratchet_state,
        x3dh_priv_key_1 = NULL,
        x3dh_priv_key_2 = NULL,
        x3dh_pub_key_1 = EXCLUDED.x3dh_pub_key_1,
        x3dh_pub_key_2 = EXCLUDED.x3dh_pub_key_2,
        pq_priv_kem = NULL,
        pq_pub_kem = EXCLUDED.pq_pub_kem
    |]
    (connId, ratchetState, x3dhPubKey1, x3dhPubKey2, CR.ARKP s <$> pqPubKem)

getSndRatchet :: DB.Connection -> ConnId -> CR.VersionE2E -> IO (Either StoreError (RatchetX448, CR.AE2ERatchetParams 'C.X448))
getSndRatchet db connId v =
  firstRow' result SEX3dhKeysNotFound $
    DB.query db "SELECT ratchet_state, x3dh_pub_key_1, x3dh_pub_key_2, pq_pub_kem FROM ratchets WHERE conn_id = ?" (Only connId)
  where
    result = \case
      (Just ratchetState, Just k1, Just k2, pKem_) -> 
        let params = case pKem_ of
              Nothing -> CR.AE2ERatchetParams CR.SRKSProposed (CR.E2ERatchetParams v k1 k2 Nothing)
              Just (CR.ARKP s pKem) -> CR.AE2ERatchetParams s (CR.E2ERatchetParams v k1 k2 (Just pKem))
         in Right (ratchetState, params)
      _ -> Left SEX3dhKeysNotFound

-- TODO remove the columns for public keys in v5.7.
createRatchet :: DB.Connection -> ConnId -> RatchetX448 -> IO ()
createRatchet db connId rc =
  DB.execute
    db
    [sql|
      INSERT INTO ratchets (conn_id, ratchet_state)
      VALUES (?, ?)
      ON CONFLICT (conn_id) DO UPDATE SET
        ratchet_state = ?,
        x3dh_priv_key_1 = NULL,
        x3dh_priv_key_2 = NULL,
        x3dh_pub_key_1 = NULL,
        x3dh_pub_key_2 = NULL,
        pq_priv_kem = NULL,
        pq_pub_kem = NULL
    |]
    (connId, rc, rc)

deleteRatchet :: DB.Connection -> ConnId -> IO ()
deleteRatchet db connId =
  DB.execute db "DELETE FROM ratchets WHERE conn_id = ?" (Only connId)

getRatchet :: DB.Connection -> ConnId -> IO (Either StoreError RatchetX448)
getRatchet db connId =
  firstRow' ratchet SERatchetNotFound $ DB.query db "SELECT ratchet_state FROM ratchets WHERE conn_id = ?" (Only connId)
  where
    ratchet = maybe (Left SERatchetNotFound) Right . fromOnly

getSkippedMsgKeys :: DB.Connection -> ConnId -> IO SkippedMsgKeys
getSkippedMsgKeys db connId =
  skipped <$> DB.query db "SELECT header_key, msg_n, msg_key FROM skipped_messages WHERE conn_id = ?" (Only connId)
  where
    skipped = foldl' addSkippedKey M.empty
    addSkippedKey smks (hk, msgN, mk) = M.alter (Just . addMsgKey) hk smks
      where
        addMsgKey = maybe (M.singleton msgN mk) (M.insert msgN mk)

updateRatchet :: DB.Connection -> ConnId -> RatchetX448 -> SkippedMsgDiff -> IO ()
updateRatchet db connId rc skipped = do
  DB.execute db "UPDATE ratchets SET ratchet_state = ? WHERE conn_id = ?" (rc, connId)
  case skipped of
    SMDNoChange -> pure ()
    SMDRemove hk msgN ->
      DB.execute db "DELETE FROM skipped_messages WHERE conn_id = ? AND header_key = ? AND msg_n = ?" (connId, hk, msgN)
    SMDAdd smks ->
      forM_ (M.assocs smks) $ \(hk, mks) ->
        forM_ (M.assocs mks) $ \(msgN, mk) ->
          DB.execute db "INSERT INTO skipped_messages (conn_id, header_key, msg_n, msg_key) VALUES (?, ?, ?, ?)" (connId, hk, msgN, mk)

createCommand :: DB.Connection -> ACorrId -> ConnId -> Maybe SMPServer -> AgentCommand -> IO (Either StoreError ())
createCommand db corrId connId srv_ cmd = runExceptT $ do
  (host_, port_, serverKeyHash_) <- serverFields
  createdAt <- liftIO getCurrentTime
  liftIO . E.handle handleErr $
    DB.execute
      db
      "INSERT INTO commands (host, port, corr_id, conn_id, command_tag, command, server_key_hash, created_at) VALUES (?,?,?,?,?,?,?,?)"
      (host_, port_, Binary corrId, connId, cmdTag, cmd, serverKeyHash_, createdAt)
  where
    cmdTag = agentCommandTag cmd
#if defined(dbPostgres)
    handleErr e = case constraintViolation e of
      Just _ -> logError $ "tried to create command " <> tshow cmdTag <> " for deleted connection"
      Nothing -> E.throwIO e
#else
    handleErr e
      | SQL.sqlError e == SQL.ErrorConstraint = logError $ "tried to create command " <> tshow cmdTag <> " for deleted connection"
      | otherwise = E.throwIO e
#endif
    serverFields :: ExceptT StoreError IO (Maybe (NonEmpty TransportHost), Maybe ServiceName, Maybe C.KeyHash)
    serverFields = case srv_ of
      Just srv@(SMPServer host port _) ->
        (Just host,Just port,) <$> ExceptT (getServerKeyHash_ db srv)
      Nothing -> pure (Nothing, Nothing, Nothing)

insertedRowId :: DB.Connection -> IO Int64
insertedRowId db = fromOnly . head <$> DB.query_ db q
  where
#if defined(dbPostgres)
    q = "SELECT lastval()"
#else
    q = "SELECT last_insert_rowid()"
#endif

getPendingCommandServers :: DB.Connection -> ConnId -> IO [Maybe SMPServer]
getPendingCommandServers db connId = do
  -- TODO review whether this can break if, e.g., the server has another key hash.
  map smpServer
    <$> DB.query
      db
      [sql|
        SELECT DISTINCT c.host, c.port, COALESCE(c.server_key_hash, s.key_hash)
        FROM commands c
        LEFT JOIN servers s ON s.host = c.host AND s.port = c.port
        WHERE conn_id = ?
      |]
      (Only connId)
  where
    smpServer (host, port, keyHash) = SMPServer <$> host <*> port <*> keyHash

getPendingServerCommand :: DB.Connection -> ConnId -> Maybe SMPServer -> IO (Either StoreError (Maybe PendingCommand))
getPendingServerCommand db connId srv_ = getWorkItem "command" getCmdId getCommand markCommandFailed
  where
    getCmdId :: IO (Maybe Int64)
    getCmdId =
      maybeFirstRow fromOnly $ case srv_ of
        Nothing ->
          DB.query
            db
            [sql|
              SELECT command_id FROM commands
              WHERE conn_id = ? AND host IS NULL AND port IS NULL AND failed = 0
              ORDER BY created_at ASC, command_id ASC
              LIMIT 1
            |]
            (Only connId)
        Just (SMPServer host port _) ->
          DB.query
            db
            [sql|
              SELECT command_id FROM commands
              WHERE conn_id = ? AND host = ? AND port = ? AND failed = 0
              ORDER BY created_at ASC, command_id ASC
              LIMIT 1
            |]
            (connId, host, port)
    getCommand :: Int64 -> IO (Either StoreError PendingCommand)
    getCommand cmdId =
      firstRow pendingCommand err $
        DB.query
          db
          [sql|
            SELECT c.corr_id, cs.user_id, c.command
            FROM commands c
            JOIN connections cs USING (conn_id)
            WHERE c.command_id = ?
          |]
          (Only cmdId)
      where
        err = SEInternal $ "command  " <> bshow cmdId <> " returned []"
        pendingCommand (corrId, userId, command) = PendingCommand {cmdId, corrId, userId, connId, command}
    markCommandFailed cmdId = DB.execute db "UPDATE commands SET failed = 1 WHERE command_id = ?" (Only cmdId)

updateCommandServer :: DB.Connection -> AsyncCmdId -> SMPServer -> IO (Either StoreError ())
updateCommandServer db cmdId srv@(SMPServer host port _) = runExceptT $ do
  serverKeyHash_ <- ExceptT $ getServerKeyHash_ db srv
  liftIO $
    DB.execute
      db
      [sql|
        UPDATE commands
        SET host = ?, port = ?, server_key_hash = ?
        WHERE command_id = ?
      |]
      (host, port, serverKeyHash_, cmdId)

deleteCommand :: DB.Connection -> AsyncCmdId -> IO ()
deleteCommand db cmdId =
  DB.execute db "DELETE FROM commands WHERE command_id = ?" (Only cmdId)

createNtfToken :: DB.Connection -> NtfToken -> IO ()
createNtfToken db NtfToken {deviceToken = DeviceToken provider token, ntfServer = srv@ProtocolServer {host, port}, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhKeys = (ntfDhPubKey, ntfDhPrivKey), ntfDhSecret, ntfTknStatus, ntfTknAction, ntfMode} = do
  upsertNtfServer_ db srv
  DB.execute
    db
    [sql|
      INSERT INTO ntf_tokens
        (provider, device_token, ntf_host, ntf_port, tkn_id, tkn_pub_key, tkn_priv_key, tkn_pub_dh_key, tkn_priv_dh_key, tkn_dh_secret, tkn_status, tkn_action, ntf_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    |]
    ((provider, token, host, port, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhPubKey, ntfDhPrivKey, ntfDhSecret) :. (ntfTknStatus, ntfTknAction, ntfMode))

getSavedNtfToken :: DB.Connection -> IO (Maybe NtfToken)
getSavedNtfToken db = do
  maybeFirstRow ntfToken $
    DB.query_
      db
      [sql|
        SELECT s.ntf_host, s.ntf_port, s.ntf_key_hash,
          t.provider, t.device_token, t.tkn_id, t.tkn_pub_key, t.tkn_priv_key, t.tkn_pub_dh_key, t.tkn_priv_dh_key, t.tkn_dh_secret,
          t.tkn_status, t.tkn_action, t.ntf_mode
        FROM ntf_tokens t
        JOIN ntf_servers s USING (ntf_host, ntf_port)
      |]
  where
    ntfToken ((host, port, keyHash) :. (provider, dt, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhPubKey, ntfDhPrivKey, ntfDhSecret) :. (ntfTknStatus, ntfTknAction, ntfMode_)) =
      let ntfServer = NtfServer host port keyHash
          ntfDhKeys = (ntfDhPubKey, ntfDhPrivKey)
          ntfMode = fromMaybe NMPeriodic ntfMode_
       in NtfToken {deviceToken = DeviceToken provider dt, ntfServer, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhKeys, ntfDhSecret, ntfTknStatus, ntfTknAction, ntfMode}

updateNtfTokenRegistration :: DB.Connection -> NtfToken -> NtfTokenId -> C.DhSecretX25519 -> IO ()
updateNtfTokenRegistration db NtfToken {deviceToken = DeviceToken provider token, ntfServer = ProtocolServer {host, port}} tknId ntfDhSecret = do
  updatedAt <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_tokens
      SET tkn_id = ?, tkn_dh_secret = ?, tkn_status = ?, tkn_action = ?, updated_at = ?
      WHERE provider = ? AND device_token = ? AND ntf_host = ? AND ntf_port = ?
    |]
    (tknId, ntfDhSecret, NTRegistered, Nothing :: Maybe NtfTknAction, updatedAt, provider, token, host, port)

updateDeviceToken :: DB.Connection -> NtfToken -> DeviceToken -> IO ()
updateDeviceToken db NtfToken {deviceToken = DeviceToken provider token, ntfServer = ProtocolServer {host, port}} (DeviceToken toProvider toToken) = do
  updatedAt <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_tokens
      SET provider = ?, device_token = ?, tkn_status = ?, tkn_action = ?, updated_at = ?
      WHERE provider = ? AND device_token = ? AND ntf_host = ? AND ntf_port = ?
    |]
    (toProvider, toToken, NTRegistered, Nothing :: Maybe NtfTknAction, updatedAt, provider, token, host, port)

updateNtfMode :: DB.Connection -> NtfToken -> NotificationsMode -> IO ()
updateNtfMode db NtfToken {deviceToken = DeviceToken provider token, ntfServer = ProtocolServer {host, port}} ntfMode = do
  updatedAt <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_tokens
      SET ntf_mode = ?, updated_at = ?
      WHERE provider = ? AND device_token = ? AND ntf_host = ? AND ntf_port = ?
    |]
    (ntfMode, updatedAt, provider, token, host, port)

updateNtfToken :: DB.Connection -> NtfToken -> NtfTknStatus -> Maybe NtfTknAction -> IO ()
updateNtfToken db NtfToken {deviceToken = DeviceToken provider token, ntfServer = ProtocolServer {host, port}} tknStatus tknAction = do
  updatedAt <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_tokens
      SET tkn_status = ?, tkn_action = ?, updated_at = ?
      WHERE provider = ? AND device_token = ? AND ntf_host = ? AND ntf_port = ?
    |]
    (tknStatus, tknAction, updatedAt, provider, token, host, port)

removeNtfToken :: DB.Connection -> NtfToken -> IO ()
removeNtfToken db NtfToken {deviceToken = DeviceToken provider token, ntfServer = ProtocolServer {host, port}} =
  DB.execute
    db
    [sql|
      DELETE FROM ntf_tokens
      WHERE provider = ? AND device_token = ? AND ntf_host = ? AND ntf_port = ?
    |]
    (provider, token, host, port)

addNtfTokenToDelete :: DB.Connection -> NtfServer -> C.APrivateAuthKey -> NtfTokenId -> IO ()
addNtfTokenToDelete db ProtocolServer {host, port, keyHash} ntfPrivKey tknId =
  DB.execute db "INSERT INTO ntf_tokens_to_delete (ntf_host, ntf_port, ntf_key_hash, tkn_id, tkn_priv_key) VALUES (?,?,?,?,?)" (host, port, keyHash, tknId, ntfPrivKey)

deleteExpiredNtfTokensToDelete :: DB.Connection -> NominalDiffTime -> IO ()
deleteExpiredNtfTokensToDelete db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.execute db "DELETE FROM ntf_tokens_to_delete WHERE created_at < ?" (Only cutoffTs)

type NtfTokenToDelete = (Int64, C.APrivateAuthKey, NtfTokenId)

getNextNtfTokenToDelete :: DB.Connection -> NtfServer -> IO (Either StoreError (Maybe NtfTokenToDelete))
getNextNtfTokenToDelete db (NtfServer ntfHost ntfPort _) =
  getWorkItem "ntf tkn del" getNtfTknDbId getNtfTknToDelete (markNtfTokenToDeleteFailed_ db)
  where
    getNtfTknDbId :: IO (Maybe Int64)
    getNtfTknDbId =
      maybeFirstRow fromOnly $
        DB.query
          db
          [sql|
            SELECT ntf_token_to_delete_id
            FROM ntf_tokens_to_delete
            WHERE ntf_host = ? AND ntf_port = ?
              AND del_failed = 0
            ORDER BY created_at ASC
            LIMIT 1
          |]
          (ntfHost, ntfPort)
    getNtfTknToDelete :: Int64 -> IO (Either StoreError NtfTokenToDelete)
    getNtfTknToDelete tknDbId =
      firstRow ntfTokenToDelete err $
        DB.query
          db
          [sql|
            SELECT tkn_priv_key, tkn_id
            FROM ntf_tokens_to_delete
            WHERE ntf_token_to_delete_id = ?
          |]
          (Only tknDbId)
      where
        err = SEInternal $ "ntf token to delete " <> bshow tknDbId <> " returned []"
        ntfTokenToDelete (tknPrivKey, tknId) = (tknDbId, tknPrivKey, tknId)

markNtfTokenToDeleteFailed_ :: DB.Connection -> Int64 -> IO ()
markNtfTokenToDeleteFailed_ db tknDbId =
  DB.execute db "UPDATE ntf_tokens_to_delete SET del_failed = 1 where ntf_token_to_delete_id = ?" (Only tknDbId)

getPendingDelTknServers :: DB.Connection -> IO [NtfServer]
getPendingDelTknServers db =
  map toNtfServer
    <$> DB.query_
      db
      [sql|
        SELECT DISTINCT ntf_host, ntf_port, ntf_key_hash
        FROM ntf_tokens_to_delete
      |]
  where
    toNtfServer (host, port, keyHash) = NtfServer host port keyHash

deleteNtfTokenToDelete :: DB.Connection -> Int64 -> IO ()
deleteNtfTokenToDelete db tknDbId =
  DB.execute db "DELETE FROM ntf_tokens_to_delete WHERE ntf_token_to_delete_id = ?" (Only tknDbId)

type NtfSupervisorSub = (NtfSubscription, Maybe (NtfSubAction, NtfActionTs))

getNtfSubscription :: DB.Connection -> ConnId -> IO (Maybe NtfSupervisorSub)
getNtfSubscription db connId =
  maybeFirstRow ntfSubscription $
    DB.query
      db
      [sql|
        SELECT c.user_id, s.host, s.port, COALESCE(nsb.smp_server_key_hash, s.key_hash), ns.ntf_host, ns.ntf_port, ns.ntf_key_hash,
          nsb.smp_ntf_id, nsb.ntf_sub_id, nsb.ntf_sub_status, nsb.ntf_sub_action, nsb.ntf_sub_smp_action, nsb.ntf_sub_action_ts
        FROM ntf_subscriptions nsb
        JOIN connections c USING (conn_id)
        JOIN servers s ON s.host = nsb.smp_host AND s.port = nsb.smp_port
        JOIN ntf_servers ns USING (ntf_host, ntf_port)
        WHERE nsb.conn_id = ?
      |]
      (Only connId)
  where
    ntfSubscription ((userId, smpHost, smpPort, smpKeyHash, ntfHost, ntfPort, ntfKeyHash) :. (ntfQueueId, ntfSubId, ntfSubStatus, ntfAction_, smpAction_, actionTs_)) =
      let smpServer = SMPServer smpHost smpPort smpKeyHash
          ntfServer = NtfServer ntfHost ntfPort ntfKeyHash
          action = case (ntfAction_, smpAction_, actionTs_) of
            (Just ntfAction, Nothing, Just actionTs) -> Just (NSANtf ntfAction, actionTs)
            (Nothing, Just smpAction, Just actionTs) -> Just (NSASMP smpAction, actionTs)
            _ -> Nothing
       in (NtfSubscription {userId, connId, smpServer, ntfQueueId, ntfServer, ntfSubId, ntfSubStatus}, action)

createNtfSubscription :: DB.Connection -> NtfSubscription -> NtfSubAction -> IO (Either StoreError ())
createNtfSubscription db ntfSubscription action = runExceptT $ do
  let NtfSubscription {connId, smpServer = smpServer@(SMPServer host port _), ntfQueueId, ntfServer = (NtfServer ntfHost ntfPort _), ntfSubId, ntfSubStatus} = ntfSubscription
  smpServerKeyHash_ <- ExceptT $ getServerKeyHash_ db smpServer
  actionTs <- liftIO getCurrentTime
  liftIO $
    DB.execute
      db
      [sql|
        INSERT INTO ntf_subscriptions
          (conn_id, smp_host, smp_port, smp_ntf_id, ntf_host, ntf_port, ntf_sub_id,
            ntf_sub_status, ntf_sub_action, ntf_sub_smp_action, ntf_sub_action_ts, smp_server_key_hash)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
      |]
      ( (connId, host, port, ntfQueueId, ntfHost, ntfPort, ntfSubId)
          :. (ntfSubStatus, ntfSubAction, ntfSubSMPAction, actionTs, smpServerKeyHash_)
      )
  where
    (ntfSubAction, ntfSubSMPAction) = ntfSubAndSMPAction action

supervisorUpdateNtfSub :: DB.Connection -> NtfSubscription -> NtfSubAction -> IO ()
supervisorUpdateNtfSub db NtfSubscription {connId, smpServer = (SMPServer smpHost smpPort _), ntfQueueId, ntfServer = (NtfServer ntfHost ntfPort _), ntfSubId, ntfSubStatus} action = do
  ts <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_subscriptions
      SET smp_host = ?, smp_port = ?, smp_ntf_id = ?, ntf_host = ?, ntf_port = ?, ntf_sub_id = ?,
          ntf_sub_status = ?, ntf_sub_action = ?, ntf_sub_smp_action = ?, ntf_sub_action_ts = ?, updated_by_supervisor = ?, updated_at = ?
      WHERE conn_id = ?
    |]
    ( (smpHost, smpPort, ntfQueueId, ntfHost, ntfPort, ntfSubId)
        :. (ntfSubStatus, ntfSubAction, ntfSubSMPAction, ts, BI True, ts, connId)
    )
  where
    (ntfSubAction, ntfSubSMPAction) = ntfSubAndSMPAction action

supervisorUpdateNtfAction :: DB.Connection -> ConnId -> NtfSubAction -> IO ()
supervisorUpdateNtfAction db connId action = do
  ts <- getCurrentTime
  DB.execute
    db
    [sql|
      UPDATE ntf_subscriptions
      SET ntf_sub_action = ?, ntf_sub_smp_action = ?, ntf_sub_action_ts = ?, updated_by_supervisor = ?, updated_at = ?
      WHERE conn_id = ?
    |]
    (ntfSubAction, ntfSubSMPAction, ts, BI True, ts, connId)
  where
    (ntfSubAction, ntfSubSMPAction) = ntfSubAndSMPAction action

updateNtfSubscription :: DB.Connection -> NtfSubscription -> NtfSubAction -> NtfActionTs -> IO ()
updateNtfSubscription db NtfSubscription {connId, ntfQueueId, ntfServer = (NtfServer ntfHost ntfPort _), ntfSubId, ntfSubStatus} action actionTs = do
  r <- maybeFirstRow fromOnlyBI $ DB.query db "SELECT updated_by_supervisor FROM ntf_subscriptions WHERE conn_id = ?" (Only connId)
  forM_ r $ \updatedBySupervisor -> do
    updatedAt <- getCurrentTime
    if updatedBySupervisor
      then
        DB.execute
          db
          [sql|
            UPDATE ntf_subscriptions
            SET smp_ntf_id = ?, ntf_sub_id = ?, ntf_sub_status = ?, updated_by_supervisor = ?, updated_at = ?
            WHERE conn_id = ?
          |]
          (ntfQueueId, ntfSubId, ntfSubStatus, BI False, updatedAt, connId)
      else
        DB.execute
          db
          [sql|
            UPDATE ntf_subscriptions
            SET smp_ntf_id = ?, ntf_host = ?, ntf_port = ?, ntf_sub_id = ?, ntf_sub_status = ?, ntf_sub_action = ?, ntf_sub_smp_action = ?, ntf_sub_action_ts = ?, updated_by_supervisor = ?, updated_at = ?
            WHERE conn_id = ?
          |]
          ((ntfQueueId, ntfHost, ntfPort, ntfSubId) :. (ntfSubStatus, ntfSubAction, ntfSubSMPAction, actionTs, BI False, updatedAt, connId))
  where
    (ntfSubAction, ntfSubSMPAction) = ntfSubAndSMPAction action

setNullNtfSubscriptionAction :: DB.Connection -> ConnId -> IO ()
setNullNtfSubscriptionAction db connId = do
  r <- maybeFirstRow fromOnlyBI $ DB.query db "SELECT updated_by_supervisor FROM ntf_subscriptions WHERE conn_id = ?" (Only connId)
  forM_ r $ \updatedBySupervisor ->
    unless updatedBySupervisor $ do
      updatedAt <- getCurrentTime
      DB.execute
        db
        [sql|
          UPDATE ntf_subscriptions
          SET ntf_sub_action = ?, ntf_sub_smp_action = ?, ntf_sub_action_ts = ?, updated_by_supervisor = ?, updated_at = ?
          WHERE conn_id = ?
        |]
        (Nothing :: Maybe NtfSubNTFAction, Nothing :: Maybe NtfSubSMPAction, Nothing :: Maybe UTCTime, BI False, updatedAt, connId)

deleteNtfSubscription :: DB.Connection -> ConnId -> IO ()
deleteNtfSubscription db connId = do
  r <- maybeFirstRow fromOnlyBI $ DB.query db "SELECT updated_by_supervisor FROM ntf_subscriptions WHERE conn_id = ?" (Only connId)
  forM_ r $ \updatedBySupervisor -> do
    updatedAt <- getCurrentTime
    if updatedBySupervisor
      then
        DB.execute
          db
          [sql|
            UPDATE ntf_subscriptions
            SET smp_ntf_id = ?, ntf_sub_id = ?, ntf_sub_status = ?, updated_by_supervisor = ?, updated_at = ?
            WHERE conn_id = ?
          |]
          (Nothing :: Maybe SMP.NotifierId, Nothing :: Maybe NtfSubscriptionId, NASDeleted, BI False, updatedAt, connId)
      else deleteNtfSubscription' db connId

deleteNtfSubscription' :: DB.Connection -> ConnId -> IO ()
deleteNtfSubscription' db connId = do
  DB.execute db "DELETE FROM ntf_subscriptions WHERE conn_id = ?" (Only connId)

getNextNtfSubNTFActions :: DB.Connection -> NtfServer -> Int -> IO (Either StoreError [Either StoreError (NtfSubNTFAction, NtfSubscription, NtfActionTs)])
getNextNtfSubNTFActions db ntfServer@(NtfServer ntfHost ntfPort _) ntfBatchSize =
  getWorkItems "ntf NTF" getNtfConnIds getNtfSubAction (markNtfSubActionNtfFailed_ db)
  where
    getNtfConnIds :: IO [ConnId]
    getNtfConnIds =
      map fromOnly
        <$> DB.query
          db
          [sql|
            SELECT conn_id
            FROM ntf_subscriptions
            WHERE ntf_host = ? AND ntf_port = ? AND ntf_sub_action IS NOT NULL
              AND (ntf_failed = 0 OR updated_by_supervisor = 1)
            ORDER BY ntf_sub_action_ts ASC
            LIMIT ?
          |]
          (ntfHost, ntfPort, ntfBatchSize)
    getNtfSubAction :: ConnId -> IO (Either StoreError (NtfSubNTFAction, NtfSubscription, NtfActionTs))
    getNtfSubAction connId = do
      markUpdatedByWorker db connId
      firstRow ntfSubAction err $
        DB.query
          db
          [sql|
            SELECT c.user_id, s.host, s.port, COALESCE(ns.smp_server_key_hash, s.key_hash),
              ns.smp_ntf_id, ns.ntf_sub_id, ns.ntf_sub_status, ns.ntf_sub_action_ts, ns.ntf_sub_action
            FROM ntf_subscriptions ns
            JOIN connections c USING (conn_id)
            JOIN servers s ON s.host = ns.smp_host AND s.port = ns.smp_port
            WHERE ns.conn_id = ?
          |]
          (Only connId)
      where
        err = SEInternal $ "ntf subscription " <> bshow connId <> " returned []"
        ntfSubAction (userId, smpHost, smpPort, smpKeyHash, ntfQueueId, ntfSubId, ntfSubStatus, actionTs, action) =
          let smpServer = SMPServer smpHost smpPort smpKeyHash
              ntfSubscription = NtfSubscription {userId, connId, smpServer, ntfQueueId, ntfServer, ntfSubId, ntfSubStatus}
           in (action, ntfSubscription, actionTs)

markNtfSubActionNtfFailed_ :: DB.Connection -> ConnId -> IO ()
markNtfSubActionNtfFailed_ db connId =
  DB.execute db "UPDATE ntf_subscriptions SET ntf_failed = 1 where conn_id = ?" (Only connId)

getNextNtfSubSMPActions :: DB.Connection -> SMPServer -> Int -> IO (Either StoreError [Either StoreError (NtfSubSMPAction, NtfSubscription)])
getNextNtfSubSMPActions db smpServer@(SMPServer smpHost smpPort _) ntfBatchSize =
  getWorkItems "ntf SMP" getNtfConnIds getNtfSubAction (markNtfSubActionSMPFailed_ db)
  where
    getNtfConnIds :: IO [ConnId]
    getNtfConnIds =
      map fromOnly
        <$> DB.query
          db
          [sql|
            SELECT conn_id
            FROM ntf_subscriptions ns
            WHERE smp_host = ? AND smp_port = ? AND ntf_sub_smp_action IS NOT NULL AND ntf_sub_action_ts IS NOT NULL
              AND (smp_failed = 0 OR updated_by_supervisor = 1)
            ORDER BY ntf_sub_action_ts ASC
            LIMIT ?
          |]
          (smpHost, smpPort, ntfBatchSize)
    getNtfSubAction :: ConnId -> IO (Either StoreError (NtfSubSMPAction, NtfSubscription))
    getNtfSubAction connId = do
      markUpdatedByWorker db connId
      firstRow ntfSubAction err $
        DB.query
          db
          [sql|
            SELECT c.user_id, s.ntf_host, s.ntf_port, s.ntf_key_hash,
              ns.smp_ntf_id, ns.ntf_sub_id, ns.ntf_sub_status, ns.ntf_sub_smp_action
            FROM ntf_subscriptions ns
            JOIN connections c USING (conn_id)
            JOIN ntf_servers s USING (ntf_host, ntf_port)
            WHERE ns.conn_id = ?
          |]
          (Only connId)
      where
        err = SEInternal $ "ntf subscription " <> bshow connId <> " returned []"
        ntfSubAction (userId, ntfHost, ntfPort, ntfKeyHash, ntfQueueId, ntfSubId, ntfSubStatus, action) =
          let ntfServer = NtfServer ntfHost ntfPort ntfKeyHash
              ntfSubscription = NtfSubscription {userId, connId, smpServer, ntfQueueId, ntfServer, ntfSubId, ntfSubStatus}
           in (action, ntfSubscription)

markNtfSubActionSMPFailed_ :: DB.Connection -> ConnId -> IO ()
markNtfSubActionSMPFailed_ db connId =
  DB.execute db "UPDATE ntf_subscriptions SET smp_failed = 1 where conn_id = ?" (Only connId)

markUpdatedByWorker :: DB.Connection -> ConnId -> IO ()
markUpdatedByWorker db connId =
  DB.execute db "UPDATE ntf_subscriptions SET updated_by_supervisor = 0 WHERE conn_id = ?" (Only connId)

getActiveNtfToken :: DB.Connection -> IO (Maybe NtfToken)
getActiveNtfToken db =
  maybeFirstRow ntfToken $
    DB.query
      db
      [sql|
        SELECT s.ntf_host, s.ntf_port, s.ntf_key_hash,
          t.provider, t.device_token, t.tkn_id, t.tkn_pub_key, t.tkn_priv_key, t.tkn_pub_dh_key, t.tkn_priv_dh_key, t.tkn_dh_secret,
          t.tkn_status, t.tkn_action, t.ntf_mode
        FROM ntf_tokens t
        JOIN ntf_servers s USING (ntf_host, ntf_port)
        WHERE t.tkn_status = ?
      |]
      (Only NTActive)
  where
    ntfToken ((host, port, keyHash) :. (provider, dt, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhPubKey, ntfDhPrivKey, ntfDhSecret) :. (ntfTknStatus, ntfTknAction, ntfMode_)) =
      let ntfServer = NtfServer host port keyHash
          ntfDhKeys = (ntfDhPubKey, ntfDhPrivKey)
          ntfMode = fromMaybe NMPeriodic ntfMode_
       in NtfToken {deviceToken = DeviceToken provider dt, ntfServer, ntfTokenId, ntfPubKey, ntfPrivKey, ntfDhKeys, ntfDhSecret, ntfTknStatus, ntfTknAction, ntfMode}

getNtfRcvQueue :: DB.Connection -> SMPQueueNtf -> IO (Either StoreError (ConnId, RcvNtfDhSecret, Maybe UTCTime))
getNtfRcvQueue db SMPQueueNtf {smpServer = (SMPServer host port _), notifierId} =
  firstRow' res SEConnNotFound $
    DB.query
      db
      [sql|
        SELECT conn_id, rcv_ntf_dh_secret, last_broker_ts
        FROM rcv_queues
        WHERE host = ? AND port = ? AND ntf_id = ? AND deleted = 0
      |]
      (host, port, notifierId)
  where
    res (connId, Just rcvNtfDhSecret, lastBrokerTs_) = Right (connId, rcvNtfDhSecret, lastBrokerTs_)
    res _ = Left SEConnNotFound

setConnectionNtfs :: DB.Connection -> ConnId -> Bool -> IO ()
setConnectionNtfs db connId enableNtfs =
  DB.execute db "UPDATE connections SET enable_ntfs = ? WHERE conn_id = ?" (BI enableNtfs, connId)

-- * Auxiliary helpers

instance ToField QueueStatus where toField = toField . serializeQueueStatus

instance FromField QueueStatus where fromField = fromTextField_ queueStatusT

instance ToField (DBQueueId 'QSStored) where toField (DBQueueId qId) = toField qId

instance FromField (DBQueueId 'QSStored) where 
#if defined(dbPostgres)
  fromField x dat = DBQueueId <$> fromField x dat
#else
  fromField x = DBQueueId <$> fromField x
#endif

instance ToField InternalRcvId where toField (InternalRcvId x) = toField x

deriving newtype instance FromField InternalRcvId

instance ToField InternalSndId where toField (InternalSndId x) = toField x

deriving newtype instance FromField InternalSndId

instance ToField InternalId where toField (InternalId x) = toField x

deriving newtype instance FromField InternalId

instance ToField AgentMessageType where toField = toField . Binary . smpEncode

instance FromField AgentMessageType where fromField = blobFieldDecoder smpDecode

instance ToField MsgIntegrity where toField = toField . Binary . strEncode

instance FromField MsgIntegrity where fromField = blobFieldDecoder strDecode

instance ToField SMPQueueUri where toField = toField . Binary . strEncode

instance FromField SMPQueueUri where fromField = blobFieldDecoder strDecode

instance ToField AConnectionRequestUri where toField = toField . Binary . strEncode

instance FromField AConnectionRequestUri where fromField = blobFieldDecoder strDecode

instance ConnectionModeI c => ToField (ConnectionRequestUri c) where toField = toField . Binary . strEncode

instance (E.Typeable c, ConnectionModeI c) => FromField (ConnectionRequestUri c) where fromField = blobFieldDecoder strDecode

instance ToField ConnectionMode where toField = toField . decodeLatin1 . strEncode

instance FromField ConnectionMode where fromField = fromTextField_ connModeT

instance ToField (SConnectionMode c) where toField = toField . connMode

instance FromField AConnectionMode where fromField = fromTextField_ $ fmap connMode' . connModeT

instance ToField MsgFlags where toField = toField . decodeLatin1 . smpEncode

instance FromField MsgFlags where fromField = fromTextField_ $ eitherToMaybe . smpDecode . encodeUtf8

instance ToField [SMPQueueInfo] where toField = toField . Binary . smpEncodeList

instance FromField [SMPQueueInfo] where fromField = blobFieldDecoder $ parseAll smpListP

instance ToField (NonEmpty TransportHost) where toField = toField . decodeLatin1 . strEncode

instance FromField (NonEmpty TransportHost) where fromField = fromTextField_ $ eitherToMaybe . strDecode . encodeUtf8

instance ToField AgentCommand where toField = toField . Binary . strEncode

instance FromField AgentCommand where fromField = blobFieldDecoder strDecode

instance ToField AgentCommandTag where toField = toField . Binary . strEncode

instance FromField AgentCommandTag where fromField = blobFieldDecoder strDecode

instance ToField MsgReceiptStatus where toField = toField . decodeLatin1 . strEncode

instance FromField MsgReceiptStatus where fromField = fromTextField_ $ eitherToMaybe . strDecode . encodeUtf8

instance ToField (Version v) where toField (Version v) = toField v

deriving newtype instance FromField (Version v)

instance ToField EntityId where toField (EntityId s) = toField $ Binary s

deriving newtype instance FromField EntityId

deriving newtype instance ToField ChunkReplicaId

deriving newtype instance FromField ChunkReplicaId

fromOnlyBI :: Only BoolInt -> Bool
fromOnlyBI (Only (BI b)) = b
{-# INLINE fromOnlyBI #-}

#if !defined(dbPostgres)
{- ORMOLU_DISABLE -}
-- SQLite.Simple only has these up to 10 fields, which is insufficient for some of our queries
instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h, FromField i, FromField j,
          FromField k) =>
  FromRow (a,b,c,d,e,f,g,h,i,j,k) where
  fromRow = (,,,,,,,,,,) <$> field <*> field <*> field <*> field <*> field
                         <*> field <*> field <*> field <*> field <*> field
                         <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h, FromField i, FromField j,
          FromField k, FromField l) =>
  FromRow (a,b,c,d,e,f,g,h,i,j,k,l) where
  fromRow = (,,,,,,,,,,,) <$> field <*> field <*> field <*> field <*> field
                          <*> field <*> field <*> field <*> field <*> field
                          <*> field <*> field

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f,
          ToField g, ToField h, ToField i, ToField j, ToField k, ToField l) =>
  ToRow (a,b,c,d,e,f,g,h,i,j,k,l) where
  toRow (a,b,c,d,e,f,g,h,i,j,k,l) =
    [ toField a, toField b, toField c, toField d, toField e, toField f,
      toField g, toField h, toField i, toField j, toField k, toField l
    ]

{- ORMOLU_ENABLE -}
#endif

-- * Server helper

-- | Creates a new server, if it doesn't exist, and returns the passed key hash if it is different from stored.
createServer_ :: DB.Connection -> SMPServer -> IO (Maybe C.KeyHash)
createServer_ db newSrv@ProtocolServer {host, port, keyHash} =
  getServerKeyHash_ db newSrv >>= \case
    Right keyHash_ -> pure keyHash_
    Left _ -> insertNewServer_ $> Nothing
  where
    insertNewServer_ =
      DB.execute db "INSERT INTO servers (host, port, key_hash) VALUES (?,?,?)" (host, port, keyHash)

-- | Returns the passed server key hash if it is different from the stored one, or the error if the server does not exist.
getServerKeyHash_ :: DB.Connection -> SMPServer -> IO (Either StoreError (Maybe C.KeyHash))
getServerKeyHash_ db ProtocolServer {host, port, keyHash} = do
  firstRow useKeyHash SEServerNotFound $
    DB.query db "SELECT key_hash FROM servers WHERE host = ? AND port = ?" (host, port)
  where
    useKeyHash (Only keyHash') = if keyHash /= keyHash' then Just keyHash else Nothing

upsertNtfServer_ :: DB.Connection -> NtfServer -> IO ()
upsertNtfServer_ db ProtocolServer {host, port, keyHash} = do
  DB.execute
    db
    [sql|
      INSERT INTO ntf_servers (ntf_host, ntf_port, ntf_key_hash) VALUES (?,?,?)
      ON CONFLICT (ntf_host, ntf_port) DO UPDATE SET
        ntf_host=excluded.ntf_host,
        ntf_port=excluded.ntf_port,
        ntf_key_hash=excluded.ntf_key_hash;
    |]
    (host, port, keyHash)

-- * createRcvConn helpers

insertRcvQueue_ :: DB.Connection -> ConnId -> NewRcvQueue -> Maybe C.KeyHash -> IO RcvQueue
insertRcvQueue_ db connId' rq@RcvQueue {..} serverKeyHash_ = do
  -- to preserve ID if the queue already exists.
  -- possibly, it can be done in one query.
  currQId_ <- maybeFirstRow fromOnly $ DB.query db "SELECT rcv_queue_id FROM rcv_queues WHERE conn_id = ? AND host = ? AND port = ? AND snd_id = ?" (connId', host server, port server, sndId)
  qId <- maybe (newQueueId_ <$> DB.query db "SELECT rcv_queue_id FROM rcv_queues WHERE conn_id = ? ORDER BY rcv_queue_id DESC LIMIT 1" (Only connId')) pure currQId_
  DB.execute
    db
    [sql|
      INSERT INTO rcv_queues
        (host, port, rcv_id, conn_id, rcv_private_key, rcv_dh_secret, e2e_priv_key, e2e_dh_secret, snd_id, snd_secure, status, rcv_queue_id, rcv_primary, replace_rcv_queue_id, smp_client_version, server_key_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    |]
    ((host server, port server, rcvId, connId', rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret) :. (sndId, BI sndSecure, status, qId, BI primary, dbReplaceQueueId, smpClientVersion, serverKeyHash_))
  pure (rq :: NewRcvQueue) {connId = connId', dbQueueId = qId}

-- * createSndConn helpers

insertSndQueue_ :: DB.Connection -> ConnId -> NewSndQueue -> Maybe C.KeyHash -> IO SndQueue
insertSndQueue_ db connId' sq@SndQueue {..} serverKeyHash_ = do
  -- to preserve ID if the queue already exists.
  -- possibly, it can be done in one query.
  currQId_ <- maybeFirstRow fromOnly $ DB.query db "SELECT snd_queue_id FROM snd_queues WHERE conn_id = ? AND host = ? AND port = ? AND snd_id = ?" (connId', host server, port server, sndId)
  qId <- maybe (newQueueId_ <$> DB.query db "SELECT snd_queue_id FROM snd_queues WHERE conn_id = ? ORDER BY snd_queue_id DESC LIMIT 1" (Only connId')) pure currQId_
  DB.execute
    db
    [sql|
      INSERT INTO snd_queues
        (host, port, snd_id, snd_secure, conn_id, snd_public_key, snd_private_key, e2e_pub_key, e2e_dh_secret,
         status, snd_queue_id, snd_primary, replace_snd_queue_id, smp_client_version, server_key_hash)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT (host, port, snd_id) DO UPDATE SET
        host=EXCLUDED.host,
        port=EXCLUDED.port,
        snd_id=EXCLUDED.snd_id,
        snd_secure=EXCLUDED.snd_secure,
        conn_id=EXCLUDED.conn_id,
        snd_public_key=EXCLUDED.snd_public_key,
        snd_private_key=EXCLUDED.snd_private_key,
        e2e_pub_key=EXCLUDED.e2e_pub_key,
        e2e_dh_secret=EXCLUDED.e2e_dh_secret,
        status=EXCLUDED.status,
        snd_queue_id=EXCLUDED.snd_queue_id,
        snd_primary=EXCLUDED.snd_primary,
        replace_snd_queue_id=EXCLUDED.replace_snd_queue_id,
        smp_client_version=EXCLUDED.smp_client_version,
        server_key_hash=EXCLUDED.server_key_hash
    |]
    ((host server, port server, sndId, BI sndSecure, connId', sndPublicKey, sndPrivateKey, e2ePubKey, e2eDhSecret) 
    :. (status, qId, BI primary, dbReplaceQueueId, smpClientVersion, serverKeyHash_))
  pure (sq :: NewSndQueue) {connId = connId', dbQueueId = qId}

newQueueId_ :: [Only Int64] -> DBQueueId 'QSStored
newQueueId_ [] = DBQueueId 1
newQueueId_ (Only maxId : _) = DBQueueId (maxId + 1)

-- * getConn helpers

getConn :: DB.Connection -> ConnId -> IO (Either StoreError SomeConn)
getConn = getAnyConn False
{-# INLINE getConn #-}

getDeletedConn :: DB.Connection -> ConnId -> IO (Either StoreError SomeConn)
getDeletedConn = getAnyConn True
{-# INLINE getDeletedConn #-}

getAnyConn :: Bool -> DB.Connection -> ConnId -> IO (Either StoreError SomeConn)
getAnyConn deleted' dbConn connId =
  getConnData dbConn connId >>= \case
    Nothing -> pure $ Left SEConnNotFound
    Just (cData@ConnData {deleted}, cMode)
      | deleted /= deleted' -> pure $ Left SEConnNotFound
      | otherwise -> do
          rQ <- getRcvQueuesByConnId_ dbConn connId
          sQ <- getSndQueuesByConnId_ dbConn connId
          pure $ case (rQ, sQ, cMode) of
            (Just rqs, Just sqs, CMInvitation) -> Right $ SomeConn SCDuplex (DuplexConnection cData rqs sqs)
            (Just (rq :| _), Nothing, CMInvitation) -> Right $ SomeConn SCRcv (RcvConnection cData rq)
            (Nothing, Just (sq :| _), CMInvitation) -> Right $ SomeConn SCSnd (SndConnection cData sq)
            (Just (rq :| _), Nothing, CMContact) -> Right $ SomeConn SCContact (ContactConnection cData rq)
            (Nothing, Nothing, _) -> Right $ SomeConn SCNew (NewConnection cData)
            _ -> Left SEConnNotFound

getConns :: DB.Connection -> [ConnId] -> IO [Either StoreError SomeConn]
getConns = getAnyConns_ False
{-# INLINE getConns #-}

getDeletedConns :: DB.Connection -> [ConnId] -> IO [Either StoreError SomeConn]
getDeletedConns = getAnyConns_ True
{-# INLINE getDeletedConns #-}

getAnyConns_ :: Bool -> DB.Connection -> [ConnId] -> IO [Either StoreError SomeConn]
getAnyConns_ deleted' db connIds = forM connIds $ E.handle handleDBError . getAnyConn deleted' db
  where
    handleDBError :: E.SomeException -> IO (Either StoreError SomeConn)
    handleDBError = pure . Left . SEInternal . bshow

getConnData :: DB.Connection -> ConnId -> IO (Maybe (ConnData, ConnectionMode))
getConnData db connId' =
  maybeFirstRow cData $
    DB.query
      db
      [sql|
        SELECT
          user_id, conn_id, conn_mode, smp_agent_version, enable_ntfs,
          last_external_snd_msg_id, deleted, ratchet_sync_state, pq_support
        FROM connections
        WHERE conn_id = ?
      |]
      (Only connId')
  where
    cData (userId, connId, cMode, connAgentVersion, enableNtfs_, lastExternalSndId, BI deleted, ratchetSyncState, pqSupport) =
      (ConnData {userId, connId, connAgentVersion, enableNtfs = maybe True unBI enableNtfs_, lastExternalSndId, deleted, ratchetSyncState, pqSupport}, cMode)

setConnDeleted :: DB.Connection -> Bool -> ConnId -> IO ()
setConnDeleted db waitDelivery connId
  | waitDelivery = do
      currentTs <- getCurrentTime
      DB.execute db "UPDATE connections SET deleted_at_wait_delivery = ? WHERE conn_id = ?" (currentTs, connId)
  | otherwise =
      DB.execute db "UPDATE connections SET deleted = ? WHERE conn_id = ?" (BI True, connId)

setConnUserId :: DB.Connection -> UserId -> ConnId -> UserId -> IO ()
setConnUserId db oldUserId connId newUserId =
  DB.execute db "UPDATE connections SET user_id = ? WHERE conn_id = ? and user_id = ?" (newUserId, connId, oldUserId)

setConnAgentVersion :: DB.Connection -> ConnId -> VersionSMPA -> IO ()
setConnAgentVersion db connId aVersion =
  DB.execute db "UPDATE connections SET smp_agent_version = ? WHERE conn_id = ?" (aVersion, connId)

setConnPQSupport :: DB.Connection -> ConnId -> PQSupport -> IO ()
setConnPQSupport db connId pqSupport =
  DB.execute db "UPDATE connections SET pq_support = ? WHERE conn_id = ?" (pqSupport, connId)

getDeletedConnIds :: DB.Connection -> IO [ConnId]
getDeletedConnIds db = map fromOnly <$> DB.query db "SELECT conn_id FROM connections WHERE deleted = ?" (Only (BI True))

getDeletedWaitingDeliveryConnIds :: DB.Connection -> IO [ConnId]
getDeletedWaitingDeliveryConnIds db =
  map fromOnly <$> DB.query_ db "SELECT conn_id FROM connections WHERE deleted_at_wait_delivery IS NOT NULL"

setConnRatchetSync :: DB.Connection -> ConnId -> RatchetSyncState -> IO ()
setConnRatchetSync db connId ratchetSyncState =
  DB.execute db "UPDATE connections SET ratchet_sync_state = ? WHERE conn_id = ?" (ratchetSyncState, connId)

addProcessedRatchetKeyHash :: DB.Connection -> ConnId -> ByteString -> IO ()
addProcessedRatchetKeyHash db connId hash =
  DB.execute db "INSERT INTO processed_ratchet_key_hashes (conn_id, hash) VALUES (?,?)" (connId, Binary hash)

checkRatchetKeyHashExists :: DB.Connection -> ConnId -> ByteString -> IO Bool
checkRatchetKeyHashExists db connId hash = do
  fromMaybe False
    <$> maybeFirstRow
      fromOnly
      ( DB.query
          db
          "SELECT 1 FROM processed_ratchet_key_hashes WHERE conn_id = ? AND hash = ? LIMIT 1"
          (connId, Binary hash)
      )

deleteRatchetKeyHashesExpired :: DB.Connection -> NominalDiffTime -> IO ()
deleteRatchetKeyHashesExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.execute db "DELETE FROM processed_ratchet_key_hashes WHERE created_at < ?" (Only cutoffTs)

-- | returns all connection queues, the first queue is the primary one
getRcvQueuesByConnId_ :: DB.Connection -> ConnId -> IO (Maybe (NonEmpty RcvQueue))
getRcvQueuesByConnId_ db connId =
  L.nonEmpty . sortBy primaryFirst . map toRcvQueue
    <$> DB.query db (rcvQueueQuery <> " WHERE q.conn_id = ? AND q.deleted = 0") (Only connId)
  where
    primaryFirst RcvQueue {primary = p, dbReplaceQueueId = i} RcvQueue {primary = p', dbReplaceQueueId = i'} =
      -- the current primary queue is ordered first, the next primary - second
      compare (Down p) (Down p') <> compare i i'

rcvQueueQuery :: Query
rcvQueueQuery =
  [sql|
    SELECT c.user_id, COALESCE(q.server_key_hash, s.key_hash), q.conn_id, q.host, q.port, q.rcv_id, q.rcv_private_key, q.rcv_dh_secret,
      q.e2e_priv_key, q.e2e_dh_secret, q.snd_id, q.snd_secure, q.status,
      q.rcv_queue_id, q.rcv_primary, q.replace_rcv_queue_id, q.switch_status, q.smp_client_version, q.delete_errors,
      q.ntf_public_key, q.ntf_private_key, q.ntf_id, q.rcv_ntf_dh_secret
    FROM rcv_queues q
    JOIN servers s ON q.host = s.host AND q.port = s.port
    JOIN connections c ON q.conn_id = c.conn_id
  |]

toRcvQueue ::
  (UserId, C.KeyHash, ConnId, NonEmpty TransportHost, ServiceName, SMP.RecipientId, SMP.RcvPrivateAuthKey, SMP.RcvDhSecret, C.PrivateKeyX25519, Maybe C.DhSecretX25519, SMP.SenderId, BoolInt)
    :. (QueueStatus, DBQueueId 'QSStored, BoolInt, Maybe Int64, Maybe RcvSwitchStatus, Maybe VersionSMPC, Int)
    :. (Maybe SMP.NtfPublicAuthKey, Maybe SMP.NtfPrivateAuthKey, Maybe SMP.NotifierId, Maybe RcvNtfDhSecret) ->
  RcvQueue
toRcvQueue ((userId, keyHash, connId, host, port, rcvId, rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret, sndId, BI sndSecure) :. (status, dbQueueId, BI primary, dbReplaceQueueId, rcvSwchStatus, smpClientVersion_, deleteErrors) :. (ntfPublicKey_, ntfPrivateKey_, notifierId_, rcvNtfDhSecret_)) =
  let server = SMPServer host port keyHash
      smpClientVersion = fromMaybe initialSMPClientVersion smpClientVersion_
      clientNtfCreds = case (ntfPublicKey_, ntfPrivateKey_, notifierId_, rcvNtfDhSecret_) of
        (Just ntfPublicKey, Just ntfPrivateKey, Just notifierId, Just rcvNtfDhSecret) -> Just $ ClientNtfCreds {ntfPublicKey, ntfPrivateKey, notifierId, rcvNtfDhSecret}
        _ -> Nothing
   in RcvQueue {userId, connId, server, rcvId, rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret, sndId, sndSecure, status, dbQueueId, primary, dbReplaceQueueId, rcvSwchStatus, smpClientVersion, clientNtfCreds, deleteErrors}

getRcvQueueById :: DB.Connection -> ConnId -> Int64 -> IO (Either StoreError RcvQueue)
getRcvQueueById db connId dbRcvId =
  firstRow toRcvQueue SEConnNotFound $
    DB.query db (rcvQueueQuery <> " WHERE q.conn_id = ? AND q.rcv_queue_id = ? AND q.deleted = 0") (connId, dbRcvId)

-- | returns all connection queues, the first queue is the primary one
getSndQueuesByConnId_ :: DB.Connection -> ConnId -> IO (Maybe (NonEmpty SndQueue))
getSndQueuesByConnId_ dbConn connId =
  L.nonEmpty . sortBy primaryFirst . map toSndQueue
    <$> DB.query dbConn (sndQueueQuery <> " WHERE q.conn_id = ?") (Only connId)
  where
    primaryFirst SndQueue {primary = p, dbReplaceQueueId = i} SndQueue {primary = p', dbReplaceQueueId = i'} =
      -- the current primary queue is ordered first, the next primary - second
      compare (Down p) (Down p') <> compare i i'

sndQueueQuery :: Query
sndQueueQuery =
  [sql|
    SELECT
      c.user_id, COALESCE(q.server_key_hash, s.key_hash), q.conn_id, q.host, q.port, q.snd_id, q.snd_secure,
      q.snd_public_key, q.snd_private_key, q.e2e_pub_key, q.e2e_dh_secret, q.status,
      q.snd_queue_id, q.snd_primary, q.replace_snd_queue_id, q.switch_status, q.smp_client_version
    FROM snd_queues q
    JOIN servers s ON q.host = s.host AND q.port = s.port
    JOIN connections c ON q.conn_id = c.conn_id
  |]

toSndQueue ::
  (UserId, C.KeyHash, ConnId, NonEmpty TransportHost, ServiceName, SenderId, BoolInt)
    :. (Maybe SndPublicAuthKey, SndPrivateAuthKey, Maybe C.PublicKeyX25519, C.DhSecretX25519, QueueStatus)
    :. (DBQueueId 'QSStored, BoolInt, Maybe Int64, Maybe SndSwitchStatus, VersionSMPC) ->
  SndQueue
toSndQueue
  ( (userId, keyHash, connId, host, port, sndId, BI sndSecure)
      :. (sndPubKey, sndPrivateKey@(C.APrivateAuthKey a pk), e2ePubKey, e2eDhSecret, status)
      :. (dbQueueId, BI primary, dbReplaceQueueId, sndSwchStatus, smpClientVersion)
    ) =
    let server = SMPServer host port keyHash
        sndPublicKey = fromMaybe (C.APublicAuthKey a (C.publicKey pk)) sndPubKey
     in SndQueue {userId, connId, server, sndId, sndSecure, sndPublicKey, sndPrivateKey, e2ePubKey, e2eDhSecret, status, dbQueueId, primary, dbReplaceQueueId, sndSwchStatus, smpClientVersion}

getSndQueueById :: DB.Connection -> ConnId -> Int64 -> IO (Either StoreError SndQueue)
getSndQueueById db connId dbSndId =
  firstRow toSndQueue SEConnNotFound $
    DB.query db (sndQueueQuery <> " WHERE q.conn_id = ? AND q.snd_queue_id = ?") (connId, dbSndId)

-- * updateRcvIds helpers

retrieveLastIdsAndHashRcv_ :: DB.Connection -> ConnId -> IO (InternalId, InternalRcvId, PrevExternalSndId, PrevRcvMsgHash)
retrieveLastIdsAndHashRcv_ dbConn connId = do
  [(lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash)] <-
    DB.query
      dbConn
      [sql|
        SELECT last_internal_msg_id, last_internal_rcv_msg_id, last_external_snd_msg_id, last_rcv_msg_hash
        FROM connections
        WHERE conn_id = ?
      |]
      (Only connId)
  return (lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash)

updateLastIdsRcv_ :: DB.Connection -> ConnId -> InternalId -> InternalRcvId -> IO ()
updateLastIdsRcv_ dbConn connId newInternalId newInternalRcvId =
  DB.execute
    dbConn
    [sql|
      UPDATE connections
      SET last_internal_msg_id = ?,
          last_internal_rcv_msg_id = ?
      WHERE conn_id = ?
    |]
    (newInternalId, newInternalRcvId, connId)

-- * createRcvMsg helpers

insertRcvMsgBase_ :: DB.Connection -> ConnId -> RcvMsgData -> IO ()
insertRcvMsgBase_ dbConn connId RcvMsgData {msgMeta, msgType, msgFlags, msgBody, internalRcvId} = do
  let MsgMeta {recipient = (internalId, internalTs), pqEncryption} = msgMeta
  DB.execute
    dbConn
    [sql|
      INSERT INTO messages
        (conn_id, internal_id, internal_ts, internal_rcv_id, internal_snd_id, msg_type, msg_flags, msg_body, pq_encryption)
        VALUES (?,?,?,?,?,?,?,?,?);
    |]
    (connId, internalId, internalTs, internalRcvId, Nothing :: Maybe Int64, msgType, msgFlags, Binary msgBody, pqEncryption)

insertRcvMsgDetails_ :: DB.Connection -> ConnId -> RcvQueue -> RcvMsgData -> IO ()
insertRcvMsgDetails_ db connId RcvQueue {dbQueueId} RcvMsgData {msgMeta, internalRcvId, internalHash, externalPrevSndHash, encryptedMsgHash} = do
  let MsgMeta {integrity, recipient, broker, sndMsgId} = msgMeta
  DB.execute
    db
    [sql|
      INSERT INTO rcv_messages
        ( conn_id, rcv_queue_id, internal_rcv_id, internal_id, external_snd_id,
          broker_id, broker_ts,
          internal_hash, external_prev_snd_hash, integrity)
      VALUES
        (?,?,?,?,?,?,?,?,?,?)
    |]
    (connId, dbQueueId, internalRcvId, fst recipient, sndMsgId, Binary (fst broker), snd broker, Binary internalHash, Binary externalPrevSndHash, integrity)
  DB.execute db "INSERT INTO encrypted_rcv_message_hashes (conn_id, hash) VALUES (?,?)" (connId, Binary encryptedMsgHash)

updateRcvMsgHash :: DB.Connection -> ConnId -> AgentMsgId -> InternalRcvId -> MsgHash -> IO ()
updateRcvMsgHash db connId sndMsgId internalRcvId internalHash =
  DB.execute
    db
    -- last_internal_rcv_msg_id equality check prevents race condition in case next id was reserved
    [sql|
      UPDATE connections
      SET last_external_snd_msg_id = ?,
          last_rcv_msg_hash = ?
      WHERE conn_id = ?
        AND last_internal_rcv_msg_id = ?
    |]
    (sndMsgId, Binary internalHash, connId, internalRcvId)

-- * updateSndIds helpers

retrieveLastIdsAndHashSnd_ :: DB.Connection -> ConnId -> IO (Either StoreError (InternalId, InternalSndId, PrevSndMsgHash))
retrieveLastIdsAndHashSnd_ dbConn connId = do
  firstRow id SEConnNotFound $
    DB.query
      dbConn
      [sql|
        SELECT last_internal_msg_id, last_internal_snd_msg_id, last_snd_msg_hash
        FROM connections
        WHERE conn_id = ?
      |]
      (Only connId)

updateLastIdsSnd_ :: DB.Connection -> ConnId -> InternalId -> InternalSndId -> IO ()
updateLastIdsSnd_ dbConn connId newInternalId newInternalSndId =
  DB.execute
    dbConn
    [sql|
      UPDATE connections
      SET last_internal_msg_id = ?,
          last_internal_snd_msg_id = ?
      WHERE conn_id = ?
    |]
    (newInternalId, newInternalSndId, connId)

-- * createSndMsg helpers

insertSndMsgBase_ :: DB.Connection -> ConnId -> SndMsgData -> IO ()
insertSndMsgBase_ db connId SndMsgData {internalId, internalTs, internalSndId, msgType, msgFlags, msgBody, pqEncryption} = do
  DB.execute
    db
    [sql|
      INSERT INTO messages
        (conn_id, internal_id, internal_ts, internal_rcv_id, internal_snd_id, msg_type, msg_flags, msg_body, pq_encryption)
      VALUES
        (?,?,?,?,?,?,?,?,?);
    |]
    (connId, internalId, internalTs, Nothing :: Maybe Int64, internalSndId, msgType, msgFlags, Binary msgBody, pqEncryption)

insertSndMsgDetails_ :: DB.Connection -> ConnId -> SndMsgData -> IO ()
insertSndMsgDetails_ dbConn connId SndMsgData {..} =
  DB.execute
    dbConn
    [sql|
      INSERT INTO snd_messages
        ( conn_id, internal_snd_id, internal_id, internal_hash, previous_msg_hash, msg_encrypt_key, padded_msg_len, snd_message_body_id)
      VALUES
        (?,?,?,?,?,?,?,?)
    |]
    (connId, internalSndId, internalId, Binary internalHash, Binary prevMsgHash, encryptKey_, paddedLen_, sndMsgBodyId_)
  where
    (encryptKey_, paddedLen_, sndMsgBodyId_) = case sndMsgPrepData_ of
      Nothing -> (Nothing, Nothing, Nothing)
      Just SndMsgPrepData {encryptKey, paddedLen, sndMsgBodyId} -> (Just encryptKey, Just paddedLen, Just sndMsgBodyId)

updateSndMsgHash :: DB.Connection -> ConnId -> InternalSndId -> MsgHash -> IO ()
updateSndMsgHash db connId internalSndId internalHash =
  DB.execute
    db
    -- last_internal_snd_msg_id equality check prevents race condition in case next id was reserved
    [sql|
      UPDATE connections
      SET last_snd_msg_hash = ?
      WHERE conn_id = ?
        AND last_internal_snd_msg_id = ?;
    |]
    (Binary internalHash, connId, internalSndId)

-- create record with a random ID
createWithRandomId :: TVar ChaChaDRG -> (ByteString -> IO ()) -> IO (Either StoreError ByteString)
createWithRandomId gVar create = fst <$$> createWithRandomId' gVar create

createWithRandomId' :: forall a. TVar ChaChaDRG -> (ByteString -> IO a) -> IO (Either StoreError (ByteString, a))
createWithRandomId' gVar create = tryCreate 3
  where
    tryCreate :: Int -> IO (Either StoreError (ByteString, a))
    tryCreate 0 = pure $ Left SEUniqueID
    tryCreate n = do
      id' <- randomId gVar 12
      E.try (create id') >>= \case
        Right r -> pure $ Right (id', r)
        Left e -> handleErr n e
#if defined(dbPostgres)
    handleErr n e = case constraintViolation e of
      Just _ -> tryCreate (n - 1)
      Nothing -> pure . Left . SEInternal $ bshow e
#else
    handleErr n e
      | SQL.sqlError e == SQL.ErrorConstraint = tryCreate (n - 1)
      | otherwise = pure . Left . SEInternal $ bshow e
#endif

randomId :: TVar ChaChaDRG -> Int -> IO ByteString
randomId gVar n = atomically $ U.encode <$> C.randomBytes n gVar

ntfSubAndSMPAction :: NtfSubAction -> (Maybe NtfSubNTFAction, Maybe NtfSubSMPAction)
ntfSubAndSMPAction (NSANtf action) = (Just action, Nothing)
ntfSubAndSMPAction (NSASMP action) = (Nothing, Just action)

createXFTPServer_ :: DB.Connection -> XFTPServer -> IO Int64
createXFTPServer_ db newSrv@ProtocolServer {host, port, keyHash} =
  getXFTPServerId_ db newSrv >>= \case
    Right srvId -> pure srvId
    Left _ -> insertNewServer_
  where
    insertNewServer_ = do
      DB.execute db "INSERT INTO xftp_servers (xftp_host, xftp_port, xftp_key_hash) VALUES (?,?,?)" (host, port, keyHash)
      insertedRowId db

getXFTPServerId_ :: DB.Connection -> XFTPServer -> IO (Either StoreError Int64)
getXFTPServerId_ db ProtocolServer {host, port, keyHash} = do
  firstRow fromOnly SEXFTPServerNotFound $
    DB.query db "SELECT xftp_server_id FROM xftp_servers WHERE xftp_host = ? AND xftp_port = ? AND xftp_key_hash = ?" (host, port, keyHash)

createRcvFile :: DB.Connection -> TVar ChaChaDRG -> UserId -> FileDescription 'FRecipient -> FilePath -> FilePath -> CryptoFile -> Bool -> IO (Either StoreError RcvFileId)
createRcvFile db gVar userId fd@FileDescription {chunks} prefixPath tmpPath file approvedRelays = runExceptT $ do
  (rcvFileEntityId, rcvFileId) <- ExceptT $ insertRcvFile db gVar userId fd prefixPath tmpPath file Nothing Nothing approvedRelays
  liftIO $
    forM_ chunks $ \fc@FileChunk {replicas} -> do
      chunkId <- insertRcvFileChunk db fc rcvFileId
      forM_ (zip [1 ..] replicas) $ \(rno, replica) -> insertRcvFileChunkReplica db rno replica chunkId
  pure rcvFileEntityId

createRcvFileRedirect :: DB.Connection -> TVar ChaChaDRG -> UserId -> FileDescription 'FRecipient -> FilePath -> FilePath -> CryptoFile -> FilePath -> CryptoFile -> Bool -> IO (Either StoreError RcvFileId)
createRcvFileRedirect _ _ _ FileDescription {redirect = Nothing} _ _ _ _ _ _ = pure $ Left $ SEInternal "createRcvFileRedirect called without redirect"
createRcvFileRedirect db gVar userId redirectFd@FileDescription {chunks = redirectChunks, redirect = Just RedirectFileInfo {size, digest}} prefixPath redirectPath redirectFile dstPath dstFile approvedRelays = runExceptT $ do
  (dstEntityId, dstId) <- ExceptT $ insertRcvFile db gVar userId dummyDst prefixPath dstPath dstFile Nothing Nothing approvedRelays
  (_, redirectId) <- ExceptT $ insertRcvFile db gVar userId redirectFd prefixPath redirectPath redirectFile (Just dstId) (Just dstEntityId) approvedRelays
  liftIO $
    forM_ redirectChunks $ \fc@FileChunk {replicas} -> do
      chunkId <- insertRcvFileChunk db fc redirectId
      forM_ (zip [1 ..] replicas) $ \(rno, replica) -> insertRcvFileChunkReplica db rno replica chunkId
  pure dstEntityId
  where
    dummyDst =
      FileDescription
        { party = SFRecipient,
          size,
          digest,
          redirect = Nothing,
          -- updated later with updateRcvFileRedirect
          key = C.unsafeSbKey $ B.replicate 32 '#',
          nonce = C.cbNonce "",
          chunkSize = FileSize 0,
          chunks = []
        }

insertRcvFile :: DB.Connection -> TVar ChaChaDRG -> UserId -> FileDescription 'FRecipient -> FilePath -> FilePath -> CryptoFile -> Maybe DBRcvFileId -> Maybe RcvFileId -> Bool -> IO (Either StoreError (RcvFileId, DBRcvFileId))
insertRcvFile db gVar userId FileDescription {size, digest, key, nonce, chunkSize, redirect} prefixPath tmpPath (CryptoFile savePath cfArgs) redirectId_ redirectEntityId_ approvedRelays = runExceptT $ do
  let (redirectDigest_, redirectSize_) = case redirect of
        Just RedirectFileInfo {digest = d, size = s} -> (Just d, Just s)
        Nothing -> (Nothing, Nothing)
  rcvFileEntityId <- ExceptT $
    createWithRandomId gVar $ \rcvFileEntityId ->
      DB.execute
        db
        "INSERT INTO rcv_files (rcv_file_entity_id, user_id, size, digest, key, nonce, chunk_size, prefix_path, tmp_path, save_path, save_file_key, save_file_nonce, status, redirect_id, redirect_entity_id, redirect_digest, redirect_size, approved_relays) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ((Binary rcvFileEntityId, userId, size, digest, key, nonce, chunkSize, prefixPath, tmpPath) :. (savePath, fileKey <$> cfArgs, fileNonce <$> cfArgs, RFSReceiving, redirectId_, Binary <$> redirectEntityId_, redirectDigest_, redirectSize_, BI approvedRelays))
  rcvFileId <- liftIO $ insertedRowId db
  pure (rcvFileEntityId, rcvFileId)

insertRcvFileChunk :: DB.Connection -> FileChunk -> DBRcvFileId -> IO Int64
insertRcvFileChunk db FileChunk {chunkNo, chunkSize, digest} rcvFileId = do
  DB.execute
    db
    "INSERT INTO rcv_file_chunks (rcv_file_id, chunk_no, chunk_size, digest) VALUES (?,?,?,?)"
    (rcvFileId, chunkNo, chunkSize, digest)
  insertedRowId db

insertRcvFileChunkReplica :: DB.Connection -> Int -> FileChunkReplica -> Int64 -> IO ()
insertRcvFileChunkReplica db replicaNo FileChunkReplica {server, replicaId, replicaKey} chunkId = do
  srvId <- createXFTPServer_ db server
  DB.execute
    db
    "INSERT INTO rcv_file_chunk_replicas (replica_number, rcv_file_chunk_id, xftp_server_id, replica_id, replica_key) VALUES (?,?,?,?,?)"
    (replicaNo, chunkId, srvId, replicaId, replicaKey)

getRcvFileByEntityId :: DB.Connection -> RcvFileId -> IO (Either StoreError RcvFile)
getRcvFileByEntityId db rcvFileEntityId = runExceptT $ do
  rcvFileId <- ExceptT $ getRcvFileIdByEntityId_ db rcvFileEntityId
  ExceptT $ getRcvFile db rcvFileId

getRcvFileIdByEntityId_ :: DB.Connection -> RcvFileId -> IO (Either StoreError DBRcvFileId)
getRcvFileIdByEntityId_ db rcvFileEntityId =
  firstRow fromOnly SEFileNotFound $
    DB.query db "SELECT rcv_file_id FROM rcv_files WHERE rcv_file_entity_id = ?" (Only (Binary rcvFileEntityId))

getRcvFileRedirects :: DB.Connection -> DBRcvFileId -> IO [RcvFile]
getRcvFileRedirects db rcvFileId = do
  redirects <- fromOnly <$$> DB.query db "SELECT rcv_file_id FROM rcv_files WHERE redirect_id = ?" (Only rcvFileId)
  fmap catMaybes . forM redirects $ getRcvFile db >=> either (const $ pure Nothing) (pure . Just)

getRcvFile :: DB.Connection -> DBRcvFileId -> IO (Either StoreError RcvFile)
getRcvFile db rcvFileId = runExceptT $ do
  f@RcvFile {rcvFileEntityId, userId, tmpPath} <- ExceptT getFile
  chunks <- maybe (pure []) (liftIO . getChunks rcvFileEntityId userId) tmpPath
  pure (f {chunks} :: RcvFile)
  where
    getFile :: IO (Either StoreError RcvFile)
    getFile = do
      firstRow toFile SEFileNotFound $
        DB.query
          db
          [sql|
            SELECT rcv_file_entity_id, user_id, size, digest, key, nonce, chunk_size, prefix_path, tmp_path, save_path, save_file_key, save_file_nonce, status, deleted, redirect_id, redirect_entity_id, redirect_size, redirect_digest
            FROM rcv_files
            WHERE rcv_file_id = ?
          |]
          (Only rcvFileId)
      where
        toFile :: (RcvFileId, UserId, FileSize Int64, FileDigest, C.SbKey, C.CbNonce, FileSize Word32, FilePath, Maybe FilePath) :. (FilePath, Maybe C.SbKey, Maybe C.CbNonce, RcvFileStatus, BoolInt, Maybe DBRcvFileId, Maybe RcvFileId, Maybe (FileSize Int64), Maybe FileDigest) -> RcvFile
        toFile ((rcvFileEntityId, userId, size, digest, key, nonce, chunkSize, prefixPath, tmpPath) :. (savePath, saveKey_, saveNonce_, status, BI deleted, redirectDbId, redirectEntityId, redirectSize_, redirectDigest_)) =
          let cfArgs = CFArgs <$> saveKey_ <*> saveNonce_
              saveFile = CryptoFile savePath cfArgs
              redirect =
                RcvFileRedirect
                  <$> redirectDbId
                  <*> redirectEntityId
                  <*> (RedirectFileInfo <$> redirectSize_ <*> redirectDigest_)
           in RcvFile {rcvFileId, rcvFileEntityId, userId, size, digest, key, nonce, chunkSize, redirect, prefixPath, tmpPath, saveFile, status, deleted, chunks = []}
    getChunks :: RcvFileId -> UserId -> FilePath -> IO [RcvFileChunk]
    getChunks rcvFileEntityId userId fileTmpPath = do
      chunks <-
        map toChunk
          <$> DB.query
            db
            [sql|
              SELECT rcv_file_chunk_id, chunk_no, chunk_size, digest, tmp_path
              FROM rcv_file_chunks
              WHERE rcv_file_id = ?
            |]
            (Only rcvFileId)
      forM chunks $ \chunk@RcvFileChunk {rcvChunkId} -> do
        replicas' <- getChunkReplicas rcvChunkId
        pure (chunk {replicas = replicas'} :: RcvFileChunk)
      where
        toChunk :: (Int64, Int, FileSize Word32, FileDigest, Maybe FilePath) -> RcvFileChunk
        toChunk (rcvChunkId, chunkNo, chunkSize, digest, chunkTmpPath) =
          RcvFileChunk {rcvFileId, rcvFileEntityId, userId, rcvChunkId, chunkNo, chunkSize, digest, fileTmpPath, chunkTmpPath, replicas = []}
    getChunkReplicas :: Int64 -> IO [RcvFileChunkReplica]
    getChunkReplicas chunkId = do
      map toReplica
        <$> DB.query
          db
          [sql|
            SELECT
              r.rcv_file_chunk_replica_id, r.replica_id, r.replica_key, r.received, r.delay, r.retries,
              s.xftp_host, s.xftp_port, s.xftp_key_hash
            FROM rcv_file_chunk_replicas r
            JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
            WHERE r.rcv_file_chunk_id = ?
          |]
          (Only chunkId)
      where
        toReplica :: (Int64, ChunkReplicaId, C.APrivateAuthKey, BoolInt, Maybe Int64, Int, NonEmpty TransportHost, ServiceName, C.KeyHash) -> RcvFileChunkReplica
        toReplica (rcvChunkReplicaId, replicaId, replicaKey, BI received, delay, retries, host, port, keyHash) =
          let server = XFTPServer host port keyHash
           in RcvFileChunkReplica {rcvChunkReplicaId, server, replicaId, replicaKey, received, delay, retries}

updateRcvChunkReplicaDelay :: DB.Connection -> Int64 -> Int64 -> IO ()
updateRcvChunkReplicaDelay db replicaId delay = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_file_chunk_replicas SET delay = ?, retries = retries + 1, updated_at = ? WHERE rcv_file_chunk_replica_id = ?" (delay, updatedAt, replicaId)

updateRcvFileChunkReceived :: DB.Connection -> Int64 -> Int64 -> FilePath -> IO ()
updateRcvFileChunkReceived db replicaId chunkId chunkTmpPath = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_file_chunk_replicas SET received = 1, updated_at = ? WHERE rcv_file_chunk_replica_id = ?" (updatedAt, replicaId)
  DB.execute db "UPDATE rcv_file_chunks SET tmp_path = ?, updated_at = ? WHERE rcv_file_chunk_id = ?" (chunkTmpPath, updatedAt, chunkId)

updateRcvFileStatus :: DB.Connection -> DBRcvFileId -> RcvFileStatus -> IO ()
updateRcvFileStatus db rcvFileId status = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_files SET status = ?, updated_at = ? WHERE rcv_file_id = ?" (status, updatedAt, rcvFileId)

updateRcvFileError :: DB.Connection -> DBRcvFileId -> String -> IO ()
updateRcvFileError db rcvFileId errStr = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_files SET tmp_path = NULL, error = ?, status = ?, updated_at = ? WHERE rcv_file_id = ?" (errStr, RFSError, updatedAt, rcvFileId)

updateRcvFileComplete :: DB.Connection -> DBRcvFileId -> IO ()
updateRcvFileComplete db rcvFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_files SET tmp_path = NULL, status = ?, updated_at = ? WHERE rcv_file_id = ?" (RFSComplete, updatedAt, rcvFileId)

updateRcvFileRedirect :: DB.Connection -> DBRcvFileId -> FileDescription 'FRecipient -> IO (Either StoreError ())
updateRcvFileRedirect db rcvFileId FileDescription {key, nonce, chunkSize, chunks} = runExceptT $ do
  updatedAt <- liftIO getCurrentTime
  liftIO $ DB.execute db "UPDATE rcv_files SET key = ?, nonce = ?, chunk_size = ?, updated_at = ? WHERE rcv_file_id = ?" (key, nonce, chunkSize, updatedAt, rcvFileId)
  liftIO $ forM_ chunks $ \fc@FileChunk {replicas} -> do
    chunkId <- insertRcvFileChunk db fc rcvFileId
    forM_ (zip [1 ..] replicas) $ \(rno, replica) -> insertRcvFileChunkReplica db rno replica chunkId

updateRcvFileNoTmpPath :: DB.Connection -> DBRcvFileId -> IO ()
updateRcvFileNoTmpPath db rcvFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_files SET tmp_path = NULL, updated_at = ? WHERE rcv_file_id = ?" (updatedAt, rcvFileId)

updateRcvFileDeleted :: DB.Connection -> DBRcvFileId -> IO ()
updateRcvFileDeleted db rcvFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE rcv_files SET deleted = 1, updated_at = ? WHERE rcv_file_id = ?" (updatedAt, rcvFileId)

deleteRcvFile' :: DB.Connection -> DBRcvFileId -> IO ()
deleteRcvFile' db rcvFileId =
  DB.execute db "DELETE FROM rcv_files WHERE rcv_file_id = ?" (Only rcvFileId)

getNextRcvChunkToDownload :: DB.Connection -> XFTPServer -> NominalDiffTime -> IO (Either StoreError (Maybe (RcvFileChunk, Bool, Maybe RcvFileId)))
getNextRcvChunkToDownload db server@ProtocolServer {host, port, keyHash} ttl = do
  getWorkItem "rcv_file_download" getReplicaId getChunkData (markRcvFileFailed db . snd)
  where
    getReplicaId :: IO (Maybe (Int64, DBRcvFileId))
    getReplicaId = do
      cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
      maybeFirstRow id $
        DB.query
          db
          [sql|
            SELECT r.rcv_file_chunk_replica_id, f.rcv_file_id
            FROM rcv_file_chunk_replicas r
            JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
            JOIN rcv_file_chunks c ON c.rcv_file_chunk_id = r.rcv_file_chunk_id
            JOIN rcv_files f ON f.rcv_file_id = c.rcv_file_id
            WHERE s.xftp_host = ? AND s.xftp_port = ? AND s.xftp_key_hash = ?
              AND r.received = 0 AND r.replica_number = 1
              AND f.status = ? AND f.deleted = 0 AND f.created_at >= ?
              AND f.failed = 0
            ORDER BY r.retries ASC, r.created_at ASC
            LIMIT 1
          |]
          (host, port, keyHash, RFSReceiving, cutoffTs)
    getChunkData :: (Int64, DBRcvFileId) -> IO (Either StoreError (RcvFileChunk, Bool, Maybe RcvFileId))
    getChunkData (rcvFileChunkReplicaId, _fileId) =
      firstRow toChunk SEFileNotFound $
        DB.query
          db
          [sql|
            SELECT
              f.rcv_file_id, f.rcv_file_entity_id, f.user_id, c.rcv_file_chunk_id, c.chunk_no, c.chunk_size, c.digest, f.tmp_path, c.tmp_path,
              r.rcv_file_chunk_replica_id, r.replica_id, r.replica_key, r.received, r.delay, r.retries,
              f.approved_relays, f.redirect_entity_id
            FROM rcv_file_chunk_replicas r
            JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
            JOIN rcv_file_chunks c ON c.rcv_file_chunk_id = r.rcv_file_chunk_id
            JOIN rcv_files f ON f.rcv_file_id = c.rcv_file_id
            WHERE r.rcv_file_chunk_replica_id = ?
          |]
          (Only rcvFileChunkReplicaId)
      where
        toChunk :: ((DBRcvFileId, RcvFileId, UserId, Int64, Int, FileSize Word32, FileDigest, FilePath, Maybe FilePath) :. (Int64, ChunkReplicaId, C.APrivateAuthKey, BoolInt, Maybe Int64, Int) :. (BoolInt, Maybe RcvFileId)) -> (RcvFileChunk, Bool, Maybe RcvFileId)
        toChunk ((rcvFileId, rcvFileEntityId, userId, rcvChunkId, chunkNo, chunkSize, digest, fileTmpPath, chunkTmpPath) :. (rcvChunkReplicaId, replicaId, replicaKey, BI received, delay, retries) :. (BI approvedRelays, redirectEntityId_)) =
          ( RcvFileChunk
              { rcvFileId,
                rcvFileEntityId,
                userId,
                rcvChunkId,
                chunkNo,
                chunkSize,
                digest,
                fileTmpPath,
                chunkTmpPath,
                replicas = [RcvFileChunkReplica {rcvChunkReplicaId, server, replicaId, replicaKey, received, delay, retries}]
              },
            approvedRelays,
            redirectEntityId_
          )

getNextRcvFileToDecrypt :: DB.Connection -> NominalDiffTime -> IO (Either StoreError (Maybe RcvFile))
getNextRcvFileToDecrypt db ttl =
  getWorkItem "rcv_file_decrypt" getFileId (getRcvFile db) (markRcvFileFailed db)
  where
    getFileId :: IO (Maybe DBRcvFileId)
    getFileId = do
      cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
      maybeFirstRow fromOnly $
        DB.query
          db
          [sql|
            SELECT rcv_file_id
            FROM rcv_files
            WHERE status IN (?,?) AND deleted = 0 AND created_at >= ?
              AND failed = 0
            ORDER BY created_at ASC LIMIT 1
          |]
          (RFSReceived, RFSDecrypting, cutoffTs)

markRcvFileFailed :: DB.Connection -> DBRcvFileId -> IO ()
markRcvFileFailed db fileId = do
  DB.execute db "UPDATE rcv_files SET failed = 1 WHERE rcv_file_id = ?" (Only fileId)

getPendingRcvFilesServers :: DB.Connection -> NominalDiffTime -> IO [XFTPServer]
getPendingRcvFilesServers db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  map toXFTPServer
    <$> DB.query
      db
      [sql|
        SELECT DISTINCT
          s.xftp_host, s.xftp_port, s.xftp_key_hash
        FROM rcv_file_chunk_replicas r
        JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
        JOIN rcv_file_chunks c ON c.rcv_file_chunk_id = r.rcv_file_chunk_id
        JOIN rcv_files f ON f.rcv_file_id = c.rcv_file_id
        WHERE r.received = 0 AND r.replica_number = 1
          AND f.status = ? AND f.deleted = 0 AND f.created_at >= ?
      |]
      (RFSReceiving, cutoffTs)

toXFTPServer :: (NonEmpty TransportHost, ServiceName, C.KeyHash) -> XFTPServer
toXFTPServer (host, port, keyHash) = XFTPServer host port keyHash

getCleanupRcvFilesTmpPaths :: DB.Connection -> IO [(DBRcvFileId, RcvFileId, FilePath)]
getCleanupRcvFilesTmpPaths db =
  DB.query
    db
    [sql|
      SELECT rcv_file_id, rcv_file_entity_id, tmp_path
      FROM rcv_files
      WHERE status IN (?,?) AND tmp_path IS NOT NULL
    |]
    (RFSComplete, RFSError)

getCleanupRcvFilesDeleted :: DB.Connection -> IO [(DBRcvFileId, RcvFileId, FilePath)]
getCleanupRcvFilesDeleted db =
  DB.query_
    db
    [sql|
      SELECT rcv_file_id, rcv_file_entity_id, prefix_path
      FROM rcv_files
      WHERE deleted = 1
    |]

getRcvFilesExpired :: DB.Connection -> NominalDiffTime -> IO [(DBRcvFileId, RcvFileId, FilePath)]
getRcvFilesExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.query
    db
    [sql|
      SELECT rcv_file_id, rcv_file_entity_id, prefix_path
      FROM rcv_files
      WHERE created_at < ?
    |]
    (Only cutoffTs)

createSndFile :: DB.Connection -> TVar ChaChaDRG -> UserId -> CryptoFile -> Int -> FilePath -> C.SbKey -> C.CbNonce -> Maybe RedirectFileInfo -> IO (Either StoreError SndFileId)
createSndFile db gVar userId (CryptoFile path cfArgs) numRecipients prefixPath key nonce redirect_ =
  createWithRandomId gVar $ \sndFileEntityId ->
    DB.execute
      db
      "INSERT INTO snd_files (snd_file_entity_id, user_id, path, src_file_key, src_file_nonce, num_recipients, prefix_path, key, nonce, status, redirect_size, redirect_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
      ((Binary sndFileEntityId, userId, path, fileKey <$> cfArgs, fileNonce <$> cfArgs, numRecipients) :. (prefixPath, key, nonce, SFSNew, redirectSize_, redirectDigest_))
  where
    (redirectSize_, redirectDigest_) =
      case redirect_ of
        Nothing -> (Nothing, Nothing)
        Just RedirectFileInfo {size, digest} -> (Just size, Just digest)

getSndFileByEntityId :: DB.Connection -> SndFileId -> IO (Either StoreError SndFile)
getSndFileByEntityId db sndFileEntityId = runExceptT $ do
  sndFileId <- ExceptT $ getSndFileIdByEntityId_ db sndFileEntityId
  ExceptT $ getSndFile db sndFileId

getSndFileIdByEntityId_ :: DB.Connection -> SndFileId -> IO (Either StoreError DBSndFileId)
getSndFileIdByEntityId_ db sndFileEntityId =
  firstRow fromOnly SEFileNotFound $
    DB.query db "SELECT snd_file_id FROM snd_files WHERE snd_file_entity_id = ?" (Only (Binary sndFileEntityId))

getSndFile :: DB.Connection -> DBSndFileId -> IO (Either StoreError SndFile)
getSndFile db sndFileId = runExceptT $ do
  f@SndFile {sndFileEntityId, userId, numRecipients, prefixPath} <- ExceptT getFile
  chunks <- maybe (pure []) (liftIO . getChunks sndFileEntityId userId numRecipients) prefixPath
  pure (f {chunks} :: SndFile)
  where
    getFile :: IO (Either StoreError SndFile)
    getFile = do
      firstRow toFile SEFileNotFound $
        DB.query
          db
          [sql|
            SELECT snd_file_entity_id, user_id, path, src_file_key, src_file_nonce, num_recipients, digest, prefix_path, key, nonce, status, deleted, redirect_size, redirect_digest
            FROM snd_files
            WHERE snd_file_id = ?
          |]
          (Only sndFileId)
      where
        toFile :: (SndFileId, UserId, FilePath, Maybe C.SbKey, Maybe C.CbNonce, Int, Maybe FileDigest, Maybe FilePath, C.SbKey, C.CbNonce) :. (SndFileStatus, BoolInt, Maybe (FileSize Int64), Maybe FileDigest) -> SndFile
        toFile ((sndFileEntityId, userId, srcPath, srcKey_, srcNonce_, numRecipients, digest, prefixPath, key, nonce) :. (status, BI deleted, redirectSize_, redirectDigest_)) =
          let cfArgs = CFArgs <$> srcKey_ <*> srcNonce_
              srcFile = CryptoFile srcPath cfArgs
              redirect = RedirectFileInfo <$> redirectSize_ <*> redirectDigest_
           in SndFile {sndFileId, sndFileEntityId, userId, srcFile, numRecipients, digest, prefixPath, key, nonce, status, deleted, redirect, chunks = []}
    getChunks :: SndFileId -> UserId -> Int -> FilePath -> IO [SndFileChunk]
    getChunks sndFileEntityId userId numRecipients filePrefixPath = do
      chunks <-
        map toChunk
          <$> DB.query
            db
            [sql|
              SELECT snd_file_chunk_id, chunk_no, chunk_offset, chunk_size, digest
              FROM snd_file_chunks
              WHERE snd_file_id = ?
            |]
            (Only sndFileId)
      forM chunks $ \chunk@SndFileChunk {sndChunkId} -> do
        replicas' <- getChunkReplicas sndChunkId
        pure (chunk {replicas = replicas'} :: SndFileChunk)
      where
        toChunk :: (Int64, Int, Int64, Word32, FileDigest) -> SndFileChunk
        toChunk (sndChunkId, chunkNo, chunkOffset, chunkSize, digest) =
          let chunkSpec = XFTPChunkSpec {filePath = sndFileEncPath filePrefixPath, chunkOffset, chunkSize}
           in SndFileChunk {sndFileId, sndFileEntityId, userId, numRecipients, sndChunkId, chunkNo, chunkSpec, filePrefixPath, digest, replicas = []}
    getChunkReplicas :: Int64 -> IO [SndFileChunkReplica]
    getChunkReplicas chunkId = do
      replicas <-
        map toReplica
          <$> DB.query
            db
            [sql|
              SELECT
                r.snd_file_chunk_replica_id, r.replica_id, r.replica_key, r.replica_status, r.delay, r.retries,
                s.xftp_host, s.xftp_port, s.xftp_key_hash
              FROM snd_file_chunk_replicas r
              JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
              WHERE r.snd_file_chunk_id = ?
            |]
            (Only chunkId)
      forM replicas $ \replica@SndFileChunkReplica {sndChunkReplicaId} -> do
        rcvIdsKeys <- getChunkReplicaRecipients_ db sndChunkReplicaId
        pure (replica :: SndFileChunkReplica) {rcvIdsKeys}
      where
        toReplica :: (Int64, ChunkReplicaId, C.APrivateAuthKey, SndFileReplicaStatus, Maybe Int64, Int, NonEmpty TransportHost, ServiceName, C.KeyHash) -> SndFileChunkReplica
        toReplica (sndChunkReplicaId, replicaId, replicaKey, replicaStatus, delay, retries, host, port, keyHash) =
          let server = XFTPServer host port keyHash
           in SndFileChunkReplica {sndChunkReplicaId, server, replicaId, replicaKey, replicaStatus, delay, retries, rcvIdsKeys = []}

getChunkReplicaRecipients_ :: DB.Connection -> Int64 -> IO [(ChunkReplicaId, C.APrivateAuthKey)]
getChunkReplicaRecipients_ db replicaId =
  DB.query
    db
    [sql|
      SELECT rcv_replica_id, rcv_replica_key
      FROM snd_file_chunk_replica_recipients
      WHERE snd_file_chunk_replica_id = ?
    |]
    (Only replicaId)

getNextSndFileToPrepare :: DB.Connection -> NominalDiffTime -> IO (Either StoreError (Maybe SndFile))
getNextSndFileToPrepare db ttl =
  getWorkItem "snd_file_prepare" getFileId (getSndFile db) (markSndFileFailed db)
  where
    getFileId :: IO (Maybe DBSndFileId)
    getFileId = do
      cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
      maybeFirstRow fromOnly $
        DB.query
          db
          [sql|
            SELECT snd_file_id
            FROM snd_files
            WHERE status IN (?,?,?) AND deleted = 0 AND created_at >= ?
              AND failed = 0
            ORDER BY created_at ASC LIMIT 1
          |]
          (SFSNew, SFSEncrypting, SFSEncrypted, cutoffTs)

markSndFileFailed :: DB.Connection -> DBSndFileId -> IO ()
markSndFileFailed db fileId =
  DB.execute db "UPDATE snd_files SET failed = 1 WHERE snd_file_id = ?" (Only fileId)

updateSndFileError :: DB.Connection -> DBSndFileId -> String -> IO ()
updateSndFileError db sndFileId errStr = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET prefix_path = NULL, error = ?, status = ?, updated_at = ? WHERE snd_file_id = ?" (errStr, SFSError, updatedAt, sndFileId)

updateSndFileStatus :: DB.Connection -> DBSndFileId -> SndFileStatus -> IO ()
updateSndFileStatus db sndFileId status = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET status = ?, updated_at = ? WHERE snd_file_id = ?" (status, updatedAt, sndFileId)

updateSndFileEncrypted :: DB.Connection -> DBSndFileId -> FileDigest -> [(XFTPChunkSpec, FileDigest)] -> IO ()
updateSndFileEncrypted db sndFileId digest chunkSpecsDigests = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET status = ?, digest = ?, updated_at = ? WHERE snd_file_id = ?" (SFSEncrypted, digest, updatedAt, sndFileId)
  forM_ (zip [1 ..] chunkSpecsDigests) $ \(chunkNo :: Int, (XFTPChunkSpec {chunkOffset, chunkSize}, chunkDigest)) ->
    DB.execute db "INSERT INTO snd_file_chunks (snd_file_id, chunk_no, chunk_offset, chunk_size, digest) VALUES (?,?,?,?,?)" (sndFileId, chunkNo, chunkOffset, chunkSize, chunkDigest)

updateSndFileComplete :: DB.Connection -> DBSndFileId -> IO ()
updateSndFileComplete db sndFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET prefix_path = NULL, status = ?, updated_at = ? WHERE snd_file_id = ?" (SFSComplete, updatedAt, sndFileId)

updateSndFileNoPrefixPath :: DB.Connection -> DBSndFileId -> IO ()
updateSndFileNoPrefixPath db sndFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET prefix_path = NULL, updated_at = ? WHERE snd_file_id = ?" (updatedAt, sndFileId)

updateSndFileDeleted :: DB.Connection -> DBSndFileId -> IO ()
updateSndFileDeleted db sndFileId = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_files SET deleted = 1, updated_at = ? WHERE snd_file_id = ?" (updatedAt, sndFileId)

deleteSndFile' :: DB.Connection -> DBSndFileId -> IO ()
deleteSndFile' db sndFileId =
  DB.execute db "DELETE FROM snd_files WHERE snd_file_id = ?" (Only sndFileId)

getSndFileDeleted :: DB.Connection -> DBSndFileId -> IO Bool
getSndFileDeleted db sndFileId =
  fromMaybe True
    <$> maybeFirstRow fromOnlyBI (DB.query db "SELECT deleted FROM snd_files WHERE snd_file_id = ?" (Only sndFileId))

createSndFileReplica :: DB.Connection -> SndFileChunk -> NewSndChunkReplica -> IO ()
createSndFileReplica db SndFileChunk {sndChunkId} = createSndFileReplica_ db sndChunkId

createSndFileReplica_ :: DB.Connection -> Int64 -> NewSndChunkReplica -> IO ()
createSndFileReplica_ db sndChunkId NewSndChunkReplica {server, replicaId, replicaKey, rcvIdsKeys} = do
  srvId <- createXFTPServer_ db server
  DB.execute
    db
    [sql|
      INSERT INTO snd_file_chunk_replicas
        (snd_file_chunk_id, replica_number, xftp_server_id, replica_id, replica_key, replica_status)
      VALUES (?,?,?,?,?,?)
    |]
    (sndChunkId, 1 :: Int, srvId, replicaId, replicaKey, SFRSCreated)
  rId <- insertedRowId db
  forM_ rcvIdsKeys $ \(rcvId, rcvKey) -> do
    DB.execute
      db
      [sql|
        INSERT INTO snd_file_chunk_replica_recipients
          (snd_file_chunk_replica_id, rcv_replica_id, rcv_replica_key)
        VALUES (?,?,?)
      |]
      (rId, rcvId, rcvKey)

getNextSndChunkToUpload :: DB.Connection -> XFTPServer -> NominalDiffTime -> IO (Either StoreError (Maybe SndFileChunk))
getNextSndChunkToUpload db server@ProtocolServer {host, port, keyHash} ttl = do
  getWorkItem "snd_file_upload" getReplicaId getChunkData (markSndFileFailed db . snd)
  where
    getReplicaId :: IO (Maybe (Int64, DBSndFileId))
    getReplicaId = do
      cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
      maybeFirstRow id $
        DB.query
          db
          [sql|
            SELECT r.snd_file_chunk_replica_id, f.snd_file_id
            FROM snd_file_chunk_replicas r
            JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
            JOIN snd_file_chunks c ON c.snd_file_chunk_id = r.snd_file_chunk_id
            JOIN snd_files f ON f.snd_file_id = c.snd_file_id
            WHERE s.xftp_host = ? AND s.xftp_port = ? AND s.xftp_key_hash = ?
              AND r.replica_status = ? AND r.replica_number = 1
              AND (f.status = ? OR f.status = ?) AND f.deleted = 0 AND f.created_at >= ?
              AND f.failed = 0
            ORDER BY r.retries ASC, r.created_at ASC
            LIMIT 1
          |]
          (host, port, keyHash, SFRSCreated, SFSEncrypted, SFSUploading, cutoffTs)
    getChunkData :: (Int64, DBSndFileId) -> IO (Either StoreError SndFileChunk)
    getChunkData (sndFileChunkReplicaId, _fileId) = do
      chunk_ <-
        firstRow toChunk SEFileNotFound $
          DB.query
            db
            [sql|
              SELECT
                f.snd_file_id, f.snd_file_entity_id, f.user_id, f.num_recipients, f.prefix_path,
                c.snd_file_chunk_id, c.chunk_no, c.chunk_offset, c.chunk_size, c.digest,
                r.snd_file_chunk_replica_id, r.replica_id, r.replica_key, r.replica_status, r.delay, r.retries
              FROM snd_file_chunk_replicas r
              JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
              JOIN snd_file_chunks c ON c.snd_file_chunk_id = r.snd_file_chunk_id
              JOIN snd_files f ON f.snd_file_id = c.snd_file_id
              WHERE r.snd_file_chunk_replica_id = ?
            |]
            (Only sndFileChunkReplicaId)
      forM chunk_ $ \chunk@SndFileChunk {replicas} -> do
        replicas' <- forM replicas $ \replica@SndFileChunkReplica {sndChunkReplicaId} -> do
          rcvIdsKeys <- getChunkReplicaRecipients_ db sndChunkReplicaId
          pure (replica :: SndFileChunkReplica) {rcvIdsKeys}
        pure (chunk {replicas = replicas'} :: SndFileChunk)
      where
        toChunk :: ((DBSndFileId, SndFileId, UserId, Int, FilePath) :. (Int64, Int, Int64, Word32, FileDigest) :. (Int64, ChunkReplicaId, C.APrivateAuthKey, SndFileReplicaStatus, Maybe Int64, Int)) -> SndFileChunk
        toChunk ((sndFileId, sndFileEntityId, userId, numRecipients, filePrefixPath) :. (sndChunkId, chunkNo, chunkOffset, chunkSize, digest) :. (sndChunkReplicaId, replicaId, replicaKey, replicaStatus, delay, retries)) =
          let chunkSpec = XFTPChunkSpec {filePath = sndFileEncPath filePrefixPath, chunkOffset, chunkSize}
           in SndFileChunk
                { sndFileId,
                  sndFileEntityId,
                  userId,
                  numRecipients,
                  sndChunkId,
                  chunkNo,
                  chunkSpec,
                  digest,
                  filePrefixPath,
                  replicas = [SndFileChunkReplica {sndChunkReplicaId, server, replicaId, replicaKey, replicaStatus, delay, retries, rcvIdsKeys = []}]
                }

updateSndChunkReplicaDelay :: DB.Connection -> Int64 -> Int64 -> IO ()
updateSndChunkReplicaDelay db replicaId delay = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_file_chunk_replicas SET delay = ?, retries = retries + 1, updated_at = ? WHERE snd_file_chunk_replica_id = ?" (delay, updatedAt, replicaId)

addSndChunkReplicaRecipients :: DB.Connection -> SndFileChunkReplica -> [(ChunkReplicaId, C.APrivateAuthKey)] -> IO SndFileChunkReplica
addSndChunkReplicaRecipients db r@SndFileChunkReplica {sndChunkReplicaId} rcvIdsKeys = do
  forM_ rcvIdsKeys $ \(rcvId, rcvKey) -> do
    DB.execute
      db
      [sql|
        INSERT INTO snd_file_chunk_replica_recipients
          (snd_file_chunk_replica_id, rcv_replica_id, rcv_replica_key)
        VALUES (?,?,?)
      |]
      (sndChunkReplicaId, rcvId, rcvKey)
  rcvIdsKeys' <- getChunkReplicaRecipients_ db sndChunkReplicaId
  pure (r :: SndFileChunkReplica) {rcvIdsKeys = rcvIdsKeys'}

updateSndChunkReplicaStatus :: DB.Connection -> Int64 -> SndFileReplicaStatus -> IO ()
updateSndChunkReplicaStatus db replicaId status = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE snd_file_chunk_replicas SET replica_status = ?, updated_at = ? WHERE snd_file_chunk_replica_id = ?" (status, updatedAt, replicaId)

getPendingSndFilesServers :: DB.Connection -> NominalDiffTime -> IO [XFTPServer]
getPendingSndFilesServers db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  map toXFTPServer
    <$> DB.query
      db
      [sql|
        SELECT DISTINCT
          s.xftp_host, s.xftp_port, s.xftp_key_hash
        FROM snd_file_chunk_replicas r
        JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
        JOIN snd_file_chunks c ON c.snd_file_chunk_id = r.snd_file_chunk_id
        JOIN snd_files f ON f.snd_file_id = c.snd_file_id
        WHERE r.replica_status = ? AND r.replica_number = 1
          AND (f.status = ? OR f.status = ?) AND f.deleted = 0 AND f.created_at >= ?
      |]
      (SFRSCreated, SFSEncrypted, SFSUploading, cutoffTs)

getCleanupSndFilesPrefixPaths :: DB.Connection -> IO [(DBSndFileId, SndFileId, FilePath)]
getCleanupSndFilesPrefixPaths db =
  DB.query
    db
    [sql|
      SELECT snd_file_id, snd_file_entity_id, prefix_path
      FROM snd_files
      WHERE status IN (?,?) AND prefix_path IS NOT NULL
    |]
    (SFSComplete, SFSError)

getCleanupSndFilesDeleted :: DB.Connection -> IO [(DBSndFileId, SndFileId, Maybe FilePath)]
getCleanupSndFilesDeleted db =
  DB.query_
    db
    [sql|
      SELECT snd_file_id, snd_file_entity_id, prefix_path
      FROM snd_files
      WHERE deleted = 1
    |]

getSndFilesExpired :: DB.Connection -> NominalDiffTime -> IO [(DBSndFileId, SndFileId, Maybe FilePath)]
getSndFilesExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.query
    db
    [sql|
      SELECT snd_file_id, snd_file_entity_id, prefix_path
      FROM snd_files
      WHERE created_at < ?
    |]
    (Only cutoffTs)

createDeletedSndChunkReplica :: DB.Connection -> UserId -> FileChunkReplica -> FileDigest -> IO ()
createDeletedSndChunkReplica db userId FileChunkReplica {server, replicaId, replicaKey} chunkDigest = do
  srvId <- createXFTPServer_ db server
  DB.execute
    db
    "INSERT INTO deleted_snd_chunk_replicas (user_id, xftp_server_id, replica_id, replica_key, chunk_digest) VALUES (?,?,?,?,?)"
    (userId, srvId, replicaId, replicaKey, chunkDigest)

getDeletedSndChunkReplica :: DB.Connection -> DBSndFileId -> IO (Either StoreError DeletedSndChunkReplica)
getDeletedSndChunkReplica db deletedSndChunkReplicaId =
  firstRow toReplica SEDeletedSndChunkReplicaNotFound $
    DB.query
      db
      [sql|
        SELECT
          r.user_id, r.replica_id, r.replica_key, r.chunk_digest, r.delay, r.retries,
          s.xftp_host, s.xftp_port, s.xftp_key_hash
        FROM deleted_snd_chunk_replicas r
        JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
        WHERE r.deleted_snd_chunk_replica_id = ?
      |]
      (Only deletedSndChunkReplicaId)
  where
    toReplica :: (UserId, ChunkReplicaId, C.APrivateAuthKey, FileDigest, Maybe Int64, Int, NonEmpty TransportHost, ServiceName, C.KeyHash) -> DeletedSndChunkReplica
    toReplica (userId, replicaId, replicaKey, chunkDigest, delay, retries, host, port, keyHash) =
      let server = XFTPServer host port keyHash
       in DeletedSndChunkReplica {deletedSndChunkReplicaId, userId, server, replicaId, replicaKey, chunkDigest, delay, retries}

getNextDeletedSndChunkReplica :: DB.Connection -> XFTPServer -> NominalDiffTime -> IO (Either StoreError (Maybe DeletedSndChunkReplica))
getNextDeletedSndChunkReplica db ProtocolServer {host, port, keyHash} ttl =
  getWorkItem "deleted replica" getReplicaId (getDeletedSndChunkReplica db) markReplicaFailed
  where
    getReplicaId :: IO (Maybe Int64)
    getReplicaId = do
      cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
      maybeFirstRow fromOnly $
        DB.query
          db
          [sql|
            SELECT r.deleted_snd_chunk_replica_id
            FROM deleted_snd_chunk_replicas r
            JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
            WHERE s.xftp_host = ? AND s.xftp_port = ? AND s.xftp_key_hash = ?
              AND r.created_at >= ?
              AND failed = 0
            ORDER BY r.retries ASC, r.created_at ASC
            LIMIT 1
          |]
          (host, port, keyHash, cutoffTs)
    markReplicaFailed :: Int64 -> IO ()
    markReplicaFailed replicaId = do
      DB.execute db "UPDATE deleted_snd_chunk_replicas SET failed = 1 WHERE deleted_snd_chunk_replica_id = ?" (Only replicaId)

updateDeletedSndChunkReplicaDelay :: DB.Connection -> Int64 -> Int64 -> IO ()
updateDeletedSndChunkReplicaDelay db deletedSndChunkReplicaId delay = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE deleted_snd_chunk_replicas SET delay = ?, retries = retries + 1, updated_at = ? WHERE deleted_snd_chunk_replica_id = ?" (delay, updatedAt, deletedSndChunkReplicaId)

deleteDeletedSndChunkReplica :: DB.Connection -> Int64 -> IO ()
deleteDeletedSndChunkReplica db deletedSndChunkReplicaId =
  DB.execute db "DELETE FROM deleted_snd_chunk_replicas WHERE deleted_snd_chunk_replica_id = ?" (Only deletedSndChunkReplicaId)

getPendingDelFilesServers :: DB.Connection -> NominalDiffTime -> IO [XFTPServer]
getPendingDelFilesServers db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  map toXFTPServer
    <$> DB.query
      db
      [sql|
        SELECT DISTINCT
          s.xftp_host, s.xftp_port, s.xftp_key_hash
        FROM deleted_snd_chunk_replicas r
        JOIN xftp_servers s ON s.xftp_server_id = r.xftp_server_id
        WHERE r.created_at >= ?
      |]
      (Only cutoffTs)

deleteDeletedSndChunkReplicasExpired :: DB.Connection -> NominalDiffTime -> IO ()
deleteDeletedSndChunkReplicasExpired db ttl = do
  cutoffTs <- addUTCTime (-ttl) <$> getCurrentTime
  DB.execute db "DELETE FROM deleted_snd_chunk_replicas WHERE created_at < ?" (Only cutoffTs)

updateServersStats :: DB.Connection -> AgentPersistedServerStats -> IO ()
updateServersStats db stats = do
  updatedAt <- getCurrentTime
  DB.execute db "UPDATE servers_stats SET servers_stats = ?, updated_at = ? WHERE servers_stats_id = 1" (stats, updatedAt)

getServersStats :: DB.Connection -> IO (Either StoreError (UTCTime, Maybe AgentPersistedServerStats))
getServersStats db =
  firstRow id SEServersStatsNotFound $
    DB.query_ db "SELECT started_at, servers_stats FROM servers_stats WHERE servers_stats_id = 1"

resetServersStats :: DB.Connection -> UTCTime -> IO ()
resetServersStats db startedAt =
  DB.execute db "UPDATE servers_stats SET servers_stats = NULL, started_at = ?, updated_at = ? WHERE servers_stats_id = 1" (startedAt, startedAt)
