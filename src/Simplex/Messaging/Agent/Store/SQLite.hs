{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Simplex.Messaging.Agent.Store.SQLite
  ( SQLiteStore (..),
    createSQLiteStore,
    connectSQLiteStore,
    withConnection,
    withTransaction,
    fromTextField_,
    firstRow,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Exception (bracket)
import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Crypto.Random (ChaChaDRG, randomBytesGenerate)
import Data.Bifunctor (second)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Base64.URL as U
import Data.Char (toLower)
import Data.Functor (($>))
import Data.List (find, foldl')
import qualified Data.Map.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (decodeLatin1)
import Database.SQLite.Simple (FromRow, NamedParam (..), Only (..), SQLData (..), SQLError, ToRow, field)
import qualified Database.SQLite.Simple as DB
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.Internal (Field (..))
import Database.SQLite.Simple.Ok (Ok (Ok))
import Database.SQLite.Simple.QQ (sql)
import Database.SQLite.Simple.ToField (ToField (..))
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite.Migrations (Migration)
import qualified Simplex.Messaging.Agent.Store.SQLite.Migrations as Migrations
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet (RatchetX448, SkippedMsgDiff (..), SkippedMsgKeys)
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (blobFieldParser)
import Simplex.Messaging.Protocol (MsgBody)
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Util (bshow, liftIOEither)
import System.Directory (copyFile, createDirectoryIfMissing, doesFileExist)
import System.Exit (exitFailure)
import System.FilePath (takeDirectory)
import System.IO (hFlush, stdout)
import qualified UnliftIO.Exception as E

-- * SQLite Store implementation

data SQLiteStore = SQLiteStore
  { dbFilePath :: FilePath,
    dbConnPool :: TBQueue DB.Connection,
    dbNew :: Bool
  }

createSQLiteStore :: FilePath -> Int -> [Migration] -> Bool -> IO SQLiteStore
createSQLiteStore dbFilePath poolSize migrations yesToMigrations = do
  let dbDir = takeDirectory dbFilePath
  createDirectoryIfMissing False dbDir
  st <- connectSQLiteStore dbFilePath poolSize
  withConnection st $ \db -> DB.execute_ db "VACUUM;"
  -- _printPragmas st
  checkThreadsafe st
  migrateSchema st migrations yesToMigrations
  pure st

_printPragmas :: SQLiteStore -> IO ()
_printPragmas st@SQLiteStore {dbFilePath} = withConnection st $ \db -> do
  foreign_keys <- DB.query_ db "PRAGMA foreign_keys;" :: IO [[Int]]
  print $ dbFilePath <> " foreign_keys: " <> show foreign_keys
  -- when run via sqlite-simple query for trusted_schema seems to return empty list
  trusted_schema <- DB.query_ db "PRAGMA trusted_schema;" :: IO [[Int]]
  print $ dbFilePath <> " trusted_schema: " <> show trusted_schema
  secure_delete <- DB.query_ db "PRAGMA secure_delete;" :: IO [[Int]]
  print $ dbFilePath <> " secure_delete: " <> show secure_delete
  auto_vacuum <- DB.query_ db "PRAGMA auto_vacuum;" :: IO [[Int]]
  print $ dbFilePath <> " auto_vacuum: " <> show auto_vacuum

checkThreadsafe :: SQLiteStore -> IO ()
checkThreadsafe st = withConnection st $ \db -> do
  compileOptions <- DB.query_ db "pragma COMPILE_OPTIONS;" :: IO [[Text]]
  let threadsafeOption = find (T.isPrefixOf "THREADSAFE=") (concat compileOptions)
  case threadsafeOption of
    Just "THREADSAFE=0" -> confirmOrExit "SQLite compiled with non-threadsafe code."
    Nothing -> putStrLn "Warning: SQLite THREADSAFE compile option not found"
    _ -> return ()

migrateSchema :: SQLiteStore -> [Migration] -> Bool -> IO ()
migrateSchema st migrations yesToMigrations = withConnection st $ \db -> do
  Migrations.initialize db
  Migrations.get db migrations >>= \case
    Left e -> confirmOrExit $ "Database error: " <> e
    Right [] -> pure ()
    Right ms -> do
      unless (dbNew st) $ do
        unless yesToMigrations $
          confirmOrExit "The app has a newer version than the database - it will be backed up and upgraded."
        let f = dbFilePath st
        copyFile f (f <> ".bak")
      Migrations.run db ms

confirmOrExit :: String -> IO ()
confirmOrExit s = do
  putStrLn s
  putStr "Continue (y/N): "
  hFlush stdout
  ok <- getLine
  when (map toLower ok /= "y") exitFailure

connectSQLiteStore :: FilePath -> Int -> IO SQLiteStore
connectSQLiteStore dbFilePath poolSize = do
  dbNew <- not <$> doesFileExist dbFilePath
  dbConnPool <- newTBQueueIO $ toEnum poolSize
  replicateM_ poolSize $
    connectDB dbFilePath >>= atomically . writeTBQueue dbConnPool
  pure SQLiteStore {dbFilePath, dbConnPool, dbNew}

connectDB :: FilePath -> IO DB.Connection
connectDB path = do
  dbConn <- DB.open path
  DB.execute_ dbConn "PRAGMA foreign_keys = ON;"
  -- DB.execute_ dbConn "PRAGMA trusted_schema = OFF;"
  DB.execute_ dbConn "PRAGMA secure_delete = ON;"
  DB.execute_ dbConn "PRAGMA auto_vacuum = FULL;"
  pure dbConn

checkConstraint :: StoreError -> IO (Either StoreError a) -> IO (Either StoreError a)
checkConstraint err action = action `E.catch` (pure . Left . handleSQLError err)

handleSQLError :: StoreError -> SQLError -> StoreError
handleSQLError err e
  | DB.sqlError e == DB.ErrorConstraint = err
  | otherwise = SEInternal $ bshow e

withConnection :: SQLiteStore -> (DB.Connection -> IO a) -> IO a
withConnection SQLiteStore {dbConnPool} =
  bracket
    (atomically $ readTBQueue dbConnPool)
    (atomically . writeTBQueue dbConnPool)

withTransaction :: forall a. SQLiteStore -> (DB.Connection -> IO a) -> IO a
withTransaction st action = withConnection st $ loop 100 100_000
  where
    loop :: Int -> Int -> DB.Connection -> IO a
    loop t tLim db =
      DB.withImmediateTransaction db (action db) `E.catch` \(e :: SQLError) ->
        if tLim > t && DB.sqlError e == DB.ErrorBusy
          then do
            threadDelay t
            loop (t * 9 `div` 8) (tLim - t) db
          else E.throwIO e

createConn_ ::
  (MonadUnliftIO m, MonadError StoreError m) =>
  SQLiteStore ->
  TVar ChaChaDRG ->
  ConnData ->
  (DB.Connection -> ByteString -> IO ()) ->
  m ByteString
createConn_ st gVar cData create =
  liftIOEither . checkConstraint SEConnDuplicate . withTransaction st $ \db ->
    case cData of
      ConnData {connId = ""} -> createWithRandomId gVar $ create db
      ConnData {connId} -> create db connId $> Right connId

instance (MonadUnliftIO m, MonadError StoreError m) => MonadAgentStore SQLiteStore m where
  createRcvConn :: SQLiteStore -> TVar ChaChaDRG -> ConnData -> RcvQueue -> SConnectionMode c -> m ConnId
  createRcvConn st gVar cData q@RcvQueue {server} cMode =
    createConn_ st gVar cData $ \db connId -> do
      upsertServer_ db server
      DB.execute db "INSERT INTO connections (conn_id, conn_mode) VALUES (?, ?)" (connId, cMode)
      insertRcvQueue_ db connId q

  createSndConn :: SQLiteStore -> TVar ChaChaDRG -> ConnData -> SndQueue -> m ConnId
  createSndConn st gVar cData q@SndQueue {server} =
    createConn_ st gVar cData $ \db connId -> do
      upsertServer_ db server
      DB.execute db "INSERT INTO connections (conn_id, conn_mode) VALUES (?, ?)" (connId, SCMInvitation)
      insertSndQueue_ db connId q

  getConn :: SQLiteStore -> ConnId -> m SomeConn
  getConn st connId =
    liftIOEither . withTransaction st $ \db ->
      getConn_ db connId

  getRcvConn :: SQLiteStore -> SMPServer -> SMP.RecipientId -> m SomeConn
  getRcvConn st SMPServer {host, port} rcvId =
    liftIOEither . withTransaction st $ \db ->
      DB.queryNamed
        db
        [sql|
          SELECT q.conn_id
          FROM rcv_queues q
          WHERE q.host = :host AND q.port = :port AND q.rcv_id = :rcv_id;
        |]
        [":host" := host, ":port" := port, ":rcv_id" := rcvId]
        >>= \case
          [Only connId] -> getConn_ db connId
          _ -> pure $ Left SEConnNotFound

  deleteConn :: SQLiteStore -> ConnId -> m ()
  deleteConn st connId =
    liftIO . withTransaction st $ \db ->
      DB.executeNamed
        db
        "DELETE FROM connections WHERE conn_id = :conn_id;"
        [":conn_id" := connId]

  upgradeRcvConnToDuplex :: SQLiteStore -> ConnId -> SndQueue -> m ()
  upgradeRcvConnToDuplex st connId sq@SndQueue {server} =
    liftIOEither . withTransaction st $ \db ->
      getConn_ db connId >>= \case
        Right (SomeConn _ RcvConnection {}) -> do
          upsertServer_ db server
          insertSndQueue_ db connId sq
          pure $ Right ()
        Right (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
        _ -> pure $ Left SEConnNotFound

  upgradeSndConnToDuplex :: SQLiteStore -> ConnId -> RcvQueue -> m ()
  upgradeSndConnToDuplex st connId rq@RcvQueue {server} =
    liftIOEither . withTransaction st $ \db ->
      getConn_ db connId >>= \case
        Right (SomeConn _ SndConnection {}) -> do
          upsertServer_ db server
          insertRcvQueue_ db connId rq
          pure $ Right ()
        Right (SomeConn c _) -> pure . Left . SEBadConnType $ connType c
        _ -> pure $ Left SEConnNotFound

  setRcvQueueStatus :: SQLiteStore -> RcvQueue -> QueueStatus -> m ()
  setRcvQueueStatus st RcvQueue {rcvId, server = SMPServer {host, port}} status =
    -- ? throw error if queue does not exist?
    liftIO . withTransaction st $ \db ->
      DB.executeNamed
        db
        [sql|
          UPDATE rcv_queues
          SET status = :status
          WHERE host = :host AND port = :port AND rcv_id = :rcv_id;
        |]
        [":status" := status, ":host" := host, ":port" := port, ":rcv_id" := rcvId]

  setRcvQueueConfirmedE2E :: SQLiteStore -> RcvQueue -> C.DhSecretX25519 -> m ()
  setRcvQueueConfirmedE2E st RcvQueue {rcvId, server = SMPServer {host, port}} e2eDhSecret =
    liftIO . withTransaction st $ \db ->
      DB.executeNamed
        db
        [sql|
          UPDATE rcv_queues
          SET e2e_dh_secret = :e2e_dh_secret,
              status = :status
          WHERE host = :host AND port = :port AND rcv_id = :rcv_id
        |]
        [ ":status" := Confirmed,
          ":e2e_dh_secret" := e2eDhSecret,
          ":host" := host,
          ":port" := port,
          ":rcv_id" := rcvId
        ]

  setSndQueueStatus :: SQLiteStore -> SndQueue -> QueueStatus -> m ()
  setSndQueueStatus st SndQueue {sndId, server = SMPServer {host, port}} status =
    -- ? throw error if queue does not exist?
    liftIO . withTransaction st $ \db ->
      DB.executeNamed
        db
        [sql|
          UPDATE snd_queues
          SET status = :status
          WHERE host = :host AND port = :port AND snd_id = :snd_id;
        |]
        [":status" := status, ":host" := host, ":port" := port, ":snd_id" := sndId]

  createConfirmation :: SQLiteStore -> TVar ChaChaDRG -> NewConfirmation -> m ConfirmationId
  createConfirmation st gVar NewConfirmation {connId, senderConf = SMPConfirmation {senderKey, e2ePubKey, connInfo}, ratchetState} =
    liftIOEither . withTransaction st $ \db ->
      createWithRandomId gVar $ \confirmationId ->
        DB.execute
          db
          [sql|
            INSERT INTO conn_confirmations
            (confirmation_id, conn_id, sender_key, e2e_snd_pub_key, ratchet_state, sender_conn_info, accepted) VALUES (?, ?, ?, ?, ?, ?, 0);
          |]
          (confirmationId, connId, senderKey, e2ePubKey, ratchetState, connInfo)

  acceptConfirmation :: SQLiteStore -> ConfirmationId -> ConnInfo -> m AcceptedConfirmation
  acceptConfirmation st confirmationId ownConnInfo =
    liftIOEither . withTransaction st $ \db -> do
      DB.executeNamed
        db
        [sql|
          UPDATE conn_confirmations
          SET accepted = 1,
              own_conn_info = :own_conn_info
          WHERE confirmation_id = :confirmation_id;
        |]
        [ ":own_conn_info" := ownConnInfo,
          ":confirmation_id" := confirmationId
        ]
      firstRow confirmation SEConfirmationNotFound $
        DB.query
          db
          [sql|
            SELECT conn_id, sender_key, e2e_snd_pub_key, ratchet_state, sender_conn_info
            FROM conn_confirmations
            WHERE confirmation_id = ?;
          |]
          (Only confirmationId)
    where
      confirmation (connId, senderKey, e2ePubKey, ratchetState, connInfo) =
        AcceptedConfirmation
          { confirmationId,
            connId,
            senderConf = SMPConfirmation {senderKey, e2ePubKey, connInfo},
            ratchetState,
            ownConnInfo
          }

  getAcceptedConfirmation :: SQLiteStore -> ConnId -> m AcceptedConfirmation
  getAcceptedConfirmation st connId =
    liftIOEither . withTransaction st $ \db ->
      firstRow confirmation SEConfirmationNotFound $
        DB.query
          db
          [sql|
            SELECT confirmation_id, sender_key, e2e_snd_pub_key, ratchet_state, sender_conn_info, own_conn_info
            FROM conn_confirmations
            WHERE conn_id = ? AND accepted = 1;
          |]
          (Only connId)
    where
      confirmation (confirmationId, senderKey, e2ePubKey, ratchetState, connInfo, ownConnInfo) =
        AcceptedConfirmation
          { confirmationId,
            connId,
            senderConf = SMPConfirmation {senderKey, e2ePubKey, connInfo},
            ratchetState,
            ownConnInfo
          }

  removeConfirmations :: SQLiteStore -> ConnId -> m ()
  removeConfirmations st connId =
    liftIO . withTransaction st $ \db ->
      DB.executeNamed
        db
        [sql|
          DELETE FROM conn_confirmations
          WHERE conn_id = :conn_id;
        |]
        [":conn_id" := connId]

  createInvitation :: SQLiteStore -> TVar ChaChaDRG -> NewInvitation -> m InvitationId
  createInvitation st gVar NewInvitation {contactConnId, connReq, recipientConnInfo} =
    liftIOEither . withTransaction st $ \db ->
      createWithRandomId gVar $ \invitationId ->
        DB.execute
          db
          [sql|
            INSERT INTO conn_invitations
            (invitation_id,  contact_conn_id, cr_invitation, recipient_conn_info, accepted) VALUES (?, ?, ?, ?, 0);
          |]
          (invitationId, contactConnId, connReq, recipientConnInfo)

  getInvitation :: SQLiteStore -> InvitationId -> m Invitation
  getInvitation st invitationId =
    liftIOEither . withTransaction st $ \db ->
      firstRow invitation SEInvitationNotFound $
        DB.query
          db
          [sql|
            SELECT contact_conn_id, cr_invitation, recipient_conn_info, own_conn_info, accepted
            FROM conn_invitations
            WHERE invitation_id = ?
              AND accepted = 0
          |]
          (Only invitationId)
    where
      invitation (contactConnId, connReq, recipientConnInfo, ownConnInfo, accepted) =
        Invitation {invitationId, contactConnId, connReq, recipientConnInfo, ownConnInfo, accepted}

  acceptInvitation :: SQLiteStore -> InvitationId -> ConnInfo -> m ()
  acceptInvitation st invitationId ownConnInfo =
    liftIO . withTransaction st $ \db -> do
      DB.executeNamed
        db
        [sql|
          UPDATE conn_invitations
          SET accepted = 1,
              own_conn_info = :own_conn_info
          WHERE invitation_id = :invitation_id
        |]
        [ ":own_conn_info" := ownConnInfo,
          ":invitation_id" := invitationId
        ]

  deleteInvitation :: SQLiteStore -> ConnId -> InvitationId -> m ()
  deleteInvitation st contactConnId invId =
    liftIOEither . withTransaction st $ \db ->
      runExceptT $
        ExceptT (getConn_ db contactConnId) >>= \case
          SomeConn SCContact _ ->
            liftIO $ DB.execute db "DELETE FROM conn_invitations WHERE contact_conn_id = ? AND invitation_id = ?" (contactConnId, invId)
          _ -> throwError SEConnNotFound

  updateRcvIds :: SQLiteStore -> ConnId -> m (InternalId, InternalRcvId, PrevExternalSndId, PrevRcvMsgHash)
  updateRcvIds st connId =
    liftIO . withTransaction st $ \db -> do
      (lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash) <- retrieveLastIdsAndHashRcv_ db connId
      let internalId = InternalId $ unId lastInternalId + 1
          internalRcvId = InternalRcvId $ unRcvId lastInternalRcvId + 1
      updateLastIdsRcv_ db connId internalId internalRcvId
      pure (internalId, internalRcvId, lastExternalSndId, lastRcvHash)

  createRcvMsg :: SQLiteStore -> ConnId -> RcvMsgData -> m ()
  createRcvMsg st connId rcvMsgData =
    liftIO . withTransaction st $ \db -> do
      insertRcvMsgBase_ db connId rcvMsgData
      insertRcvMsgDetails_ db connId rcvMsgData
      updateHashRcv_ db connId rcvMsgData

  updateSndIds :: SQLiteStore -> ConnId -> m (InternalId, InternalSndId, PrevSndMsgHash)
  updateSndIds st connId =
    liftIO . withTransaction st $ \db -> do
      (lastInternalId, lastInternalSndId, prevSndHash) <- retrieveLastIdsAndHashSnd_ db connId
      let internalId = InternalId $ unId lastInternalId + 1
          internalSndId = InternalSndId $ unSndId lastInternalSndId + 1
      updateLastIdsSnd_ db connId internalId internalSndId
      pure (internalId, internalSndId, prevSndHash)

  createSndMsg :: SQLiteStore -> ConnId -> SndMsgData -> m ()
  createSndMsg st connId sndMsgData =
    liftIO . withTransaction st $ \db -> do
      insertSndMsgBase_ db connId sndMsgData
      insertSndMsgDetails_ db connId sndMsgData
      updateHashSnd_ db connId sndMsgData

  getPendingMsgData :: SQLiteStore -> ConnId -> InternalId -> m (Maybe RcvQueue, (AgentMessageType, MsgBody, InternalTs))
  getPendingMsgData st connId msgId =
    liftIOEither . withTransaction st $ \db -> runExceptT $ do
      rq_ <- liftIO $ getRcvQueueByConnId_ db connId
      msgData <-
        ExceptT . firstRow id SEMsgNotFound $
          DB.query
            db
            [sql|
                SELECT m.msg_type, m.msg_body, m.internal_ts
                FROM messages m
                JOIN snd_messages s ON s.conn_id = m.conn_id AND s.internal_id = m.internal_id
                WHERE m.conn_id = ? AND m.internal_id = ?
              |]
            (connId, msgId)
      pure (rq_, msgData)

  getPendingMsgs :: SQLiteStore -> ConnId -> m [InternalId]
  getPendingMsgs st connId =
    liftIO . withTransaction st $ \db ->
      map fromOnly
        <$> DB.query db "SELECT internal_id FROM snd_messages WHERE conn_id = ?" (Only connId)

  checkRcvMsg :: SQLiteStore -> ConnId -> InternalId -> m ()
  checkRcvMsg st connId msgId =
    liftIOEither . withTransaction st $ \db ->
      hasMsg
        <$> DB.query
          db
          [sql|
            SELECT conn_id, internal_id
            FROM rcv_messages
            WHERE conn_id = ? AND internal_id = ?
          |]
          (connId, msgId)
    where
      hasMsg :: [(ConnId, InternalId)] -> Either StoreError ()
      hasMsg r = if null r then Left SEMsgNotFound else Right ()

  deleteMsg :: SQLiteStore -> ConnId -> InternalId -> m ()
  deleteMsg st connId msgId =
    liftIO . withTransaction st $ \db ->
      DB.execute db "DELETE FROM messages WHERE conn_id = ? AND internal_id = ?;" (connId, msgId)

  createRatchetX3dhKeys :: SQLiteStore -> ConnId -> C.PrivateKeyX448 -> C.PrivateKeyX448 -> m ()
  createRatchetX3dhKeys st connId x3dhPrivKey1 x3dhPrivKey2 =
    liftIO . withTransaction st $ \db ->
      DB.execute db "INSERT INTO ratchets (conn_id, x3dh_priv_key_1, x3dh_priv_key_2) VALUES (?, ?, ?)" (connId, x3dhPrivKey1, x3dhPrivKey2)

  getRatchetX3dhKeys :: SQLiteStore -> ConnId -> m (C.PrivateKeyX448, C.PrivateKeyX448)
  getRatchetX3dhKeys st connId =
    liftIOEither . withTransaction st $ \db ->
      fmap hasKeys $
        firstRow id SEX3dhKeysNotFound $
          DB.query db "SELECT x3dh_priv_key_1, x3dh_priv_key_2 FROM ratchets WHERE conn_id = ?" (Only connId)
    where
      hasKeys = \case
        Right (Just k1, Just k2) -> Right (k1, k2)
        _ -> Left SEX3dhKeysNotFound

  createRatchet :: SQLiteStore -> ConnId -> RatchetX448 -> m ()
  createRatchet st connId rc =
    liftIO . withTransaction st $ \db -> do
      DB.executeNamed
        db
        [sql|
          INSERT INTO ratchets (conn_id, ratchet_state)
          VALUES (:conn_id, :ratchet_state)
          ON CONFLICT (conn_id) DO UPDATE SET
            ratchet_state = :ratchet_state,
            x3dh_priv_key_1 = NULL,
            x3dh_priv_key_2 = NULL
        |]
        [":conn_id" := connId, ":ratchet_state" := rc]

  getRatchet :: SQLiteStore -> ConnId -> m RatchetX448
  getRatchet st connId =
    liftIOEither . withTransaction st $ \db ->
      ratchet
        <$> DB.query db "SELECT ratchet_state FROM ratchets WHERE conn_id = ?" (Only connId)
    where
      ratchet (Only (Just rc) : _) = Right rc
      ratchet _ = Left SERatchetNotFound

  getSkippedMsgKeys :: SQLiteStore -> ConnId -> m SkippedMsgKeys
  getSkippedMsgKeys st connId =
    liftIO . withTransaction st $ \db ->
      skipped <$> DB.query db "SELECT header_key, msg_n, msg_key FROM skipped_messages WHERE conn_id = ?" (Only connId)
    where
      skipped ms = foldl' addSkippedKey M.empty ms
      addSkippedKey smks (hk, msgN, mk) = M.alter (Just . addMsgKey) hk smks
        where
          addMsgKey = maybe (M.singleton msgN mk) (M.insert msgN mk)

  updateRatchet :: SQLiteStore -> ConnId -> RatchetX448 -> SkippedMsgDiff -> m ()
  updateRatchet st connId rc skipped =
    liftIO . withTransaction st $ \db -> do
      DB.execute db "UPDATE ratchets SET ratchet_state = ? WHERE conn_id = ?" (rc, connId)
      case skipped of
        SMDNoChange -> pure ()
        SMDRemove hk msgN ->
          DB.execute db "DELETE FROM skipped_messages WHERE conn_id = ? AND header_key = ? AND msg_n = ?" (connId, hk, msgN)
        SMDAdd smks ->
          forM_ (M.assocs smks) $ \(hk, mks) ->
            forM_ (M.assocs mks) $ \(msgN, mk) ->
              DB.execute db "INSERT INTO skipped_messages (conn_id, header_key, msg_n, msg_key) VALUES (?, ?, ?, ?)" (connId, hk, msgN, mk)

-- * Auxiliary helpers

instance ToField QueueStatus where toField = toField . serializeQueueStatus

instance FromField QueueStatus where fromField = fromTextField_ queueStatusT

instance ToField InternalRcvId where toField (InternalRcvId x) = toField x

instance FromField InternalRcvId where fromField x = InternalRcvId <$> fromField x

instance ToField InternalSndId where toField (InternalSndId x) = toField x

instance FromField InternalSndId where fromField x = InternalSndId <$> fromField x

instance ToField InternalId where toField (InternalId x) = toField x

instance FromField InternalId where fromField x = InternalId <$> fromField x

instance ToField AgentMessageType where toField = toField . smpEncode

instance FromField AgentMessageType where fromField = blobFieldParser smpP

instance ToField MsgIntegrity where toField = toField . strEncode

instance FromField MsgIntegrity where fromField = blobFieldParser strP

instance ToField SMPQueueUri where toField = toField . strEncode

instance FromField SMPQueueUri where fromField = blobFieldParser strP

instance ToField AConnectionRequestUri where toField = toField . strEncode

instance FromField AConnectionRequestUri where fromField = blobFieldParser strP

instance ConnectionModeI c => ToField (ConnectionRequestUri c) where toField = toField . strEncode

instance (E.Typeable c, ConnectionModeI c) => FromField (ConnectionRequestUri c) where fromField = blobFieldParser strP

instance ToField ConnectionMode where toField = toField . decodeLatin1 . strEncode

instance FromField ConnectionMode where fromField = fromTextField_ connModeT

instance ToField (SConnectionMode c) where toField = toField . connMode

instance FromField AConnectionMode where fromField = fromTextField_ $ fmap connMode' . connModeT

fromTextField_ :: (E.Typeable a) => (Text -> Maybe a) -> Field -> Ok a
fromTextField_ fromText = \case
  f@(Field (SQLText t) _) ->
    case fromText t of
      Just x -> Ok x
      _ -> returnError ConversionFailed f ("invalid text: " <> T.unpack t)
  f -> returnError ConversionFailed f "expecting SQLText column type"

listToEither :: e -> [a] -> Either e a
listToEither _ (x : _) = Right x
listToEither e _ = Left e

firstRow :: (a -> b) -> e -> IO [a] -> IO (Either e b)
firstRow f e a = second f . listToEither e <$> a

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

-- * Server upsert helper

upsertServer_ :: DB.Connection -> SMPServer -> IO ()
upsertServer_ dbConn SMPServer {host, port, keyHash} = do
  DB.executeNamed
    dbConn
    [sql|
      INSERT INTO servers (host, port, key_hash) VALUES (:host,:port,:key_hash)
      ON CONFLICT (host, port) DO UPDATE SET
        host=excluded.host,
        port=excluded.port,
        key_hash=excluded.key_hash;
    |]
    [":host" := host, ":port" := port, ":key_hash" := keyHash]

-- * createRcvConn helpers

insertRcvQueue_ :: DB.Connection -> ConnId -> RcvQueue -> IO ()
insertRcvQueue_ dbConn connId RcvQueue {..} = do
  DB.execute
    dbConn
    [sql|
      INSERT INTO rcv_queues
        ( host, port, rcv_id, conn_id, rcv_private_key, rcv_dh_secret, e2e_priv_key, e2e_dh_secret, snd_id, status) VALUES (?,?,?,?,?,?,?,?,?,?);
    |]
    (host server, port server, rcvId, connId, rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret, sndId, status)

-- * createSndConn helpers

insertSndQueue_ :: DB.Connection -> ConnId -> SndQueue -> IO ()
insertSndQueue_ dbConn connId SndQueue {..} = do
  DB.execute
    dbConn
    [sql|
      INSERT INTO snd_queues
        (host, port, snd_id, conn_id, snd_public_key, snd_private_key, e2e_pub_key, e2e_dh_secret, status) VALUES (?,?,?,?,?, ?,?, ?,?);
    |]
    (host server, port server, sndId, connId, sndPublicKey, sndPrivateKey, e2ePubKey, e2eDhSecret, status)

-- * getConn helpers

getConn_ :: DB.Connection -> ConnId -> IO (Either StoreError SomeConn)
getConn_ dbConn connId =
  getConnData_ dbConn connId >>= \case
    Nothing -> pure $ Left SEConnNotFound
    Just (connData, cMode) -> do
      rQ <- getRcvQueueByConnId_ dbConn connId
      sQ <- getSndQueueByConnId_ dbConn connId
      pure $ case (rQ, sQ, cMode) of
        (Just rcvQ, Just sndQ, CMInvitation) -> Right $ SomeConn SCDuplex (DuplexConnection connData rcvQ sndQ)
        (Just rcvQ, Nothing, CMInvitation) -> Right $ SomeConn SCRcv (RcvConnection connData rcvQ)
        (Nothing, Just sndQ, CMInvitation) -> Right $ SomeConn SCSnd (SndConnection connData sndQ)
        (Just rcvQ, Nothing, CMContact) -> Right $ SomeConn SCContact (ContactConnection connData rcvQ)
        _ -> Left SEConnNotFound

getConnData_ :: DB.Connection -> ConnId -> IO (Maybe (ConnData, ConnectionMode))
getConnData_ dbConn connId' =
  connData
    <$> DB.query dbConn "SELECT conn_id, conn_mode FROM connections WHERE conn_id = ?;" (Only connId')
  where
    connData [(connId, cMode)] = Just (ConnData {connId}, cMode)
    connData _ = Nothing

getRcvQueueByConnId_ :: DB.Connection -> ConnId -> IO (Maybe RcvQueue)
getRcvQueueByConnId_ dbConn connId =
  rcvQueue
    <$> DB.query
      dbConn
      [sql|
        SELECT s.key_hash, q.host, q.port, q.rcv_id, q.rcv_private_key, q.rcv_dh_secret,
          q.e2e_priv_key, q.e2e_dh_secret, q.snd_id, q.status
        FROM rcv_queues q
        INNER JOIN servers s ON q.host = s.host AND q.port = s.port
        WHERE q.conn_id = ?;
      |]
      (Only connId)
  where
    rcvQueue [(keyHash, host, port, rcvId, rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret, sndId, status)] =
      let server = SMPServer host port keyHash
       in Just RcvQueue {server, rcvId, rcvPrivateKey, rcvDhSecret, e2ePrivKey, e2eDhSecret, sndId, status}
    rcvQueue _ = Nothing

getSndQueueByConnId_ :: DB.Connection -> ConnId -> IO (Maybe SndQueue)
getSndQueueByConnId_ dbConn connId =
  sndQueue
    <$> DB.query
      dbConn
      [sql|
        SELECT s.key_hash, q.host, q.port, q.snd_id, q.snd_public_key, q.snd_private_key, q.e2e_pub_key, q.e2e_dh_secret, q.status
        FROM snd_queues q
        INNER JOIN servers s ON q.host = s.host AND q.port = s.port
        WHERE q.conn_id = ?;
      |]
      (Only connId)
  where
    sndQueue [(keyHash, host, port, sndId, sndPublicKey, sndPrivateKey, e2ePubKey, e2eDhSecret, status)] =
      let server = SMPServer host port keyHash
       in Just SndQueue {server, sndId, sndPublicKey, sndPrivateKey, e2ePubKey, e2eDhSecret, status}
    sndQueue _ = Nothing

-- * updateRcvIds helpers

retrieveLastIdsAndHashRcv_ :: DB.Connection -> ConnId -> IO (InternalId, InternalRcvId, PrevExternalSndId, PrevRcvMsgHash)
retrieveLastIdsAndHashRcv_ dbConn connId = do
  [(lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash)] <-
    DB.queryNamed
      dbConn
      [sql|
        SELECT last_internal_msg_id, last_internal_rcv_msg_id, last_external_snd_msg_id, last_rcv_msg_hash
        FROM connections
        WHERE conn_id = :conn_id;
      |]
      [":conn_id" := connId]
  return (lastInternalId, lastInternalRcvId, lastExternalSndId, lastRcvHash)

updateLastIdsRcv_ :: DB.Connection -> ConnId -> InternalId -> InternalRcvId -> IO ()
updateLastIdsRcv_ dbConn connId newInternalId newInternalRcvId =
  DB.executeNamed
    dbConn
    [sql|
      UPDATE connections
      SET last_internal_msg_id = :last_internal_msg_id,
          last_internal_rcv_msg_id = :last_internal_rcv_msg_id
      WHERE conn_id = :conn_id;
    |]
    [ ":last_internal_msg_id" := newInternalId,
      ":last_internal_rcv_msg_id" := newInternalRcvId,
      ":conn_id" := connId
    ]

-- * createRcvMsg helpers

insertRcvMsgBase_ :: DB.Connection -> ConnId -> RcvMsgData -> IO ()
insertRcvMsgBase_ dbConn connId RcvMsgData {msgMeta, msgType, msgBody, internalRcvId} = do
  let MsgMeta {recipient = (internalId, internalTs)} = msgMeta
  DB.executeNamed
    dbConn
    [sql|
      INSERT INTO messages
        ( conn_id, internal_id, internal_ts, internal_rcv_id, internal_snd_id, msg_type, msg_body)
      VALUES
        (:conn_id,:internal_id,:internal_ts,:internal_rcv_id,            NULL,:msg_type, :msg_body);
    |]
    [ ":conn_id" := connId,
      ":internal_id" := internalId,
      ":internal_ts" := internalTs,
      ":internal_rcv_id" := internalRcvId,
      ":msg_type" := msgType,
      ":msg_body" := msgBody
    ]

insertRcvMsgDetails_ :: DB.Connection -> ConnId -> RcvMsgData -> IO ()
insertRcvMsgDetails_ dbConn connId RcvMsgData {msgMeta, internalRcvId, internalHash, externalPrevSndHash} = do
  let MsgMeta {integrity, recipient, broker, sndMsgId} = msgMeta
  DB.executeNamed
    dbConn
    [sql|
      INSERT INTO rcv_messages
        ( conn_id, internal_rcv_id, internal_id, external_snd_id,
          broker_id, broker_ts,
          internal_hash, external_prev_snd_hash, integrity)
      VALUES
        (:conn_id,:internal_rcv_id,:internal_id,:external_snd_id,
         :broker_id,:broker_ts,
         :internal_hash,:external_prev_snd_hash,:integrity);
    |]
    [ ":conn_id" := connId,
      ":internal_rcv_id" := internalRcvId,
      ":internal_id" := fst recipient,
      ":external_snd_id" := sndMsgId,
      ":broker_id" := fst broker,
      ":broker_ts" := snd broker,
      ":internal_hash" := internalHash,
      ":external_prev_snd_hash" := externalPrevSndHash,
      ":integrity" := integrity
    ]

updateHashRcv_ :: DB.Connection -> ConnId -> RcvMsgData -> IO ()
updateHashRcv_ dbConn connId RcvMsgData {msgMeta, internalHash, internalRcvId} =
  DB.executeNamed
    dbConn
    -- last_internal_rcv_msg_id equality check prevents race condition in case next id was reserved
    [sql|
      UPDATE connections
      SET last_external_snd_msg_id = :last_external_snd_msg_id,
          last_rcv_msg_hash = :last_rcv_msg_hash
      WHERE conn_id = :conn_id
        AND last_internal_rcv_msg_id = :last_internal_rcv_msg_id;
    |]
    [ ":last_external_snd_msg_id" := sndMsgId (msgMeta :: MsgMeta),
      ":last_rcv_msg_hash" := internalHash,
      ":conn_id" := connId,
      ":last_internal_rcv_msg_id" := internalRcvId
    ]

-- * updateSndIds helpers

retrieveLastIdsAndHashSnd_ :: DB.Connection -> ConnId -> IO (InternalId, InternalSndId, PrevSndMsgHash)
retrieveLastIdsAndHashSnd_ dbConn connId = do
  [(lastInternalId, lastInternalSndId, lastSndHash)] <-
    DB.queryNamed
      dbConn
      [sql|
        SELECT last_internal_msg_id, last_internal_snd_msg_id, last_snd_msg_hash
        FROM connections
        WHERE conn_id = :conn_id;
      |]
      [":conn_id" := connId]
  return (lastInternalId, lastInternalSndId, lastSndHash)

updateLastIdsSnd_ :: DB.Connection -> ConnId -> InternalId -> InternalSndId -> IO ()
updateLastIdsSnd_ dbConn connId newInternalId newInternalSndId =
  DB.executeNamed
    dbConn
    [sql|
      UPDATE connections
      SET last_internal_msg_id = :last_internal_msg_id,
          last_internal_snd_msg_id = :last_internal_snd_msg_id
      WHERE conn_id = :conn_id;
    |]
    [ ":last_internal_msg_id" := newInternalId,
      ":last_internal_snd_msg_id" := newInternalSndId,
      ":conn_id" := connId
    ]

-- * createSndMsg helpers

insertSndMsgBase_ :: DB.Connection -> ConnId -> SndMsgData -> IO ()
insertSndMsgBase_ dbConn connId SndMsgData {..} = do
  DB.executeNamed
    dbConn
    [sql|
      INSERT INTO messages
        ( conn_id, internal_id, internal_ts, internal_rcv_id, internal_snd_id, msg_type, msg_body)
      VALUES
        (:conn_id,:internal_id,:internal_ts,            NULL,:internal_snd_id,:msg_type, :msg_body);
    |]
    [ ":conn_id" := connId,
      ":internal_id" := internalId,
      ":internal_ts" := internalTs,
      ":internal_snd_id" := internalSndId,
      ":msg_type" := msgType,
      ":msg_body" := msgBody
    ]

insertSndMsgDetails_ :: DB.Connection -> ConnId -> SndMsgData -> IO ()
insertSndMsgDetails_ dbConn connId SndMsgData {..} =
  DB.executeNamed
    dbConn
    [sql|
      INSERT INTO snd_messages
        ( conn_id, internal_snd_id, internal_id, internal_hash, previous_msg_hash)
      VALUES
        (:conn_id,:internal_snd_id,:internal_id,:internal_hash,:previous_msg_hash);
    |]
    [ ":conn_id" := connId,
      ":internal_snd_id" := internalSndId,
      ":internal_id" := internalId,
      ":internal_hash" := internalHash,
      ":previous_msg_hash" := prevMsgHash
    ]

updateHashSnd_ :: DB.Connection -> ConnId -> SndMsgData -> IO ()
updateHashSnd_ dbConn connId SndMsgData {..} =
  DB.executeNamed
    dbConn
    -- last_internal_snd_msg_id equality check prevents race condition in case next id was reserved
    [sql|
      UPDATE connections
      SET last_snd_msg_hash = :last_snd_msg_hash
      WHERE conn_id = :conn_id
        AND last_internal_snd_msg_id = :last_internal_snd_msg_id;
    |]
    [ ":last_snd_msg_hash" := internalHash,
      ":conn_id" := connId,
      ":last_internal_snd_msg_id" := internalSndId
    ]

-- create record with a random ID
createWithRandomId :: TVar ChaChaDRG -> (ByteString -> IO ()) -> IO (Either StoreError ByteString)
createWithRandomId gVar create = tryCreate 3
  where
    tryCreate :: Int -> IO (Either StoreError ByteString)
    tryCreate 0 = pure $ Left SEUniqueID
    tryCreate n = do
      id' <- randomId gVar 12
      E.try (create id') >>= \case
        Right _ -> pure $ Right id'
        Left e
          | DB.sqlError e == DB.ErrorConstraint -> tryCreate (n - 1)
          | otherwise -> pure . Left . SEInternal $ bshow e

randomId :: TVar ChaChaDRG -> Int -> IO ByteString
randomId gVar n = U.encode <$> (atomically . stateTVar gVar $ randomBytesGenerate n)
