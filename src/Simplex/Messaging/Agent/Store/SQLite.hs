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

module Simplex.Messaging.Agent.Store.SQLite
  ( DBOpts (..),
    Migrations.appMigrations,
    Migrations.getCurrentMigrations,
    createDBStore,
    closeDBStore,
    reopenDBStore,
    execSQL,
    -- used in Simplex.Chat.Archive
    sqlString,
    keyString,
    storeKey,
    -- used in tests
    connectSQLiteStore,
    openSQLiteStore,
  )
where

import Control.Monad
import Data.ByteArray (ScrubbedBytes)
import qualified Data.ByteArray as BA
import Data.Functor (($>))
import Data.IORef
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Database.SQLite.Simple (Query (..))
import qualified Database.SQLite.Simple as SQL
import Database.SQLite.Simple.QQ (sql)
import qualified Database.SQLite3 as SQLite3
import Simplex.Messaging.Agent.Store.Migrations (DBMigrate (..), sharedMigrateSchema)
import qualified Simplex.Messaging.Agent.Store.SQLite.Migrations as Migrations
import Simplex.Messaging.Agent.Store.SQLite.Common
import qualified Simplex.Messaging.Agent.Store.SQLite.DB as DB
import Simplex.Messaging.Agent.Store.Shared (Migration (..), MigrationConfirmation (..), MigrationError (..))
import Simplex.Messaging.Util (ifM, safeDecodeUtf8)
import System.Directory (copyFile, createDirectoryIfMissing, doesFileExist)
import System.FilePath (takeDirectory)
import UnliftIO.Exception (bracketOnError, onException)
import UnliftIO.MVar
import UnliftIO.STM

-- * SQLite Store implementation

createDBStore :: DBOpts -> [Migration] -> MigrationConfirmation -> IO (Either MigrationError DBStore)
createDBStore DBOpts {dbFilePath, dbKey, keepKey, track, vacuum} migrations confirmMigrations = do
  let dbDir = takeDirectory dbFilePath
  createDirectoryIfMissing True dbDir
  st <- connectSQLiteStore dbFilePath dbKey keepKey track
  r <- migrateSchema st `onException` closeDBStore st
  case r of
    Right () -> pure $ Right st
    Left e -> closeDBStore st $> Left e
  where
    migrateSchema st =
      let initialize = Migrations.initialize st
          getCurrent = withTransaction st Migrations.getCurrentMigrations
          run = Migrations.run st vacuum
          backup = copyFile dbFilePath (dbFilePath <> ".bak")
          dbm = DBMigrate {initialize, getCurrent, run, backup}
       in sharedMigrateSchema dbm (dbNew st) migrations confirmMigrations

connectSQLiteStore :: FilePath -> ScrubbedBytes -> Bool -> DB.TrackQueries -> IO DBStore
connectSQLiteStore dbFilePath key keepKey track = do
  dbNew <- not <$> doesFileExist dbFilePath
  dbConn <- dbBusyLoop (connectDB dbFilePath key track)
  dbConnection <- newMVar dbConn
  dbKey <- newTVarIO $! storeKey key keepKey
  dbClosed <- newTVarIO False
  dbSem <- newTVarIO 0
  pure DBStore {dbFilePath, dbKey, dbSem, dbConnection, dbNew, dbClosed}

connectDB :: FilePath -> ScrubbedBytes -> DB.TrackQueries -> IO DB.Connection
connectDB path key track = do
  db <- DB.open path track
  prepare db `onException` DB.close db
  -- _printPragmas db path
  pure db
  where
    prepare db = do
      let exec = SQLite3.exec $ SQL.connectionHandle $ DB.conn db
      unless (BA.null key) . exec $ "PRAGMA key = " <> keyString key <> ";"
      exec . fromQuery $
        [sql|
          PRAGMA busy_timeout = 100;
          PRAGMA foreign_keys = ON;
          -- PRAGMA trusted_schema = OFF;
          PRAGMA secure_delete = ON;
          PRAGMA auto_vacuum = FULL;
        |]

closeDBStore :: DBStore -> IO ()
closeDBStore st@DBStore {dbClosed} =
  ifM (readTVarIO dbClosed) (putStrLn "closeDBStore: already closed") $
    withConnection st $ \conn -> do
      DB.close conn
      atomically $ writeTVar dbClosed True

openSQLiteStore :: DBStore -> ScrubbedBytes -> Bool -> IO ()
openSQLiteStore st@DBStore {dbClosed} key keepKey =
  ifM (readTVarIO dbClosed) (openSQLiteStore_ st key keepKey) (putStrLn "openSQLiteStore: already opened")

openSQLiteStore_ :: DBStore -> ScrubbedBytes -> Bool -> IO ()
openSQLiteStore_ DBStore {dbConnection, dbFilePath, dbKey, dbClosed} key keepKey =
  bracketOnError
    (takeMVar dbConnection)
    (tryPutMVar dbConnection)
    $ \DB.Connection {slow, track} -> do
      DB.Connection {conn} <- connectDB dbFilePath key track
      atomically $ do
        writeTVar dbClosed False
        writeTVar dbKey $! storeKey key keepKey
      putMVar dbConnection DB.Connection {conn, slow, track}

reopenDBStore :: DBStore -> IO ()
reopenDBStore st@DBStore {dbKey, dbClosed} =
  ifM (readTVarIO dbClosed) open (putStrLn "reopenDBStore: already opened")
  where
    open =
      readTVarIO dbKey >>= \case
        Just key -> openSQLiteStore_ st key True
        Nothing -> fail "reopenDBStore: no key"

keyString :: ScrubbedBytes -> Text
keyString = sqlString . safeDecodeUtf8 . BA.convert

sqlString :: Text -> Text
sqlString s = quote <> T.replace quote "''" s <> quote
  where
    quote = "'"

-- _printPragmas :: DB.Connection -> FilePath -> IO ()
-- _printPragmas db path = do
--   foreign_keys <- DB.query_ db "PRAGMA foreign_keys;" :: IO [[Int]]
--   print $ path <> " foreign_keys: " <> show foreign_keys
--   -- when run via sqlite-simple query for trusted_schema seems to return empty list
--   trusted_schema <- DB.query_ db "PRAGMA trusted_schema;" :: IO [[Int]]
--   print $ path <> " trusted_schema: " <> show trusted_schema
--   secure_delete <- DB.query_ db "PRAGMA secure_delete;" :: IO [[Int]]
--   print $ path <> " secure_delete: " <> show secure_delete
--   auto_vacuum <- DB.query_ db "PRAGMA auto_vacuum;" :: IO [[Int]]
--   print $ path <> " auto_vacuum: " <> show auto_vacuum

execSQL :: DB.Connection -> Text -> IO [Text]
execSQL db query = do
  rs <- newIORef []
  SQLite3.execWithCallback (SQL.connectionHandle $ DB.conn db) query (addSQLResultRow rs)
  reverse <$> readIORef rs

addSQLResultRow :: IORef [Text] -> SQLite3.ColumnIndex -> [Text] -> [Maybe Text] -> IO ()
addSQLResultRow rs _count names values = modifyIORef' rs $ \case
  [] -> [showValues values, T.intercalate "|" names]
  rs' -> showValues values : rs'
  where
    showValues = T.intercalate "|" . map (fromMaybe "")
