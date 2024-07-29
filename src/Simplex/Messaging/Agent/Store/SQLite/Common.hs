{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Agent.Store.SQLite.Common
  ( SQLiteStore (..),
    withConnection,
    withConnection',
    withTransaction,
    withTransaction',
    dbBusyLoop,
    storeKey,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (retry)
import Data.ByteArray (ScrubbedBytes)
import qualified Data.ByteArray as BA
import Database.SQLite.Simple (SQLError)
import qualified Database.SQLite.Simple as SQL
import qualified Simplex.Messaging.Agent.Store.SQLite.DB as DB
import Simplex.Messaging.Util (ifM, unlessM)
import qualified UnliftIO.Exception as E
import UnliftIO.MVar
import UnliftIO.STM

storeKey :: ScrubbedBytes -> Bool -> Maybe ScrubbedBytes
storeKey key keepKey = if keepKey || BA.null key then Just key else Nothing

data SQLiteStore = SQLiteStore
  { dbFilePath :: FilePath,
    dbKey :: TVar (Maybe ScrubbedBytes),
    dbSem :: TVar Int,
    dbConnection :: MVar DB.Connection,
    dbClosed :: TVar Bool,
    dbNew :: Bool
  }

withConnection_ :: SQLiteStore -> Bool -> (DB.Connection -> IO a) -> IO a
withConnection_ SQLiteStore {dbSem, dbConnection} priority action
  | priority = E.bracket_ signal release $ withMVar dbConnection action
  | otherwise = lowPriority
  where
    lowPriority = wait >> withMVar dbConnection (\db -> ifM free (action db) lowPriority)
    signal = atomically $ modifyTVar' dbSem (+ 1)
    release = atomically $ modifyTVar' dbSem $ \sem -> if sem > 0 then sem - 1 else 0
    wait = atomically $ unlessM ((0 ==) <$> readTVar dbSem) retry
    free = (0 ==) <$> readTVarIO dbSem

withConnection :: SQLiteStore -> (DB.Connection -> IO a) -> IO a
withConnection st = withConnection_ st False

withConnection' :: SQLiteStore -> (SQL.Connection -> IO a) -> IO a
withConnection' st action = withConnection st $ action . DB.conn

withTransaction' :: SQLiteStore -> (SQL.Connection -> IO a) -> IO a
withTransaction' st action = withTransaction st $ action . DB.conn

withTransaction :: SQLiteStore -> (DB.Connection -> IO a) -> IO a
withTransaction st = withTransaction_ st False
{-# INLINE withTransaction #-}

withPriorityTransaction :: SQLiteStore -> (DB.Connection -> IO a) -> IO a
withPriorityTransaction st = withTransaction_ st True
{-# INLINE withPriorityTransaction #-}

withTransaction_ :: SQLiteStore -> Bool -> (DB.Connection -> IO a) -> IO a
withTransaction_ st priority action = withConnection_ st priority $ dbBusyLoop . transaction
  where
    transaction db@DB.Connection {conn} = SQL.withImmediateTransaction conn $ action db

dbBusyLoop :: forall a. IO a -> IO a
dbBusyLoop action = loop 500 3000000
  where
    loop :: Int -> Int -> IO a
    loop t tLim =
      action `E.catch` \(e :: SQLError) ->
        let se = SQL.sqlError e
         in if tLim > t && (se == SQL.ErrorBusy || se == SQL.ErrorLocked)
              then do
                threadDelay t
                loop (t * 9 `div` 8) (tLim - t)
              else E.throwIO e
