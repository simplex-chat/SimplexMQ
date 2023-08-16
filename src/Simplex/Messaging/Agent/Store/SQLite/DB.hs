{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE StrictData #-}

module Simplex.Messaging.Agent.Store.SQLite.DB
  ( Connection (..),
    SlowQueryStats (..),
    open,
    close,
    execute,
    execute_,
    executeNamed,
    executeMany,
    query,
    query_,
    queryNamed,
  )
where

import Control.Concurrent.STM
import Control.Monad (when)
import Data.Aeson (ToJSON (..))
import qualified Data.Aeson as J
import Data.Int (Int64)
import Data.Time (diffUTCTime, getCurrentTime)
import Database.SQLite.Simple (FromRow, NamedParam, Query, ToRow)
import qualified Database.SQLite.Simple as SQL
import GHC.Generics (Generic)
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (diffToMilliseconds)

data Connection = Connection
  { conn :: SQL.Connection,
    slow :: TMap Query SlowQueryStats
  }

data SlowQueryStats = QueryStats
  { count :: Int64,
    timeMax :: Int64,
    timeAvgApprx :: Int64
  }
  deriving (Show, Generic)

instance ToJSON SlowQueryStats where toEncoding = J.genericToEncoding J.defaultOptions

timeIt :: TMap Query SlowQueryStats -> Query -> IO a -> IO a
timeIt slow sql a = do
  t <- getCurrentTime
  r <- a
  t' <- getCurrentTime
  let diff = diffToMilliseconds $ diffUTCTime t' t
  atomically $ when (diff > 50) $ TM.alter (updateQueryStats diff) sql slow
  pure r
  where
    updateQueryStats :: Int64 -> Maybe SlowQueryStats -> Maybe SlowQueryStats
    updateQueryStats diff Nothing = Just $ QueryStats 1 diff diff
    updateQueryStats diff (Just QueryStats {count, timeMax, timeAvgApprx}) =
      Just $
        QueryStats
          { count = count + 1,
            timeMax = max timeMax diff,
            timeAvgApprx = (timeAvgApprx * count + diff) `div` (count + 1)
          }

open :: String -> IO Connection
open f = do
  conn <- SQL.open f
  slow <- atomically $ TM.empty
  pure Connection {conn, slow}

close :: Connection -> IO ()
close = SQL.close . conn

execute :: ToRow q => Connection -> Query -> q -> IO ()
execute Connection {conn, slow} sql = timeIt slow sql . SQL.execute conn sql
{-# INLINE execute #-}

execute_ :: Connection -> Query -> IO ()
execute_ Connection {conn, slow} sql = timeIt slow sql $ SQL.execute_ conn sql
{-# INLINE execute_ #-}

executeNamed :: Connection -> Query -> [NamedParam] -> IO ()
executeNamed Connection {conn, slow} sql = timeIt slow sql . SQL.executeNamed conn sql
{-# INLINE executeNamed #-}

executeMany :: ToRow q => Connection -> Query -> [q] -> IO ()
executeMany Connection {conn, slow} sql = timeIt slow sql . SQL.executeMany conn sql
{-# INLINE executeMany #-}

query :: (ToRow q, FromRow r) => Connection -> Query -> q -> IO [r]
query Connection {conn, slow} sql = timeIt slow sql . SQL.query conn sql
{-# INLINE query #-}

query_ :: FromRow r => Connection -> Query -> IO [r]
query_ Connection {conn, slow} sql = timeIt slow sql $ SQL.query_ conn sql
{-# INLINE query_ #-}

queryNamed :: FromRow r => Connection -> Query -> [NamedParam] -> IO [r]
queryNamed Connection {conn, slow} sql = timeIt slow sql . SQL.queryNamed conn sql
{-# INLINE queryNamed #-}
