{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}

module Simplex.Messaging.Agent.Store.SQLite.DB
  ( BoolInt (..),
    Binary (..),
    Connection (..),
    SlowQueryStats (..),
    TrackQueries (..),
    FromField (..),
    ToField (..),
    open,
    close,
    execute,
    execute_,
    executeMany,
    query,
    query_,
    blobFieldDecoder,
    fromTextField_,
  )
where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad (when)
import qualified Data.Aeson.TH as J
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import Database.SQLite.Simple (FromRow, ResultError (..), Query, SQLData (..), ToRow)
import qualified Database.SQLite.Simple as SQL
import Database.SQLite.Simple.FromField (FieldParser, FromField (..), returnError)
import Database.SQLite.Simple.Internal (Field (..))
import Database.SQLite.Simple.Ok (Ok (Ok))
import Database.SQLite.Simple.ToField (ToField (..))
import Simplex.Messaging.Parsers (defaultJSON)
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (diffToMicroseconds, tshow)

newtype BoolInt = BI {unBI :: Bool}
  deriving newtype (FromField, ToField)

newtype Binary = Binary {fromBinary :: ByteString}
  deriving newtype (FromField, ToField)

data Connection = Connection
  { conn :: SQL.Connection,
    track :: TrackQueries,
    slow :: TMap Query SlowQueryStats
  }

data TrackQueries = TQAll | TQSlow Int64 | TQOff
  deriving (Eq)

data SlowQueryStats = SlowQueryStats
  { count :: Int64,
    timeMax :: Int64,
    timeAvg :: Int64,
    errs :: Map Text Int
  }
  deriving (Show)

timeIt :: Connection -> Query -> IO a -> IO a
timeIt Connection {slow, track} sql a
  | track == TQOff = makeQuery
  | otherwise = do
      t <- getCurrentTime
      r <- makeQuery
      t' <- getCurrentTime
      let diff = diffToMicroseconds $ diffUTCTime t' t
      when (trackQuery diff) $ atomically $ TM.alter (updateQueryStats diff) sql slow
      pure r
  where
    makeQuery =
      a `catch` \e -> do
        atomically $ TM.alter (Just . updateQueryErrors e) sql slow
        throwIO e
    trackQuery diff = case track of
      TQOff -> False
      TQSlow t -> diff > t
      TQAll -> True
    updateQueryErrors :: SomeException -> Maybe SlowQueryStats -> SlowQueryStats
    updateQueryErrors e Nothing = SlowQueryStats 0 0 0 $ M.singleton (tshow e) 1
    updateQueryErrors e (Just st@SlowQueryStats {errs}) =
      st {errs = M.alter (Just . maybe 1 (+ 1)) (tshow e) errs}
    updateQueryStats :: Int64 -> Maybe SlowQueryStats -> Maybe SlowQueryStats
    updateQueryStats diff Nothing = Just $ SlowQueryStats 1 diff diff M.empty
    updateQueryStats diff (Just SlowQueryStats {count, timeMax, timeAvg, errs}) =
      Just $
        SlowQueryStats
          { count = count + 1,
            timeMax = max timeMax diff,
            timeAvg = (timeAvg * count + diff) `div` (count + 1),
            errs
          }

open :: String -> TrackQueries -> IO Connection
open f track = do
  conn <- SQL.open f
  slow <- TM.emptyIO
  pure Connection {conn, slow, track}

close :: Connection -> IO ()
close = SQL.close . conn

execute :: ToRow q => Connection -> Query -> q -> IO ()
execute c sql = timeIt c sql . SQL.execute (conn c) sql
{-# INLINE execute #-}

execute_ :: Connection -> Query -> IO ()
execute_ c sql = timeIt c sql $ SQL.execute_ (conn c) sql
{-# INLINE execute_ #-}

executeMany :: ToRow q => Connection -> Query -> [q] -> IO ()
executeMany c sql = timeIt c sql . SQL.executeMany (conn c) sql
{-# INLINE executeMany #-}

query :: (ToRow q, FromRow r) => Connection -> Query -> q -> IO [r]
query c sql = timeIt c sql . SQL.query (conn c) sql
{-# INLINE query #-}

query_ :: FromRow r => Connection -> Query -> IO [r]
query_ c sql = timeIt c sql $ SQL.query_ (conn c) sql
{-# INLINE query_ #-}

blobFieldDecoder :: Typeable k => (ByteString -> Either String k) -> FieldParser k
blobFieldDecoder dec = \case
  f@(Field (SQLBlob b) _) ->
    case dec b of
      Right k -> Ok k
      Left e -> returnError ConversionFailed f ("couldn't parse field: " ++ e)
  f -> returnError ConversionFailed f "expecting SQLBlob column type"

fromTextField_ :: Typeable a => (Text -> Maybe a) -> Field -> Ok a
fromTextField_ fromText = \case
  f@(Field (SQLText t) _) ->
    case fromText t of
      Just x -> Ok x
      _ -> returnError ConversionFailed f ("invalid text: " <> T.unpack t)
  f -> returnError ConversionFailed f "expecting SQLText column type"

$(J.deriveJSON defaultJSON ''SlowQueryStats)
