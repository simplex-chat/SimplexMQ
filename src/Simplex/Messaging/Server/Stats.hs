{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Server.Stats where

import Control.Applicative (optional)
import qualified Data.Attoparsec.ByteString.Char8 as A
import qualified Data.ByteString.Char8 as B
import Data.Set (Set)
import qualified Data.Set as S
import Data.Time.Calendar.Month.Compat (pattern MonthDay)
import Data.Time.Calendar.OrdinalDate (mondayStartWeek)
import Data.Time.Clock (UTCTime (..))
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (RecipientId)
import UnliftIO.STM

data ServerStats = ServerStats
  { fromTime :: TVar UTCTime,
    qCreated :: TVar Int,
    qSecured :: TVar Int,
    qDeleted :: TVar Int,
    msgSent :: TVar Int,
    msgRecv :: TVar Int,
    activeQueues :: PeriodStats RecipientId
  }

data ServerStatsData = ServerStatsData
  { _fromTime :: UTCTime,
    _qCreated :: Int,
    _qSecured :: Int,
    _qDeleted :: Int,
    _msgSent :: Int,
    _msgRecv :: Int,
    _activeQueues :: PeriodStatsData RecipientId
  }

newServerStats :: UTCTime -> STM ServerStats
newServerStats ts = do
  fromTime <- newTVar ts
  qCreated <- newTVar 0
  qSecured <- newTVar 0
  qDeleted <- newTVar 0
  msgSent <- newTVar 0
  msgRecv <- newTVar 0
  activeQueues <- newPeriodStats
  pure ServerStats {fromTime, qCreated, qSecured, qDeleted, msgSent, msgRecv, activeQueues}

getServerStatsData :: ServerStats -> STM ServerStatsData
getServerStatsData s = do
  _fromTime <- readTVar $ fromTime s
  _qCreated <- readTVar $ qCreated s
  _qSecured <- readTVar $ qSecured s
  _qDeleted <- readTVar $ qDeleted s
  _msgSent <- readTVar $ msgSent s
  _msgRecv <- readTVar $ msgRecv s
  _activeQueues <- getPeriodStatsData $ activeQueues s
  pure ServerStatsData {_fromTime, _qCreated, _qSecured, _qDeleted, _msgSent, _msgRecv, _activeQueues}

setServerStats :: ServerStats -> ServerStatsData -> STM ()
setServerStats s d = do
  writeTVar (fromTime s) (_fromTime d)
  writeTVar (qCreated s) (_qCreated d)
  writeTVar (qSecured s) (_qSecured d)
  writeTVar (qDeleted s) (_qDeleted d)
  writeTVar (msgSent s) (_msgSent d)
  writeTVar (msgRecv s) (_msgRecv d)
  setPeriodStats (activeQueues s) (_activeQueues d)

instance StrEncoding ServerStatsData where
  strEncode ServerStatsData {_fromTime, _qCreated, _qSecured, _qDeleted, _msgSent, _msgRecv, _activeQueues} =
    B.unlines
      [ "fromTime=" <> strEncode _fromTime,
        "qCreated=" <> strEncode _qCreated,
        "qSecured=" <> strEncode _qSecured,
        "qDeleted=" <> strEncode _qDeleted,
        "msgSent=" <> strEncode _msgSent,
        "msgRecv=" <> strEncode _msgRecv,
        "activeQueues:",
        strEncode _activeQueues
      ]
  strP = do
    _fromTime <- "fromTime=" *> strP <* A.endOfLine
    _qCreated <- "qCreated=" *> strP <* A.endOfLine
    _qSecured <- "qSecured=" *> strP <* A.endOfLine
    _qDeleted <- "qDeleted=" *> strP <* A.endOfLine
    _msgSent <- "msgSent=" *> strP <* A.endOfLine
    _msgRecv <- "msgRecv=" *> strP <* A.endOfLine
    r <- optional ("activeQueues:" <* A.endOfLine)
    _activeQueues <- case r of
      Just _ -> strP <* optional A.endOfLine
      _ -> do
        _day <- "dayMsgQueues=" *> strP <* A.endOfLine
        _week <- "weekMsgQueues=" *> strP <* A.endOfLine
        _month <- "monthMsgQueues=" *> strP <* optional A.endOfLine
        pure PeriodStatsData {_day, _week, _month}
    pure ServerStatsData {_fromTime, _qCreated, _qSecured, _qDeleted, _msgSent, _msgRecv, _activeQueues}

data PeriodStats a = PeriodStats
  { day :: TVar (Set a),
    week :: TVar (Set a),
    month :: TVar (Set a)
  }

newPeriodStats :: STM (PeriodStats a)
newPeriodStats = do
  day <- newTVar S.empty
  week <- newTVar S.empty
  month <- newTVar S.empty
  pure PeriodStats {day, week, month}

data PeriodStatsData a = PeriodStatsData
  { _day :: Set a,
    _week :: Set a,
    _month :: Set a
  }

getPeriodStatsData :: PeriodStats a -> STM (PeriodStatsData a)
getPeriodStatsData s = do
  _day <- readTVar $ day s
  _week <- readTVar $ week s
  _month <- readTVar $ month s
  pure PeriodStatsData {_day, _week, _month}

setPeriodStats :: PeriodStats a -> PeriodStatsData a -> STM ()
setPeriodStats s d = do
  writeTVar (day s) (_day d)
  writeTVar (week s) (_week d)
  writeTVar (month s) (_month d)

instance (Ord a, StrEncoding a) => StrEncoding (PeriodStatsData a) where
  strEncode PeriodStatsData {_day, _week, _month} =
    "day=" <> strEncode _day <> "\nweek=" <> strEncode _week <> "\nmonth=" <> strEncode _month
  strP = do
    _day <- "day=" *> strP <* A.endOfLine
    _week <- "week=" *> strP <* A.endOfLine
    _month <- "month=" *> strP
    pure PeriodStatsData {_day, _week, _month}

data PeriodStatCounts = PeriodStatCounts
  { dayCount :: String,
    weekCount :: String,
    monthCount :: String
  }

periodStatCounts :: forall a. PeriodStats a -> UTCTime -> STM PeriodStatCounts
periodStatCounts ps ts = do
  let d = utctDay ts
      (_, wDay) = mondayStartWeek d
      MonthDay _ mDay = d
  dayCount <- periodCount 1 $ day ps
  weekCount <- periodCount wDay $ week ps
  monthCount <- periodCount mDay $ month ps
  pure PeriodStatCounts {dayCount, weekCount, monthCount}
  where
    periodCount :: Int -> TVar (Set a) -> STM String
    periodCount 1 pVar = show . S.size <$> swapTVar pVar S.empty
    periodCount _ _ = pure ""

updatePeriodStats :: Ord a => PeriodStats a -> a -> STM ()
updatePeriodStats stats pId = do
  updatePeriod day
  updatePeriod week
  updatePeriod month
  where
    updatePeriod pSel = modifyTVar (pSel stats) (S.insert pId)
