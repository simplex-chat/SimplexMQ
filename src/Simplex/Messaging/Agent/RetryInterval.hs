{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Agent.RetryInterval
  ( RetryInterval (..),
    RetryInterval2 (..),
    RetryIntervalMode (..),
    RI2State (..),
    withRetryInterval,
    withRetryIntervalCount,
    withRetryLock2,
    updateRetryInterval2,
    nextRetryDelay,
  )
where

import Control.Concurrent (forkIO)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Int (Int64)
import Simplex.Messaging.Util (threadDelay', whenM)
import UnliftIO.STM

data RetryInterval = RetryInterval
  { initialInterval :: Int64,
    increaseAfter :: Int64,
    maxInterval :: Int64
  }

data RetryInterval2 = RetryInterval2
  { riSlow :: RetryInterval,
    riFast :: RetryInterval
  }

data RI2State = RI2State
  { slowInterval :: Int64,
    fastInterval :: Int64
  }
  deriving (Show)

updateRetryInterval2 :: RI2State -> RetryInterval2 -> RetryInterval2
updateRetryInterval2 RI2State {slowInterval, fastInterval} RetryInterval2 {riSlow, riFast} =
  RetryInterval2
    { riSlow = riSlow {initialInterval = slowInterval, increaseAfter = 0},
      riFast = riFast {initialInterval = fastInterval, increaseAfter = 0}
    }

data RetryIntervalMode = RISlow | RIFast
  deriving (Eq, Show)

withRetryInterval :: forall m a. MonadIO m => RetryInterval -> (Int64 -> m a -> m a) -> m a
withRetryInterval ri = withRetryIntervalCount ri . const

withRetryIntervalCount :: forall m a. MonadIO m => RetryInterval -> (Int -> Int64 -> m a -> m a) -> m a
withRetryIntervalCount ri action = callAction 0 0 $ initialInterval ri
  where
    callAction :: Int -> Int64 -> Int64 -> m a
    callAction n elapsed delay = action n delay loop
      where
        loop = do
          liftIO $ threadDelay' delay
          let elapsed' = elapsed + delay
          callAction (n + 1) elapsed' $ nextRetryDelay elapsed' delay ri

-- This function allows action to toggle between slow and fast retry intervals.
withRetryLock2 :: forall m. MonadIO m => RetryInterval2 -> TMVar () -> (RI2State -> (RetryIntervalMode -> m ()) -> m ()) -> m ()
withRetryLock2 RetryInterval2 {riSlow, riFast} lock action =
  callAction (0, initialInterval riSlow) (0, initialInterval riFast)
  where
    callAction :: (Int64, Int64) -> (Int64, Int64) -> m ()
    callAction slow fast = action (RI2State (snd slow) (snd fast)) loop
      where
        loop = \case
          RISlow -> run slow riSlow (`callAction` fast)
          RIFast -> run fast riFast (callAction slow)
        run (elapsed, delay) ri call = do
          wait delay
          let elapsed' = elapsed + delay
              delay' = nextRetryDelay elapsed' delay ri
          call (elapsed', delay')
        wait delay = do
          waiting <- newTVarIO True
          _ <- liftIO . forkIO $ do
            threadDelay' delay
            atomically $ whenM (readTVar waiting) $ void $ tryPutTMVar lock ()
          atomically $ do
            takeTMVar lock
            writeTVar waiting False

nextRetryDelay :: Int64 -> Int64 -> RetryInterval -> Int64
nextRetryDelay elapsed delay RetryInterval {increaseAfter, maxInterval} =
  if elapsed < increaseAfter || delay == maxInterval
    then delay
    else min (delay * 3 `div` 2) maxInterval
