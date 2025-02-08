{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Redundant multi-way if" #-}

module Simplex.Messaging.Server.MsgStore.Types where

import Control.Concurrent.STM
import Control.Monad.Trans.Except
import Data.Functor (($>))
import Data.Int (Int64)
import Data.Kind
import Data.Time.Clock.System (SystemTime (systemSeconds))
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Util ((<$$>), ($>>=))

class (Monad (StoreMonad s), QueueStoreClass (StoreQueue s) (QueueStore s)) => MsgStoreClass s where
  type StoreMonad s = (m :: Type -> Type) | m -> s
  type MsgStoreConfig s = c | c -> s
  type StoreQueue s = q | q -> s
  type QueueStore s = qs | qs -> s
  newMsgStore :: MsgStoreConfig s -> IO s
  closeMsgStore :: s -> IO ()
  withActiveMsgQueues :: Monoid a => s -> (StoreQueue s -> IO a) -> IO a
  withAllMsgQueues :: Monoid a => Bool -> s -> (StoreQueue s -> IO a) -> IO a
  logQueueStates :: s -> IO ()
  logQueueState :: StoreQueue s -> StoreMonad s ()
  queueStore :: s -> QueueStore s

  -- message store methods
  getPeekMsgQueue :: s -> StoreQueue s -> StoreMonad s (Maybe (MsgQueue (StoreQueue s), Message))
  getMsgQueue :: s -> StoreQueue s -> StoreMonad s (MsgQueue (StoreQueue s))

  -- the journal queue will be closed after action if it was initially closed or idle longer than interval in config
  withIdleMsgQueue :: Int64 -> s -> StoreQueue s -> (MsgQueue (StoreQueue s) -> StoreMonad s a) -> StoreMonad s (Maybe a, Int)
  deleteQueue :: s -> StoreQueue s -> IO (Either ErrorType QueueRec)
  deleteQueueSize :: s -> StoreQueue s -> IO (Either ErrorType (QueueRec, Int))
  getQueueMessages_ :: Bool -> MsgQueue (StoreQueue s) -> StoreMonad s [Message]
  writeMsg :: s -> StoreQueue s -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  setOverQuota_ :: StoreQueue s -> IO () -- can ONLY be used while restoring messages, not while server running
  getQueueSize_ :: MsgQueue (StoreQueue s) -> StoreMonad s Int
  tryPeekMsg_ :: StoreQueue s -> MsgQueue (StoreQueue s) -> StoreMonad s (Maybe Message)
  tryDeleteMsg_ :: StoreQueue s -> MsgQueue (StoreQueue s) -> Bool -> StoreMonad s ()
  isolateQueue :: s -> StoreQueue s -> String -> StoreMonad s a -> ExceptT ErrorType IO a

data MSType = MSMemory | MSJournal

data QSType = QSMemory | QSPostgres

data SMSType :: MSType -> Type where
  SMSMemory :: SMSType 'MSMemory
  SMSJournal :: SMSType 'MSJournal

data SQSType :: QSType -> Type where
  SQSMemory :: SQSType 'QSMemory
  SQSPostgres :: SQSType 'QSPostgres

getQueueRec :: forall s p. (QueueStoreClass (StoreQueue s) (QueueStore s), DirectParty p) => QueueStore s -> SParty p -> QueueId -> IO (Either ErrorType (StoreQueue s, QueueRec))
getQueueRec st party qId =
  getQueue @(StoreQueue s) st party qId
    $>>= (\q -> maybe (Left AUTH) (Right . (q,)) <$> readTVarIO (queueRec q))

getQueueMessages :: MsgStoreClass s => Bool -> s -> StoreQueue s -> ExceptT ErrorType IO [Message]
getQueueMessages drainMsgs st q = withPeekMsgQueue st q "getQueueSize" $ maybe (pure []) (getQueueMessages_ drainMsgs . fst)
{-# INLINE getQueueMessages #-}

getQueueSize :: MsgStoreClass s => s -> StoreQueue s -> ExceptT ErrorType IO Int
getQueueSize st q = withPeekMsgQueue st q "getQueueSize" $ maybe (pure 0) (getQueueSize_ . fst)
{-# INLINE getQueueSize #-}

tryPeekMsg :: MsgStoreClass s => s -> StoreQueue s -> ExceptT ErrorType IO (Maybe Message)
tryPeekMsg st q = snd <$$> withPeekMsgQueue st q "tryPeekMsg" pure
{-# INLINE tryPeekMsg #-}

tryDelMsg :: MsgStoreClass s => s -> StoreQueue s -> MsgId -> ExceptT ErrorType IO (Maybe Message)
tryDelMsg st q msgId' =
  withPeekMsgQueue st q "tryDelMsg" $
    maybe (pure Nothing) $ \(mq, msg) ->
      if
        | messageId msg == msgId' ->
            tryDeleteMsg_ q mq True $> Just msg
        | otherwise -> pure Nothing

-- atomic delete (== read) last and peek next message if available
tryDelPeekMsg :: MsgStoreClass s => s -> StoreQueue s -> MsgId -> ExceptT ErrorType IO (Maybe Message, Maybe Message)
tryDelPeekMsg st q msgId' =
  withPeekMsgQueue st q "tryDelPeekMsg" $
    maybe (pure (Nothing, Nothing)) $ \(mq, msg) ->
      if
        | messageId msg == msgId' -> (Just msg,) <$> (tryDeleteMsg_ q mq True >> tryPeekMsg_ q mq)
        | otherwise -> pure (Nothing, Just msg)

-- The action is called with Nothing when it is known that the queue is empty
withPeekMsgQueue :: MsgStoreClass s => s -> StoreQueue s -> String -> (Maybe (MsgQueue (StoreQueue s), Message) -> StoreMonad s a) -> ExceptT ErrorType IO a
withPeekMsgQueue st q op a = isolateQueue st q op $ getPeekMsgQueue st q >>= a
{-# INLINE withPeekMsgQueue #-}

deleteExpiredMsgs :: MsgStoreClass s => s -> StoreQueue s -> Int64 -> ExceptT ErrorType IO Int
deleteExpiredMsgs st q old =
  isolateQueue st q "deleteExpiredMsgs" $
    getMsgQueue st q >>= deleteExpireMsgs_ old q

-- closed and idle queues will be closed after expiration
-- returns (expired count, queue size after expiration)
idleDeleteExpiredMsgs :: MsgStoreClass s => Int64 -> s -> StoreQueue s -> Int64 -> ExceptT ErrorType IO (Maybe Int, Int)
idleDeleteExpiredMsgs now st q old =
  isolateQueue st q "idleDeleteExpiredMsgs" $
    withIdleMsgQueue now st q (deleteExpireMsgs_ old q)

deleteExpireMsgs_ :: MsgStoreClass s => Int64 -> StoreQueue s -> MsgQueue (StoreQueue s) -> StoreMonad s Int
deleteExpireMsgs_ old q mq = do
  n <- loop 0
  logQueueState q
  pure n
  where
    loop dc =
      tryPeekMsg_ q mq >>= \case
        Just Message {msgTs}
          | systemSeconds msgTs < old ->
              tryDeleteMsg_ q mq False >> loop (dc + 1)
        _ -> pure dc
