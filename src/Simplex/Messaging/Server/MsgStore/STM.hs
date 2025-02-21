{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Server.MsgStore.STM
  ( STMMsgStore (..),
    STMStoreConfig (..),
    STMQueue,
  )
where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Data.Functor (($>))
import Data.Int (Int64)
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.STM
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Util ((<$$>), ($>>=))

data STMMsgStore = STMMsgStore
  { storeConfig :: STMStoreConfig,
    queueStore_ :: STMQueueStore STMQueue
  }

data STMQueue = STMQueue
  { -- To avoid race conditions and errors when restoring queues,
    -- Nothing is written to TVar when queue is deleted.
    recipientId' :: RecipientId,
    queueRec' :: TVar (Maybe QueueRec),
    msgQueue' :: TVar (Maybe STMMsgQueue)
  }

data STMMsgQueue = STMMsgQueue
  { msgTQueue :: TQueue Message,
    canWrite :: TVar Bool,
    size :: TVar Int
  }

data STMStoreConfig = STMStoreConfig
  { storePath :: Maybe FilePath,
    quota :: Int
  }

instance StoreQueueClass STMQueue where
  type MsgQueue STMQueue = STMMsgQueue
  recipientId = recipientId'
  {-# INLINE recipientId #-}
  queueRec = queueRec'
  {-# INLINE queueRec #-}
  msgQueue = msgQueue'
  {-# INLINE msgQueue #-}
  withQueueLock _ _ = id
  {-# INLINE withQueueLock #-}

instance MsgStoreClass STMMsgStore where
  type StoreMonad STMMsgStore = STM
  type QueueStore STMMsgStore = STMQueueStore STMQueue
  type StoreQueue STMMsgStore = STMQueue
  type MsgStoreConfig STMMsgStore = STMStoreConfig

  newMsgStore :: STMStoreConfig -> IO STMMsgStore
  newMsgStore storeConfig = do
    queueStore_ <- newQueueStore @STMQueue ()
    pure STMMsgStore {storeConfig, queueStore_}

  closeMsgStore st = readTVarIO (storeLog $ queueStore_ st) >>= mapM_ closeStoreLog

  withActiveMsgQueues = withLoadedQueues . queueStore_
  {-# INLINE withActiveMsgQueues #-}
  withAllMsgQueues _ = withLoadedQueues . queueStore_
  {-# INLINE withAllMsgQueues #-}
  logQueueStates _ = pure ()
  {-# INLINE logQueueStates #-}
  logQueueState _ = pure ()
  {-# INLINE logQueueState #-}
  queueStore = queueStore_
  {-# INLINE queueStore #-}

  mkQueue _ rId qr = STMQueue rId <$> newTVarIO (Just qr) <*> newTVarIO Nothing
  {-# INLINE mkQueue #-}

  getMsgQueue :: STMMsgStore -> STMQueue -> Bool -> STM STMMsgQueue
  getMsgQueue _ STMQueue {msgQueue'} _ = readTVar msgQueue' >>= maybe newQ pure
    where
      newQ = do
        msgTQueue <- newTQueue
        canWrite <- newTVar True
        size <- newTVar 0
        let q = STMMsgQueue {msgTQueue, canWrite, size}
        writeTVar msgQueue' (Just q)
        pure q

  getPeekMsgQueue :: STMMsgStore -> STMQueue -> STM (Maybe (STMMsgQueue, Message))
  getPeekMsgQueue _ q@STMQueue {msgQueue'} = readTVar msgQueue' $>>= \mq -> (mq,) <$$> tryPeekMsg_ q mq

  -- does not create queue if it does not exist, does not delete it if it does (can't just close in-memory queue)
  withIdleMsgQueue :: Int64 -> STMMsgStore -> STMQueue -> (STMMsgQueue -> STM a) -> STM (Maybe a, Int)
  withIdleMsgQueue _ _ STMQueue {msgQueue'} action = readTVar msgQueue' >>= \case
    Just q -> do
      r <- action q
      sz <- getQueueSize_ q
      pure (Just r, sz)
    Nothing -> pure (Nothing, 0)

  deleteQueue :: STMMsgStore -> STMQueue -> IO (Either ErrorType QueueRec)
  deleteQueue ms q = fst <$$> deleteStoreQueue (queueStore_ ms) q

  deleteQueueSize :: STMMsgStore -> STMQueue -> IO (Either ErrorType (QueueRec, Int))
  deleteQueueSize ms q = deleteStoreQueue (queueStore_ ms) q >>= mapM (traverse getSize)
    -- traverse operates on the second tuple element
    where
      getSize = maybe (pure 0) (\STMMsgQueue {size} -> readTVarIO size)

  getQueueMessages_ :: Bool -> STMQueue -> STMMsgQueue -> STM [Message]
  getQueueMessages_ drainMsgs _ = (if drainMsgs then flushTQueue else snapshotTQueue) . msgTQueue
    where
      snapshotTQueue q = do
        msgs <- flushTQueue q
        mapM_ (writeTQueue q) msgs
        pure msgs

  writeMsg :: STMMsgStore -> STMQueue -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  writeMsg ms q' _logState msg = liftIO $ atomically $ do
    STMMsgQueue {msgTQueue = q, canWrite, size} <- getMsgQueue ms q' True
    canWrt <- readTVar canWrite
    empty <- isEmptyTQueue q
    if canWrt || empty
      then do
        canWrt' <- (quota >) <$> readTVar size
        writeTVar canWrite $! canWrt'
        modifyTVar' size (+ 1)
        if canWrt'
          then (writeTQueue q $! msg) $> Just (msg, empty)
          else (writeTQueue q $! msgQuota) $> Nothing
      else pure Nothing
    where
      STMMsgStore {storeConfig = STMStoreConfig {quota}} = ms
      msgQuota = MessageQuota {msgId = messageId msg, msgTs = messageTs msg}

  setOverQuota_ :: STMQueue -> IO ()
  setOverQuota_ q = readTVarIO (msgQueue' q) >>= mapM_ (\mq -> atomically $ writeTVar (canWrite mq) False)

  getQueueSize_ :: STMMsgQueue -> STM Int
  getQueueSize_ STMMsgQueue {size} = readTVar size

  tryPeekMsg_ :: STMQueue -> STMMsgQueue -> STM (Maybe Message)
  tryPeekMsg_ _ = tryPeekTQueue . msgTQueue
  {-# INLINE tryPeekMsg_ #-}

  tryDeleteMsg_ :: STMQueue -> STMMsgQueue -> Bool -> STM ()
  tryDeleteMsg_ _ STMMsgQueue {msgTQueue = q, size} _logState =
    tryReadTQueue q >>= \case
      Just _ -> modifyTVar' size (subtract 1)
      _ -> pure ()

  isolateQueue :: STMQueue -> String -> STM a -> ExceptT ErrorType IO a
  isolateQueue _ _ = liftIO . atomically
