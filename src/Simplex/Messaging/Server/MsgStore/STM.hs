{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Server.MsgStore.STM
  ( STMMsgStore (..),
    STMStoreConfig (..),
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
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Util ((<$$>), ($>>=))
import System.IO (IOMode (..))

data STMMsgStore = STMMsgStore
  { storeConfig :: STMStoreConfig,
    queueStore :: STMQueueStore STMQueue
  }

data STMQueue = STMQueue
  { -- To avoid race conditions and errors when restoring queues,
    -- Nothing is written to TVar when queue is deleted.
    recipientId :: RecipientId,
    queueRec :: TVar (Maybe QueueRec),
    msgQueue_ :: TVar (Maybe STMMsgQueue)
  }

data STMMsgQueue = STMMsgQueue
  { msgQueue :: TQueue Message,
    canWrite :: TVar Bool,
    size :: TVar Int
  }

data STMStoreConfig = STMStoreConfig
  { storePath :: Maybe FilePath,
    quota :: Int
  }

instance STMStoreClass STMMsgStore where
  stmQueueStore = queueStore
  mkQueue _ rId qr = STMQueue rId <$> newTVar (Just qr) <*> newTVar Nothing
  msgQueue_' = msgQueue_

instance MsgStoreClass STMMsgStore where
  type StoreMonad STMMsgStore = STM
  type StoreQueue STMMsgStore = STMQueue
  type MsgQueue STMMsgStore = STMMsgQueue
  type MsgStoreConfig STMMsgStore = STMStoreConfig

  newMsgStore :: STMStoreConfig -> IO STMMsgStore
  newMsgStore storeConfig = do
    queueStore <- newQueueStore
    pure STMMsgStore {storeConfig, queueStore}

  setStoreLog :: STMMsgStore -> StoreLog 'WriteMode -> IO ()
  setStoreLog st sl = atomically $ writeTVar (storeLog $ queueStore st) (Just sl)

  closeMsgStore st = readTVarIO (storeLog $ queueStore st) >>= mapM_ closeStoreLog

  withAllMsgQueues _ = withActiveMsgQueues
  {-# INLINE withAllMsgQueues #-}

  logQueueStates _ = pure ()
  {-# INLINE logQueueStates #-}

  logQueueState _ = pure ()
  {-# INLINE logQueueState #-}

  recipientId' = recipientId
  {-# INLINE recipientId' #-}

  queueRec' = queueRec
  {-# INLINE queueRec' #-}

  getMsgQueue :: STMMsgStore -> STMQueue -> Bool -> STM STMMsgQueue
  getMsgQueue _ STMQueue {msgQueue_} _ = readTVar msgQueue_ >>= maybe newQ pure
    where
      newQ = do
        msgQueue <- newTQueue
        canWrite <- newTVar True
        size <- newTVar 0
        let q = STMMsgQueue {msgQueue, canWrite, size}
        writeTVar msgQueue_ (Just q)
        pure q

  getPeekMsgQueue :: STMMsgStore -> STMQueue -> STM (Maybe (STMMsgQueue, Message))
  getPeekMsgQueue _ q@STMQueue {msgQueue_} = readTVar msgQueue_ $>>= \mq -> (mq,) <$$> tryPeekMsg_ q mq

  -- does not create queue if it does not exist, does not delete it if it does (can't just close in-memory queue)
  withIdleMsgQueue :: Int64 -> STMMsgStore -> STMQueue -> (STMMsgQueue -> STM a) -> STM (Maybe a, Int)
  withIdleMsgQueue _ _ STMQueue {msgQueue_} action = readTVar msgQueue_ >>= \case
    Just q -> do
      r <- action q
      sz <- getQueueSize_ q
      pure (Just r, sz)
    Nothing -> pure (Nothing, 0)

  deleteQueue :: STMMsgStore -> STMQueue -> IO (Either ErrorType QueueRec)
  deleteQueue ms q = fst <$$> deleteQueue' ms q

  deleteQueueSize :: STMMsgStore -> STMQueue -> IO (Either ErrorType (QueueRec, Int))
  deleteQueueSize ms q = deleteQueue' ms q >>= mapM (traverse getSize)
    -- traverse operates on the second tuple element
    where
      getSize = maybe (pure 0) (\STMMsgQueue {size} -> readTVarIO size)

  getQueueMessages_ :: Bool -> STMMsgQueue -> STM [Message]
  getQueueMessages_ drainMsgs = (if drainMsgs then flushTQueue else snapshotTQueue) . msgQueue
    where
      snapshotTQueue q = do
        msgs <- flushTQueue q
        mapM_ (writeTQueue q) msgs
        pure msgs

  writeMsg :: STMMsgStore -> STMQueue -> Bool -> Message -> ExceptT ErrorType IO (Maybe (Message, Bool))
  writeMsg ms q' _logState msg = liftIO $ atomically $ do
    STMMsgQueue {msgQueue = q, canWrite, size} <- getMsgQueue ms q' True
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
  setOverQuota_ q = readTVarIO (msgQueue_ q) >>= mapM_ (\mq -> atomically $ writeTVar (canWrite mq) False)

  getQueueSize_ :: STMMsgQueue -> STM Int
  getQueueSize_ STMMsgQueue {size} = readTVar size

  tryPeekMsg_ :: STMQueue -> STMMsgQueue -> STM (Maybe Message)
  tryPeekMsg_ _ = tryPeekTQueue . msgQueue
  {-# INLINE tryPeekMsg_ #-}

  tryDeleteMsg_ :: STMQueue -> STMMsgQueue -> Bool -> STM ()
  tryDeleteMsg_ _ STMMsgQueue {msgQueue = q, size} _logState =
    tryReadTQueue q >>= \case
      Just _ -> modifyTVar' size (subtract 1)
      _ -> pure ()

  isolateQueue :: STMQueue -> String -> STM a -> ExceptT ErrorType IO a
  isolateQueue _ _ = liftIO . atomically
