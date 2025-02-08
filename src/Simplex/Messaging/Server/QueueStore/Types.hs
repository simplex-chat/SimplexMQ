{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Simplex.Messaging.Server.QueueStore.Types where

import Control.Concurrent.STM
import Control.Monad
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.TMap (TMap)

class StoreQueueClass q where
  type MsgQueue q = mq | mq -> q
  recipientId :: q -> RecipientId
  queueRec :: q -> TVar (Maybe QueueRec)
  msgQueue :: q -> TVar (Maybe (MsgQueue q))
  mkQueue :: RecipientId -> QueueRec -> IO q

class StoreQueueClass q => QueueStoreClass q s where
  type QueueStoreCfg s
  newQueueStore :: QueueStoreCfg s -> IO s
  queueCounts :: s -> IO QueueCounts
  loadedQueues :: s -> TMap RecipientId q
  addQueue :: s -> RecipientId -> QueueRec -> IO (Either ErrorType q)
  getQueue :: DirectParty p => s -> SParty p -> QueueId -> IO (Either ErrorType q)
  secureQueue :: s -> q -> SndPublicAuthKey -> IO (Either ErrorType ())
  addQueueNotifier :: s -> q -> NtfCreds -> IO (Either ErrorType (Maybe NotifierId))
  deleteQueueNotifier :: s -> q -> IO (Either ErrorType (Maybe NotifierId))
  suspendQueue :: s -> q -> IO (Either ErrorType ())
  blockQueue :: s -> q -> BlockingInfo -> IO (Either ErrorType ())
  unblockQueue :: s -> q -> IO (Either ErrorType ())
  updateQueueTime :: s -> q -> RoundedSystemTime -> IO (Either ErrorType QueueRec)
  deleteStoreQueue :: s -> q -> IO (Either ErrorType (QueueRec, Maybe (MsgQueue q)))

data QueueCounts = QueueCounts
  { queueCount :: Int,
    notifierCount :: Int
  }

withLoadedQueues :: (Monoid a, QueueStoreClass q s) => s -> (q -> IO a) -> IO a
withLoadedQueues st f = readTVarIO (loadedQueues st) >>= foldM run mempty
  where
    run !acc = fmap (acc <>) . f
