{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Server.Env.STM where

import Control.Concurrent (ThreadId)
import Control.Monad.IO.Unlift
import Crypto.Random
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Network.Socket (ServiceName)
import Numeric.Natural
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.MsgStore.STM
import Simplex.Messaging.Server.QueueStore (QueueRec (..))
import Simplex.Messaging.Server.QueueStore.STM
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Transport (ATransport)
import System.IO (IOMode (..))
import UnliftIO.STM

data ServerConfig = ServerConfig
  { transports :: [(ServiceName, ATransport)],
    tbqSize :: Natural,
    msgQueueQuota :: Natural,
    queueIdBytes :: Int,
    msgIdBytes :: Int,
    storeLog :: Maybe (StoreLog 'ReadMode),
    blockSize :: Int,
    serverPrivateKey :: C.FullPrivateKey
    -- serverId :: ByteString
  }

data Env = Env
  { config :: ServerConfig,
    server :: Server,
    queueStore :: QueueStore,
    msgStore :: STMMsgStore,
    idsDrg :: TVar ChaChaDRG,
    serverKeyPair :: C.FullKeyPair,
    storeLog :: Maybe (StoreLog 'WriteMode)
  }

data Server = Server
  { subscribedQ :: TBQueue (RecipientId, Client),
    subscribers :: TVar (Map RecipientId Client),
    ntfSubscribedQ :: TBQueue (NotifierId, Client),
    notifiers :: TVar (Map NotifierId Client)
  }

data Client = Client
  { subscriptions :: TVar (Map RecipientId Sub),
    ntfSubscriptions :: TVar (Map NotifierId ()),
    rcvQ :: TBQueue Transmission,
    sndQ :: TBQueue Transmission
  }

data SubscriptionThread = NoSub | SubPending | SubThread ThreadId

data Sub = Sub
  { subThread :: SubscriptionThread,
    delivered :: TMVar ()
  }

newServer :: Natural -> STM Server
newServer qSize = do
  subscribedQ <- newTBQueue qSize
  subscribers <- newTVar M.empty
  ntfSubscribedQ <- newTBQueue qSize
  notifiers <- newTVar M.empty
  return Server {subscribedQ, subscribers, ntfSubscribedQ, notifiers}

newClient :: Natural -> STM Client
newClient qSize = do
  subscriptions <- newTVar M.empty
  ntfSubscriptions <- newTVar M.empty
  rcvQ <- newTBQueue qSize
  sndQ <- newTBQueue qSize
  return Client {subscriptions, ntfSubscriptions, rcvQ, sndQ}

newSubscription :: STM Sub
newSubscription = do
  delivered <- newEmptyTMVar
  return Sub {subThread = NoSub, delivered}

newEnv :: forall m. (MonadUnliftIO m, MonadRandom m) => ServerConfig -> m Env
newEnv config = do
  server <- atomically $ newServer (tbqSize config)
  queueStore <- atomically newQueueStore
  msgStore <- atomically newMsgStore
  idsDrg <- drgNew >>= newTVarIO
  s' <- restoreQueues queueStore `mapM` storeLog (config :: ServerConfig)
  let pk = serverPrivateKey config
      serverKeyPair = (C.publicKey' pk, pk)
  return Env {config, server, queueStore, msgStore, idsDrg, serverKeyPair, storeLog = s'}
  where
    restoreQueues :: QueueStore -> StoreLog 'ReadMode -> m (StoreLog 'WriteMode)
    restoreQueues queueStore s = do
      (queues, s') <- liftIO $ readWriteStoreLog s
      atomically $
        modifyTVar queueStore $ \d ->
          d
            { queues,
              senders = M.foldr' addSender M.empty queues,
              notifiers = M.foldr' addNotifier M.empty queues
            }
      pure s'
    addSender :: QueueRec -> Map SenderId RecipientId -> Map SenderId RecipientId
    addSender q = M.insert (senderId q) (recipientId q)
    addNotifier :: QueueRec -> Map NotifierId RecipientId -> Map NotifierId RecipientId
    addNotifier q = case notifier q of
      Nothing -> id
      Just (nId, _) -> M.insert nId (recipientId q)
