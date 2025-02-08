{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Simplex.Messaging.Server.QueueStore.Postgres where

import Control.Concurrent.STM
import qualified Control.Exception as E
import Control.Logger.Simple
import Control.Monad
import Data.Bitraversable (bimapM)
import Data.Functor (($>))
import qualified Data.Text as T
import Database.PostgreSQL.Simple (Binary (..), Only (..), (:.) (..))
import Database.PostgreSQL.Simple.SqlQQ (sql)
import Simplex.Messaging.Agent.Store.Postgres.Common
import qualified Simplex.Messaging.Agent.Store.Postgres.DB as DB
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.MsgStore.Types
import Simplex.Messaging.Server.QueueStore
import Simplex.Messaging.Server.QueueStore.STM (setStatus, withQueueRec)
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Util (firstRow, ($>>=), (<$$))

data PostgresQueueStore q = PostgresQueueStore
  { dbStore :: DBStore,
    -- this map caches all created and opened queues
    queues :: TMap RecipientId q,
    -- this map only cashes the queues that were attempted to send messages to,
    senders :: TMap SenderId RecipientId,
    -- this map only cashes the queues that were attempted to be subscribed to,
    notifiers :: TMap NotifierId RecipientId
  }

instance StoreQueueClass q => QueueStoreClass q (PostgresQueueStore q) where
  type QueueStoreCfg (PostgresQueueStore q) = DBStore

  newQueueStore :: DBStore -> IO (PostgresQueueStore q)
  newQueueStore dbStore = do
    queues <- TM.emptyIO
    senders <- TM.emptyIO
    notifiers <- TM.emptyIO
    pure PostgresQueueStore {dbStore, queues, senders, notifiers}

  loadedQueues = queues
  {-# INLINE loadedQueues #-}

  queueCounts :: PostgresQueueStore q -> IO QueueCounts
  queueCounts st = undefined
    -- withConnection (dbStore st) $ \db -> do
    --   (queueCount, notifierCount) <-
    --     DB.query_
    --       db
    --       [sql|
    --         SELECT
    --           (SELECT COUNT(1) FROM msg_queues) AS queue_count, 
    --           (SELECT COUNT(1) FROM msg_notifiers) AS notifier_count
    --       |]
    --   pure QueueCounts {queueCount, notifierCount}

  addQueue :: PostgresQueueStore q -> RecipientId -> QueueRec -> IO (Either ErrorType q)
  addQueue ms rId qr = undefined
    -- mkQueue rId qr >>= \q -> q <$$ addStoreQueue (queueStore_ ms) qr q

  -- addStoreQueue :: PostgresQueueStore q -> QueueRec -> q -> IO (Either ErrorType ())
  -- addStoreQueue st qr sq = undefined -- do
  --   withDB' "addStoreQueue" (dbStore st) $ \db ->
  --     -- TODO handle duplicate IDs
  --     DB.execute
  --       db
  --       [sql|
  --         WITH inserted AS (
  --           INSERT INTO msg_queues
  --             (recipient_id, recipient_key, rcv_dh_secret, sender_id, sender_key, snd_secure, status, updated_at)
  --           VALUES (?,?,?,?,?,?,?,?)
  --           RETURNING msg_queue_id
  --         )
  --         INSERT INTO msg_notifiers (msg_queue_id, notifier_id, notifier_key, rcv_ntf_dh_secret)
  --         SELECT inserted.msg_queue_id, ?, ?, ?
  --         FROM inserted
  --         WHERE ? IS NOT NULL;
  --       |]
  --       (rId, recipientKey, rcvDhSecret, senderId, senderKey, sndSecure, status, updatedAt)
  --         :. (notifierId, notifierKey, rcvNtfDhSecret, notifierId)
  --   -- sender's ID is not cached here as many queues are never sent to
  --   atomically $ TM.insert rId sq $ queues st
  --   pure $ Right ()
  --   where
  --     rId = recipientId sq
  --     QueueRec {recipientKey, rcvDhSecret, senderId, senderKey, sndSecure, notifier, status, updatedAt} = qr
  --     (notifierId, notifierKey, rcvNtfDhSecret) = case notifier of
  --       Just NtfCreds {notifierId, notifierKey, rcvNtfDhSecret} -> (Just notifierId, Just notifierKey, Just rcvNtfDhSecret)
  --       Nothing -> (Nothing, Nothing, Nothing)

  getQueue :: DirectParty p => PostgresQueueStore q -> SParty p -> QueueId -> IO (Either ErrorType q)
  getQueue st party qId = undefined -- do
  --   case party of
  --     SRecipient -> getQueue_ qId
  --     SSender -> TM.lookupIO qId senders >>= maybe loadSndQueue getQueue_
  --     SNotifier -> loadRefQueue $ \_ -> pure ()
  --   where
  --     -- TODO isolate
  --     PostgresQueueStore {queues, senders, notifiers} = st
  --     getQueue_ rId = TM.lookupIO rId queues >>= maybe loadQueue (pure . Right)
  --     loadQueue =
  --       loadQueueRec $>>= \(rId, qRec) ->  -- isolate by rId
  --         cacheQueue rId qRec
  --     loadSndQueue = loadRefQueue $ \rId -> atomically $ TM.insert qId rId senders
  --     loadRefQueue insertRef =
  --       loadQueueRec $>>= \(rId, qRec) -> -- isolate by rId
  --         TM.lookupIO rId queues >>= \case
  --           Just sq -> pure $ Right sq
  --           Nothing -> insertRef rId >> cacheQueue rId qRec
  --     cacheQueue rId qRec = do
  --       sq <- mkQueue rId qRec
  --       atomically $ TM.insert rId sq queues
  --       pure $ Right sq
  --     loadQueueRec =
  --       withDB "loadQueueRec" st $ \db -> firstRow toQueueRec AUTH $
  --         DB.query
  --           db
  --           ( [sql|
  --               SELECT q.recipient_id, q.recipient_key, q.rcv_dh_secret, q.sender_id, q.sender_key, q.snd_secure, q.status, q.updated_at,
  --                 n.notifier_id, n.notifier_key, n.rcv_ntf_dh_secret
  --               FROM msg_queues q
  --               LEFT JOIN msg_notifiers n USING (msg_queue_id)
  --             |]
  --               <> cond
  --           )
  --            (Only qId)
  --     toQueueRec :: ( (RecipientId, RcvPublicAuthKey, RcvDhSecret, SenderId, Maybe SndPublicAuthKey, SenderCanSecure, ServerEntityStatus, Maybe RoundedSystemTime)
  --                      :. (Maybe NotifierId, Maybe NtfPublicAuthKey, Maybe RcvNtfDhSecret)
  --                   ) -> (RecipientId, QueueRec)
  --     toQueueRec ((rId, recipientKey, rcvDhSecret, senderId, senderKey, sndSecure, status, updatedAt) :. (notifierId_, notifierKey_, rcvNtfDhSecret_)) =
  --       let notifier = NtfCreds <$> notifierId_ <*> notifierKey_ <*> rcvNtfDhSecret_
  --        in (rId, QueueRec {recipientKey, rcvDhSecret, senderId, senderKey, sndSecure, notifier, status, updatedAt})
  --     cond = case party of
  --       SRecipient -> " WHERE q.recipient_id = ?"
  --       SSender -> " WHERE q.sender_id = ?"
  --       SNotifier -> " WHERE n.notifier_id = ?"

  secureQueue :: PostgresQueueStore q -> q -> SndPublicAuthKey -> IO (Either ErrorType ())
  secureQueue st sq sKey = undefined
    -- atomically (readQueueRec qr $>>= secure)
    --   $>>= \_ -> withLog "secureQueue" st $ \s -> logSecureQueue s (recipientId sq) sKey
    -- where
    --   qr = queueRec sq
    --   secure q = case senderKey q of
    --     Just k -> pure $ if sKey == k then Right () else Left AUTH
    --     Nothing -> do
    --       writeTVar qr $ Just q {senderKey = Just sKey}
    --       pure $ Right ()

  addQueueNotifier :: PostgresQueueStore q -> q -> NtfCreds -> IO (Either ErrorType (Maybe NotifierId))
  addQueueNotifier st sq ntfCreds@NtfCreds {notifierId = nId} = undefined -- TODO
    -- atomically (readQueueRec qr $>>= add)
    --   $>>= \nId_ -> nId_ <$$ withLog "addQueueNotifier" st (\s -> logAddNotifier s rId ntfCreds)
    -- where
    --   rId = recipientId sq
    --   qr = queueRec sq
    --   PostgresQueueStore {notifiers} = st
    --   add q = ifM (TM.member nId notifiers) (pure $ Left DUPLICATE_) $ do
    --     nId_ <- forM (notifier q) $ \NtfCreds {notifierId} -> TM.delete notifierId notifiers $> notifierId
    --     let !q' = q {notifier = Just ntfCreds}
    --     writeTVar qr $ Just q'
    --     TM.insert nId rId notifiers
    --     pure $ Right nId_

  deleteQueueNotifier :: PostgresQueueStore q -> q -> IO (Either ErrorType (Maybe NotifierId))
  deleteQueueNotifier st sq = withQueueRec qr delete $>>= deleteDB
    where
      qr = queueRec sq
      delete q = forM (notifier q) $ \NtfCreds {notifierId} -> do
        TM.delete notifierId $ notifiers st
        writeTVar qr $! Just q {notifier = Nothing}
        pure notifierId
      deleteDB nId_ = do
        forM_ nId_ $ \(EntityId nId) -> withDB' "deleteQueueNotifier" st $ \db ->
          DB.execute db "DELETE FROM msg_notifiers WHERE notifier_id = ?" (Only $ Binary nId)
        pure $ Right nId_

  suspendQueue :: PostgresQueueStore q -> q -> IO (Either ErrorType ())
  suspendQueue st sq =
    setStatus (queueRec sq) EntityOff
      $>>= \_ -> setStatusDB "suspendQueue" st (recipientId sq) EntityOff

  blockQueue :: PostgresQueueStore q -> q -> BlockingInfo -> IO (Either ErrorType ())
  blockQueue st sq info =
    setStatus (queueRec sq) (EntityBlocked info)
      $>>= \_ -> setStatusDB "blockQueue" st (recipientId sq) (EntityBlocked info)

  unblockQueue :: PostgresQueueStore q -> q -> IO (Either ErrorType ())
  unblockQueue st sq =
    setStatus (queueRec sq) EntityActive
      $>>= \_ -> setStatusDB "unblockQueue" st (recipientId sq) EntityActive

  updateQueueTime :: PostgresQueueStore q -> q -> RoundedSystemTime -> IO (Either ErrorType QueueRec)
  updateQueueTime st sq t = withQueueRec qr update $>>= updateDB
    where
      qr = queueRec sq
      update q@QueueRec {updatedAt}
        | updatedAt == Just t = pure (q, False)
        | otherwise =
            let !q' = q {updatedAt = Just t}
             in (writeTVar qr $! Just q') $> (q', True)
      updateDB (q, changed)
        | changed = q <$$ withDB' "updateQueueTime" st (\db -> DB.execute db "UPDATE msg_queues SET updated_at = ? WHERE recipient_id = ?" (t, Binary $ unEntityId $ recipientId sq))
        | otherwise = pure $ Right q

  deleteStoreQueue :: PostgresQueueStore q -> q -> IO (Either ErrorType (QueueRec, Maybe (MsgQueue q)))
  deleteStoreQueue st sq =
    withQueueRec qr delete
      $>>= \q -> deleteDB
      >>= mapM (\_ -> (q,) <$> atomically (swapTVar (msgQueue sq) Nothing))
    where
      qr = queueRec sq
      delete q = do
        writeTVar qr Nothing
        TM.delete (senderId q) $ senders st
        -- forM_ (notifier q) $ \NtfCreds {notifierId} -> TM.delete notifierId $ notifiers st
        pure q
      deleteDB =
        withDB' "deleteStoreQueue" st $ \db ->
          DB.execute db "DELETE FROM msg_queues WHERE recipient_id = ?" (Only $ Binary $ unEntityId $ recipientId sq)

setStatusDB :: String -> PostgresQueueStore q -> RecipientId -> ServerEntityStatus -> IO (Either ErrorType ())
setStatusDB name st (EntityId rId) status =
  withDB' name st $ \db ->
    DB.execute db "UPDATE msg_queues SET status = ? WHERE recipient_id = ?" (status, Binary rId)

withDB' :: String -> PostgresQueueStore q -> (DB.Connection -> IO a) -> IO (Either ErrorType a)
withDB' name st' action = withDB name st' $ fmap Right . action

withDB :: forall a q. String -> PostgresQueueStore q -> (DB.Connection -> IO (Either ErrorType a)) -> IO (Either ErrorType a)
withDB name st' action =
  E.try (withConnection (dbStore st') action) >>= either logErr pure
  where
    logErr :: E.SomeException -> IO (Either ErrorType a)
    logErr e = logError ("STORE: " <> T.pack err) $> Left (STORE err)
      where
        err = name <> ", withLog, " <> show e
