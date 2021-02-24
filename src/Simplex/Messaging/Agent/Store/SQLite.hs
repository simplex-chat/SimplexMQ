{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE UndecidableInstances #-}

module Simplex.Messaging.Agent.Store.SQLite
  ( SQLiteStore (..),
    newSQLiteStore,
  )
where

import Control.Monad.Except
import Control.Monad.IO.Unlift (MonadUnliftIO)
import qualified Database.SQLite.Simple as DB
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite.Schema (createSchema)
import Simplex.Messaging.Agent.Store.SQLite.Util
import Simplex.Messaging.Agent.Transmission
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Types (MsgBody)
import Simplex.Messaging.Util (liftIOEither)

data SQLiteStore = SQLiteStore
  { dbFilename :: String,
    dbConn :: DB.Connection
  }

newSQLiteStore :: MonadUnliftIO m => String -> m SQLiteStore
newSQLiteStore dbFilename = do
  dbConn <- liftIO $ DB.open dbFilename
  liftIO $ createSchema dbConn
  return SQLiteStore {dbFilename, dbConn}

instance (MonadUnliftIO m, MonadError StoreError m) => MonadAgentStore SQLiteStore m where
  createRcvConn :: SQLiteStore -> ReceiveQueue -> m ()
  createRcvConn SQLiteStore {dbConn} rcvQueue =
    liftIO $
      createRcvQueueAndConn dbConn rcvQueue

  createSndConn :: SQLiteStore -> SendQueue -> m ()
  createSndConn SQLiteStore {dbConn} sndQueue =
    liftIO $
      createSndQueueAndConn dbConn sndQueue

  getConn :: SQLiteStore -> ConnAlias -> m SomeConn
  getConn SQLiteStore {dbConn} connAlias = do
    queues <-
      liftIO $
        retrieveConnQueues dbConn connAlias
    case queues of
      (Just rcvQ, Just sndQ) -> return $ SomeConn SCDuplex (DuplexConnection connAlias rcvQ sndQ)
      (Just rcvQ, Nothing) -> return $ SomeConn SCReceive (ReceiveConnection connAlias rcvQ)
      (Nothing, Just sndQ) -> return $ SomeConn SCSend (SendConnection connAlias sndQ)
      _ -> throwError SEBadConn

  getRcvQueue :: SQLiteStore -> SMPServer -> SMP.RecipientId -> m ReceiveQueue
  getRcvQueue SQLiteStore {dbConn} SMPServer {host, port} rcvId = do
    rcvQueue <-
      liftIO $
        retrieveRcvQueue dbConn host port rcvId
    case rcvQueue of
      Just rcvQ -> return rcvQ
      _ -> throwError SENotFound

  deleteConn :: SQLiteStore -> ConnAlias -> m ()
  deleteConn SQLiteStore {dbConn} connAlias =
    liftIO $
      deleteConnCascade dbConn connAlias

  upgradeRcvConnToDuplex :: SQLiteStore -> ConnAlias -> SendQueue -> m ()
  upgradeRcvConnToDuplex SQLiteStore {dbConn} connAlias sndQueue =
    liftIOEither $
      updateRcvConnWithSndQueue dbConn connAlias sndQueue

  upgradeSndConnToDuplex :: SQLiteStore -> ConnAlias -> ReceiveQueue -> m ()
  upgradeSndConnToDuplex SQLiteStore {dbConn} connAlias rcvQueue =
    liftIOEither $
      updateSndConnWithRcvQueue dbConn connAlias rcvQueue

  setRcvQueueStatus :: SQLiteStore -> ReceiveQueue -> QueueStatus -> m ()
  setRcvQueueStatus SQLiteStore {dbConn} rcvQueue status =
    liftIO $
      updateRcvQueueStatus dbConn rcvQueue status

  setSndQueueStatus :: SQLiteStore -> SendQueue -> QueueStatus -> m ()
  setSndQueueStatus SQLiteStore {dbConn} sndQueue status =
    liftIO $
      updateSndQueueStatus dbConn sndQueue status

  createRcvMsg :: SQLiteStore -> ConnAlias -> MsgBody -> ExternalSndId -> ExternalSndTs -> BrokerId -> BrokerTs -> m ()
  createRcvMsg SQLiteStore {dbConn} connAlias msgBody externalSndId externalSndTs brokerId brokerTs =
    liftIOEither $
      insertRcvMsg dbConn connAlias msgBody externalSndId externalSndTs brokerId brokerTs

  createSndMsg :: SQLiteStore -> ConnAlias -> MsgBody -> m ()
  createSndMsg SQLiteStore {dbConn} connAlias msgBody =
    liftIOEither $
      insertSndMsg dbConn connAlias msgBody

  getMsg :: SQLiteStore -> ConnAlias -> InternalId -> m Msg
  getMsg _st _connAlias _id = throwError SENotImplemented
