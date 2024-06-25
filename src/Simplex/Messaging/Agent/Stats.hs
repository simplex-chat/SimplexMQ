{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TemplateHaskell #-}

module Simplex.Messaging.Agent.Stats where

import qualified Data.Aeson.TH as J
import Data.Map (Map)
import Database.SQLite.Simple.FromField (FromField (..))
import Database.SQLite.Simple.ToField (ToField (..))
import Simplex.Messaging.Agent.Protocol (UserId)
import Simplex.Messaging.Parsers (defaultJSON, fromTextField_)
import Simplex.Messaging.Protocol (SMPServer, XFTPServer)
import Simplex.Messaging.Util (decodeJSON, encodeJSON)
import UnliftIO.STM

data AgentSMPServerStats = AgentSMPServerStats
  { sentDirect :: TVar Int, -- successfully sent messages
    sentViaProxy :: TVar Int, -- successfully sent messages via proxy
    sentProxied :: TVar Int, -- successfully sent messages to other destination server via this as proxy
    sentDirectAttempts :: TVar Int, -- direct sending attempts (min 1 for each sent message)
    sentViaProxyAttempts :: TVar Int, -- proxy sending attempts
    sentProxiedAttempts :: TVar Int, -- attempts sending to other destination server via this as proxy
    sentAuthErrs :: TVar Int, -- send AUTH errors
    sentQuotaErrs :: TVar Int, -- send QUOTA permanent errors (message expired)
    sentExpiredErrs :: TVar Int, -- send expired errors
    sentOtherErrs :: TVar Int, -- other send permanent errors (excluding above)
    recvMsgs :: TVar Int, -- total messages received
    recvDuplicates :: TVar Int, -- duplicate messages received
    recvCryptoErrs :: TVar Int, -- message decryption errors
    recvErrs :: TVar Int, -- receive errors
    connCreated :: TVar Int,
    connSecured :: TVar Int,
    connCompleted :: TVar Int,
    connDeleted :: TVar Int,
    connSubscribed :: TVar Int, -- total successful subscription
    connSubAttempts :: TVar Int, -- subscription attempts
    connSubErrs :: TVar Int -- permanent subscription errors (temporary accounted for in attempts)
  }

data AgentSMPServerStatsData = AgentSMPServerStatsData
  { _sentDirect :: Int,
    _sentViaProxy :: Int,
    _sentProxied :: Int,
    _sentDirectAttempts :: Int,
    _sentViaProxyAttempts :: Int,
    _sentProxiedAttempts :: Int,
    _sentAuthErrs :: Int,
    _sentQuotaErrs :: Int,
    _sentExpiredErrs :: Int,
    _sentOtherErrs :: Int,
    _recvMsgs :: Int,
    _recvDuplicates :: Int,
    _recvCryptoErrs :: Int,
    _recvErrs :: Int,
    _connCreated :: Int,
    _connSecured :: Int,
    _connCompleted :: Int,
    _connDeleted :: Int,
    _connSubscribed :: Int,
    _connSubAttempts :: Int,
    _connSubErrs :: Int
  }
  deriving (Show)

newAgentSMPServerStats :: STM AgentSMPServerStats
newAgentSMPServerStats = do
  sentDirect <- newTVar 0
  sentViaProxy <- newTVar 0
  sentProxied <- newTVar 0
  sentDirectAttempts <- newTVar 0
  sentViaProxyAttempts <- newTVar 0
  sentProxiedAttempts <- newTVar 0
  sentAuthErrs <- newTVar 0
  sentQuotaErrs <- newTVar 0
  sentExpiredErrs <- newTVar 0
  sentOtherErrs <- newTVar 0
  recvMsgs <- newTVar 0
  recvDuplicates <- newTVar 0
  recvCryptoErrs <- newTVar 0
  recvErrs <- newTVar 0
  connCreated <- newTVar 0
  connSecured <- newTVar 0
  connCompleted <- newTVar 0
  connDeleted <- newTVar 0
  connSubscribed <- newTVar 0
  connSubAttempts <- newTVar 0
  connSubErrs <- newTVar 0
  pure
    AgentSMPServerStats
      { sentDirect,
        sentViaProxy,
        sentProxied,
        sentDirectAttempts,
        sentViaProxyAttempts,
        sentProxiedAttempts,
        sentAuthErrs,
        sentQuotaErrs,
        sentExpiredErrs,
        sentOtherErrs,
        recvMsgs,
        recvDuplicates,
        recvCryptoErrs,
        recvErrs,
        connCreated,
        connSecured,
        connCompleted,
        connDeleted,
        connSubscribed,
        connSubAttempts,
        connSubErrs
      }

newAgentSMPServerStats' :: AgentSMPServerStatsData -> STM AgentSMPServerStats
newAgentSMPServerStats' s = do
  sentDirect <- newTVar $ _sentDirect s
  sentViaProxy <- newTVar $ _sentViaProxy s
  sentProxied <- newTVar $ _sentProxied s
  sentDirectAttempts <- newTVar $ _sentDirectAttempts s
  sentViaProxyAttempts <- newTVar $ _sentViaProxyAttempts s
  sentProxiedAttempts <- newTVar $ _sentProxiedAttempts s
  sentAuthErrs <- newTVar $ _sentAuthErrs s
  sentQuotaErrs <- newTVar $ _sentQuotaErrs s
  sentExpiredErrs <- newTVar $ _sentExpiredErrs s
  sentOtherErrs <- newTVar $ _sentOtherErrs s
  recvMsgs <- newTVar $ _recvMsgs s
  recvDuplicates <- newTVar $ _recvDuplicates s
  recvCryptoErrs <- newTVar $ _recvCryptoErrs s
  recvErrs <- newTVar $ _recvErrs s
  connCreated <- newTVar $ _connCreated s
  connSecured <- newTVar $ _connSecured s
  connCompleted <- newTVar $ _connCompleted s
  connDeleted <- newTVar $ _connDeleted s
  connSubscribed <- newTVar $ _connSubscribed s
  connSubAttempts <- newTVar $ _connSubAttempts s
  connSubErrs <- newTVar $ _connSubErrs s
  pure
    AgentSMPServerStats
      { sentDirect,
        sentViaProxy,
        sentProxied,
        sentDirectAttempts,
        sentViaProxyAttempts,
        sentProxiedAttempts,
        sentAuthErrs,
        sentQuotaErrs,
        sentExpiredErrs,
        sentOtherErrs,
        recvMsgs,
        recvDuplicates,
        recvCryptoErrs,
        recvErrs,
        connCreated,
        connSecured,
        connCompleted,
        connDeleted,
        connSubscribed,
        connSubAttempts,
        connSubErrs
      }

-- as this is used to periodically update stats in db,
-- this is not STM to decrease contention with stats updates
getAgentSMPServerStats :: AgentSMPServerStats -> IO AgentSMPServerStatsData
getAgentSMPServerStats s = do
  _sentDirect <- readTVarIO $ sentDirect s
  _sentViaProxy <- readTVarIO $ sentViaProxy s
  _sentProxied <- readTVarIO $ sentProxied s
  _sentDirectAttempts <- readTVarIO $ sentDirectAttempts s
  _sentViaProxyAttempts <- readTVarIO $ sentViaProxyAttempts s
  _sentProxiedAttempts <- readTVarIO $ sentProxiedAttempts s
  _sentAuthErrs <- readTVarIO $ sentAuthErrs s
  _sentQuotaErrs <- readTVarIO $ sentQuotaErrs s
  _sentExpiredErrs <- readTVarIO $ sentExpiredErrs s
  _sentOtherErrs <- readTVarIO $ sentOtherErrs s
  _recvMsgs <- readTVarIO $ recvMsgs s
  _recvDuplicates <- readTVarIO $ recvDuplicates s
  _recvCryptoErrs <- readTVarIO $ recvCryptoErrs s
  _recvErrs <- readTVarIO $ recvErrs s
  _connCreated <- readTVarIO $ connCreated s
  _connSecured <- readTVarIO $ connSecured s
  _connCompleted <- readTVarIO $ connCompleted s
  _connDeleted <- readTVarIO $ connDeleted s
  _connSubscribed <- readTVarIO $ connSubscribed s
  _connSubAttempts <- readTVarIO $ connSubAttempts s
  _connSubErrs <- readTVarIO $ connSubErrs s
  pure
    AgentSMPServerStatsData
      { _sentDirect,
        _sentViaProxy,
        _sentProxied,
        _sentDirectAttempts,
        _sentViaProxyAttempts,
        _sentProxiedAttempts,
        _sentAuthErrs,
        _sentQuotaErrs,
        _sentExpiredErrs,
        _sentOtherErrs,
        _recvMsgs,
        _recvDuplicates,
        _recvCryptoErrs,
        _recvErrs,
        _connCreated,
        _connSecured,
        _connCompleted,
        _connDeleted,
        _connSubscribed,
        _connSubAttempts,
        _connSubErrs
      }

data AgentXFTPServerStats = AgentXFTPServerStats
  { uploads :: TVar Int, -- total replicas uploaded to server
    uploadAttempts :: TVar Int, -- upload attempts
    uploadErrs :: TVar Int, -- upload errors
    downloads :: TVar Int, -- total replicas downloaded from server
    downloadAttempts :: TVar Int, -- download attempts
    downloadAuthErrs :: TVar Int, -- download AUTH errors
    downloadErrs :: TVar Int, -- other download errors (excluding above)
    deletions :: TVar Int, -- total replicas deleted from server
    deleteAttempts :: TVar Int, -- delete attempts
    deleteErrs :: TVar Int -- delete errors
  }

data AgentXFTPServerStatsData = AgentXFTPServerStatsData
  { _uploads :: Int,
    _uploadAttempts :: Int,
    _uploadErrs :: Int,
    _downloads :: Int,
    _downloadAttempts :: Int,
    _downloadAuthErrs :: Int,
    _downloadErrs :: Int,
    _deletions :: Int,
    _deleteAttempts :: Int,
    _deleteErrs :: Int
  }
  deriving (Show)

newAgentXFTPServerStats :: STM AgentXFTPServerStats
newAgentXFTPServerStats = do
  uploads <- newTVar 0
  uploadAttempts <- newTVar 0
  uploadErrs <- newTVar 0
  downloads <- newTVar 0
  downloadAttempts <- newTVar 0
  downloadAuthErrs <- newTVar 0
  downloadErrs <- newTVar 0
  deletions <- newTVar 0
  deleteAttempts <- newTVar 0
  deleteErrs <- newTVar 0
  pure
    AgentXFTPServerStats
      { uploads,
        uploadAttempts,
        uploadErrs,
        downloads,
        downloadAttempts,
        downloadAuthErrs,
        downloadErrs,
        deletions,
        deleteAttempts,
        deleteErrs
      }

newAgentXFTPServerStats' :: AgentXFTPServerStatsData -> STM AgentXFTPServerStats
newAgentXFTPServerStats' s = do
  uploads <- newTVar $ _uploads s
  uploadAttempts <- newTVar $ _uploadAttempts s
  uploadErrs <- newTVar $ _uploadErrs s
  downloads <- newTVar $ _downloads s
  downloadAttempts <- newTVar $ _downloadAttempts s
  downloadAuthErrs <- newTVar $ _downloadAuthErrs s
  downloadErrs <- newTVar $ _downloadErrs s
  deletions <- newTVar $ _deletions s
  deleteAttempts <- newTVar $ _deleteAttempts s
  deleteErrs <- newTVar $ _deleteErrs s
  pure
    AgentXFTPServerStats
      { uploads,
        uploadAttempts,
        uploadErrs,
        downloads,
        downloadAttempts,
        downloadAuthErrs,
        downloadErrs,
        deletions,
        deleteAttempts,
        deleteErrs
      }

-- as this is used to periodically update stats in db,
-- this is not STM to decrease contention with stats updates
getAgentXFTPServerStats :: AgentXFTPServerStats -> IO AgentXFTPServerStatsData
getAgentXFTPServerStats s = do
  _uploads <- readTVarIO $ uploads s
  _uploadAttempts <- readTVarIO $ uploadAttempts s
  _uploadErrs <- readTVarIO $ uploadErrs s
  _downloads <- readTVarIO $ downloads s
  _downloadAttempts <- readTVarIO $ downloadAttempts s
  _downloadAuthErrs <- readTVarIO $ downloadAuthErrs s
  _downloadErrs <- readTVarIO $ downloadErrs s
  _deletions <- readTVarIO $ deletions s
  _deleteAttempts <- readTVarIO $ deleteAttempts s
  _deleteErrs <- readTVarIO $ deleteErrs s
  pure
    AgentXFTPServerStatsData
      { _uploads,
        _uploadAttempts,
        _uploadErrs,
        _downloads,
        _downloadAttempts,
        _downloadAuthErrs,
        _downloadErrs,
        _deletions,
        _deleteAttempts,
        _deleteErrs
      }

-- Type for gathering both smp and xftp stats across all users and servers,
-- to then be persisted to db as a single json.
data AgentPersistedServerStats = AgentPersistedServerStats
  { smpServersStats :: Map (UserId, SMPServer) AgentSMPServerStatsData,
    xftpServersStats :: Map (UserId, XFTPServer) AgentXFTPServerStatsData
  }
  deriving (Show)

$(J.deriveJSON defaultJSON ''AgentSMPServerStatsData)

$(J.deriveJSON defaultJSON ''AgentXFTPServerStatsData)

$(J.deriveJSON defaultJSON ''AgentPersistedServerStats)

instance ToField AgentPersistedServerStats where
  toField = toField . encodeJSON

instance FromField AgentPersistedServerStats where
  fromField = fromTextField_ decodeJSON
