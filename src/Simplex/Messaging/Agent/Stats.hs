{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TemplateHaskell #-}

module Simplex.Messaging.Agent.Stats where

import Data.Aeson (FromJSON (..), FromJSONKey, ToJSON (..))
import qualified Data.Aeson.TH as J
import Data.Int (Int64)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Simplex.Messaging.Agent.Protocol (UserId)
import Simplex.Messaging.Agent.Store.DB (FromField (..), ToField (..), fromTextField_)
import Simplex.Messaging.Parsers (defaultJSON)
import Simplex.Messaging.Protocol (NtfServer, SMPServer, XFTPServer)
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
    ackMsgs :: TVar Int, -- total messages acknowledged
    ackAttempts :: TVar Int, -- acknowledgement attempts
    ackNoMsgErrs :: TVar Int, -- NO_MSG ack errors
    ackOtherErrs :: TVar Int, -- other permanent ack errors (temporary accounted for in attempts)
    -- conn stats are accounted for rcv queue server
    connCreated :: TVar Int, -- total connections created
    connSecured :: TVar Int, -- connections secured
    connCompleted :: TVar Int, -- connections completed
    connDeleted :: TVar Int, -- total connections deleted
    connDelAttempts :: TVar Int, -- total connection deletion attempts
    connDelErrs :: TVar Int, -- permanent connection deletion errors (temporary accounted for in attempts)
    connSubscribed :: TVar Int, -- total successful subscription
    connSubAttempts :: TVar Int, -- subscription attempts
    connSubIgnored :: TVar Int, -- subscription results ignored (client switched to different session or it was not pending)
    connSubErrs :: TVar Int, -- permanent subscription errors (temporary accounted for in attempts)
    -- notifications stats
    ntfKey :: TVar Int,
    ntfKeyAttempts :: TVar Int,
    ntfKeyDeleted :: TVar Int,
    ntfKeyDeleteAttempts :: TVar Int
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
    _ackMsgs :: Int,
    _ackAttempts :: Int,
    _ackNoMsgErrs :: Int,
    _ackOtherErrs :: Int,
    _connCreated :: Int,
    _connSecured :: Int,
    _connCompleted :: Int,
    _connDeleted :: Int,
    _connDelAttempts :: Int,
    _connDelErrs :: Int,
    _connSubscribed :: Int,
    _connSubAttempts :: Int,
    _connSubIgnored :: Int,
    _connSubErrs :: Int,
    _ntfKey :: OptionalInt,
    _ntfKeyAttempts :: OptionalInt,
    _ntfKeyDeleted :: OptionalInt,
    _ntfKeyDeleteAttempts :: OptionalInt
  }
  deriving (Show)

newtype OptionalInt = OInt {toInt :: Int}
  deriving (Num, Show, ToJSON)

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
  ackMsgs <- newTVar 0
  ackAttempts <- newTVar 0
  ackNoMsgErrs <- newTVar 0
  ackOtherErrs <- newTVar 0
  connCreated <- newTVar 0
  connSecured <- newTVar 0
  connCompleted <- newTVar 0
  connDeleted <- newTVar 0
  connDelAttempts <- newTVar 0
  connDelErrs <- newTVar 0
  connSubscribed <- newTVar 0
  connSubAttempts <- newTVar 0
  connSubIgnored <- newTVar 0
  connSubErrs <- newTVar 0
  ntfKey <- newTVar 0
  ntfKeyAttempts <- newTVar 0
  ntfKeyDeleted <- newTVar 0
  ntfKeyDeleteAttempts <- newTVar 0
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
        ackMsgs,
        ackAttempts,
        ackNoMsgErrs,
        ackOtherErrs,
        connCreated,
        connSecured,
        connCompleted,
        connDeleted,
        connDelAttempts,
        connDelErrs,
        connSubscribed,
        connSubAttempts,
        connSubIgnored,
        connSubErrs,
        ntfKey,
        ntfKeyAttempts,
        ntfKeyDeleted,
        ntfKeyDeleteAttempts
      }

newAgentSMPServerStatsData :: AgentSMPServerStatsData
newAgentSMPServerStatsData =
  AgentSMPServerStatsData
    { _sentDirect = 0,
      _sentViaProxy = 0,
      _sentProxied = 0,
      _sentDirectAttempts = 0,
      _sentViaProxyAttempts = 0,
      _sentProxiedAttempts = 0,
      _sentAuthErrs = 0,
      _sentQuotaErrs = 0,
      _sentExpiredErrs = 0,
      _sentOtherErrs = 0,
      _recvMsgs = 0,
      _recvDuplicates = 0,
      _recvCryptoErrs = 0,
      _recvErrs = 0,
      _ackMsgs = 0,
      _ackAttempts = 0,
      _ackNoMsgErrs = 0,
      _ackOtherErrs = 0,
      _connCreated = 0,
      _connSecured = 0,
      _connCompleted = 0,
      _connDeleted = 0,
      _connDelAttempts = 0,
      _connDelErrs = 0,
      _connSubscribed = 0,
      _connSubAttempts = 0,
      _connSubIgnored = 0,
      _connSubErrs = 0,
      _ntfKey = 0,
      _ntfKeyAttempts = 0,
      _ntfKeyDeleted = 0,
      _ntfKeyDeleteAttempts = 0
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
  ackMsgs <- newTVar $ _ackMsgs s
  ackAttempts <- newTVar $ _ackAttempts s
  ackNoMsgErrs <- newTVar $ _ackNoMsgErrs s
  ackOtherErrs <- newTVar $ _ackOtherErrs s
  connCreated <- newTVar $ _connCreated s
  connSecured <- newTVar $ _connSecured s
  connCompleted <- newTVar $ _connCompleted s
  connDeleted <- newTVar $ _connDeleted s
  connDelAttempts <- newTVar $ _connDelAttempts s
  connDelErrs <- newTVar $ _connDelErrs s
  connSubscribed <- newTVar $ _connSubscribed s
  connSubAttempts <- newTVar $ _connSubAttempts s
  connSubIgnored <- newTVar $ _connSubIgnored s
  connSubErrs <- newTVar $ _connSubErrs s
  ntfKey <- newTVar $ toInt $ _ntfKey s
  ntfKeyAttempts <- newTVar $ toInt $ _ntfKeyAttempts s
  ntfKeyDeleted <- newTVar $ toInt $ _ntfKeyDeleted s
  ntfKeyDeleteAttempts <- newTVar $ toInt $ _ntfKeyDeleteAttempts s
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
        ackMsgs,
        ackAttempts,
        ackNoMsgErrs,
        ackOtherErrs,
        connCreated,
        connSecured,
        connCompleted,
        connDeleted,
        connDelAttempts,
        connDelErrs,
        connSubscribed,
        connSubAttempts,
        connSubIgnored,
        connSubErrs,
        ntfKey,
        ntfKeyAttempts,
        ntfKeyDeleted,
        ntfKeyDeleteAttempts
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
  _ackMsgs <- readTVarIO $ ackMsgs s
  _ackAttempts <- readTVarIO $ ackAttempts s
  _ackNoMsgErrs <- readTVarIO $ ackNoMsgErrs s
  _ackOtherErrs <- readTVarIO $ ackOtherErrs s
  _connCreated <- readTVarIO $ connCreated s
  _connSecured <- readTVarIO $ connSecured s
  _connCompleted <- readTVarIO $ connCompleted s
  _connDeleted <- readTVarIO $ connDeleted s
  _connDelAttempts <- readTVarIO $ connDelAttempts s
  _connDelErrs <- readTVarIO $ connDelErrs s
  _connSubscribed <- readTVarIO $ connSubscribed s
  _connSubAttempts <- readTVarIO $ connSubAttempts s
  _connSubIgnored <- readTVarIO $ connSubIgnored s
  _connSubErrs <- readTVarIO $ connSubErrs s
  _ntfKey <- OInt <$> readTVarIO (ntfKey s)
  _ntfKeyAttempts <- OInt <$> readTVarIO (ntfKeyAttempts s)
  _ntfKeyDeleted <- OInt <$> readTVarIO (ntfKeyDeleted s)
  _ntfKeyDeleteAttempts <- OInt <$> readTVarIO (ntfKeyDeleteAttempts s)
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
        _ackMsgs,
        _ackAttempts,
        _ackNoMsgErrs,
        _ackOtherErrs,
        _connCreated,
        _connSecured,
        _connCompleted,
        _connDeleted,
        _connDelAttempts,
        _connDelErrs,
        _connSubscribed,
        _connSubAttempts,
        _connSubIgnored,
        _connSubErrs,
        _ntfKey,
        _ntfKeyAttempts,
        _ntfKeyDeleted,
        _ntfKeyDeleteAttempts
      }

addSMPStatsData :: AgentSMPServerStatsData -> AgentSMPServerStatsData -> AgentSMPServerStatsData
addSMPStatsData sd1 sd2 =
  AgentSMPServerStatsData
    { _sentDirect = _sentDirect sd1 + _sentDirect sd2,
      _sentViaProxy = _sentViaProxy sd1 + _sentViaProxy sd2,
      _sentProxied = _sentProxied sd1 + _sentProxied sd2,
      _sentDirectAttempts = _sentDirectAttempts sd1 + _sentDirectAttempts sd2,
      _sentViaProxyAttempts = _sentViaProxyAttempts sd1 + _sentViaProxyAttempts sd2,
      _sentProxiedAttempts = _sentProxiedAttempts sd1 + _sentProxiedAttempts sd2,
      _sentAuthErrs = _sentAuthErrs sd1 + _sentAuthErrs sd2,
      _sentQuotaErrs = _sentQuotaErrs sd1 + _sentQuotaErrs sd2,
      _sentExpiredErrs = _sentExpiredErrs sd1 + _sentExpiredErrs sd2,
      _sentOtherErrs = _sentOtherErrs sd1 + _sentOtherErrs sd2,
      _recvMsgs = _recvMsgs sd1 + _recvMsgs sd2,
      _recvDuplicates = _recvDuplicates sd1 + _recvDuplicates sd2,
      _recvCryptoErrs = _recvCryptoErrs sd1 + _recvCryptoErrs sd2,
      _recvErrs = _recvErrs sd1 + _recvErrs sd2,
      _ackMsgs = _ackMsgs sd1 + _ackMsgs sd2,
      _ackAttempts = _ackAttempts sd1 + _ackAttempts sd2,
      _ackNoMsgErrs = _ackNoMsgErrs sd1 + _ackNoMsgErrs sd2,
      _ackOtherErrs = _ackOtherErrs sd1 + _ackOtherErrs sd2,
      _connCreated = _connCreated sd1 + _connCreated sd2,
      _connSecured = _connSecured sd1 + _connSecured sd2,
      _connCompleted = _connCompleted sd1 + _connCompleted sd2,
      _connDeleted = _connDeleted sd1 + _connDeleted sd2,
      _connDelAttempts = _connDelAttempts sd1 + _connDelAttempts sd2,
      _connDelErrs = _connDelErrs sd1 + _connDelErrs sd2,
      _connSubscribed = _connSubscribed sd1 + _connSubscribed sd2,
      _connSubAttempts = _connSubAttempts sd1 + _connSubAttempts sd2,
      _connSubIgnored = _connSubIgnored sd1 + _connSubIgnored sd2,
      _connSubErrs = _connSubErrs sd1 + _connSubErrs sd2,
      _ntfKey = _ntfKey sd1 + _ntfKey sd2,
      _ntfKeyAttempts = _ntfKeyAttempts sd1 + _ntfKeyAttempts sd2,
      _ntfKeyDeleted = _ntfKeyDeleted sd1 + _ntfKeyDeleted sd2,
      _ntfKeyDeleteAttempts = _ntfKeyDeleteAttempts sd1 + _ntfKeyDeleteAttempts sd2
    }

data AgentXFTPServerStats = AgentXFTPServerStats
  { uploads :: TVar Int, -- total replicas uploaded to server
    uploadsSize :: TVar Int64, -- total size of uploaded replicas in KB
    uploadAttempts :: TVar Int, -- upload attempts
    uploadErrs :: TVar Int, -- upload errors
    downloads :: TVar Int, -- total replicas downloaded from server
    downloadsSize :: TVar Int64, -- total size of downloaded replicas in KB
    downloadAttempts :: TVar Int, -- download attempts
    downloadAuthErrs :: TVar Int, -- download AUTH errors
    downloadErrs :: TVar Int, -- other download errors (excluding above)
    deletions :: TVar Int, -- total replicas deleted from server
    deleteAttempts :: TVar Int, -- delete attempts
    deleteErrs :: TVar Int -- delete errors
  }

data AgentXFTPServerStatsData = AgentXFTPServerStatsData
  { _uploads :: Int,
    _uploadsSize :: Int64,
    _uploadAttempts :: Int,
    _uploadErrs :: Int,
    _downloads :: Int,
    _downloadsSize :: Int64,
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
  uploadsSize <- newTVar 0
  uploadAttempts <- newTVar 0
  uploadErrs <- newTVar 0
  downloads <- newTVar 0
  downloadsSize <- newTVar 0
  downloadAttempts <- newTVar 0
  downloadAuthErrs <- newTVar 0
  downloadErrs <- newTVar 0
  deletions <- newTVar 0
  deleteAttempts <- newTVar 0
  deleteErrs <- newTVar 0
  pure
    AgentXFTPServerStats
      { uploads,
        uploadsSize,
        uploadAttempts,
        uploadErrs,
        downloads,
        downloadsSize,
        downloadAttempts,
        downloadAuthErrs,
        downloadErrs,
        deletions,
        deleteAttempts,
        deleteErrs
      }

newAgentXFTPServerStatsData :: AgentXFTPServerStatsData
newAgentXFTPServerStatsData =
  AgentXFTPServerStatsData
    { _uploads = 0,
      _uploadsSize = 0,
      _uploadAttempts = 0,
      _uploadErrs = 0,
      _downloads = 0,
      _downloadsSize = 0,
      _downloadAttempts = 0,
      _downloadAuthErrs = 0,
      _downloadErrs = 0,
      _deletions = 0,
      _deleteAttempts = 0,
      _deleteErrs = 0
    }

newAgentXFTPServerStats' :: AgentXFTPServerStatsData -> STM AgentXFTPServerStats
newAgentXFTPServerStats' s = do
  uploads <- newTVar $ _uploads s
  uploadsSize <- newTVar $ _uploadsSize s
  uploadAttempts <- newTVar $ _uploadAttempts s
  uploadErrs <- newTVar $ _uploadErrs s
  downloads <- newTVar $ _downloads s
  downloadsSize <- newTVar $ _downloadsSize s
  downloadAttempts <- newTVar $ _downloadAttempts s
  downloadAuthErrs <- newTVar $ _downloadAuthErrs s
  downloadErrs <- newTVar $ _downloadErrs s
  deletions <- newTVar $ _deletions s
  deleteAttempts <- newTVar $ _deleteAttempts s
  deleteErrs <- newTVar $ _deleteErrs s
  pure
    AgentXFTPServerStats
      { uploads,
        uploadsSize,
        uploadAttempts,
        uploadErrs,
        downloads,
        downloadsSize,
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
  _uploadsSize <- readTVarIO $ uploadsSize s
  _uploadAttempts <- readTVarIO $ uploadAttempts s
  _uploadErrs <- readTVarIO $ uploadErrs s
  _downloads <- readTVarIO $ downloads s
  _downloadsSize <- readTVarIO $ downloadsSize s
  _downloadAttempts <- readTVarIO $ downloadAttempts s
  _downloadAuthErrs <- readTVarIO $ downloadAuthErrs s
  _downloadErrs <- readTVarIO $ downloadErrs s
  _deletions <- readTVarIO $ deletions s
  _deleteAttempts <- readTVarIO $ deleteAttempts s
  _deleteErrs <- readTVarIO $ deleteErrs s
  pure
    AgentXFTPServerStatsData
      { _uploads,
        _uploadsSize,
        _uploadAttempts,
        _uploadErrs,
        _downloads,
        _downloadsSize,
        _downloadAttempts,
        _downloadAuthErrs,
        _downloadErrs,
        _deletions,
        _deleteAttempts,
        _deleteErrs
      }

addXFTPStatsData :: AgentXFTPServerStatsData -> AgentXFTPServerStatsData -> AgentXFTPServerStatsData
addXFTPStatsData sd1 sd2 =
  AgentXFTPServerStatsData
    { _uploads = _uploads sd1 + _uploads sd2,
      _uploadsSize = _uploadsSize sd1 + _uploadsSize sd2,
      _uploadAttempts = _uploadAttempts sd1 + _uploadAttempts sd2,
      _uploadErrs = _uploadErrs sd1 + _uploadErrs sd2,
      _downloads = _downloads sd1 + _downloads sd2,
      _downloadsSize = _downloadsSize sd1 + _downloadsSize sd2,
      _downloadAttempts = _downloadAttempts sd1 + _downloadAttempts sd2,
      _downloadAuthErrs = _downloadAuthErrs sd1 + _downloadAuthErrs sd2,
      _downloadErrs = _downloadErrs sd1 + _downloadErrs sd2,
      _deletions = _deletions sd1 + _deletions sd2,
      _deleteAttempts = _deleteAttempts sd1 + _deleteAttempts sd2,
      _deleteErrs = _deleteErrs sd1 + _deleteErrs sd2
    }

data AgentNtfServerStats = AgentNtfServerStats
  { ntfCreated :: TVar Int,
    ntfCreateAttempts :: TVar Int,
    ntfChecked :: TVar Int,
    ntfCheckAttempts :: TVar Int,
    ntfDeleted :: TVar Int,
    ntfDelAttempts :: TVar Int
  }

data AgentNtfServerStatsData = AgentNtfServerStatsData
  { _ntfCreated :: Int,
    _ntfCreateAttempts :: Int,
    _ntfChecked :: Int,
    _ntfCheckAttempts :: Int,
    _ntfDeleted :: Int,
    _ntfDelAttempts :: Int
  }
  deriving (Show)

newAgentNtfServerStats :: STM AgentNtfServerStats
newAgentNtfServerStats = do
  ntfCreated <- newTVar 0
  ntfCreateAttempts <- newTVar 0
  ntfChecked <- newTVar 0
  ntfCheckAttempts <- newTVar 0
  ntfDeleted <- newTVar 0
  ntfDelAttempts <- newTVar 0
  pure
    AgentNtfServerStats
      { ntfCreated,
        ntfCreateAttempts,
        ntfChecked,
        ntfCheckAttempts,
        ntfDeleted,
        ntfDelAttempts
      }

newAgentNtfServerStatsData :: AgentNtfServerStatsData
newAgentNtfServerStatsData =
  AgentNtfServerStatsData
    { _ntfCreated = 0,
      _ntfCreateAttempts = 0,
      _ntfChecked = 0,
      _ntfCheckAttempts = 0,
      _ntfDeleted = 0,
      _ntfDelAttempts = 0
    }

newAgentNtfServerStats' :: AgentNtfServerStatsData -> STM AgentNtfServerStats
newAgentNtfServerStats' s = do
  ntfCreated <- newTVar $ _ntfCreated s
  ntfCreateAttempts <- newTVar $ _ntfCreateAttempts s
  ntfChecked <- newTVar $ _ntfChecked s
  ntfCheckAttempts <- newTVar $ _ntfCheckAttempts s
  ntfDeleted <- newTVar $ _ntfDeleted s
  ntfDelAttempts <- newTVar $ _ntfDelAttempts s
  pure
    AgentNtfServerStats
      { ntfCreated,
        ntfCreateAttempts,
        ntfChecked,
        ntfCheckAttempts,
        ntfDeleted,
        ntfDelAttempts
      }

getAgentNtfServerStats :: AgentNtfServerStats -> IO AgentNtfServerStatsData
getAgentNtfServerStats s = do
  _ntfCreated <- readTVarIO $ ntfCreated s
  _ntfCreateAttempts <- readTVarIO $ ntfCreateAttempts s
  _ntfChecked <- readTVarIO $ ntfChecked s
  _ntfCheckAttempts <- readTVarIO $ ntfCheckAttempts s
  _ntfDeleted <- readTVarIO $ ntfDeleted s
  _ntfDelAttempts <- readTVarIO $ ntfDelAttempts s
  pure
    AgentNtfServerStatsData
      { _ntfCreated,
        _ntfCreateAttempts,
        _ntfChecked,
        _ntfCheckAttempts,
        _ntfDeleted,
        _ntfDelAttempts
      }

addNtfStatsData :: AgentNtfServerStatsData -> AgentNtfServerStatsData -> AgentNtfServerStatsData
addNtfStatsData sd1 sd2 =
  AgentNtfServerStatsData
    { _ntfCreated = _ntfCreated sd1 + _ntfCreated sd2,
      _ntfCreateAttempts = _ntfCreateAttempts sd1 + _ntfCreateAttempts sd2,
      _ntfChecked = _ntfChecked sd1 + _ntfChecked sd2,
      _ntfCheckAttempts = _ntfCheckAttempts sd1 + _ntfCheckAttempts sd2,
      _ntfDeleted = _ntfDeleted sd1 + _ntfDeleted sd2,
      _ntfDelAttempts = _ntfDelAttempts sd1 + _ntfDelAttempts sd2
    }

-- Type for gathering both smp and xftp stats across all users and servers,
-- to then be persisted to db as a single json.
data AgentPersistedServerStats = AgentPersistedServerStats
  { smpServersStats :: Map (UserId, SMPServer) AgentSMPServerStatsData,
    xftpServersStats :: Map (UserId, XFTPServer) AgentXFTPServerStatsData,
    ntfServersStats :: OptionalMap (UserId, NtfServer) AgentNtfServerStatsData
  }
  deriving (Show)

instance FromJSON OptionalInt where
  parseJSON v = OInt <$> parseJSON v
  omittedField = Just (OInt 0)

newtype OptionalMap k v = OptionalMap (Map k v)
  deriving (Show, ToJSON)

instance (FromJSONKey k, Ord k, FromJSON v) => FromJSON (OptionalMap k v) where
  parseJSON v = OptionalMap <$> parseJSON v
  omittedField = Just (OptionalMap M.empty)

$(J.deriveJSON defaultJSON ''AgentSMPServerStatsData)

$(J.deriveJSON defaultJSON ''AgentXFTPServerStatsData)

$(J.deriveJSON defaultJSON ''AgentNtfServerStatsData)

$(J.deriveJSON defaultJSON ''AgentPersistedServerStats)

instance ToField AgentPersistedServerStats where
  toField = toField . encodeJSON

instance FromField AgentPersistedServerStats where
  fromField = fromTextField_ decodeJSON
