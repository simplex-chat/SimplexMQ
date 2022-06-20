{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Simplex.Messaging.Agent.Env.SQLite
  ( AgentMonad,
    AgentConfig (..),
    InitialAgentServers (..),
    defaultAgentConfig,
    defaultReconnectInterval,
    Env (..),
    AgentOperation (..),
    disallowedOperations,
    newSMPAgentEnv,
    NtfSupervisor (..),
    NtfSupervisorCommand (..),
  )
where

import Control.Monad.Except
import Control.Monad.IO.Unlift
import Control.Monad.Reader
import Crypto.Random
import Data.List.NonEmpty (NonEmpty)
import Data.Time.Clock (NominalDiffTime, nominalDay)
import Network.Socket
import Numeric.Natural
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Agent.Store.SQLite
import qualified Simplex.Messaging.Agent.Store.SQLite.Migrations as Migrations
import Simplex.Messaging.Client
import Simplex.Messaging.Client.Agent ()
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Notifications.Client (NtfServer, NtfToken)
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM
import Simplex.Messaging.Transport (TLS, Transport (..))
import Simplex.Messaging.Version
import System.Random (StdGen, newStdGen)
import UnliftIO (Async)
import UnliftIO.STM

-- | Agent monad with MonadReader Env and MonadError AgentErrorType
type AgentMonad m = (MonadUnliftIO m, MonadReader Env m, MonadError AgentErrorType m)

data InitialAgentServers = InitialAgentServers
  { smp :: NonEmpty SMPServer,
    ntf :: [NtfServer]
  }

data AgentConfig = AgentConfig
  { tcpPort :: ServiceName,
    cmdSignAlg :: C.SignAlg,
    connIdBytes :: Int,
    tbqSize :: Natural,
    dbFile :: FilePath,
    yesToMigrations :: Bool,
    smpCfg :: ProtocolClientConfig,
    ntfCfg :: ProtocolClientConfig,
    reconnectInterval :: RetryInterval,
    helloTimeout :: NominalDiffTime,
    resubscriptionConcurrency :: Int,
    ntfWorkerThrottle :: Int,
    ntfSubCheckInterval :: NominalDiffTime,
    ntfMaxMessages :: Int,
    caCertificateFile :: FilePath,
    privateKeyFile :: FilePath,
    certificateFile :: FilePath,
    smpAgentVersion :: Version,
    smpAgentVRange :: VersionRange
  }

defaultReconnectInterval :: RetryInterval
defaultReconnectInterval =
  RetryInterval
    { initialInterval = second,
      increaseAfter = 10 * second,
      maxInterval = 10 * second
    }
  where
    second = 1_000_000

defaultAgentConfig :: AgentConfig
defaultAgentConfig =
  AgentConfig
    { tcpPort = "5224",
      cmdSignAlg = C.SignAlg C.SEd448,
      connIdBytes = 12,
      tbqSize = 64,
      dbFile = "smp-agent.db",
      yesToMigrations = False,
      smpCfg = defaultClientConfig {defaultTransport = ("5223", transport @TLS)},
      ntfCfg = defaultClientConfig {defaultTransport = ("443", transport @TLS)},
      reconnectInterval = defaultReconnectInterval,
      helloTimeout = 2 * nominalDay,
      resubscriptionConcurrency = 16,
      ntfWorkerThrottle = 1000000, -- microseconds
      ntfSubCheckInterval = nominalDay,
      ntfMaxMessages = 4,
      -- CA certificate private key is not needed for initialization
      -- ! we do not generate these
      caCertificateFile = "/etc/opt/simplex-agent/ca.crt",
      privateKeyFile = "/etc/opt/simplex-agent/agent.key",
      certificateFile = "/etc/opt/simplex-agent/agent.crt",
      smpAgentVersion = currentSMPAgentVersion,
      smpAgentVRange = supportedSMPAgentVRange
    }

data Env = Env
  { config :: AgentConfig,
    store :: SQLiteStore,
    idsDrg :: TVar ChaChaDRG,
    clientCounter :: TVar Int,
    randomServer :: TVar StdGen,
    agentPhase :: TVar (AgentPhase, Bool),
    agentOperations :: TMap AgentOperation Int,
    ntfSupervisor :: NtfSupervisor
  }

data AgentOperation = AONetwork | AODatabase
  deriving (Eq, Ord)

disallowedOperations :: AgentPhase -> [AgentOperation]
disallowedOperations = \case
  APActive -> []
  APPaused -> [AONetwork]
  APSuspended -> [AONetwork, AODatabase]

newSMPAgentEnv :: (MonadUnliftIO m, MonadRandom m) => AgentConfig -> m Env
newSMPAgentEnv config@AgentConfig {dbFile, yesToMigrations} = do
  idsDrg <- newTVarIO =<< drgNew
  store <- liftIO $ createSQLiteStore dbFile Migrations.app yesToMigrations
  clientCounter <- newTVarIO 0
  randomServer <- newTVarIO =<< liftIO newStdGen
  agentPhase <- newTVarIO (APActive, True)
  agentOperations <- atomically TM.empty
  ntfSupervisor <- atomically . newNtfSubSupervisor $ tbqSize config
  return Env {config, store, idsDrg, clientCounter, randomServer, agentPhase, agentOperations, ntfSupervisor}

data NtfSupervisor = NtfSupervisor
  { ntfTkn :: TVar (Maybe NtfToken),
    ntfSubQ :: TBQueue (ConnId, NtfSupervisorCommand),
    ntfWorkers :: TMap NtfServer (TMVar (), Async ()),
    ntfSMPWorkers :: TMap SMPServer (TMVar (), Async ())
  }

data NtfSupervisorCommand = NSCCreate | NSCDelete | NSCNtfWorker NtfServer

newNtfSubSupervisor :: Natural -> STM NtfSupervisor
newNtfSubSupervisor qSize = do
  ntfTkn <- newTVar Nothing
  ntfSubQ <- newTBQueue qSize
  ntfWorkers <- TM.empty
  ntfSMPWorkers <- TM.empty
  pure NtfSupervisor {ntfTkn, ntfSubQ, ntfWorkers, ntfSMPWorkers}
