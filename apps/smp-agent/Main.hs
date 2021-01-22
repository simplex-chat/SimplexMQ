{-# LANGUAGE DuplicateRecordFields #-}

module Main where

import Control.Logger.Simple
import Simplex.Messaging.Agent (runSMPAgent)
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Client (smpDefaultConfig)

cfg :: AgentConfig
cfg =
  AgentConfig
    { tcpPort = "5224",
      tbqSize = 16,
      connIdBytes = 12,
      dbFile = "smp-agent.db",
      smpCfg = smpDefaultConfig
    }

logCfg :: LogConfig
logCfg = LogConfig {lc_file = Nothing, lc_stderr = True}

main :: IO ()
main = do
  putStrLn $ "SMP agent listening on port " ++ tcpPort (cfg :: AgentConfig)
  setLogLevel LogInfo -- LogError
  withGlobalLogging logCfg $
    runSMPAgent cfg
