{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Logger.Simple
import qualified Data.List.NonEmpty as L
import Simplex.Messaging.Agent (runSMPAgent)
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Client (smpDefaultConfig)
import Simplex.Messaging.Transport (TCP, Transport (..))

cfg :: AgentConfig
cfg =
  AgentConfig
    { tcpPort = "5224",
      smpServers = L.fromList ["localhost:5223#bU0K+bRg24xWW//lS0umO1Zdw/SXqpJNtm1/RrPLViE="],
      rsaKeySize = 2048 `div` 8,
      connIdBytes = 12,
      tbqSize = 16,
      dbFile = "smp-agent.db",
      smpCfg = smpDefaultConfig
    }

logCfg :: LogConfig
logCfg = LogConfig {lc_file = Nothing, lc_stderr = True}

main :: IO ()
main = do
  putStrLn $ "SMP agent listening on port " ++ tcpPort (cfg :: AgentConfig)
  setLogLevel LogInfo -- LogError
  withGlobalLogging logCfg $ runSMPAgent (transport @TCP) cfg
