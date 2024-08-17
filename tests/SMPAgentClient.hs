{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module SMPAgentClient where

import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as L
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import NtfClient (ntfTestPort)
import SMPClient (proxyVRangeV8, testPort)
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol
import Simplex.Messaging.Agent.RetryInterval
import Simplex.Messaging.Client (ProtocolClientConfig (..), SMPProxyFallback, SMPProxyMode, defaultNetworkConfig, defaultSMPClientConfig)
import Simplex.Messaging.Notifications.Client (defaultNTFClientConfig)
import Simplex.Messaging.Protocol (NtfServer, ProtoServerWithAuth (..), ProtocolServer)
import Simplex.Messaging.Transport
import XFTPClient (testXFTPServer)

testDB :: FilePath
testDB = "tests/tmp/smp-agent.test.protocol.db"

testDB2 :: FilePath
testDB2 = "tests/tmp/smp-agent2.test.protocol.db"

testDB3 :: FilePath
testDB3 = "tests/tmp/smp-agent3.test.protocol.db"

testSMPServer :: SMPServer
testSMPServer = "smp://LcJUMfVhwD8yxjAiSaDzzGF3-kLG4Uh0Fl_ZIjrRwjI=@localhost:5001"

testSMPServer2 :: SMPServer
testSMPServer2 = "smp://LcJUMfVhwD8yxjAiSaDzzGF3-kLG4Uh0Fl_ZIjrRwjI=@localhost:5002"

testNtfServer :: NtfServer
testNtfServer = "ntf://LcJUMfVhwD8yxjAiSaDzzGF3-kLG4Uh0Fl_ZIjrRwjI=@localhost:6001"

testNtfServer2 :: NtfServer
testNtfServer2 = "ntf://LcJUMfVhwD8yxjAiSaDzzGF3-kLG4Uh0Fl_ZIjrRwjI=@localhost:6002"

initAgentServers :: InitialAgentServers
initAgentServers =
  InitialAgentServers
    { smp = userServers [testSMPServer],
      ntf = [testNtfServer],
      xftp = userServers [testXFTPServer],
      netCfg = defaultNetworkConfig {tcpTimeout = 500_000, tcpConnectTimeout = 500_000}
    }

initAgentServers2 :: InitialAgentServers
initAgentServers2 = initAgentServers {smp = userServers [testSMPServer, testSMPServer2]}

initAgentServersProxy :: SMPProxyMode -> SMPProxyFallback -> InitialAgentServers
initAgentServersProxy smpProxyMode smpProxyFallback =
  initAgentServers {netCfg = (netCfg initAgentServers) {smpProxyMode, smpProxyFallback}}

agentCfg :: AgentConfig
agentCfg =
  defaultAgentConfig
    { tcpPort = Nothing,
      tbqSize = 4,
      -- database = testDB,
      smpCfg = defaultSMPClientConfig {qSize = 1, defaultTransport = (testPort, transport @TLS), networkConfig},
      ntfCfg = defaultNTFClientConfig {qSize = 1, defaultTransport = (ntfTestPort, transport @TLS), networkConfig},
      reconnectInterval = fastRetryInterval,
      persistErrorInterval = 1,
      caCertificateFile = "tests/fixtures/ca.crt",
      privateKeyFile = "tests/fixtures/server.key",
      certificateFile = "tests/fixtures/server.crt"
    }
  where
    networkConfig = defaultNetworkConfig {tcpConnectTimeout = 1_000_000, tcpTimeout = 2_000_000}

agentProxyCfgV8 :: AgentConfig
agentProxyCfgV8 = agentCfg {smpCfg = (smpCfg agentCfg) {serverVRange = proxyVRangeV8}}

fastRetryInterval :: RetryInterval
fastRetryInterval = defaultReconnectInterval {initialInterval = 50_000}

fastMessageRetryInterval :: RetryInterval2
fastMessageRetryInterval = RetryInterval2 {riFast = fastRetryInterval, riSlow = fastRetryInterval}

userServers :: NonEmpty (ProtocolServer p) -> Map UserId (NonEmpty (ServerCfg p))
userServers = userServers' . L.map noAuthSrv

userServers' :: NonEmpty (ProtoServerWithAuth p) -> Map UserId (NonEmpty (ServerCfg p))
userServers' srvs = M.fromList [(1, L.map (presetServerCfg True) srvs)]

noAuthSrvCfg :: ProtocolServer p -> ServerCfg p
noAuthSrvCfg = presetServerCfg True . noAuthSrv
