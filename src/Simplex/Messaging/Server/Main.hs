{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}

module Simplex.Messaging.Server.Main where

import Control.Concurrent.STM
import Control.Exception (finally)
import Control.Logger.Simple
import Control.Monad
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Char (isAlpha, isAscii, toUpper)
import Data.Either (fromRight)
import Data.Functor (($>))
import Data.Ini (Ini, lookupValue, readIniFile)
import Data.List (find, isPrefixOf)
import qualified Data.List.NonEmpty as L
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (decodeLatin1, encodeUtf8)
import qualified Data.Text.IO as T
import Network.Socket (HostName)
import Options.Applicative
import Simplex.Messaging.Agent.Protocol (connReqUriP')
import Simplex.Messaging.Client (HostMode (..), NetworkConfig (..), ProtocolClientConfig (..), SocksMode (..), defaultNetworkConfig, textToHostMode)
import Simplex.Messaging.Client.Agent (SMPClientAgentConfig (..), defaultSMPClientAgentConfig)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (parseAll)
import Simplex.Messaging.Protocol (BasicAuth (..), ProtoServerWithAuth (ProtoServerWithAuth), pattern SMPServer)
import Simplex.Messaging.Server (AttachHTTP, exportMessages, importMessages, printMessageStats, runSMPServer)
import Simplex.Messaging.Server.CLI
import Simplex.Messaging.Server.Env.STM
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Server.Information
import Simplex.Messaging.Server.MsgStore.Types (AMSType (..), SMSType (..), newMsgStore)
import Simplex.Messaging.Server.QueueStore.STM (readQueueStore)
import Simplex.Messaging.Transport (simplexMQVersion, supportedProxyClientSMPRelayVRange, supportedServerSMPRelayVRange)
import Simplex.Messaging.Transport.Client (SocksProxy, TransportHost (..), defaultSocksProxy)
import Simplex.Messaging.Transport.Server (ServerCredentials (..), TransportServerConfig (..), defaultTransportServerConfig)
import Simplex.Messaging.Util (eitherToMaybe, ifM, safeDecodeUtf8, tshow)
import System.Directory (createDirectoryIfMissing, doesDirectoryExist, doesFileExist)
import System.Exit (exitFailure)
import System.FilePath (combine)
import System.IO (BufferMode (..), hSetBuffering, stderr, stdout)
import Text.Read (readMaybe)

smpServerCLI :: FilePath -> FilePath -> IO ()
smpServerCLI = smpServerCLI_ (\_ _ _ -> pure ()) (\_ -> pure ()) (\_ -> error "attachStaticFiles not available")

smpServerCLI_ ::
  (ServerInformation -> Maybe TransportHost -> FilePath -> IO ()) ->
  (EmbeddedWebParams -> IO ()) ->
  (FilePath -> (AttachHTTP -> IO ()) -> IO ()) ->
  FilePath ->
  FilePath ->
  IO ()
smpServerCLI_ generateSite serveStaticFiles attachStaticFiles cfgPath logPath =
  getCliCommand' (cliCommandP cfgPath logPath iniFile) serverVersion >>= \case
    Init opts ->
      doesFileExist iniFile >>= \case
        True -> exitError $ "Error: server is already initialized (" <> iniFile <> " exists).\nRun `" <> executableName <> " start`."
        _ -> initializeServer opts
    OnlineCert certOpts -> withIniFile $ \_ -> genOnline cfgPath certOpts
    Start -> withIniFile runServer
    Delete -> do
      confirmOrExit
        "WARNING: deleting the server will make all queues inaccessible, because the server identity (certificate fingerprint) will change.\nTHIS CANNOT BE UNDONE!"
        "Server NOT deleted"
      deleteDirIfExists cfgPath
      deleteDirIfExists logPath
      putStrLn "Deleted configuration and log files"
    Journal cmd -> withIniFile $ \ini -> do
      msgsDirExists <- doesDirectoryExist storeMsgsJournalDir
      msgsFileExists <- doesFileExist storeMsgsFilePath
      let enableStoreLog = settingIsOn "STORE_LOG" "enable" ini
      storeLogFile <- case enableStoreLog $> storeLogFilePath of
        Just storeLogFile -> do
          ifM
            (doesFileExist storeLogFile)
            (pure storeLogFile)
            (putStrLn ("Store log file " <> storeLogFile <> " not found") >> exitFailure)
        Nothing -> putStrLn "Store log disabled, see `[STORE_LOG] enable`" >> exitFailure
      case cmd of
        JCImport
          | msgsFileExists && msgsDirExists -> exitConfigureMsgStorage
          | msgsDirExists -> do
              putStrLn $ storeMsgsJournalDir <> " directory already exists."
              exitFailure
          | not msgsFileExists -> do
              putStrLn $ storeMsgsFilePath <> " file does not exists."
              exitFailure
          | otherwise -> do
              confirmOrExit
                ("WARNING: message log file " <> storeMsgsFilePath <> " will be imported to journal directory " <> storeMsgsJournalDir)
                "Messages not imported"
              ms <- newJournalMsgStore
              readQueueStore storeLogFile ms
              msgStats <- importMessages True ms storeMsgsFilePath Nothing -- no expiration
              putStrLn "Import completed"
              printMessageStats "Messages" msgStats
              putStrLn $ case readMsgStoreType ini of
                Right (AMSType SMSMemory) -> "store_messages set to `memory`, update it to `journal` in INI file"
                Right (AMSType SMSJournal) -> "store_messages set to `journal`"
                Left e -> e <> ", update it to `journal` in INI file"
        JCExport
          | msgsFileExists && msgsDirExists -> exitConfigureMsgStorage
          | msgsFileExists -> do
              putStrLn $ storeMsgsFilePath <> " file already exists."
              exitFailure
          | otherwise -> do
              confirmOrExit
                ("WARNING: journal directory " <> storeMsgsJournalDir <> " will be exported to message log file " <> storeMsgsFilePath)
                "Journal not exported"
              ms <- newJournalMsgStore
              readQueueStore storeLogFile ms
              exportMessages True ms storeMsgsFilePath False
              putStrLn "Export completed"
              putStrLn $ case readMsgStoreType ini of
                Right (AMSType SMSMemory) -> "store_messages set to `memory`"
                Right (AMSType SMSJournal) -> "store_messages set to `journal`, update it to `memory` in INI file"
                Left e -> e <> ", update it to `memory` in INI file"
        JCDelete
          | not msgsDirExists -> do
              putStrLn $ storeMsgsJournalDir <> " directory does not exists."
              exitFailure
          | otherwise -> do
              confirmOrExit
                ("WARNING: journal directory " <> storeMsgsJournalDir <> " will be permanently deleted.\nTHIS CANNOT BE UNDONE!")
                "Messages NOT deleted"
              deleteDirIfExists storeMsgsJournalDir
              putStrLn $ "Deleted all messages in journal " <> storeMsgsJournalDir
  where
    withIniFile a =
      doesFileExist iniFile >>= \case
        True -> readIniFile iniFile >>= either exitError a
        _ -> exitError $ "Error: server is not initialized (" <> iniFile <> " does not exist).\nRun `" <> executableName <> " init`."
    newJournalMsgStore =
      let cfg = mkJournalStoreConfig storeMsgsJournalDir defaultMsgQueueQuota defaultMaxJournalMsgCount defaultMaxJournalStateLines $ checkInterval defaultMessageExpiration
       in newMsgStore cfg
    iniFile = combine cfgPath "smp-server.ini"
    serverVersion = "SMP server v" <> simplexMQVersion
    defaultServerPorts = "5223,443"
    executableName = "smp-server"
    storeLogFilePath = combine logPath "smp-server-store.log"
    storeMsgsFilePath = combine logPath "smp-server-messages.log"
    storeMsgsJournalDir = combine logPath "messages"
    storeNtfsFilePath = combine logPath "smp-server-ntfs.log"
    readMsgStoreType :: Ini -> Either String AMSType
    readMsgStoreType = textToMsgStoreType . fromRight "memory" . lookupValue "STORE_LOG" "store_messages"
    textToMsgStoreType = \case
      "memory" -> Right $ AMSType SMSMemory
      "journal" -> Right $ AMSType SMSJournal
      s -> Left $ "invalid store_messages: " <> T.unpack s
    httpsCertFile = combine cfgPath "web.crt"
    httpsKeyFile = combine cfgPath "web.key"
    defaultStaticPath = combine logPath "www"
    initializeServer opts@InitOptions {ip, fqdn, sourceCode = src', webStaticPath = sp', disableWeb = noWeb', scripted}
      | scripted = initialize opts
      | otherwise = do
          putStrLn "Use `smp-server init -h` for available options."
          checkInitOptions opts
          void $ withPrompt "SMP server will be initialized (press Enter)" getLine
          enableStoreLog <- onOffPrompt "Enable store log to restore queues and messages on server restart" True
          logStats <- onOffPrompt "Enable logging daily statistics" False
          putStrLn "Require a password to create new messaging queues?"
          password <- withPrompt "'r' for random (default), 'n' - no password, or enter password: " serverPassword
          let host = fromMaybe ip fqdn
          host' <- withPrompt ("Enter server FQDN or IP address for certificate (" <> host <> "): ") getLine
          sourceCode' <- withPrompt ("Enter server source code URI (" <> maybe simplexmqSource T.unpack src' <> "): ") getServerSourceCode
          staticPath' <- withPrompt ("Enter path to store generated static site with server information (" <> fromMaybe defaultStaticPath sp' <> "): ") getLine
          initialize
            opts
              { enableStoreLog,
                logStats,
                fqdn = if null host' then fqdn else Just host',
                password,
                sourceCode = (T.pack <$> sourceCode') <|> src' <|> Just (T.pack simplexmqSource),
                webStaticPath = if null staticPath' then sp' else Just staticPath',
                disableWeb = noWeb'
              }
      where
        serverPassword =
          getLine >>= \case
            "" -> pure $ Just SPRandom
            "r" -> pure $ Just SPRandom
            "n" -> pure Nothing
            s ->
              case strDecode $ encodeUtf8 $ T.pack s of
                Right auth -> pure . Just $ ServerPassword auth
                _ -> putStrLn "Invalid password. Only latin letters, digits and symbols other than '@' and ':' are allowed" >> serverPassword
        checkInitOptions InitOptions {sourceCode, serverInfo, operatorCountry, hostingCountry} = do
          let err_
                | isNothing sourceCode && hasServerInfo serverInfo =
                    Just "Error: passing any server information requires passing --source-code"
                | isNothing (operator serverInfo) && isJust operatorCountry =
                    Just "Error: passing --operator-country requires passing --operator"
                | isNothing (hosting serverInfo) && isJust hostingCountry =
                    Just "Error: passing --hosting-country requires passing --hosting"
                | otherwise = Nothing
          forM_ err_ $ \err -> putStrLn err >> exitFailure
        initialize opts'@InitOptions {enableStoreLog, logStats, signAlgorithm, password, controlPort, socksProxy, ownDomains, sourceCode, webStaticPath, disableWeb} = do
          checkInitOptions opts'
          clearDirIfExists cfgPath
          clearDirIfExists logPath
          createDirectoryIfMissing True cfgPath
          createDirectoryIfMissing True logPath
          let x509cfg = defaultX509Config {commonName = fromMaybe ip fqdn, signAlgorithm}
          fp <- createServerX509 cfgPath x509cfg
          basicAuth <- mapM createServerPassword password
          controlPortPwds <- forM controlPort $ \_ -> let pwd = decodeLatin1 <$> randomBase64 18 in (,) <$> pwd <*> pwd
          let host = fromMaybe (if ip == "127.0.0.1" then "<hostnames>" else ip) fqdn
              srv = ProtoServerWithAuth (SMPServer [THDomainName host] "" (C.KeyHash fp)) basicAuth
          T.writeFile iniFile $ iniFileContent host basicAuth controlPortPwds
          putStrLn $ "Server initialized, please provide additional server information in " <> iniFile <> "."
          putStrLn $ "Run `" <> executableName <> " start` to start server."
          warnCAPrivateKeyFile cfgPath x509cfg
          printServiceInfo serverVersion srv
          printSourceCode sourceCode
          where
            createServerPassword = \case
              ServerPassword s -> pure s
              SPRandom -> BasicAuth <$> randomBase64 32
            randomBase64 n = strEncode <$> (atomically . C.randomBytes n =<< C.newRandom)
            iniFileContent host basicAuth controlPortPwds =
              informationIniContent opts'
                <> "[STORE_LOG]\n\
                   \# The server uses STM memory for persistence,\n\
                   \# that will be lost on restart (e.g., as with redis).\n\
                   \# This option enables saving memory to append only log,\n\
                   \# and restoring it when the server is started.\n\
                   \# Log is compacted on start (deleted objects are removed).\n"
                <> ("enable: " <> onOff enableStoreLog <> "\n\n")
                <> "# Message storage mode: `memory` or `journal`.\n\
                   \store_messages: memory\n\n\
                   \# When store_messages is `memory`, undelivered messages are optionally saved and restored\n\
                   \# when the server restarts, they are preserved in the .bak file until the next restart.\n"
                <> ("restore_messages: " <> onOff enableStoreLog <> "\n\n")
                <> "# Messages and notifications expiration periods.\n"
                <> ("expire_messages_days: " <> tshow defMsgExpirationDays <> "\n")
                <> "expire_messages_on_start: on\n"
                <> ("expire_ntfs_hours: " <> tshow defNtfExpirationHours <> "\n\n")
                <> "# Log daily server statistics to CSV file\n"
                <> ("log_stats: " <> onOff logStats <> "\n\n")
                <> "# Log interval for real-time Prometheus metrics\n\
                   \# prometheus_interval: 300\n\n\
                   \[AUTH]\n\
                   \# Set new_queues option to off to completely prohibit creating new messaging queues.\n\
                   \# This can be useful when you want to decommission the server, but not all connections are switched yet.\n\
                   \new_queues: on\n\n\
                   \# Use create_password option to enable basic auth to create new messaging queues.\n\
                   \# The password should be used as part of server address in client configuration:\n\
                   \# smp://fingerprint:password@host1,host2\n\
                   \# The password will not be shared with the connecting contacts, you must share it only\n\
                   \# with the users who you want to allow creating messaging queues on your server.\n"
                <> ( let noPassword = "password to create new queues and forward messages (any printable ASCII characters without whitespace, '@', ':' and '/')"
                      in optDisabled basicAuth <> "create_password: " <> maybe noPassword (safeDecodeUtf8 . strEncode) basicAuth
                   )
                <> "\n\n"
                <> (optDisabled controlPortPwds <> "control_port_admin_password: " <> maybe "" fst controlPortPwds <> "\n")
                <> (optDisabled controlPortPwds <> "control_port_user_password: " <> maybe "" snd controlPortPwds <> "\n")
                <> "\n\
                   \[TRANSPORT]\n\
                   \# Host is only used to print server address on start.\n\
                   \# You can specify multiple server ports.\n"
                <> ("host: " <> T.pack host <> "\n")
                <> ("port: " <> T.pack defaultServerPorts <> "\n")
                <> "log_tls_errors: off\n\n\
                   \# Use `websockets: 443` to run websockets server in addition to plain TLS.\n\
                   \# This option is deprecated and should be used for testing only.\n\
                   \# , port 443 should be specified in port above\n\
                   \websockets: off\n"
                <> (optDisabled controlPort <> "control_port: " <> tshow (fromMaybe defaultControlPort controlPort))
                <> "\n\n\
                   \[PROXY]\n\
                   \# Network configuration for SMP proxy client.\n\
                   \# `host_mode` can be 'public' (default) or 'onion'.\n\
                   \# It defines prefferred hostname for destination servers with multiple hostnames.\n\
                   \# host_mode: public\n\
                   \# required_host_mode: off\n\n\
                   \# The domain suffixes of the relays you operate (space-separated) to count as separate proxy statistics.\n"
                <> (optDisabled ownDomains <> "own_server_domains: " <> maybe "" (safeDecodeUtf8 . strEncode) ownDomains)
                <> "\n\n\
                   \# SOCKS proxy port for forwarding messages to destination servers.\n\
                   \# You may need a separate instance of SOCKS proxy for incoming single-hop requests.\n"
                <> (optDisabled socksProxy <> "socks_proxy: " <> maybe "localhost:9050" (safeDecodeUtf8 . strEncode) socksProxy)
                <> "\n\n\
                   \# `socks_mode` can be 'onion' for SOCKS proxy to be used for .onion destination hosts only (default)\n\
                   \# or 'always' to be used for all destination hosts (can be used if it is an .onion server).\n\
                   \# socks_mode: onion\n\n\
                   \# Limit number of threads a client can spawn to process proxy commands in parrallel.\n"
                <> ("# client_concurrency: " <> tshow defaultProxyClientConcurrency)
                <> "\n\n\
                   \[INACTIVE_CLIENTS]\n\
                   \# TTL and interval to check inactive clients\n\
                   \disconnect: on\n"
                <> ("ttl: " <> tshow (ttl defaultInactiveClientExpiration) <> "\n")
                <> ("check_interval: " <> tshow (checkInterval defaultInactiveClientExpiration))
                <> "\n\n\
                   \[WEB]\n\
                   \# Set path to generate static mini-site for server information and qr codes/links\n"
                <> ("static_path: " <> T.pack (fromMaybe defaultStaticPath webStaticPath) <> "\n\n")
                <> "# Run an embedded server on this port\n\
                   \# Onion sites can use any port and register it in the hidden service config.\n\
                   \# Running on a port 80 may require setting process capabilities.\n\
                   \# http: 8000\n\n\
                   \# You can run an embedded TLS web server too if you provide port and cert and key files.\n\
                   \# Not required for running relay on onion address.\n"
                <> (webDisabled <> "https: 443\n")
                <> (webDisabled <> "cert: " <> T.pack httpsCertFile <> "\n")
                <> (webDisabled <> "key: " <> T.pack httpsKeyFile <> "\n")
              where
                webDisabled = if disableWeb then "# " else ""
    runServer ini = do
      hSetBuffering stdout LineBuffering
      hSetBuffering stderr LineBuffering
      fp <- checkSavedFingerprint cfgPath defaultX509Config
      let host = either (const "<hostnames>") T.unpack $ lookupValue "TRANSPORT" "host" ini
          port = T.unpack $ strictIni "TRANSPORT" "port" ini
          cfg@ServerConfig {information, storeLogFile, msgStoreType, newQueueBasicAuth, messageExpiration, inactiveClientExpiration} = serverConfig
          sourceCode' = (\ServerPublicInfo {sourceCode} -> sourceCode) <$> information
          srv = ProtoServerWithAuth (SMPServer [THDomainName host] (if port == "5223" then "" else port) (C.KeyHash fp)) newQueueBasicAuth
      printServiceInfo serverVersion srv
      printSourceCode sourceCode'
      printServerConfig transports storeLogFile
      checkMsgStoreMode msgStoreType
      putStrLn $ case messageExpiration of
        Just ExpirationConfig {ttl} -> "expiring messages after " <> showTTL ttl
        _ -> "not expiring messages"
      putStrLn $ case inactiveClientExpiration of
        Just ExpirationConfig {ttl, checkInterval} -> "expiring clients inactive for " <> show ttl <> " seconds every " <> show checkInterval <> " seconds"
        _ -> "not expiring inactive clients"
      putStrLn $
        "creating new queues "
          <> if allowNewQueues cfg
            then maybe "allowed" (const "requires password") newQueueBasicAuth
            else "NOT allowed"
      -- print information
      let persistence
            | isNothing storeLogFile = SPMMemoryOnly
            | isJust (storeMsgsFile cfg) = SPMMessages
            | otherwise = SPMQueues
      let config =
            ServerPublicConfig
              { persistence,
                messageExpiration = ttl <$> messageExpiration,
                statsEnabled = isJust logStats,
                newQueuesAllowed = allowNewQueues cfg,
                basicAuthEnabled = isJust newQueueBasicAuth
              }
      case webStaticPath' of
        Just path | sharedHTTP -> do
          runWebServer path Nothing ServerInformation {config, information}
          attachStaticFiles path $ \attachHTTP -> do
            logDebug "Allocated web server resources"
            runSMPServer cfg (Just attachHTTP) `finally` logDebug "Releasing web server resources..."
        Just path -> do
          runWebServer path webHttpsParams' ServerInformation {config, information}
          runSMPServer cfg Nothing
        Nothing -> do
          logWarn "No server static path set"
          runSMPServer cfg Nothing
      logDebug "Bye"
      where
        enableStoreLog = settingIsOn "STORE_LOG" "enable" ini
        logStats = settingIsOn "STORE_LOG" "log_stats" ini
        c = combine cfgPath . ($ defaultX509Config)
        restoreMessagesFile path = case iniOnOff "STORE_LOG" "restore_messages" ini of
          Just True -> Just path
          Just False -> Nothing
          -- if the setting is not set, it is enabled when store log is enabled
          _ -> enableStoreLog $> path
        transports = iniTransports ini
        sharedHTTP = any (\(_, _, addHTTP) -> addHTTP) transports
        iniMsgStoreType = either error id $! readMsgStoreType ini
        serverConfig =
          ServerConfig
            { transports,
              smpHandshakeTimeout = 120000000,
              tbqSize = 128,
              msgStoreType = iniMsgStoreType,
              msgQueueQuota = defaultMsgQueueQuota,
              maxJournalMsgCount = defaultMaxJournalMsgCount,
              maxJournalStateLines = defaultMaxJournalStateLines,
              queueIdBytes = 24,
              msgIdBytes = 24, -- must be at least 24 bytes, it is used as 192-bit nonce for XSalsa20
              smpCredentials =
                ServerCredentials
                  { caCertificateFile = Just $ c caCrtFile,
                    privateKeyFile = c serverKeyFile,
                    certificateFile = c serverCrtFile
                  },
              httpCredentials = (\WebHttpsParams {key, cert} -> ServerCredentials {caCertificateFile = Nothing, privateKeyFile = key, certificateFile = cert}) <$> webHttpsParams',
              storeLogFile = enableStoreLog $> storeLogFilePath,
              storeMsgsFile = case iniMsgStoreType of
                AMSType SMSMemory -> restoreMessagesFile storeMsgsFilePath
                AMSType SMSJournal -> Just storeMsgsJournalDir,
              storeNtfsFile = restoreMessagesFile storeNtfsFilePath,
              -- allow creating new queues by default
              allowNewQueues = fromMaybe True $ iniOnOff "AUTH" "new_queues" ini,
              newQueueBasicAuth = either error id <$!> strDecodeIni "AUTH" "create_password" ini,
              controlPortAdminAuth = either error id <$!> strDecodeIni "AUTH" "control_port_admin_password" ini,
              controlPortUserAuth = either error id <$!> strDecodeIni "AUTH" "control_port_user_password" ini,
              messageExpiration =
                Just
                  defaultMessageExpiration
                    { ttl = 86400 * readIniDefault defMsgExpirationDays "STORE_LOG" "expire_messages_days" ini
                    },
              expireMessagesOnStart = fromMaybe True $ iniOnOff "STORE_LOG" "expire_messages_on_start" ini,
              idleQueueInterval = defaultIdleQueueInterval,
              notificationExpiration =
                defaultNtfExpiration
                  { ttl = 3600 * readIniDefault defNtfExpirationHours "STORE_LOG" "expire_ntfs_hours" ini
                  },
              inactiveClientExpiration =
                settingIsOn "INACTIVE_CLIENTS" "disconnect" ini
                  $> ExpirationConfig
                    { ttl = readStrictIni "INACTIVE_CLIENTS" "ttl" ini,
                      checkInterval = readStrictIni "INACTIVE_CLIENTS" "check_interval" ini
                    },
              logStatsInterval = logStats $> 86400, -- seconds
              logStatsStartTime = 0, -- seconds from 00:00 UTC
              serverStatsLogFile = combine logPath "smp-server-stats.daily.log",
              serverStatsBackupFile = logStats $> combine logPath "smp-server-stats.log",
              prometheusInterval = eitherToMaybe $ read . T.unpack <$> lookupValue "STORE_LOG" "prometheus_interval" ini,
              prometheusMetricsFile = combine logPath "smp-server-metrics.txt",
              pendingENDInterval = 15000000, -- 15 seconds
              ntfDeliveryInterval = 3000000, -- 3 seconds
              smpServerVRange = supportedServerSMPRelayVRange,
              transportConfig =
                defaultTransportServerConfig
                  { logTLSErrors = fromMaybe False $ iniOnOff "TRANSPORT" "log_tls_errors" ini
                  },
              controlPort = eitherToMaybe $ T.unpack <$> lookupValue "TRANSPORT" "control_port" ini,
              smpAgentCfg =
                defaultSMPClientAgentConfig
                  { smpCfg =
                      (smpCfg defaultSMPClientAgentConfig)
                        { serverVRange = supportedProxyClientSMPRelayVRange,
                          agreeSecret = True,
                          proxyServer = True,
                          networkConfig =
                            defaultNetworkConfig
                              { socksProxy = either error id <$!> strDecodeIni "PROXY" "socks_proxy" ini,
                                socksMode = maybe SMOnion (either error id) $! strDecodeIni "PROXY" "socks_mode" ini,
                                hostMode = either (const HMPublic) (either error id . textToHostMode) $ lookupValue "PROXY" "host_mode" ini,
                                requiredHostMode = fromMaybe False $ iniOnOff "PROXY" "required_host_mode" ini
                              }
                        },
                    ownServerDomains = either (const []) textToOwnServers $ lookupValue "PROXY" "own_server_domains" ini,
                    persistErrorInterval = 30 -- seconds
                  },
              allowSMPProxy = True,
              serverClientConcurrency = readIniDefault defaultProxyClientConcurrency "PROXY" "client_concurrency" ini,
              information = serverPublicInfo ini
            }
        textToOwnServers :: Text -> [ByteString]
        textToOwnServers = map encodeUtf8 . T.words
        runWebServer webStaticPath webHttpsParams si = do
          let onionHost =
                either (const Nothing) (find isOnion) $
                  strDecode @(L.NonEmpty TransportHost) . encodeUtf8 =<< lookupValue "TRANSPORT" "host" ini
              webHttpPort = eitherToMaybe $ read . T.unpack <$> lookupValue "WEB" "http" ini
          generateSite si onionHost webStaticPath
          when (isJust webHttpPort || isJust webHttpsParams) $
            serveStaticFiles EmbeddedWebParams {webStaticPath, webHttpPort, webHttpsParams}
          where
            isOnion = \case THOnionHost _ -> True; _ -> False
        webHttpsParams' =
          eitherToMaybe $ do
            port <- read . T.unpack <$> lookupValue "WEB" "https" ini
            cert <- T.unpack <$> lookupValue "WEB" "cert" ini
            key <- T.unpack <$> lookupValue "WEB" "key" ini
            pure WebHttpsParams {port, cert, key}
        webStaticPath' = eitherToMaybe $ T.unpack <$> lookupValue "WEB" "static_path" ini

    checkMsgStoreMode :: AMSType -> IO ()
    checkMsgStoreMode mode = do
      msgsDirExists <- doesDirectoryExist storeMsgsJournalDir
      msgsFileExists <- doesFileExist storeMsgsFilePath
      case mode of
        _ | msgsFileExists && msgsDirExists -> exitConfigureMsgStorage
        AMSType SMSJournal
          | msgsFileExists -> do
              putStrLn $ "Error: store_messages is `journal` with " <> storeMsgsFilePath <> " file present."
              putStrLn "Set store_messages to `memory` or use `smp-server journal export` to migrate."
              exitFailure
          | not msgsDirExists ->
              putStrLn $ "store_messages is `journal`, " <> storeMsgsJournalDir <> " directory will be created."
        AMSType SMSMemory
          | msgsDirExists -> do
              putStrLn $ "Error: store_messages is `memory` with " <> storeMsgsJournalDir <> " directory present."
              putStrLn "Set store_messages to `journal` or use `smp-server journal import` to migrate."
              exitFailure
        _ -> pure ()

    exitConfigureMsgStorage = do
      putStrLn $ "Error: both " <> storeMsgsFilePath <> " file and " <> storeMsgsJournalDir <> " directory are present."
      putStrLn "Configure memory storage."
      exitFailure

data EmbeddedWebParams = EmbeddedWebParams
  { webStaticPath :: FilePath,
    webHttpPort :: Maybe Int,
    webHttpsParams :: Maybe WebHttpsParams
  }

data WebHttpsParams = WebHttpsParams
  { port :: Int,
    cert :: FilePath,
    key :: FilePath
  }

getServerSourceCode :: IO (Maybe String)
getServerSourceCode =
  getLine >>= \case
    "" -> pure Nothing
    s | "https://" `isPrefixOf` s || "http://" `isPrefixOf` s -> pure $ Just s
    _ -> putStrLn "Invalid source code. URI should start from http:// or https://" >> getServerSourceCode

simplexmqSource :: String
simplexmqSource = "https://github.com/simplex-chat/simplexmq"

defaultControlPort :: Int
defaultControlPort = 5224

informationIniContent :: InitOptions -> Text
informationIniContent InitOptions {sourceCode, serverInfo} =
  "[INFORMATION]\n\
  \# AGPLv3 license requires that you make any source code modifications\n\
  \# available to the end users of the server.\n\
  \# LICENSE: https://github.com/simplex-chat/simplexmq/blob/stable/LICENSE\n\
  \# Include correct source code URI in case the server source code is modified in any way.\n\
  \# If any other information fields are present, source code property also MUST be present.\n\n"
    <> (optDisabled sourceCode <> "source_code: " <> fromMaybe "URI" sourceCode)
    <> "\n\n\
       \# Declaring all below information is optional, any of these fields can be omitted.\n\
       \\n\
       \# Server usage conditions and amendments.\n\
       \# It is recommended to use standard conditions with any amendments in a separate document.\n\
       \# usage_conditions: https://github.com/simplex-chat/simplex-chat/blob/stable/PRIVACY.md\n\
       \# condition_amendments: link\n\
       \\n\
       \# Server location and operator.\n"
    <> countryStr "server" serverCountry
    <> enitiyStrs "operator" operator
    <> (optDisabled website <> "website: " <> fromMaybe "" website)
    <> "\n\n\
       \# Administrative contacts.\n\
       \# admin_simplex: SimpleX address\n\
       \# admin_email:\n\
       \# admin_pgp:\n\
       \# admin_pgp_fingerprint:\n\
       \\n\
       \# Contacts for complaints and feedback.\n\
       \# complaints_simplex: SimpleX address\n\
       \# complaints_email:\n\
       \# complaints_pgp:\n\
       \# complaints_pgp_fingerprint:\n\
       \\n\
       \# Hosting provider.\n"
    <> enitiyStrs "hosting" hosting
    <> "\n\
       \# Hosting type can be `virtual`, `dedicated`, `colocation`, `owned`\n"
    <> ("hosting_type: " <> maybe "virtual" (decodeLatin1 . strEncode) hostingType <> "\n\n")
  where
    ServerPublicInfo {operator, website, hosting, hostingType, serverCountry} = serverInfo
    countryStr optName country = optDisabled country <> optName <> "_country: " <> fromMaybe "ISO-3166 2-letter code" country <> "\n"
    enitiyStrs optName entity =
      optDisabled entity
        <> optName
        <> ": "
        <> maybe "entity (organization or person name)" name entity
        <> "\n"
        <> countryStr optName (country =<< entity)

serverPublicInfo :: Ini -> Maybe ServerPublicInfo
serverPublicInfo ini = serverInfo <$!> infoValue "source_code"
  where
    serverInfo sourceCode =
      ServerPublicInfo
        { sourceCode,
          usageConditions =
            (\conditions -> ServerConditions {conditions, amendments = infoValue "condition_amendments"})
              <$!> infoValue "usage_conditions",
          serverCountry = countryValue "server_country",
          operator = iniEntity "operator" "operator_country",
          website = infoValue "website",
          adminContacts = iniContacts "admin_simplex" "admin_email" "admin_pgp" "admin_pgp_fingerprint",
          complaintsContacts = iniContacts "complaints_simplex" "complaints_email" "complaints_pgp" "complaints_pgp_fingerprint",
          hosting = iniEntity "hosting" "hosting_country",
          hostingType = either error id <$!> strDecodeIni "INFORMATION" "hosting_type" ini
        }
    infoValue name = eitherToMaybe $ lookupValue "INFORMATION" name ini
    iniEntity nameField countryField =
      (\name -> Entity {name, country = countryValue countryField})
        <$!> infoValue nameField
    countryValue field = (either error id . validCountryValue (T.unpack field) . T.unpack) <$!> infoValue field
    iniContacts simplexField emailField pgpKeyUriField pgpKeyFingerprintField =
      let simplex = either error id . parseAll (connReqUriP' Nothing) . encodeUtf8 <$!> eitherToMaybe (lookupValue "INFORMATION" simplexField ini)
          email = infoValue emailField
          pkURI_ = infoValue pgpKeyUriField
          pkFingerprint_ = infoValue pgpKeyFingerprintField
       in case (simplex, email, pkURI_, pkFingerprint_) of
            (Nothing, Nothing, Nothing, _) -> Nothing
            (Nothing, Nothing, _, Nothing) -> Nothing
            (_, _, pkURI, pkFingerprint) -> Just ServerContactAddress {simplex, email, pgp = PGPKey <$> pkURI <*> pkFingerprint}

optDisabled :: Maybe a -> Text
optDisabled p = if isNothing p then "# " else ""

validCountryValue :: String -> String -> Either String Text
validCountryValue field s
  | length s == 2 && all (\c -> isAscii c && isAlpha c) s = Right $ T.pack $ map toUpper s
  | otherwise = Left $ "Use ISO3166 2-letter code for " <> field

printSourceCode :: Maybe Text -> IO ()
printSourceCode = \case
  Just sourceCode -> T.putStrLn $ "Server source code: " <> sourceCode
  Nothing -> do
    putStrLn "Warning: server source code is not specified."
    putStrLn "Add 'source_code' property to [INFORMATION] section of INI file."

data CliCommand
  = Init InitOptions
  | OnlineCert CertOptions
  | Start
  | Delete
  | Journal JournalCmd

data JournalCmd = JCImport | JCExport | JCDelete

data InitOptions = InitOptions
  { enableStoreLog :: Bool,
    logStats :: Bool,
    signAlgorithm :: SignAlgorithm,
    ip :: HostName,
    fqdn :: Maybe HostName,
    password :: Maybe ServerPassword,
    controlPort :: Maybe Int,
    socksProxy :: Maybe SocksProxy,
    ownDomains :: Maybe (L.NonEmpty TransportHost),
    sourceCode :: Maybe Text,
    serverInfo :: ServerPublicInfo,
    operatorCountry :: Maybe Text,
    hostingCountry :: Maybe Text,
    webStaticPath :: Maybe FilePath,
    disableWeb :: Bool,
    scripted :: Bool
  }
  deriving (Show)

data ServerPassword = ServerPassword BasicAuth | SPRandom
  deriving (Show)

cliCommandP :: FilePath -> FilePath -> FilePath -> Parser CliCommand
cliCommandP cfgPath logPath iniFile =
  hsubparser
    ( command "init" (info (Init <$> initP) (progDesc $ "Initialize server - creates " <> cfgPath <> " and " <> logPath <> " directories and configuration files"))
        <> command "cert" (info (OnlineCert <$> certOptionsP) (progDesc $ "Generate new online TLS server credentials (configuration: " <> iniFile <> ")"))
        <> command "start" (info (pure Start) (progDesc $ "Start server (configuration: " <> iniFile <> ")"))
        <> command "delete" (info (pure Delete) (progDesc "Delete configuration and log files"))
        <> command "journal" (info (Journal <$> journalCmdP) (progDesc "Import/export messages to/from journal storage"))
    )
  where
    initP :: Parser InitOptions
    initP = do
      enableStoreLog <-
        switch
          ( long "store-log"
              <> short 'l'
              <> help "Enable store log for persistence"
          )
      logStats <-
        switch
          ( long "daily-stats"
              <> short 's'
              <> help "Enable logging daily server statistics"
          )
      signAlgorithm <-
        option
          (maybeReader readMaybe)
          ( long "sign-algorithm"
              <> short 'a'
              <> help "Signature algorithm used for TLS certificates: ED25519, ED448"
              <> value ED25519
              <> showDefault
              <> metavar "ALG"
          )
      ip <-
        strOption
          ( long "ip"
              <> help
                "Server IP address, used as Common Name for TLS online certificate if FQDN is not supplied"
              <> value "127.0.0.1"
              <> showDefault
              <> metavar "IP"
          )
      fqdn <-
        (optional . strOption)
          ( long "fqdn"
              <> short 'n'
              <> help "Server FQDN used as Common Name for TLS online certificate"
              <> metavar "FQDN"
          )
      password <-
        flag' Nothing (long "no-password" <> help "Allow creating new queues without password")
          <|> Just
            <$> option
              parseBasicAuth
              ( long "password"
                  <> metavar "PASSWORD"
                  <> help "Set password to create new messaging queues"
                  <> value SPRandom
              )
      controlPort <-
        flag' (Just defaultControlPort) (long "control-port" <> help ("Enable control port on " <> show defaultControlPort))
          <|> option strParse (long "control-port" <> help "Enable control port" <> metavar "PORT" <> value Nothing)
      socksProxy <-
        flag' (Just defaultSocksProxy) (long "socks-proxy" <> help "Outgoing SOCKS proxy on port 9050")
          <|> option
            strParse
            ( long "socks-proxy"
                <> metavar "PROXY"
                <> help "Outgoing SOCKS proxy to forward messages to onion-only servers"
                <> value Nothing
            )
      ownDomains :: Maybe (L.NonEmpty TransportHost) <-
        option
          strParse
          ( long "own-domains"
              <> metavar "DOMAINS"
              <> help "Own server domain names (comma-separated)"
              <> value Nothing
          )
      sourceCode <-
        flag' (Just simplexmqSource) (long "source-code" <> help ("Server source code (default: " <> simplexmqSource <> ")"))
          <|> (optional . strOption) (long "source-code" <> metavar "URI" <> help "Server source code")
      operator_ <- entityP "operator" "OPERATOR" "Server operator"
      hosting_ <- entityP "hosting" "HOSTING" "Hosting provider"
      hostingType <-
        option
          strParse
          ( long "hosting-type"
              <> metavar "HOSTING_TYPE"
              <> help "Hosting type: virtual, dedicated, colocation, owned"
              <> value Nothing
          )
      serverCountry <- countryP "server" "SERVER" "Server datacenter"
      website <-
        (optional . strOption)
          ( long "operator-website"
              <> help "Operator public website"
              <> metavar "WEBSITE"
          )
      webStaticPath <-
        (optional . strOption)
          ( long "web-path"
              <> help "Directory to store generated static site with server information"
              <> metavar "PATH"
          )
      disableWeb <-
        switch
          ( long "disable-web"
              <> help "Disable starting static web server with server information"
          )
      scripted <-
        switch
          ( long "yes"
              <> short 'y'
              <> help "Non-interactive initialization using command-line options"
          )
      pure
        InitOptions
          { enableStoreLog,
            logStats,
            signAlgorithm,
            ip,
            fqdn,
            password,
            controlPort,
            socksProxy,
            ownDomains,
            sourceCode = T.pack <$> sourceCode,
            serverInfo =
              ServerPublicInfo
                { sourceCode = T.pack simplexmqSource,
                  usageConditions = Nothing,
                  operator = fst operator_,
                  website,
                  adminContacts = Nothing,
                  complaintsContacts = Nothing,
                  hosting = fst hosting_,
                  hostingType,
                  serverCountry
                },
            operatorCountry = snd operator_,
            hostingCountry = snd hosting_,
            webStaticPath,
            disableWeb,
            scripted
          }
    journalCmdP =
      hsubparser
        ( command "import" (info (pure JCImport) (progDesc "Import message log file into a new journal storage"))
            <> command "export" (info (pure JCExport) (progDesc "Export journal storage to message log file"))
            <> command "delete" (info (pure JCDelete) (progDesc "Delete journal storage"))
        )

    parseBasicAuth :: ReadM ServerPassword
    parseBasicAuth = eitherReader $ fmap ServerPassword . strDecode . B.pack
    entityP :: String -> String -> String -> Parser (Maybe Entity, Maybe Text)
    entityP opt' metavar' help' = do
      name_ <-
        (optional . strOption)
          ( long opt'
              <> metavar (metavar' <> "_NAME")
              <> help (help' <> " name")
          )
      country <- countryP opt' metavar' help'
      pure ((\name -> Entity {name, country}) <$> name_, country)
    countryP :: String -> String -> String -> Parser (Maybe Text)
    countryP opt' metavar' help' =
      (optional . option (eitherReader $ validCountryValue opt'))
        ( long (opt' <> "-country")
            <> metavar (metavar' <> "_COUNTRY")
            <> help (help' <> " country")
        )
    strParse :: StrEncoding a => ReadM a
    strParse = eitherReader $ parseAll strP . encodeUtf8 . T.pack
