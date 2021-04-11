{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import ChatOptions
import ChatTerminal
import ChatTerminal.Core
import Control.Applicative ((<|>))
import Control.Concurrent.STM
import Control.Logger.Simple
import Control.Monad.Reader
import Data.Attoparsec.ByteString.Char8 (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Functor (($>))
import qualified Data.Text as T
import Data.Text.Encoding
import Numeric.Natural
import Simplex.Markdown
import Simplex.Messaging.Agent (getSMPAgentClient, runSMPAgentClient)
import Simplex.Messaging.Agent.Client (AgentClient (..))
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Transmission
import Simplex.Messaging.Client (smpDefaultConfig)
import Simplex.Messaging.Util (raceAny_)
import Styled
import System.Console.ANSI.Types
import System.Directory (getAppUserDataDirectory)
import Types

cfg :: AgentConfig
cfg =
  AgentConfig
    { tcpPort = undefined, -- TODO maybe take it out of config
      rsaKeySize = 2048 `div` 8,
      connIdBytes = 12,
      tbqSize = 16,
      dbFile = "smp-chat.db",
      smpCfg = smpDefaultConfig
    }

logCfg :: LogConfig
logCfg = LogConfig {lc_file = Nothing, lc_stderr = True}

data ChatClient = ChatClient
  { inQ :: TBQueue ChatCommand,
    outQ :: TBQueue ChatResponse,
    smpServer :: SMPServer
  }

-- | GroupMessage ChatGroup ByteString
-- | AddToGroup Contact
data ChatCommand
  = ChatHelp
  | MarkdownHelp
  | AddConnection Contact
  | Connect Contact SMPQueueInfo
  | ResetChat
  | SendMessage Contact ByteString

chatCommandP :: Parser ChatCommand
chatCommandP =
  ("/help" <|> "/h") $> ChatHelp
    <|> ("/markdown" <|> "/m") $> MarkdownHelp
    <|> ("/add " <|> "/a ") *> (AddConnection <$> contact)
    <|> ("/connect " <> "/c ") *> connect
    <|> ("/reset" <> "/r") $> ResetChat
    <|> "@" *> sendMessage
  where
    connect = Connect <$> contact <* A.space <*> smpQueueInfoP
    sendMessage = SendMessage <$> contact <* A.space <*> A.takeByteString
    contact = Contact <$> A.takeTill (== ' ')

data ChatResponse
  = ChatHelpInfo
  | MarkdownInfo
  | Invitation SMPQueueInfo
  | Connected Contact
  | ReceivedMessage Contact ByteString
  | Disconnected Contact
  | YesYes
  | ErrorInput ByteString
  | ChatError AgentErrorType
  | NoChatResponse

serializeChatResponse :: ChatResponse -> [StyledString]
serializeChatResponse = \case
  ChatHelpInfo -> chatHelpInfo
  MarkdownInfo -> markdownInfo
  Invitation qInfo -> ["ask your contact to enter: /connect <any_name_for_you> " <> (bPlain . serializeSmpQueueInfo) qInfo]
  Connected c -> [ttyContact c <> " connected"]
  ReceivedMessage c t -> prependFirst (ttyFromContact c) $ msgPlain t
  Disconnected c -> ["disconnected from " <> ttyContact c <> " - try \"/chat " <> bPlain (toBs c) <> "\""]
  YesYes -> ["you got it!"]
  ErrorInput t -> ["invalid input: " <> bPlain t]
  ChatError e -> ["chat error: " <> plain (show e)]
  NoChatResponse -> [""]
  where
    prependFirst :: StyledString -> [StyledString] -> [StyledString]
    prependFirst s [] = [s]
    prependFirst s (s' : ss) = (s <> s') : ss
    msgPlain :: ByteString -> [StyledString]
    msgPlain = map styleMarkdownText . T.lines . safeDecodeUtf8

chatHelpInfo :: [StyledString]
chatHelpInfo =
  map
    styleMarkdown
    [ "Using chat:",
      highlight "/add <name>" <> "       - create invitation to send out-of-band",
      "                    to your contact <name>",
      "                    (any unique string without spaces)",
      highlight "/accept <name> <invitation>" <> " - accept <invitation>",
      "                    (a string that starts from \"smp::\")",
      "                    from your contact <name>",
      highlight "/name <name>" <> "      - set <name> to use in invitations",
      highlight "@<name> <message>" <> " - send <message> (any string) to contact <name>",
      "                    @<name> can be omitted to send to previous",
      highlight "/md" <> "               - markdown cheat-sheet"
    ]
  where
    highlight = Markdown (Colored Cyan)

markdownInfo :: [StyledString]
markdownInfo =
  map
    styleMarkdown
    [ "Markdown:",
      "  *bold*          - " <> Markdown Bold "bold text",
      "  _italic_        - " <> Markdown Italic "italic text" <> " (shown as underlined)",
      "  +underlined+    - " <> Markdown Underline "underlined text",
      "  ~strikethrough~ - " <> Markdown StrikeThrough "strikethrough text" <> " (shown as inverse)",
      "  `code snippet`  - " <> Markdown Snippet "a + b // no *markdown* here",
      "  !r text!        - " <> red "red text" <> " (red, green, blue, yellow, cyan, magenta)",
      "  !1 text!        - " <> red "also red text" <> " (1-6)",
      "  #secret#        - " <> Markdown Secret "secret text" <> " (can be copy-pasted)"
    ]
  where
    red = Markdown (Colored Red)

main :: IO ()
main = do
  ChatOpts {dbFileName, smpServer, termMode} <- welcomeGetOpts
  t <- getChatClient smpServer
  ct <- newChatTerminal (tbqSize cfg) termMode
  -- setLogLevel LogInfo -- LogError
  -- withGlobalLogging logCfg $
  env <- newSMPAgentEnv cfg {dbFile = dbFileName}
  dogFoodChat t ct env

welcomeGetOpts :: IO ChatOpts
welcomeGetOpts = do
  appDir <- getAppUserDataDirectory "simplex"
  opts@ChatOpts {dbFileName} <- getChatOpts appDir
  putStrLn "simpleX chat prototype"
  putStrLn $ "db: " <> dbFileName
  putStrLn "type \"/help\" for usage information"
  pure opts

dogFoodChat :: ChatClient -> ChatTerminal -> Env -> IO ()
dogFoodChat t ct env = do
  c <- runReaderT getSMPAgentClient env
  raceAny_
    [ runReaderT (runSMPAgentClient c) env,
      sendToAgent t ct c,
      sendToChatTerm t ct,
      receiveFromAgent t ct c,
      receiveFromChatTerm t ct,
      chatTerminal ct
    ]

getChatClient :: SMPServer -> IO ChatClient
getChatClient srv = atomically $ newChatClient (tbqSize cfg) srv

newChatClient :: Natural -> SMPServer -> STM ChatClient
newChatClient qSize smpServer = do
  inQ <- newTBQueue qSize
  outQ <- newTBQueue qSize
  return ChatClient {inQ, outQ, smpServer}

receiveFromChatTerm :: ChatClient -> ChatTerminal -> IO ()
receiveFromChatTerm t ct = forever $ do
  atomically (readTBQueue $ inputQ ct)
    >>= processOrError . A.parseOnly (chatCommandP <* A.endOfInput) . encodeUtf8 . T.pack
  where
    processOrError = \case
      Left err -> writeOutQ . ErrorInput $ B.pack err
      Right ChatHelp -> writeOutQ ChatHelpInfo
      Right MarkdownHelp -> writeOutQ MarkdownInfo
      Right cmd -> atomically $ writeTBQueue (inQ t) cmd
    writeOutQ = atomically . writeTBQueue (outQ t)

sendToChatTerm :: ChatClient -> ChatTerminal -> IO ()
sendToChatTerm ChatClient {outQ} ChatTerminal {outputQ} = forever $ do
  atomically (readTBQueue outQ) >>= \case
    NoChatResponse -> return ()
    resp -> atomically . writeTBQueue outputQ $ serializeChatResponse resp

sendToAgent :: ChatClient -> ChatTerminal -> AgentClient -> IO ()
sendToAgent ChatClient {inQ, smpServer} ct AgentClient {rcvQ} = do
  atomically $ writeTBQueue rcvQ ("1", "", SUBALL) -- hack for subscribing to all
  forever . atomically $ do
    cmd <- readTBQueue inQ
    writeTBQueue rcvQ `mapM_` agentTransmission cmd
    setActiveContact cmd
  where
    setActiveContact :: ChatCommand -> STM ()
    setActiveContact cmd =
      writeTVar (activeContact ct) $ case cmd of
        SendMessage a _ -> Just a
        _ -> Nothing
    agentTransmission :: ChatCommand -> Maybe (ATransmission 'Client)
    agentTransmission = \case
      AddConnection a -> transmission a $ NEW smpServer
      Connect a qInfo -> transmission a $ JOIN qInfo $ ReplyVia smpServer
      ResetChat -> transmission (Contact "") SUBALL
      SendMessage a msg -> transmission a $ SEND msg
      ChatHelp -> Nothing
      MarkdownHelp -> Nothing
    transmission :: Contact -> ACommand 'Client -> Maybe (ATransmission 'Client)
    transmission (Contact a) cmd = Just ("1", a, cmd)

receiveFromAgent :: ChatClient -> ChatTerminal -> AgentClient -> IO ()
receiveFromAgent t ct c = forever . atomically $ do
  resp <- chatResponse <$> readTBQueue (sndQ c)
  writeTBQueue (outQ t) resp
  setActiveContact resp
  where
    chatResponse :: ATransmission 'Agent -> ChatResponse
    chatResponse (_, a, resp) = case resp of
      INV qInfo -> Invitation qInfo
      CON -> Connected contact
      END -> Disconnected contact
      MSG {m_body} -> ReceivedMessage contact m_body
      SENT _ -> NoChatResponse
      OK -> Connected contact -- hack for subscribing to all
      ERR e -> ChatError e
      where
        contact = Contact a
    setActiveContact :: ChatResponse -> STM ()
    setActiveContact = \case
      Connected a -> set $ Just a
      ReceivedMessage a _ -> set $ Just a
      Disconnected _ -> set Nothing
      _ -> return ()
      where
        set a = writeTVar (activeContact ct) a
