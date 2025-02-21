{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Simplex.Messaging.Server.StoreLog.ReadWrite where

import Control.Concurrent.STM
import Control.Logger.Simple
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import Data.Text.Encoding (decodeLatin1)
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server.QueueStore (QueueRec)
import Simplex.Messaging.Server.QueueStore.Types
import Simplex.Messaging.Server.StoreLog
import Simplex.Messaging.Util (tshow)
import System.IO

writeQueueStore :: forall q s. QueueStoreClass q s => StoreLog 'WriteMode -> s -> IO ()
writeQueueStore s st = withLoadedQueues st $ writeQueue
  where
    writeQueue :: q -> IO ()
    writeQueue q = do
      let rId = recipientId q
      readTVarIO (queueRec q) >>= \case
        Just q' -> logCreateQueue s rId q'
        Nothing -> pure ()

readQueueStore :: forall q s. QueueStoreClass q s => Bool -> (RecipientId -> QueueRec -> IO q) -> FilePath -> s -> IO ()
readQueueStore tty mkQ f st = readLogLines tty f $ \_ -> processLine
  where
    processLine :: B.ByteString -> IO ()
    processLine s = either printError procLogRecord (strDecode s)
      where
        procLogRecord :: StoreLogRecord -> IO ()
        procLogRecord = \case
          CreateQueue rId qr -> addQueue_ st mkQ rId qr >>= qError rId "CreateQueue"
          SecureQueue qId sKey -> withQueue qId "SecureQueue" $ \q -> secureQueue st q sKey
          AddNotifier qId ntfCreds -> withQueue qId "AddNotifier" $ \q -> addQueueNotifier st q ntfCreds
          SuspendQueue qId -> withQueue qId "SuspendQueue" $ suspendQueue st
          BlockQueue qId info -> withQueue qId "BlockQueue" $ \q -> blockQueue st q info
          UnblockQueue qId -> withQueue qId "UnblockQueue" $ unblockQueue st
          DeleteQueue qId -> withQueue qId "DeleteQueue" $ deleteStoreQueue st
          DeleteNotifier qId -> withQueue qId "DeleteNotifier" $ deleteQueueNotifier st
          UpdateTime qId t -> withQueue qId "UpdateTime" $ \q -> updateQueueTime st q t
        printError :: String -> IO ()
        printError e = B.putStrLn $ "Error parsing log: " <> B.pack e <> " - " <> s
        withQueue :: forall a. RecipientId -> T.Text -> (q -> IO (Either ErrorType a)) -> IO ()
        withQueue qId op a = runExceptT go >>= qError qId op
          where
            go = do
              q <- ExceptT $ getQueue_ st mkQ SRecipient qId
              liftIO (readTVarIO $ queueRec q) >>= \case
                Nothing -> logWarn $ logPfx qId op <> "already deleted"
                Just _ -> void $ ExceptT $ a q
        qError qId op = \case
          Left e -> logError $ logPfx qId op <> tshow e
          Right _ -> pure ()
        logPfx qId op = "STORE: " <> op <> ", stored queue " <> decodeLatin1 (strEncode qId) <> ", "
