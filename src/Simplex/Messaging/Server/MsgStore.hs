{-# LANGUAGE FunctionalDependencies #-}

module Simplex.Messaging.Server.MsgStore where

import Data.Int (Int64)
import Data.Time.Clock.System (SystemTime)
import Numeric.Natural
import Simplex.Messaging.Protocol (MsgBody, MsgFlags, MsgId, RecipientId)

data Message = Message
  { msgId :: MsgId,
    ts :: SystemTime,
    msgFlags :: MsgFlags,
    msgBody :: MsgBody
  }

class MonadMsgStore s q m | s -> q where
  getMsgQueue :: s -> RecipientId -> Natural -> m q
  delMsgQueue :: s -> RecipientId -> m ()

class MonadMsgQueue q m where
  isFull :: q -> m Bool
  writeMsg :: q -> Message -> m () -- non blocking
  tryPeekMsg :: q -> m (Maybe Message) -- non blocking
  peekMsg :: q -> m Message -- blocking
  tryDelMsg :: q -> MsgId -> m Bool -- non blocking
  tryDelPeekMsg :: q -> MsgId -> m (Bool, Maybe Message) -- atomic delete (== read) last and peek next message, if available
  deleteExpiredMsgs :: q -> Int64 -> m ()
