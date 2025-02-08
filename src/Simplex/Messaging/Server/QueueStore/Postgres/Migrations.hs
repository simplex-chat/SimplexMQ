{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE QuasiQuotes #-}

module Simplex.Messaging.Server.QueueStore.Postgres.Migrations where

import Data.List (sortOn)
import Data.Text (Text)
import qualified Data.Text as T
import Simplex.Messaging.Agent.Store.Shared
import Text.RawString.QQ (r)

serverSchemaMigrations :: [(String, Text, Maybe Text)]
serverSchemaMigrations =
  [ ("20250207_initial", m20250207_initial, Nothing)
  ]

-- | The list of migrations in ascending order by date
serverMigrations :: [Migration]
serverMigrations = sortOn name $ map migration serverSchemaMigrations
  where
    migration (name, up, down) = Migration {name, up, down = down}

m20250207_initial :: Text
m20250207_initial =
  T.pack
    [r|
CREATE TABLE msg_queues(
  msg_queue_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  recipient_id BYTEA NOT NULL,
  recipient_key BYTEA NOT NULL,
  rcv_dh_secret BYTEA NOT NULL,
  sender_id BYTEA NOT NULL,
  sender_key BYTEA NOT NULL,
  snd_secure BOOLEAN NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
)

CREATE TABLE msg_notifiers(
  msg_notifier_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  msg_queue_id BIGINT NOT NULL REFERENCES msg_queues ON DELETE CASCADE ON UPDATE RESTRICT,
  notifier_id BYTEA NOT NULL,
  notifier_key BYTEA NOT NULL,
  rcv_ntf_dh_secret BYTEA NOT NULL
)

CREATE UNIQUE INDEX idx_msg_queues_recipient_id ON msg_queues(recipient_id);
CREATE UNIQUE INDEX idx_msg_queues_sender_id ON msg_queues(sender_id);
CREATE UNIQUE INDEX idx_msg_notifiers_notifier_id ON msg_notifiers(notifier_id);
    |]
