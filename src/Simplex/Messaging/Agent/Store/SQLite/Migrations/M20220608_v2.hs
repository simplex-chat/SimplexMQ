{-# LANGUAGE QuasiQuotes #-}

module Simplex.Messaging.Agent.Store.SQLite.Migrations.M20220608_v2 where

import Database.SQLite.Simple (Query)
import Database.SQLite.Simple.QQ (sql)

m20220608_v2 :: Query
m20220608_v2 =
  [sql|
ALTER TABLE messages ADD COLUMN msg_flags TEXT NULL;

ALTER TABLE conn_confirmations ADD COLUMN smp_reply_queues BLOB NULL;

ALTER TABLE connections ADD COLUMN duplex_handshake INTEGER NULL DEFAULT 0;

ALTER TABLE rcv_messages ADD COLUMN user_ack INTEGER NULL DEFAULT 0;

ALTER TABLE rcv_queues ADD COLUMN ntf_public_key BLOB;

ALTER TABLE rcv_queues ADD COLUMN ntf_private_key BLOB;

ALTER TABLE rcv_queues ADD COLUMN ntf_id BLOB;

CREATE UNIQUE INDEX idx_rcv_queues_ntf ON rcv_queues (host, port, ntf_id);

CREATE TABLE ntf_subscriptions (
  smp_host TEXT NULL,
  smp_port TEXT NULL,
  smp_rcv_id BLOB NULL,
  smp_ntf_id BLOB,
  -- ntf_provider TEXT NOT NULL, -- apn
  -- ntf_token TEXT NOT NULL, -- or BLOB?
  ntf_host TEXT NOT NULL,
  ntf_port TEXT NOT NULL,
  ntf_sub_id BLOB,
  ntf_sub_status TEXT NOT NULL, -- started, key, new, created, active, pending, error_auth
  ntf_sub_action TEXT, -- if there is an action required on this subscription: create / check / delete
  ntf_sub_smp_action TEXT, -- action with SMP server: nkey; only one of this and ntf_sub_action can (should) be not null in same record
  ntf_sub_action_ts TEXT, -- the earliest time for the action, e.g. checks can be scheduled every X hours
  updated_by_supervisor INTEGER NOT NULL DEFAULT 0, -- to be checked on updates by workers to not overwrite supervisor command (state still should be updated)
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')), -- this is to check subscription status periodically to know when it was last checked
  PRIMARY KEY (smp_host, smp_port, smp_rcv_id),
  FOREIGN KEY (smp_host, smp_port, smp_rcv_id) REFERENCES rcv_queues (host, port, rcv_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  FOREIGN KEY (ntf_host, ntf_port) REFERENCES ntf_servers
    ON DELETE RESTRICT ON UPDATE CASCADE
  -- FOREIGN KEY (ntf_provider, ntf_token, ntf_host, ntf_port) REFERENCES ntf_tokens (provider, device_token, ntf_host, ntf_port)
  --   ON DELETE RESTRICT ON UPDATE CASCADE
) WITHOUT ROWID;
|]
