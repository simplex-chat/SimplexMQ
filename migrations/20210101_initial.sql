CREATE TABLE servers (
  host TEXT NOT NULL,
  port TEXT,
  key_hash BLOB,
  PRIMARY KEY (host, port)
) WITHOUT ROWID;

CREATE TABLE connections (
  conn_alias BLOB NOT NULL PRIMARY KEY,
  conn_mode TEXT NOT NULL,
  last_internal_msg_id INTEGER NOT NULL DEFAULT 0,
  last_internal_rcv_msg_id INTEGER NOT NULL DEFAULT 0,
  last_internal_snd_msg_id INTEGER NOT NULL DEFAULT 0,
  last_external_snd_msg_id INTEGER NOT NULL DEFAULT 0,
  last_rcv_msg_hash BLOB NOT NULL DEFAULT x'',
  last_snd_msg_hash BLOB NOT NULL DEFAULT x''
) WITHOUT ROWID;

CREATE TABLE rcv_queues (
  host TEXT NOT NULL,
  port TEXT,
  rcv_id BLOB NOT NULL,
  conn_alias BLOB NOT NULL REFERENCES connections ON DELETE CASCADE,
  rcv_private_key BLOB NOT NULL,
  rcv_dh_secret BLOB NOT NULL,
  e2e_priv_key BLOB NOT NULL,
  e2e_snd_pub_key BLOB,
  e2e_dh_secret BLOB,
  snd_id BLOB NOT NULL,
  snd_key BLOB,
  status TEXT NOT NULL,
  PRIMARY KEY (host, port, rcv_id),
  FOREIGN KEY (host, port) REFERENCES servers
    ON DELETE RESTRICT ON UPDATE CASCADE,
  UNIQUE (host, port, snd_id)
) WITHOUT ROWID;

CREATE TABLE snd_queues (
  host TEXT NOT NULL,
  port TEXT,
  snd_id BLOB NOT NULL,
  conn_alias BLOB NOT NULL REFERENCES connections ON DELETE CASCADE,
  snd_private_key BLOB NOT NULL,
  e2e_pub_key BLOB NOT NULL,
  e2e_dh_secret BLOB NOT NULL,
  status TEXT NOT NULL,
  PRIMARY KEY (host, port, snd_id),
  FOREIGN KEY (host, port) REFERENCES servers
    ON DELETE RESTRICT ON UPDATE CASCADE
) WITHOUT ROWID;

CREATE TABLE messages (
  conn_alias BLOB NOT NULL REFERENCES connections (conn_alias)
    ON DELETE CASCADE,
  internal_id INTEGER NOT NULL,
  internal_ts TEXT NOT NULL,
  internal_rcv_id INTEGER,
  internal_snd_id INTEGER,
  msg_body BLOB NOT NULL DEFAULT x'',
  PRIMARY KEY (conn_alias, internal_id),
  FOREIGN KEY (conn_alias, internal_rcv_id) REFERENCES rcv_messages
    ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (conn_alias, internal_snd_id) REFERENCES snd_messages
    ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
) WITHOUT ROWID;

CREATE TABLE rcv_messages (
  conn_alias BLOB NOT NULL,
  internal_rcv_id INTEGER NOT NULL,
  internal_id INTEGER NOT NULL,
  external_snd_id INTEGER NOT NULL,
  broker_id BLOB NOT NULL,
  broker_ts TEXT NOT NULL,
  rcv_status TEXT NOT NULL,
  ack_brocker_ts TEXT,
  internal_hash BLOB NOT NULL,
  external_prev_snd_hash BLOB NOT NULL,
  integrity BLOB NOT NULL,
  PRIMARY KEY (conn_alias, internal_rcv_id),
  FOREIGN KEY (conn_alias, internal_id) REFERENCES messages
    ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE snd_messages (
  conn_alias BLOB NOT NULL,
  internal_snd_id INTEGER NOT NULL,
  internal_id INTEGER NOT NULL,
  snd_status TEXT NOT NULL,
  sent_ts TEXT,
  internal_hash BLOB NOT NULL,
  previous_msg_hash BLOB NOT NULL DEFAULT x'',
  PRIMARY KEY (conn_alias, internal_snd_id),
  FOREIGN KEY (conn_alias, internal_id) REFERENCES messages
    ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE conn_confirmations (
  confirmation_id BLOB NOT NULL PRIMARY KEY,
  conn_alias BLOB NOT NULL REFERENCES connections ON DELETE CASCADE,
  e2e_snd_pub_key BLOB NOT NULL,
  sender_key BLOB NOT NULL,
  sender_conn_info BLOB NOT NULL,
  accepted INTEGER NOT NULL,
  own_conn_info BLOB,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
) WITHOUT ROWID;

CREATE TABLE conn_invitations (
  invitation_id BLOB NOT NULL PRIMARY KEY,
  contact_conn_id BLOB NOT NULL REFERENCES connections ON DELETE CASCADE,
  cr_invitation BLOB NOT NULL,
  recipient_conn_info BLOB NOT NULL,
  accepted INTEGER NOT NULL DEFAULT 0,
  own_conn_info BLOB,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
) WITHOUT ROWID;
