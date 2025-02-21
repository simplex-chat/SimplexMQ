{-# LANGUAGE CPP #-}

module Simplex.Messaging.Agent.Store.DB
#if defined(dbPostgres)
  ( module Simplex.Messaging.Agent.Store.Postgres.DB,
    FromField (..),
    ToField (..),
  )
  where
import Simplex.Messaging.Agent.Store.Postgres.DB
#else
  ( module Simplex.Messaging.Agent.Store.SQLite.DB,
    FromField (..),
    ToField (..),
  )
  where
import Simplex.Messaging.Agent.Store.SQLite.DB
#endif
