{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Simplex.Messaging.Notifications.Server.Store where

import Control.Concurrent.STM
import Control.Monad (join)
import Data.ByteString.Char8 (ByteString)
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Notifications.Protocol
import Simplex.Messaging.TMap (TMap)
import qualified Simplex.Messaging.TMap as TM

type RegistrationKey = (DeviceToken, ByteString)

data NtfStore = NtfStore
  { tokens :: TMap NtfTokenId NtfTknData,
    tokenRegistrations :: TMap RegistrationKey NtfTokenId
  }

newNtfStore :: STM NtfStore
newNtfStore = do
  tokens <- TM.empty
  tokenRegistrations <- TM.empty
  pure NtfStore {tokens, tokenRegistrations}

data NtfTknData = NtfTknData
  { tokenId :: NtfTokenId,
    token :: DeviceToken,
    tknStatus :: TVar NtfTknStatus,
    tknVerifyKey :: C.APublicVerifyKey,
    tknDhKeys :: C.KeyPair 'C.X25519,
    tknDhSecret :: C.DhSecretX25519,
    tknRegCode :: NtfRegCode
  }

mkNtfTknData :: NtfTokenId -> NewNtfEntity 'Token -> C.KeyPair 'C.X25519 -> C.DhSecretX25519 -> NtfRegCode -> STM NtfTknData
mkNtfTknData tokenId (NewNtfTkn token tknVerifyKey _) tknDhKeys tknDhSecret tknRegCode = do
  tknStatus <- newTVar NTRegistered
  pure NtfTknData {tokenId, token, tknStatus, tknVerifyKey, tknDhKeys, tknDhSecret, tknRegCode}

-- data NtfSubscriptionsStore = NtfSubscriptionsStore

-- { subscriptions :: TMap NtfSubsciptionId NtfSubsciption,
--   activeSubscriptions :: TMap (SMPServer, NotifierId) NtfSubsciptionId
-- }
-- do
-- subscriptions <- newTVar M.empty
-- activeSubscriptions <- newTVar M.empty
-- pure NtfSubscriptionsStore {subscriptions, activeSubscriptions}

data NtfSubData = NtfSubData
  { smpQueue :: SMPQueueNtf,
    tokenId :: NtfTokenId,
    subStatus :: TVar NtfSubStatus
  }

data NtfEntityRec (e :: NtfEntity) where
  NtfTkn :: NtfTknData -> NtfEntityRec 'Token
  NtfSub :: NtfSubData -> NtfEntityRec 'Subscription

getNtfToken :: NtfStore -> NtfTokenId -> STM (Maybe NtfTknData)
getNtfToken st tknId = TM.lookup tknId (tokens st)

tknRegKey :: NtfTknData -> RegistrationKey
tknRegKey NtfTknData {token, tknVerifyKey} = (token, C.toPubKey C.pubKeyBytes tknVerifyKey)

newTknRegKey :: NewNtfEntity 'Token -> RegistrationKey
newTknRegKey (NewNtfTkn token tknVerifyKey _) = (token, C.toPubKey C.pubKeyBytes tknVerifyKey)

addNtfToken :: NtfStore -> NtfTokenId -> NtfTknData -> STM ()
addNtfToken st tknId tkn = do
  TM.insert tknId tkn $ tokens st
  TM.insert (tknRegKey tkn) tknId $ tokenRegistrations st

getNtfTokenRegistration :: NtfStore -> NewNtfEntity 'Token -> STM (Maybe NtfTknData)
getNtfTokenRegistration st tkn = do
  TM.lookup (newTknRegKey tkn) (tokenRegistrations st)
    >>= fmap join . mapM (`TM.lookup` tokens st)

deleteNtfToken :: NtfStore -> NtfTokenId -> STM ()
deleteNtfToken st tknId = do
  TM.lookupDelete tknId (tokens st)
    >>= mapM_ (\tkn -> TM.delete (tknRegKey tkn) $ tokenRegistrations st)

-- getNtfRec :: NtfStore -> SNtfEntity e -> NtfEntityId -> STM (Maybe (NtfEntityRec e))
-- getNtfRec st ent entId = case ent of
--   SToken -> NtfTkn <$$> TM.lookup entId (tokens st)
--   SSubscription -> pure Nothing

-- getNtfVerifyKey :: NtfStore -> SNtfEntity e -> NtfEntityId -> STM (Maybe (NtfEntityRec e, C.APublicVerifyKey))
-- getNtfVerifyKey st ent entId =
--   getNtfRec st ent entId >>= \case
--     Just r@(NtfTkn NtfTknData {tknVerifyKey}) -> pure $ Just (r, tknVerifyKey)
--     Just r@(NtfSub NtfSubData {tokenId}) ->
--       getNtfRec st SToken tokenId >>= \case
--         Just (NtfTkn NtfTknData {tknVerifyKey}) -> pure $ Just (r, tknVerifyKey)
--         _ -> pure Nothing
--     _ -> pure Nothing

-- mkNtfSubsciption :: SMPQueueNtf -> NtfTokenId -> STM NtfSubsciption
-- mkNtfSubsciption smpQueue tokenId = do
--   subStatus <- newTVar NSNew
--   pure NtfSubsciption {smpQueue, tokenId, subStatus}

-- getNtfSub :: NtfSubscriptionsStore -> NtfSubsciptionId -> STM (Maybe NtfSubsciption)
-- getNtfSub st subId = pure Nothing -- maybe (pure $ Left AUTH) (fmap Right . readTVar) . M.lookup subId . subscriptions =<< readTVar st

-- getNtfSubViaSMPQueue :: NtfSubscriptionsStore -> SMPQueueNtf -> STM (Maybe NtfSubsciption)
-- getNtfSubViaSMPQueue st smpQueue = pure Nothing

-- -- replace keeping status
-- updateNtfSub :: NtfSubscriptionsStore -> NtfSubsciption -> SMPQueueNtf -> NtfTokenId -> C.DhSecretX25519 -> STM (Maybe ())
-- updateNtfSub st sub smpQueue tokenId dhSecret = pure Nothing

-- addNtfSub :: NtfSubscriptionsStore -> NtfSubsciptionId -> NtfSubsciption -> STM (Maybe ())
-- addNtfSub st subId sub = pure Nothing

-- deleteNtfSub :: NtfSubscriptionsStore -> NtfSubsciptionId -> STM ()
-- deleteNtfSub st subId = pure ()
