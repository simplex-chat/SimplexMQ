{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module ServerTests where

import Control.Concurrent (ThreadId, killThread, threadDelay)
import Control.Concurrent.STM
import Control.Exception (SomeException, try)
import Control.Monad
import Control.Monad.IO.Class
import CoreTests.MsgStoreTests (testJournalStoreCfg)
import Data.Bifunctor (first)
import Data.ByteString.Base64
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Hashable (hash)
import qualified Data.IntSet as IS
import Data.Type.Equality
import GHC.Stack (withFrozenCallStack)
import SMPClient
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (parseAll)
import Simplex.Messaging.Protocol
import Simplex.Messaging.Server (exportMessages)
import Simplex.Messaging.Server.Env.STM (AServerStoreCfg (..), AStoreType (..), ServerConfig (..), ServerStoreCfg (..), readWriteQueueStore)
import Simplex.Messaging.Server.Expiration
import Simplex.Messaging.Server.MsgStore.Journal (JournalStoreConfig (..), QStoreCfg (..))
import Simplex.Messaging.Server.MsgStore.Types (MsgStoreClass (..), SQSType (..), SMSType (..), newMsgStore)
import Simplex.Messaging.Server.Stats (PeriodStatsData (..), ServerStatsData (..))
import Simplex.Messaging.Server.StoreLog (StoreLogRecord (..), closeStoreLog)
import Simplex.Messaging.Transport
import Simplex.Messaging.Util (whenM)
import Simplex.Messaging.Version (mkVersionRange)
import System.Directory (doesDirectoryExist, doesFileExist, removeDirectoryRecursive, removeFile)
import System.IO (IOMode (..), withFile)
import System.TimeIt (timeItT)
import System.Timeout
import Test.HUnit
import Test.Hspec
import Util (removeFileIfExists)

serverTests :: SpecWith (ATransport, AStoreType)
serverTests = do
  describe "SMP queues" $ do
    describe "NEW and KEY commands, SEND messages" testCreateSecure
    describe "NEW and SKEY commands" $ do
      testCreateSndSecure
      testSndSecureProhibited
    describe "NEW, OFF and DEL commands, SEND messages" testCreateDelete
    describe "Stress test" stressTest
    describe "allowNewQueues setting" testAllowNewQueues
  describe "SMP messages" $ do
    describe "duplex communication over 2 SMP connections" testDuplex
    describe "switch subscription to another TCP connection" testSwitchSub
    describe "GET command" testGetCommand
    describe "GET & SUB commands" testGetSubCommands
    describe "Exceeding queue quota" testExceedQueueQuota
  describe "Store log" testWithStoreLog
  describe "Restore messages" testRestoreMessages
  describe "Restore messages (old / v2)" testRestoreExpireMessages
  describe "Save prometheus metrics" testPrometheusMetrics
  describe "Timing of AUTH error" testTiming
  describe "Message notifications" testMessageNotifications
  describe "Message expiration" $ do
    testMsgExpireOnSend
    testMsgExpireOnInterval
    testMsgNOTExpireOnInterval
  describe "Blocking queues" $ testBlockMessageQueue

pattern Resp :: CorrId -> QueueId -> BrokerMsg -> SignedTransmission ErrorType BrokerMsg
pattern Resp corrId queueId command <- (_, _, (corrId, queueId, Right command))

pattern Ids :: RecipientId -> SenderId -> RcvPublicDhKey -> BrokerMsg
pattern Ids rId sId srvDh <- IDS (QIK rId sId srvDh _sndSecure)

pattern Msg :: MsgId -> MsgBody -> BrokerMsg
pattern Msg msgId body <- MSG RcvMessage {msgId, msgBody = EncRcvMsgBody body}

sendRecv :: forall c p. (Transport c, PartyI p) => THandleSMP c 'TClient -> (Maybe TransmissionAuth, ByteString, EntityId, Command p) -> IO (SignedTransmission ErrorType BrokerMsg)
sendRecv h@THandle {params} (sgn, corrId, qId, cmd) = do
  let TransmissionForAuth {tToSend} = encodeTransmissionForAuth params (CorrId corrId, qId, cmd)
  Right () <- tPut1 h (sgn, tToSend)
  tGet1 h

signSendRecv :: forall c p. (Transport c, PartyI p) => THandleSMP c 'TClient -> C.APrivateAuthKey -> (ByteString, EntityId, Command p) -> IO (SignedTransmission ErrorType BrokerMsg)
signSendRecv h@THandle {params} (C.APrivateAuthKey a pk) (corrId, qId, cmd) = do
  let TransmissionForAuth {tForAuth, tToSend} = encodeTransmissionForAuth params (CorrId corrId, qId, cmd)
  Right () <- tPut1 h (authorize tForAuth, tToSend)
  tGet1 h
  where
    authorize t = case a of
      C.SEd25519 -> Just . TASignature . C.ASignature C.SEd25519 $ C.sign' pk t
      C.SEd448 -> Just . TASignature . C.ASignature C.SEd448 $ C.sign' pk t
      C.SX25519 -> (\THAuthClient {serverPeerPubKey = k} -> TAAuthenticator $ C.cbAuthenticate k pk (C.cbNonce corrId) t) <$> thAuth params
#if !MIN_VERSION_base(4,18,0)
      _sx448 -> undefined -- ghc8107 fails to the branch excluded by types
#endif

tPut1 :: Transport c => THandle v c 'TClient -> SentRawTransmission -> IO (Either TransportError ())
tPut1 h t = do
  [r] <- tPut h [Right t]
  pure r

tGet1 :: (ProtocolEncoding v err cmd, Transport c) => THandle v c 'TClient -> IO (SignedTransmission err cmd)
tGet1 h = do
  [r] <- liftIO $ tGet h
  pure r

(#==) :: (HasCallStack, Eq a, Show a) => (a, a) -> String -> Assertion
(actual, expected) #== message = assertEqual message expected actual

_SEND :: MsgBody -> Command 'Sender
_SEND = SEND noMsgFlags

_SEND' :: MsgBody -> Command 'Sender
_SEND' = SEND MsgFlags {notification = True}

decryptMsgV2 :: C.DhSecret 'C.X25519 -> ByteString -> ByteString -> Either C.CryptoError ByteString
decryptMsgV2 dhShared = C.cbDecrypt dhShared . C.cbNonce

decryptMsgV3 :: C.DhSecret 'C.X25519 -> ByteString -> ByteString -> Either String MsgBody
decryptMsgV3 dhShared nonce body =
  case parseAll clientRcvMsgBodyP =<< first show (C.cbDecrypt dhShared (C.cbNonce nonce) body) of
    Right ClientRcvMsgBody {msgBody} -> Right msgBody
    Right ClientRcvMsgQuota {} -> Left "ClientRcvMsgQuota"
    Left e -> Left e

testCreateSecure :: SpecWith (ATransport, AStoreType)
testCreateSecure =
  it "should create (NEW) and secure (KEY) queue" $ \(ATransport t, msType) ->
    smpTest2 t msType $ \r s -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" rId1 (Ids rId sId srvDh) <- signSendRecv r rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
      let dec = decryptMsgV3 $ C.dh' srvDh dhPriv
      (rId1, NoEntity) #== "creates queue"

      Resp "bcda" sId1 ok1 <- sendRecv s ("", "bcda", sId, _SEND "hello")
      (ok1, OK) #== "accepts unsigned SEND"
      (sId1, sId) #== "same queue ID in response 1"

      Resp "" _ (Msg mId1 msg1) <- tGet1 r
      (dec mId1 msg1, Right "hello") #== "delivers message"

      Resp "cdab" _ ok4 <- signSendRecv r rKey ("cdab", rId, ACK mId1)
      (ok4, OK) #== "replies OK when message acknowledged if no more messages"

      Resp "dabc" _ err6 <- signSendRecv r rKey ("dabc", rId, ACK mId1)
      (err6, ERR NO_MSG) #== "replies ERR when message acknowledged without messages"

      (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "abcd" sId2 err1 <- signSendRecv s sKey ("abcd", sId, _SEND "hello")
      (err1, ERR AUTH) #== "rejects signed SEND"
      (sId2, sId) #== "same queue ID in response 2"

      Resp "bcda" _ err2 <- sendRecv r (sampleSig, "bcda", rId, KEY sPub)
      (err2, ERR AUTH) #== "rejects KEY with wrong signature"

      Resp "cdab" _ err3 <- signSendRecv r rKey ("cdab", sId, KEY sPub)
      (err3, ERR AUTH) #== "rejects KEY with sender's ID"

      Resp "dabc" rId2 ok2 <- signSendRecv r rKey ("dabc", rId, KEY sPub)
      (ok2, OK) #== "secures queue"
      (rId2, rId) #== "same queue ID in response 3"

      Resp "abcd" _ OK <- signSendRecv r rKey ("abcd", rId, KEY sPub)
      (sPub', _) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "abcd" _ err4 <- signSendRecv r rKey ("abcd", rId, KEY sPub')
      (err4, ERR AUTH) #== "rejects if secured with different key"

      Resp "bcda" _ ok3 <- signSendRecv s sKey ("bcda", sId, _SEND "hello again")
      (ok3, OK) #== "accepts signed SEND"

      Resp "" _ (Msg mId2 msg2) <- tGet1 r
      (dec mId2 msg2, Right "hello again") #== "delivers message 2"

      Resp "cdab" _ ok5 <- signSendRecv r rKey ("cdab", rId, ACK mId2)
      (ok5, OK) #== "replies OK when message acknowledged 2"

      Resp "dabc" _ err5 <- sendRecv s ("", "dabc", sId, _SEND "hello")
      (err5, ERR AUTH) #== "rejects unsigned SEND"

      let maxAllowedMessage = B.replicate (maxMessageLength currentClientSMPRelayVersion) '-'
      Resp "bcda" _ OK <- signSendRecv s sKey ("bcda", sId, _SEND maxAllowedMessage)
      Resp "" _ (Msg mId3 msg3) <- tGet1 r
      (dec mId3 msg3, Right maxAllowedMessage) #== "delivers message of max size"

      let biggerMessage = B.replicate (maxMessageLength currentClientSMPRelayVersion + 1) '-'
      Resp "bcda" _ (ERR LARGE_MSG) <- signSendRecv s sKey ("bcda", sId, _SEND biggerMessage)
      pure ()

testCreateSndSecure :: SpecWith (ATransport, AStoreType)
testCreateSndSecure =
  it "should create (NEW) and secure (SKEY) queue by sender" $ \(ATransport t, msType) ->
    smpTest2 t msType $ \r s -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" rId1 (Ids rId sId srvDh) <- signSendRecv r rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe True)
      let dec = decryptMsgV3 $ C.dh' srvDh dhPriv
      (rId1, NoEntity) #== "creates queue"

      (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "bcda" _ err2 <- sendRecv s (sampleSig, "bcda", sId, SKEY sPub)
      (err2, ERR AUTH) #== "rejects SKEY with wrong signature"

      Resp "cdab" _ err3 <- signSendRecv s sKey ("cdab", rId, SKEY sPub)
      (err3, ERR AUTH) #== "rejects SKEY with recipients's ID"

      Resp "dabc" sId2 OK <- signSendRecv s sKey ("dabc", sId, SKEY sPub)
      (sId2, sId) #== "secures queue, same queue ID in response"

      (sPub', _) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "abcd" _ err4 <- signSendRecv s sKey ("abcd", sId, SKEY sPub')
      (err4, ERR AUTH) #== "rejects if secured with different key"
      Resp "abcd" _ OK <- signSendRecv s sKey ("abcd", sId, SKEY sPub)

      Resp "bcda" _ ok3 <- signSendRecv s sKey ("bcda", sId, _SEND "hello")
      (ok3, OK) #== "accepts signed SEND"

      Resp "" _ (Msg mId2 msg2) <- tGet1 r
      (dec mId2 msg2, Right "hello") #== "delivers message"

      Resp "cdab" _ ok5 <- signSendRecv r rKey ("cdab", rId, ACK mId2)
      (ok5, OK) #== "replies OK when message acknowledged"

      Resp "dabc" _ err5 <- sendRecv s ("", "dabc", sId, _SEND "hello")
      (err5, ERR AUTH) #== "rejects unsigned SEND"

      let maxAllowedMessage = B.replicate (maxMessageLength currentClientSMPRelayVersion) '-'
      Resp "bcda" _ OK <- signSendRecv s sKey ("bcda", sId, _SEND maxAllowedMessage)
      Resp "" _ (Msg mId3 msg3) <- tGet1 r
      (dec mId3 msg3, Right maxAllowedMessage) #== "delivers message of max size"

      let biggerMessage = B.replicate (maxMessageLength currentClientSMPRelayVersion + 1) '-'
      Resp "bcda" _ (ERR LARGE_MSG) <- signSendRecv s sKey ("bcda", sId, _SEND biggerMessage)
      pure ()

testSndSecureProhibited :: SpecWith (ATransport, AStoreType)
testSndSecureProhibited =
  it "should create (NEW) without allowing sndSecure and fail to and secure queue by sender (SKEY)" $ \(ATransport t, msType) ->
    smpTest2 t msType $ \r s -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (dhPub, _dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" rId1 (Ids _rId sId _srvDh) <- signSendRecv r rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
      (rId1, NoEntity) #== "creates queue"

      (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "dabc" sId2 err <- signSendRecv s sKey ("dabc", sId, SKEY sPub)
      (sId2, sId) #== "secures queue, same queue ID in response"
      (err, ERR AUTH) #== "rejects SKEY when not allowed in NEW command"

testCreateDelete :: SpecWith (ATransport, AStoreType)
testCreateDelete =
  it "should create (NEW), suspend (OFF) and delete (DEL) queue" $ \(ATransport t, msType) ->
    smpTest2 t msType $ \rh sh -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
      (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" rId1 (Ids rId sId srvDh) <- signSendRecv rh rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
      let dec = decryptMsgV3 $ C.dh' srvDh dhPriv
      (rId1, NoEntity) #== "creates queue"

      (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
      Resp "bcda" _ ok1 <- signSendRecv rh rKey ("bcda", rId, KEY sPub)
      (ok1, OK) #== "secures queue"

      Resp "cdab" _ ok2 <- signSendRecv sh sKey ("cdab", sId, _SEND "hello")
      (ok2, OK) #== "accepts signed SEND"

      Resp "dabc" _ ok7 <- signSendRecv sh sKey ("dabc", sId, _SEND "hello 2")
      (ok7, OK) #== "accepts signed SEND 2 - this message is not delivered because the first is not ACKed"

      Resp "" _ (Msg mId1 msg1) <- tGet1 rh
      (dec mId1 msg1, Right "hello") #== "delivers message"

      Resp "abcd" _ err1 <- sendRecv rh (sampleSig, "abcd", rId, OFF)
      (err1, ERR AUTH) #== "rejects OFF with wrong signature"

      Resp "bcda" _ err2 <- signSendRecv rh rKey ("bcda", sId, OFF)
      (err2, ERR AUTH) #== "rejects OFF with sender's ID"

      Resp "cdab" rId2 ok3 <- signSendRecv rh rKey ("cdab", rId, OFF)
      (ok3, OK) #== "suspends queue"
      (rId2, rId) #== "same queue ID in response 2"

      Resp "dabc" _ err3 <- signSendRecv sh sKey ("dabc", sId, _SEND "hello")
      (err3, ERR AUTH) #== "rejects signed SEND"

      Resp "abcd" _ err4 <- sendRecv sh ("", "abcd", sId, _SEND "hello")
      (err4, ERR AUTH) #== "reject unsigned SEND too"

      Resp "bcda" _ ok4 <- signSendRecv rh rKey ("bcda", rId, OFF)
      (ok4, OK) #== "accepts OFF when suspended"

      Resp "cdab" _ (Msg mId2 msg2) <- signSendRecv rh rKey ("cdab", rId, SUB)
      (dec mId2 msg2, Right "hello") #== "accepts SUB when suspended and delivers the message again (because was not ACKed)"

      Resp "dabc" _ err5 <- sendRecv rh (sampleSig, "dabc", rId, DEL)
      (err5, ERR AUTH) #== "rejects DEL with wrong signature"

      Resp "abcd" _ err6 <- signSendRecv rh rKey ("abcd", sId, DEL)
      (err6, ERR AUTH) #== "rejects DEL with sender's ID"

      Resp "bcda" rId3 ok6 <- signSendRecv rh rKey ("bcda", rId, DEL)
      (ok6, OK) #== "deletes queue"
      (rId3, rId) #== "same queue ID in response 3"

      Resp "cdab" _ err7 <- signSendRecv sh sKey ("cdab", sId, _SEND "hello")
      (err7, ERR AUTH) #== "rejects signed SEND when deleted"

      Resp "dabc" _ err8 <- sendRecv sh ("", "dabc", sId, _SEND "hello")
      (err8, ERR AUTH) #== "rejects unsigned SEND too when deleted"

      Resp "abcd" _ err11 <- signSendRecv rh rKey ("abcd", rId, ACK "")
      (err11, ERR AUTH) #== "rejects ACK when conn deleted - the second message is deleted"

      Resp "bcda" _ err9 <- signSendRecv rh rKey ("bcda", rId, OFF)
      (err9, ERR AUTH) #== "rejects OFF when deleted"

      Resp "cdab" _ err10 <- signSendRecv rh rKey ("cdab", rId, SUB)
      (err10, ERR AUTH) #== "rejects SUB when deleted"

stressTest :: SpecWith (ATransport, AStoreType)
stressTest =
  it "should create many queues, disconnect and re-connect" $ \(ATransport t, msType) ->
    smpTest3 t msType $ \h1 h2 h3 -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
      (dhPub, _ :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      rIds <- forM ([1 .. 50] :: [Int]) . const $ do
        Resp "" NoEntity (Ids rId _ _) <- signSendRecv h1 rKey ("", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
        pure rId
      let subscribeQueues h = forM_ rIds $ \rId -> do
            Resp "" rId' OK <- signSendRecv h rKey ("", rId, SUB)
            rId' `shouldBe` rId
      closeConnection $ connection h1
      subscribeQueues h2
      closeConnection $ connection h2
      subscribeQueues h3

testAllowNewQueues :: SpecWith (ATransport, AStoreType)
testAllowNewQueues =
  it "should prohibit creating new queues with allowNewQueues = False" $ \(ATransport (t :: TProxy c), msType) ->
    withSmpServerConfigOn (ATransport t) (cfgMS msType) {allowNewQueues = False} testPort $ \_ ->
      testSMPClient @c $ \h -> do
        g <- C.newRandom
        (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
        (dhPub, _ :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
        Resp "abcd" NoEntity (ERR AUTH) <- signSendRecv h rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
        pure ()

testDuplex :: SpecWith (ATransport, AStoreType)
testDuplex =
  it "should create 2 simplex connections and exchange messages" $ \(ATransport t, msType) ->
    smpTest2 t msType $ \alice bob -> do
      g <- C.newRandom
      (arPub, arKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (aDhPub, aDhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" _ (Ids aRcv aSnd aSrvDh) <- signSendRecv alice arKey ("abcd", NoEntity, NEW arPub aDhPub Nothing SMSubscribe False)
      let aDec = decryptMsgV3 $ C.dh' aSrvDh aDhPriv
      -- aSnd ID is passed to Bob out-of-band

      (bsPub, bsKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "bcda" _ OK <- sendRecv bob ("", "bcda", aSnd, _SEND $ "key " <> strEncode bsPub)
      -- "key ..." is ad-hoc, not a part of SMP protocol

      Resp "" _ (Msg mId1 msg1) <- tGet1 alice
      Resp "cdab" _ OK <- signSendRecv alice arKey ("cdab", aRcv, ACK mId1)
      Right ["key", bobKey] <- pure $ B.words <$> aDec mId1 msg1
      (bobKey, strEncode bsPub) #== "key received from Bob"
      Resp "dabc" _ OK <- signSendRecv alice arKey ("dabc", aRcv, KEY bsPub)

      (brPub, brKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (bDhPub, bDhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" _ (Ids bRcv bSnd bSrvDh) <- signSendRecv bob brKey ("abcd", NoEntity, NEW brPub bDhPub Nothing SMSubscribe False)
      let bDec = decryptMsgV3 $ C.dh' bSrvDh bDhPriv
      Resp "bcda" _ OK <- signSendRecv bob bsKey ("bcda", aSnd, _SEND $ "reply_id " <> encode (unEntityId bSnd))
      -- "reply_id ..." is ad-hoc, not a part of SMP protocol

      Resp "" _ (Msg mId2 msg2) <- tGet1 alice
      Resp "cdab" _ OK <- signSendRecv alice arKey ("cdab", aRcv, ACK mId2)
      Right ["reply_id", bId] <- pure $ B.words <$> aDec mId2 msg2
      (bId, encode (unEntityId bSnd)) #== "reply queue ID received from Bob"

      (asPub, asKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "dabc" _ OK <- sendRecv alice ("", "dabc", bSnd, _SEND $ "key " <> strEncode asPub)
      -- "key ..." is ad-hoc, not a part of  SMP protocol

      Resp "" _ (Msg mId3 msg3) <- tGet1 bob
      Resp "abcd" _ OK <- signSendRecv bob brKey ("abcd", bRcv, ACK mId3)
      Right ["key", aliceKey] <- pure $ B.words <$> bDec mId3 msg3
      (aliceKey, strEncode asPub) #== "key received from Alice"
      Resp "bcda" _ OK <- signSendRecv bob brKey ("bcda", bRcv, KEY asPub)

      Resp "cdab" _ OK <- signSendRecv bob bsKey ("cdab", aSnd, _SEND "hi alice")

      Resp "" _ (Msg mId4 msg4) <- tGet1 alice
      Resp "dabc" _ OK <- signSendRecv alice arKey ("dabc", aRcv, ACK mId4)
      (aDec mId4 msg4, Right "hi alice") #== "message received from Bob"

      Resp "abcd" _ OK <- signSendRecv alice asKey ("abcd", bSnd, _SEND "how are you bob")

      Resp "" _ (Msg mId5 msg5) <- tGet1 bob
      Resp "bcda" _ OK <- signSendRecv bob brKey ("bcda", bRcv, ACK mId5)
      (bDec mId5 msg5, Right "how are you bob") #== "message received from alice"

testSwitchSub :: SpecWith (ATransport, AStoreType)
testSwitchSub =
  it "should create simplex connections and switch subscription to another TCP connection" $ \(ATransport t, msType) ->
    smpTest3 t msType $ \rh1 rh2 sh -> do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" _ (Ids rId sId srvDh) <- signSendRecv rh1 rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
      let dec = decryptMsgV3 $ C.dh' srvDh dhPriv
      Resp "bcda" _ ok1 <- sendRecv sh ("", "bcda", sId, _SEND "test1")
      (ok1, OK) #== "sent test message 1"
      Resp "cdab" _ ok2 <- sendRecv sh ("", "cdab", sId, _SEND "test2, no ACK")
      (ok2, OK) #== "sent test message 2"

      Resp "" _ (Msg mId1 msg1) <- tGet1 rh1
      (dec mId1 msg1, Right "test1") #== "test message 1 delivered to the 1st TCP connection"
      Resp "abcd" _ (Msg mId2 msg2) <- signSendRecv rh1 rKey ("abcd", rId, ACK mId1)
      (dec mId2 msg2, Right "test2, no ACK") #== "test message 2 delivered, no ACK"

      Resp "bcda" _ (Msg mId2' msg2') <- signSendRecv rh2 rKey ("bcda", rId, SUB)
      (dec mId2' msg2', Right "test2, no ACK") #== "same simplex queue via another TCP connection, tes2 delivered again (no ACK in 1st queue)"
      Resp "cdab" _ OK <- signSendRecv rh2 rKey ("cdab", rId, ACK mId2')

      Resp "" _ end <- tGet1 rh1
      (end, END) #== "unsubscribed the 1st TCP connection"

      Resp "dabc" _ OK <- sendRecv sh ("", "dabc", sId, _SEND "test3")

      Resp "" _ (Msg mId3 msg3) <- tGet1 rh2
      (dec mId3 msg3, Right "test3") #== "delivered to the 2nd TCP connection"

      Resp "abcd" _ err <- signSendRecv rh1 rKey ("abcd", rId, ACK mId3)
      (err, ERR NO_MSG) #== "rejects ACK from the 1st TCP connection"

      Resp "bcda" _ ok3 <- signSendRecv rh2 rKey ("bcda", rId, ACK mId3)
      (ok3, OK) #== "accepts ACK from the 2nd TCP connection"

      Resp "cdab" _ OK <- signSendRecv rh1 rKey ("cdab", rId, DEL)
      Resp "" rId' DELD <- tGet1 rh2
      (rId', rId) #== "connection deleted event delivered to subscribed client"

      1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg rh1 >>= \case
        Nothing -> return ()
        Just _ -> error "nothing else is delivered to the 1st TCP connection"

testGetCommand :: SpecWith (ATransport, AStoreType)
testGetCommand =
  it "should retrieve messages from the queue using GET command" $ \(ATransport (t :: TProxy c), msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    smpTest t msType $ \sh -> do
      queue <- newEmptyTMVarIO
      testSMPClient @c $ \rh ->
        atomically . putTMVar queue =<< createAndSecureQueue rh sPub
      testSMPClient @c $ \rh -> do
        (sId, rId, rKey, dhShared) <- atomically $ takeTMVar queue
        let dec = decryptMsgV3 dhShared
        Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello")
        Resp "2" _ (Msg mId1 msg1) <- signSendRecv rh rKey ("2", rId, GET)
        (dec mId1 msg1, Right "hello") #== "retrieved from queue"
        Resp "3" _ OK <- signSendRecv rh rKey ("3", rId, ACK mId1)
        Resp "4" _ OK <- signSendRecv rh rKey ("4", rId, GET)
        pure ()

testGetSubCommands :: SpecWith (ATransport, AStoreType)
testGetSubCommands =
  it "should retrieve messages with GET and receive with SUB, only one ACK would work" $ \(ATransport t, msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    smpTest3 t msType $ \rh1 rh2 sh -> do
      (sId, rId, rKey, dhShared) <- createAndSecureQueue rh1 sPub
      let dec = decryptMsgV3 dhShared
      Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello 1")
      Resp "1a" _ OK <- signSendRecv sh sKey ("1a", sId, _SEND "hello 2")
      Resp "1b" _ OK <- signSendRecv sh sKey ("1b", sId, _SEND "hello 3")
      Resp "1c" _ OK <- signSendRecv sh sKey ("1c", sId, _SEND "hello 4")
      -- both get the same if not ACK'd
      Resp "" _ (Msg mId1 msg1) <- tGet1 rh1
      Resp "2" _ (Msg mId1' msg1') <- signSendRecv rh2 rKey ("2", rId, GET)
      (dec mId1 msg1, Right "hello 1") #== "received from queue via SUB"
      (dec mId1' msg1', Right "hello 1") #== "retrieved from queue with GET"
      mId1 `shouldBe` mId1'
      msg1 `shouldBe` msg1'
      -- subscriber cannot GET, getter cannot SUB
      Resp "3" _ (ERR (CMD PROHIBITED)) <- signSendRecv rh1 rKey ("3", rId, GET)
      Resp "3a" _ (ERR (CMD PROHIBITED)) <- signSendRecv rh2 rKey ("3a", rId, SUB)
      -- ACK for SUB delivers the next message
      Resp "4" _ (Msg mId2 msg2) <- signSendRecv rh1 rKey ("4", rId, ACK mId1)
      (dec mId2 msg2, Right "hello 2") #== "received from queue via SUB"
      -- bad msgId returns error
      Resp "5" _ (ERR NO_MSG) <- signSendRecv rh2 rKey ("5", rId, ACK "1234")
      -- already ACK'd by subscriber, but still returns OK when msgId matches
      Resp "5a" _ OK <- signSendRecv rh2 rKey ("5a", rId, ACK mId1)
      -- msg2 is not lost - even if subscriber does not ACK it, it is delivered to getter
      Resp "6" _ (Msg mId2' msg2') <- signSendRecv rh2 rKey ("6", rId, GET)
      (dec mId2' msg2', Right "hello 2") #== "retrieved from queue with GET"
      mId2 `shouldBe` mId2'
      msg2 `shouldBe` msg2'
      -- getter ACK returns OK, even though there is the next message
      Resp "7" _ OK <- signSendRecv rh2 rKey ("7", rId, ACK mId2')
      Resp "8" _ (Msg mId3 msg3) <- signSendRecv rh2 rKey ("8", rId, GET)
      (dec mId3 msg3, Right "hello 3") #== "retrieved from queue with GET"
      -- subscriber ACK does not lose message
      Resp "9" _ (Msg mId3' msg3') <- signSendRecv rh1 rKey ("9", rId, ACK mId2')
      (dec mId3' msg3', Right "hello 3") #== "retrieved from queue with GET"
      mId3 `shouldBe` mId3'
      msg3 `shouldBe` msg3'
      Resp "10" _ (Msg mId4 msg4) <- signSendRecv rh1 rKey ("10", rId, ACK mId3)
      (dec mId4 msg4, Right "hello 4") #== "retrieved from queue with GET"
      Resp "11" _ OK <- signSendRecv rh1 rKey ("11", rId, ACK mId4)
      -- no more messages for getter too
      Resp "12" _ OK <- signSendRecv rh2 rKey ("12", rId, GET)
      pure ()

testExceedQueueQuota :: SpecWith (ATransport, AStoreType)
testExceedQueueQuota =
  it "should reply with ERR QUOTA to sender and send QUOTA message to the recipient" $ \(ATransport (t :: TProxy c), msType) -> do
    withSmpServerConfigOn (ATransport t) (cfgMS msType) {msgQueueQuota = 2} testPort $ \_ ->
      testSMPClient @c $ \sh -> testSMPClient @c $ \rh -> do
        g <- C.newRandom
        (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
        (sId, rId, rKey, dhShared) <- createAndSecureQueue rh sPub
        let dec = decryptMsgV3 dhShared
        Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello 1")
        Resp "2" _ OK <- signSendRecv sh sKey ("2", sId, _SEND "hello 2")
        Resp "3" _ (ERR QUOTA) <- signSendRecv sh sKey ("3", sId, _SEND "hello 3")
        Resp "" _ (Msg mId1 msg1) <- tGet1 rh
        (dec mId1 msg1, Right "hello 1") #== "hello 1"
        Resp "4" _ (Msg mId2 msg2) <- signSendRecv rh rKey ("4", rId, ACK mId1)
        (dec mId2 msg2, Right "hello 2") #== "hello 2"
        Resp "5" _ (ERR QUOTA) <- signSendRecv sh sKey ("5", sId, _SEND "hello 3")
        Resp "6" _ (Msg mId3 msg3) <- signSendRecv rh rKey ("6", rId, ACK mId2)
        (dec mId3 msg3, Left "ClientRcvMsgQuota") #== "ClientRcvMsgQuota"
        Resp "7" _ (ERR QUOTA) <- signSendRecv sh sKey ("7", sId, _SEND "hello 3")
        Resp "8" _ OK <- signSendRecv rh rKey ("8", rId, ACK mId3)
        Resp "9" _ OK <- signSendRecv sh sKey ("9", sId, _SEND "hello 3")
        Resp "" _ (Msg mId4 msg4) <- tGet1 rh
        (dec mId4 msg4, Right "hello 3") #== "hello 3"
        Resp "10" _ OK <- signSendRecv rh rKey ("10", rId, ACK mId4)
        pure ()

testWithStoreLog :: SpecWith (ATransport, AStoreType)
testWithStoreLog =
  xit "should store simplex queues to log and restore them after server restart" $ \(at@(ATransport t), msType) -> do
    g <- C.newRandom
    (sPub1, sKey1) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    (sPub2, sKey2) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    (nPub, nKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    recipientId1 <- newTVarIO NoEntity
    recipientKey1 <- newTVarIO Nothing
    dhShared1 <- newTVarIO Nothing
    senderId1 <- newTVarIO NoEntity
    senderId2 <- newTVarIO NoEntity
    notifierId <- newTVarIO NoEntity

    withSmpServerStoreLogOnMS at msType testPort . runTest t $ \h -> runClient t $ \h1 -> do
      (sId1, rId1, rKey1, dhShared) <- createAndSecureQueue h sPub1
      (rcvNtfPubDhKey, _) <- atomically $ C.generateKeyPair g
      Resp "abcd" _ (NID nId _) <- signSendRecv h rKey1 ("abcd", rId1, NKEY nPub rcvNtfPubDhKey)
      atomically $ do
        writeTVar recipientId1 rId1
        writeTVar recipientKey1 $ Just rKey1
        writeTVar dhShared1 $ Just dhShared
        writeTVar senderId1 sId1
        writeTVar notifierId nId
      Resp "dabc" _ OK <- signSendRecv h1 nKey ("dabc", nId, NSUB)
      (mId1, msg1) <-
        signSendRecv h sKey1 ("bcda", sId1, _SEND' "hello") >>= \case
          Resp "" _ (Msg mId1 msg1) -> pure (mId1, msg1)
          r -> error $ "unexpected response " <> take 100 (show r)
      Resp "bcda" _ OK <- tGet1 h
      (decryptMsgV3 dhShared mId1 msg1, Right "hello") #== "delivered from queue 1"
      Resp "" _ (NMSG _ _) <- tGet1 h1

      (sId2, rId2, rKey2, dhShared2) <- createAndSecureQueue h sPub2
      atomically $ writeTVar senderId2 sId2
      (mId2, msg2) <-
        signSendRecv h sKey2 ("cdab", sId2, _SEND "hello too") >>= \case
          Resp "" _ (Msg mId2 msg2) -> pure (mId2, msg2)
          r -> error $ "unexpected response " <> take 100 (show r)
      Resp "cdab" _ OK <- tGet1 h
      (decryptMsgV3 dhShared2 mId2 msg2, Right "hello too") #== "delivered from queue 2"

      Resp "dabc" _ OK <- signSendRecv h rKey2 ("dabc", rId2, DEL)
      pure ()

    logSize testStoreLogFile `shouldReturn` 6

    let cfg' = cfg {serverStoreCfg = ASSCfg SQSMemory SMSMemory $ SSCMemory Nothing}
    withSmpServerConfigOn at cfg' testPort . runTest t $ \h -> do
      sId1 <- readTVarIO senderId1
      -- fails if store log is disabled
      Resp "bcda" _ (ERR AUTH) <- signSendRecv h sKey1 ("bcda", sId1, _SEND "hello")
      pure ()

    withSmpServerStoreLogOnMS at msType testPort . runTest t $ \h -> runClient t $ \h1 -> do
      -- this queue is restored
      rId1 <- readTVarIO recipientId1
      Just rKey1 <- readTVarIO recipientKey1
      Just dh1 <- readTVarIO dhShared1
      sId1 <- readTVarIO senderId1
      nId <- readTVarIO notifierId
      Resp "dabc" _ OK <- signSendRecv h1 nKey ("dabc", nId, NSUB)
      Resp "bcda" _ OK <- signSendRecv h sKey1 ("bcda", sId1, _SEND' "hello")
      Resp "cdab" _ (Msg mId3 msg3) <- signSendRecv h rKey1 ("cdab", rId1, SUB)
      (decryptMsgV3 dh1 mId3 msg3, Right "hello") #== "delivered from restored queue"
      Resp "" _ (NMSG _ _) <- tGet1 h1
      -- this queue is removed - not restored
      sId2 <- readTVarIO senderId2
      Resp "cdab" _ (ERR AUTH) <- signSendRecv h sKey2 ("cdab", sId2, _SEND "hello too")
      pure ()

    logSize testStoreLogFile `shouldReturn` 1
    removeFile testStoreLogFile
  where
    runTest :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> ThreadId -> Expectation
    runTest _ test' server = do
      testSMPClient test' `shouldReturn` ()
      killThread server

    runClient :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> Expectation
    runClient _ test' = testSMPClient test' `shouldReturn` ()

logSize :: FilePath -> IO Int
logSize f =
  try (length . B.lines <$> B.readFile f) >>= \case
    Right l -> pure l
    Left (_ :: SomeException) -> logSize f

testRestoreMessages :: SpecWith (ATransport, AStoreType)
testRestoreMessages =
  it "should store messages on exit and restore on start" $ \(at@(ATransport t), msType) -> do
    removeFileIfExists testStoreLogFile
    removeFileIfExists testStoreMsgsFile
    whenM (doesDirectoryExist testStoreMsgsDir) $ removeDirectoryRecursive testStoreMsgsDir
    removeFileIfExists testServerStatsBackupFile

    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    recipientId <- newTVarIO NoEntity
    recipientKey <- newTVarIO Nothing
    dhShared <- newTVarIO Nothing
    senderId <- newTVarIO NoEntity

    withSmpServerStoreMsgLogOnMS at msType testPort . runTest t $ \h -> do
      runClient t $ \h1 -> do
        (sId, rId, rKey, dh) <- createAndSecureQueue h1 sPub
        atomically $ do
          writeTVar recipientId rId
          writeTVar recipientKey $ Just rKey
          writeTVar dhShared $ Just dh
          writeTVar senderId sId
        Resp "1" _ OK <- signSendRecv h sKey ("1", sId, _SEND "hello")
        Resp "" _ (Msg mId1 msg1) <- tGet1 h1
        Resp "1a" _ OK <- signSendRecv h1 rKey ("1a", rId, ACK mId1)
        (decryptMsgV3 dh mId1 msg1, Right "hello") #== "message delivered"
      -- messages below are delivered after server restart
      sId <- readTVarIO senderId
      Resp "2" _ OK <- signSendRecv h sKey ("2", sId, _SEND "hello 2")
      Resp "3" _ OK <- signSendRecv h sKey ("3", sId, _SEND "hello 3")
      Resp "4" _ OK <- signSendRecv h sKey ("4", sId, _SEND "hello 4")
      Resp "5" _ OK <- signSendRecv h sKey ("5", sId, _SEND "hello 5")
      Resp "6" _ (ERR QUOTA) <- signSendRecv h sKey ("6", sId, _SEND "hello 6")
      pure ()

    rId <- readTVarIO recipientId

    logSize testStoreLogFile `shouldReturn` 2
    -- logSize testStoreMsgsFile `shouldReturn` 5
    logSize testServerStatsBackupFile `shouldReturn` 76
    Right stats1 <- strDecode <$> B.readFile testServerStatsBackupFile
    checkStats stats1 [rId] 5 1

    withSmpServerStoreMsgLogOnMS at msType testPort . runTest t $ \h -> do
      Just rKey <- readTVarIO recipientKey
      Just dh <- readTVarIO dhShared
      let dec = decryptMsgV3 dh
      Resp "2" _ (Msg mId2 msg2) <- signSendRecv h rKey ("2", rId, SUB)
      (dec mId2 msg2, Right "hello 2") #== "restored message delivered"
      Resp "3" _ (Msg mId3 msg3) <- signSendRecv h rKey ("3", rId, ACK mId2)
      (dec mId3 msg3, Right "hello 3") #== "restored message delivered"
      Resp "4" _ (Msg mId4 msg4) <- signSendRecv h rKey ("4", rId, ACK mId3)
      (dec mId4 msg4, Right "hello 4") #== "restored message delivered"

    logSize testStoreLogFile `shouldReturn` 1
    -- the last message is not removed because it was not ACK'd
    -- logSize testStoreMsgsFile `shouldReturn` 3
    logSize testServerStatsBackupFile `shouldReturn` 76
    Right stats2 <- strDecode <$> B.readFile testServerStatsBackupFile
    checkStats stats2 [rId] 5 3

    withSmpServerStoreMsgLogOnMS at msType testPort . runTest t $ \h -> do
      Just rKey <- readTVarIO recipientKey
      Just dh <- readTVarIO dhShared
      let dec = decryptMsgV3 dh
      Resp "4" _ (Msg mId4 msg4) <- signSendRecv h rKey ("4", rId, SUB)
      (dec mId4 msg4, Right "hello 4") #== "restored message delivered"
      Resp "5" _ (Msg mId5 msg5) <- signSendRecv h rKey ("5", rId, ACK mId4)
      (dec mId5 msg5, Right "hello 5") #== "restored message delivered"
      Resp "6" _ (Msg mId6 msg6) <- signSendRecv h rKey ("6", rId, ACK mId5)
      (dec mId6 msg6, Left "ClientRcvMsgQuota") #== "restored message delivered"
      Resp "7" _ OK <- signSendRecv h rKey ("7", rId, ACK mId6)
      pure ()
    logSize testStoreLogFile `shouldReturn` 1
    -- logSize testStoreMsgsFile `shouldReturn` 0
    logSize testServerStatsBackupFile `shouldReturn` 76
    Right stats3 <- strDecode <$> B.readFile testServerStatsBackupFile
    checkStats stats3 [rId] 5 5

    removeFile testStoreLogFile
    removeFileIfExists testStoreMsgsFile
    whenM (doesDirectoryExist testStoreMsgsDir) $ removeDirectoryRecursive testStoreMsgsDir
    removeFile testServerStatsBackupFile
  where
    runTest :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> ThreadId -> Expectation
    runTest _ test' server = do
      testSMPClient test' `shouldReturn` ()
      killThread server

    runClient :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> Expectation
    runClient _ test' = testSMPClient test' `shouldReturn` ()

checkStats :: ServerStatsData -> [RecipientId] -> Int -> Int -> Expectation
checkStats s qs sent received = do
  _qCreated s `shouldBe` length qs
  _qSecured s `shouldBe` length qs
  _qDeletedAll s `shouldBe` 0
  _qDeletedNew s `shouldBe` 0
  _qDeletedSecured s `shouldBe` 0
  _msgSent s `shouldBe` sent
  _msgRecv s `shouldBe` received
  _msgSentNtf s `shouldBe` 0
  _msgRecvNtf s `shouldBe` 0
  let PeriodStatsData {_day, _week, _month} = _activeQueues s
  IS.toList _day `shouldBe` map (hash . unEntityId) qs
  IS.toList _week `shouldBe` map (hash . unEntityId) qs
  IS.toList _month `shouldBe` map (hash . unEntityId) qs

testRestoreExpireMessages :: SpecWith (ATransport, AStoreType)
testRestoreExpireMessages =
  it "should store messages on exit and restore on start" $ \(at@(ATransport t), msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    recipientId <- newTVarIO NoEntity
    recipientKey <- newTVarIO Nothing
    dhShared <- newTVarIO Nothing
    senderId <- newTVarIO NoEntity

    withSmpServerStoreMsgLogOnMS at msType testPort . runTest t $ \h -> do
      runClient t $ \h1 -> do
        (sId, rId, rKey, dh) <- createAndSecureQueue h1 sPub
        atomically $ do
          writeTVar recipientId rId
          writeTVar recipientKey $ Just rKey
          writeTVar dhShared $ Just dh
          writeTVar senderId sId
      sId <- readTVarIO senderId
      Resp "1" _ OK <- signSendRecv h sKey ("1", sId, _SEND "hello 1")
      Resp "2" _ OK <- signSendRecv h sKey ("2", sId, _SEND "hello 2")
      threadDelay 3000000
      Resp "3" _ OK <- signSendRecv h sKey ("3", sId, _SEND "hello 3")
      Resp "4" _ OK <- signSendRecv h sKey ("4", sId, _SEND "hello 4")
      pure ()

    logSize testStoreLogFile `shouldReturn` 2
    exportStoreMessages msType
    msgs <- B.readFile testStoreMsgsFile
    length (B.lines msgs) `shouldBe` 4

    let expCfg1 = Just ExpirationConfig {ttl = 86400, checkInterval = 43200}
        cfg1 = (cfgMS msType) {messageExpiration = expCfg1, serverStatsBackupFile = Just testServerStatsBackupFile}
    withSmpServerConfigOn at cfg1 testPort . runTest t $ \_ -> pure ()

    logSize testStoreLogFile `shouldReturn` 1
    exportStoreMessages msType
    msgs' <- B.readFile testStoreMsgsFile
    msgs' `shouldBe` msgs

    let expCfg2 = Just ExpirationConfig {ttl = 2, checkInterval = 43200}
        cfg2 = (cfgMS msType) {messageExpiration = expCfg2, serverStatsBackupFile = Just testServerStatsBackupFile}
    withSmpServerConfigOn at cfg2 testPort . runTest t $ \_ -> pure ()

    logSize testStoreLogFile `shouldReturn` 1
    -- two messages expired
    exportStoreMessages msType
    msgs'' <- B.readFile testStoreMsgsFile
    length (B.lines msgs'') `shouldBe` 2
    B.lines msgs'' `shouldBe` drop 2 (B.lines msgs)
    Right ServerStatsData {_msgExpired} <- strDecode <$> B.readFile testServerStatsBackupFile
    _msgExpired `shouldBe` 2
  where
    exportStoreMessages :: AStoreType -> IO ()
    exportStoreMessages = \case
      ASType _ SMSJournal -> export
      ASType _ SMSMemory -> pure ()
      where
        export = do
          ms <- newMsgStore (testJournalStoreCfg MQStoreCfg) {quota = 4}
          readWriteQueueStore True (mkQueue ms) testStoreLogFile (queueStore ms) >>= closeStoreLog
          removeFileIfExists testStoreMsgsFile
          exportMessages False ms testStoreMsgsFile False
    runTest :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> ThreadId -> Expectation
    runTest _ test' server = do
      testSMPClient test' `shouldReturn` ()
      killThread server

    runClient :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO ()) -> Expectation
    runClient _ test' = testSMPClient test' `shouldReturn` ()

testPrometheusMetrics :: SpecWith (ATransport, AStoreType)
testPrometheusMetrics =
  it "should save Prometheus metrics" $ \(at, msType) -> do
    let cfg' = (cfgMS msType) {prometheusInterval = Just 1}
    withSmpServerConfigOn at cfg' testPort $ \_ -> threadDelay 1000000
    doesFileExist testPrometheusMetricsFile `shouldReturn` True

createAndSecureQueue :: Transport c => THandleSMP c 'TClient -> SndPublicAuthKey -> IO (SenderId, RecipientId, RcvPrivateAuthKey, RcvDhSecret)
createAndSecureQueue h sPub = do
  g <- C.newRandom
  (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
  (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
  Resp "abcd" NoEntity (Ids rId sId srvDh) <- signSendRecv h rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
  let dhShared = C.dh' srvDh dhPriv
  Resp "dabc" rId' OK <- signSendRecv h rKey ("dabc", rId, KEY sPub)
  (rId', rId) #== "same queue ID"
  pure (sId, rId, rKey, dhShared)

testTiming :: SpecWith (ATransport, AStoreType)
testTiming =
  describe "should have similar time for auth error, whether queue exists or not, for all key types" $
    forM_ timingTests $ \tst ->
      it (testName tst) $ \(ATransport t, msType) ->
        smpTest2Cfg (cfgMS msType) (mkVersionRange minServerSMPRelayVersion authCmdsSMPVersion) t $ \rh sh ->
          testSameTiming rh sh tst
  where
    testName :: (C.AuthAlg, C.AuthAlg, Int) -> String
    testName (C.AuthAlg goodKeyAlg, C.AuthAlg badKeyAlg, _) = unwords ["queue key:", show goodKeyAlg, "/ used key:", show badKeyAlg]
    timingTests :: [(C.AuthAlg, C.AuthAlg, Int)]
    timingTests =
      [ (C.AuthAlg C.SEd25519, C.AuthAlg C.SEd25519, 200), -- correct key type
      -- (C.AuthAlg C.SEd25519, C.AuthAlg C.SEd448, 150),
      -- (C.AuthAlg C.SEd25519, C.AuthAlg C.SX25519, 200),
        (C.AuthAlg C.SEd448, C.AuthAlg C.SEd25519, 200),
        (C.AuthAlg C.SEd448, C.AuthAlg C.SEd448, 150), -- correct key type
        (C.AuthAlg C.SEd448, C.AuthAlg C.SX25519, 200),
        (C.AuthAlg C.SX25519, C.AuthAlg C.SEd25519, 200),
        (C.AuthAlg C.SX25519, C.AuthAlg C.SEd448, 150),
        (C.AuthAlg C.SX25519, C.AuthAlg C.SX25519, 200) -- correct key type
      ]
    timeRepeat n = fmap fst . timeItT . forM_ (replicate n ()) . const
    similarTime t1 t2 = abs (t2 / t1 - 1) < 0.25 -- normally the difference between "no queue" and "wrong key" is less than 5%
    testSameTiming :: forall c. Transport c => THandleSMP c 'TClient -> THandleSMP c 'TClient -> (C.AuthAlg, C.AuthAlg, Int) -> Expectation
    testSameTiming rh sh (C.AuthAlg goodKeyAlg, C.AuthAlg badKeyAlg, n) = do
      g <- C.newRandom
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair goodKeyAlg g
      (dhPub, dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" NoEntity (Ids rId sId srvDh) <- signSendRecv rh rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe False)
      let dec = decryptMsgV3 $ C.dh' srvDh dhPriv
      Resp "cdab" _ OK <- signSendRecv rh rKey ("cdab", rId, SUB)

      (_, badKey) <- atomically $ C.generateAuthKeyPair badKeyAlg g
      runTimingTest rh badKey rId SUB

      (sPub, sKey) <- atomically $ C.generateAuthKeyPair goodKeyAlg g
      Resp "dabc" _ OK <- signSendRecv rh rKey ("dabc", rId, KEY sPub)

      Resp "bcda" _ OK <- signSendRecv sh sKey ("bcda", sId, _SEND "hello")
      Resp "" _ (Msg mId msg) <- tGet1 rh
      (dec mId msg, Right "hello") #== "delivered from queue"

      runTimingTest sh badKey sId $ _SEND "hello"
      where
        runTimingTest :: PartyI p => THandleSMP c 'TClient -> C.APrivateAuthKey -> EntityId -> Command p -> IO ()
        runTimingTest h badKey qId cmd = do
          threadDelay 100000
          _ <- timeRepeat n $ do
            -- "warm up" the server
            Resp "dabc" _ (ERR AUTH) <- signSendRecv h badKey ("dabc", EntityId "1234", cmd)
            return ()
          threadDelay 100000
          timeWrongKey <- timeRepeat n $ do
            Resp "cdab" _ (ERR AUTH) <- signSendRecv h badKey ("cdab", qId, cmd)
            return ()
          threadDelay 100000
          timeNoQueue <- timeRepeat n $ do
            Resp "dabc" _ (ERR AUTH) <- signSendRecv h badKey ("dabc", EntityId "1234", cmd)
            return ()
          let ok = similarTime timeNoQueue timeWrongKey
          unless ok . putStrLn . unwords $
            [ show goodKeyAlg,
              show badKeyAlg,
              show timeWrongKey,
              show timeNoQueue,
              show $ timeWrongKey / timeNoQueue - 1
            ]
          ok `shouldBe` True

testMessageNotifications :: SpecWith (ATransport, AStoreType)
testMessageNotifications =
  it "should create simplex connection, subscribe notifier and deliver notifications" $ \(ATransport t, msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    (nPub, nKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    smpTest4 t msType $ \rh sh nh1 nh2 -> do
      (sId, rId, rKey, dhShared) <- createAndSecureQueue rh sPub
      let dec = decryptMsgV3 dhShared
      (rcvNtfPubDhKey, _) <- atomically $ C.generateKeyPair g
      Resp "1" _ (NID nId' _) <- signSendRecv rh rKey ("1", rId, NKEY nPub rcvNtfPubDhKey)
      Resp "1a" _ (NID nId _) <- signSendRecv rh rKey ("1a", rId, NKEY nPub rcvNtfPubDhKey)
      nId' `shouldNotBe` nId
      Resp "2" _ OK <- signSendRecv nh1 nKey ("2", nId, NSUB)
      Resp "3" _ OK <- signSendRecv sh sKey ("3", sId, _SEND' "hello")
      Resp "" _ (Msg mId1 msg1) <- tGet1 rh
      (dec mId1 msg1, Right "hello") #== "delivered from queue"
      Resp "3a" _ OK <- signSendRecv rh rKey ("3a", rId, ACK mId1)
      Resp "" _ (NMSG _ _) <- tGet1 nh1
      Resp "4" _ OK <- signSendRecv nh2 nKey ("4", nId, NSUB)
      Resp "" nId2 END <- tGet1 nh1
      nId2 `shouldBe` nId
      Resp "5" _ OK <- signSendRecv sh sKey ("5", sId, _SEND' "hello again")
      Resp "" _ (Msg mId2 msg2) <- tGet1 rh
      Resp "5a" _ OK <- signSendRecv rh rKey ("5a", rId, ACK mId2)
      (dec mId2 msg2, Right "hello again") #== "delivered from queue again"
      Resp "" _ (NMSG _ _) <- tGet1 nh2
      1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg nh1 >>= \case
        Nothing -> pure ()
        Just _ -> error "nothing else should be delivered to the 1st notifier's TCP connection"
      Resp "6" _ OK <- signSendRecv rh rKey ("6", rId, NDEL)
      Resp "" nId3 DELD <- tGet1 nh2
      nId3 `shouldBe` nId
      Resp "7" _ OK <- signSendRecv sh sKey ("7", sId, _SEND' "hello there")
      Resp "" _ (Msg mId3 msg3) <- tGet1 rh
      (dec mId3 msg3, Right "hello there") #== "delivered from queue again"
      1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg nh2 >>= \case
        Nothing -> pure ()
        Just _ -> error "nothing else should be delivered to the 2nd notifier's TCP connection"

testMsgExpireOnSend :: SpecWith (ATransport, AStoreType)
testMsgExpireOnSend =
  it "should expire messages that are not received before messageTTL on SEND" $ \(ATransport (t :: TProxy c), msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    let cfg' = (cfgMS msType) {messageExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 10000}}
    withSmpServerConfigOn (ATransport t) cfg' testPort $ \_ ->
      testSMPClient @c $ \sh -> do
        (sId, rId, rKey, dhShared) <- testSMPClient @c $ \rh -> createAndSecureQueue rh sPub
        let dec = decryptMsgV3 dhShared
        Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello (should expire)")
        threadDelay 2500000
        Resp "2" _ OK <- signSendRecv sh sKey ("2", sId, _SEND "hello (should NOT expire)")
        testSMPClient @c $ \rh -> do
          Resp "3" _ (Msg mId msg) <- signSendRecv rh rKey ("3", rId, SUB)
          (dec mId msg, Right "hello (should NOT expire)") #== "delivered"
          1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg rh >>= \case
            Nothing -> return ()
            Just _ -> error "nothing else should be delivered"

testMsgExpireOnInterval :: SpecWith (ATransport, AStoreType)
testMsgExpireOnInterval =
  -- fails on ubuntu
  xit' "should expire messages that are not received before messageTTL after expiry interval" $ \(ATransport (t :: TProxy c), msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    let cfg' = (cfgMS msType) {messageExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 1}, idleQueueInterval = 1}
    withSmpServerConfigOn (ATransport t) cfg' testPort $ \_ ->
      testSMPClient @c $ \sh -> do
        (sId, rId, rKey, _) <- testSMPClient @c $ \rh -> createAndSecureQueue rh sPub
        Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello (should expire)")
        threadDelay 3000000
        testSMPClient @c $ \rh -> do
          signSendRecv rh rKey ("2", rId, SUB) >>= \case
            Resp "2" _ OK -> pure ()
            r -> unexpected r
          1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg rh >>= \case
            Nothing -> return ()
            Just _ -> error "nothing should be delivered"

testMsgNOTExpireOnInterval :: SpecWith (ATransport, AStoreType)
testMsgNOTExpireOnInterval =
  it "should block and unblock message queues" $ \(ATransport (t :: TProxy c), msType) -> do
    g <- C.newRandom
    (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd25519 g
    let cfg' = (cfgMS msType) {messageExpiration = Just ExpirationConfig {ttl = 1, checkInterval = 10000}}
    withSmpServerConfigOn (ATransport t) cfg' testPort $ \_ ->
      testSMPClient @c $ \sh -> do
        (sId, rId, rKey, dhShared) <- testSMPClient @c $ \rh -> createAndSecureQueue rh sPub
        let dec = decryptMsgV3 dhShared
        Resp "1" _ OK <- signSendRecv sh sKey ("1", sId, _SEND "hello (should NOT expire)")
        threadDelay 2500000
        testSMPClient @c $ \rh -> do
          Resp "2" _ (Msg mId msg) <- signSendRecv rh rKey ("2", rId, SUB)
          (dec mId msg, Right "hello (should NOT expire)") #== "delivered"
          1000 `timeout` tGet @SMPVersion @ErrorType @BrokerMsg rh >>= \case
            Nothing -> return ()
            Just _ -> error "nothing else should be delivered"

testBlockMessageQueue :: SpecWith (ATransport, AStoreType)
testBlockMessageQueue =
  it "should return BLOCKED error when queue is blocked" $ \(at@(ATransport (t :: TProxy c)), msType) -> do
    g <- C.newRandom
    (rId, sId) <- withSmpServerStoreLogOnMS at msType testPort $ runTest t $ \h -> do
      (rPub, rKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      (dhPub, _dhPriv :: C.PrivateKeyX25519) <- atomically $ C.generateKeyPair g
      Resp "abcd" rId1 (Ids rId sId _srvDh) <- signSendRecv h rKey ("abcd", NoEntity, NEW rPub dhPub Nothing SMSubscribe True)
      (rId1, NoEntity) #== "creates queue"
      pure (rId, sId)

    withFile testStoreLogFile AppendMode $ \h -> B.hPutStrLn h $ strEncode $ BlockQueue rId $ BlockingInfo BRContent

    withSmpServerStoreLogOnMS at msType testPort $ runTest t $ \h -> do
      (sPub, sKey) <- atomically $ C.generateAuthKeyPair C.SEd448 g
      Resp "dabc" sId2 (ERR (BLOCKED (BlockingInfo BRContent))) <- signSendRecv h sKey ("dabc", sId, SKEY sPub)
      (sId2, sId) #== "same queue ID in response"
  where
    runTest :: Transport c => TProxy c -> (THandleSMP c 'TClient -> IO a) -> ThreadId -> IO a
    runTest _ test' server = do
      a <- testSMPClient test'
      killThread server
      pure a

samplePubKey :: C.APublicVerifyKey
samplePubKey = C.APublicVerifyKey C.SEd25519 "MCowBQYDK2VwAyEAfAOflyvbJv1fszgzkQ6buiZJVgSpQWsucXq7U6zjMgY="

sampleDhPubKey :: C.PublicKey 'C.X25519
sampleDhPubKey = "MCowBQYDK2VuAyEAriy+HcARIhqsgSjVnjKqoft+y6pxrxdY68zn4+LjYhQ="

sampleSig :: Maybe TransmissionAuth
sampleSig = Just $ TASignature "e8JK+8V3fq6kOLqco/SaKlpNaQ7i1gfOrXoqekEl42u4mF8Bgu14T5j0189CGcUhJHw2RwCMvON+qbvQ9ecJAA=="

noAuth :: (Char, Maybe BasicAuth)
noAuth = ('A', Nothing)

deriving instance Eq TransmissionAuth

instance Eq C.ASignature where
  C.ASignature a s == C.ASignature a' s' = case testEquality a a' of
    Just Refl -> s == s'
    _ -> False

serverSyntaxTests :: ATransport -> Spec
serverSyntaxTests (ATransport t) = do
  it "unknown command" $ ("", "abcd", "1234", ('H', 'E', 'L', 'L', 'O')) >#> ("", "abcd", "1234", ERR $ CMD UNKNOWN)
  describe "NEW" $ do
    it "no parameters" $ (sampleSig, "bcda", "", NEW_) >#> ("", "bcda", "", ERR $ CMD SYNTAX)
    it "many parameters" $ (sampleSig, "cdab", "", (NEW_, ' ', ('\x01', 'A'), samplePubKey, sampleDhPubKey)) >#> ("", "cdab", "", ERR $ CMD SYNTAX)
    it "no signature" $ ("", "dabc", "", (NEW_, ' ', samplePubKey, sampleDhPubKey, '0', SMSubscribe, False)) >#> ("", "dabc", "", ERR $ CMD NO_AUTH)
    it "queue ID" $ (sampleSig, "abcd", "12345678", (NEW_, ' ', samplePubKey, sampleDhPubKey, '0', SMSubscribe, False)) >#> ("", "abcd", "12345678", ERR $ CMD HAS_AUTH)
  describe "KEY" $ do
    it "valid syntax" $ (sampleSig, "bcda", "12345678", (KEY_, ' ', samplePubKey)) >#> ("", "bcda", "12345678", ERR AUTH)
    it "no parameters" $ (sampleSig, "cdab", "12345678", KEY_) >#> ("", "cdab", "12345678", ERR $ CMD SYNTAX)
    it "many parameters" $ (sampleSig, "dabc", "12345678", (KEY_, ' ', ('\x01', 'A'), samplePubKey)) >#> ("", "dabc", "12345678", ERR $ CMD SYNTAX)
    it "no signature" $ ("", "abcd", "12345678", (KEY_, ' ', samplePubKey)) >#> ("", "abcd", "12345678", ERR $ CMD NO_AUTH)
    it "no queue ID" $ (sampleSig, "bcda", "", (KEY_, ' ', samplePubKey)) >#> ("", "bcda", "", ERR $ CMD NO_AUTH)
  noParamsSyntaxTest "SUB" SUB_
  noParamsSyntaxTest "OFF" OFF_
  noParamsSyntaxTest "DEL" DEL_
  describe "SEND" $ do
    it "valid syntax" $ (sampleSig, "cdab", "12345678", (SEND_, ' ', noMsgFlags, ' ', "hello" :: ByteString)) >#> ("", "cdab", "12345678", ERR AUTH)
    it "no parameters" $ (sampleSig, "abcd", "12345678", SEND_) >#> ("", "abcd", "12345678", ERR $ CMD SYNTAX)
    it "no queue ID" $ (sampleSig, "bcda", "", (SEND_, ' ', noMsgFlags, ' ', "hello" :: ByteString)) >#> ("", "bcda", "", ERR $ CMD NO_ENTITY)
  describe "ACK" $ do
    it "valid syntax" $ (sampleSig, "cdab", "12345678", (ACK_, ' ', "1234" :: ByteString)) >#> ("", "cdab", "12345678", ERR AUTH)
    it "no parameters" $ (sampleSig, "abcd", "12345678", ACK_) >#> ("", "abcd", "12345678", ERR $ CMD SYNTAX)
    it "no queue ID" $ (sampleSig, "bcda", "", (ACK_, ' ', "1234" :: ByteString)) >#> ("", "bcda", "", ERR $ CMD NO_AUTH)
    it "no signature" $ ("", "cdab", "12345678", (ACK_, ' ', "1234" :: ByteString)) >#> ("", "cdab", "12345678", ERR $ CMD NO_AUTH)
  describe "PING" $ do
    it "valid syntax" $ ("", "abcd", "", PING_) >#> ("", "abcd", "", PONG)
  describe "broker response not allowed" $ do
    it "OK" $ (sampleSig, "bcda", "12345678", OK_) >#> ("", "bcda", "12345678", ERR $ CMD UNKNOWN)
  where
    noParamsSyntaxTest :: PartyI p => String -> CommandTag p -> Spec
    noParamsSyntaxTest description cmd = describe description $ do
      it "valid syntax" $ (sampleSig, "abcd", "12345678", cmd) >#> ("", "abcd", "12345678", ERR AUTH)
      it "wrong terminator" $ (sampleSig, "bcda", "12345678", (cmd, '=')) >#> ("", "bcda", "12345678", ERR $ CMD UNKNOWN)
      it "no signature" $ ("", "cdab", "12345678", cmd) >#> ("", "cdab", "12345678", ERR $ CMD NO_AUTH)
      it "no queue ID" $ (sampleSig, "dabc", "", cmd) >#> ("", "dabc", "", ERR $ CMD NO_AUTH)
    (>#>) ::
      Encoding smp =>
      (Maybe TransmissionAuth, ByteString, ByteString, smp) ->
      (Maybe TransmissionAuth, ByteString, ByteString, BrokerMsg) ->
      Expectation
    command >#> response = withFrozenCallStack $ smpServerTest t command `shouldReturn` response
