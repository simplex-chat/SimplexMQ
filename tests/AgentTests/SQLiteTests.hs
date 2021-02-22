{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module AgentTests.SQLiteTests (storeTests) where

import Control.Monad.Except (ExceptT, runExceptT)
import qualified Crypto.PubKey.RSA as R
import Data.Word (Word32)
import qualified Database.SQLite.Simple as DB
import Database.SQLite.Simple.QQ (sql)
import Simplex.Messaging.Agent.Store
import Simplex.Messaging.Agent.Store.SQLite
import Simplex.Messaging.Agent.Store.Types
import Simplex.Messaging.Agent.Transmission
import qualified Simplex.Messaging.Crypto as C
import System.Random (Random (randomIO))
import Test.Hspec
import UnliftIO.Directory (removeFile)

testDB :: String
testDB = "smp-agent.test.db"

withStore :: SpecWith SQLiteStore -> Spec
withStore = before createStore . after removeStore
  where
    createStore :: IO SQLiteStore
    createStore = do
      -- Randomize DB file name to avoid SQLite IO errors supposedly caused by asynchronous
      -- IO operations on multiple similarly named files; error specific to some environments
      r <- randomIO :: IO Word32
      newSQLiteStore $ testDB <> show r

    removeStore :: SQLiteStore -> IO ()
    removeStore store = do
      DB.close $ dbConn store
      removeFile $ dbFilename store

returnsResult :: (Eq a, Eq e, Show a, Show e) => ExceptT e IO a -> a -> Expectation
action `returnsResult` r = runExceptT action `shouldReturn` Right r

throwsError :: (Eq a, Eq e, Show a, Show e) => ExceptT e IO a -> e -> Expectation
action `throwsError` e = runExceptT action `shouldReturn` Left e

-- TODO add null port tests
storeTests :: Spec
storeTests = withStore do
  describe "foreign keys enabled" testForeignKeysEnabled
  describe "store methods" do
    describe "createRcvConn" testCreateRcvConn
    describe "createSndConn" testCreateSndConn
    describe "getRcvQueue" testGetRcvQueue
    describe "deleteConn" do
      describe "Receive connection" testDeleteConnReceive
      describe "Send connection" testDeleteConnSend
      describe "Duplex connection" testDeleteConnDuplex
    describe "upgradeRcvConnToDuplex" testUpgradeRcvConnToDuplex
    describe "upgradeSndConnToDuplex" testUpgradeSndConnToDuplex
    describe "Set queue status" do
      describe "setRcvQueueStatus" testSetRcvQueueStatus
      describe "setSndQueueStatus" testSetSndQueueStatus
      describe "Duplex connection" testSetQueueStatusConnDuplex
      xdescribe "Nonexistent send queue" testSetNonexistentSendQueueStatus
      xdescribe "Nonexistent receive queue" testSetNonexistentReceiveQueueStatus
    describe "createMsg" do
      describe "A_MSG in RCV direction" testCreateMsgRcv
      describe "A_MSG in SND direction" testCreateMsgSnd
      describe "HELLO message" testCreateMsgHello
      describe "REPLY message" testCreateMsgReply
      describe "Bad queue direction - SND" testCreateMsgBadDirectionSnd
      describe "Bad queue direction - RCV" testCreateMsgBadDirectionRcv

testForeignKeysEnabled :: SpecWith SQLiteStore
testForeignKeysEnabled = do
  it "should throw error if foreign keys are enabled" $ \store -> do
    let inconsistent_query =
          [sql|
            INSERT INTO connections
              (conn_alias, rcv_host, rcv_port, rcv_id, snd_host, snd_port, snd_id)
            VALUES
              ("conn1", "smp.simplex.im", "5223", "1234", "smp.simplex.im", "5223", "2345");
          |]
    DB.execute_ (dbConn store) inconsistent_query
      `shouldThrow` (\e -> DB.sqlError e == DB.ErrorConstraint)

rcvQueue1 :: ReceiveQueue
rcvQueue1 =
  ReceiveQueue
    { server = SMPServer "smp.simplex.im" (Just "5223") (Just "1234"),
      rcvId = "1234",
      connAlias = "conn1",
      rcvPrivateKey = C.PrivateKey 1 2 3,
      sndId = Just "2345",
      sndKey = Nothing,
      decryptKey = C.PrivateKey 1 2 3,
      verifyKey = Nothing,
      status = New
    }

sndQueue1 :: SendQueue
sndQueue1 =
  SendQueue
    { server = SMPServer "smp.simplex.im" (Just "5223") (Just "1234"),
      sndId = "3456",
      connAlias = "conn1",
      sndPrivateKey = C.PrivateKey 1 2 3,
      encryptKey = C.PublicKey $ R.PublicKey 1 2 3,
      signKey = C.PrivateKey 1 2 3,
      status = New
    }

-- sndQueue2 :: SendQueue
-- sndQueue2 =
--           SendQueue
--             { server = SMPServer "smp.simplex.im" (Just "5223") (Just "1234"),
--               sndId = "1234",
--               connAlias = "conn1",
--               sndPrivateKey = "abcd",
--               encryptKey = "dcba",
--               signKey = "edcb",
--               status = New
--             }

testCreateRcvConn :: SpecWith SQLiteStore
testCreateRcvConn = do
  it "should create receive connection and add send queue" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCReceive (ReceiveConnection "conn1" rcvQueue1)
    upgradeRcvConnToDuplex store "conn1" sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCDuplex (DuplexConnection "conn1" rcvQueue1 sndQueue1)

testCreateSndConn :: SpecWith SQLiteStore
testCreateSndConn = do
  it "should create send connection and add receive queue" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCSend (SendConnection "conn1" sndQueue1)
    upgradeSndConnToDuplex store "conn1" rcvQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCDuplex (DuplexConnection "conn1" rcvQueue1 sndQueue1)

testGetRcvQueue :: SpecWith SQLiteStore
testGetRcvQueue = do
  it "should get receive queue and conn alias" $ \store -> do
    let smpServer = SMPServer "smp.simplex.im" (Just "5223") (Just "1234")
    let recipientId = "1234"
    createRcvConn store rcvQueue1
      `returnsResult` ()
    getRcvQueue store smpServer recipientId
      `returnsResult` rcvQueue1

testDeleteConnReceive :: SpecWith SQLiteStore
testDeleteConnReceive = do
  it "should create receive connection and delete it" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCReceive (ReceiveConnection "conn1" rcvQueue1)
    deleteConn store "conn1"
      `returnsResult` ()
    -- TODO check queues are deleted as well
    getConn store "conn1"
      `throwsError` SEBadConn

testDeleteConnSend :: SpecWith SQLiteStore
testDeleteConnSend = do
  it "should create send connection and delete it" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCSend (SendConnection "conn1" sndQueue1)
    deleteConn store "conn1"
      `returnsResult` ()
    -- TODO check queues are deleted as well
    getConn store "conn1"
      `throwsError` SEBadConn

testDeleteConnDuplex :: SpecWith SQLiteStore
testDeleteConnDuplex = do
  it "should create duplex connection and delete it" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    upgradeRcvConnToDuplex store "conn1" sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCDuplex (DuplexConnection "conn1" rcvQueue1 sndQueue1)
    deleteConn store "conn1"
      `returnsResult` ()
    -- TODO check queues are deleted as well
    getConn store "conn1"
      `throwsError` SEBadConn

testUpgradeRcvConnToDuplex :: SpecWith SQLiteStore
testUpgradeRcvConnToDuplex = do
  it "should throw error on attempts to add send queue to SendConnection or DuplexConnection" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    let anotherSndQueue =
          SendQueue
            { server = SMPServer "smp.simplex.im" (Just "5223") (Just "1234"),
              sndId = "2345",
              connAlias = "conn1",
              sndPrivateKey = C.PrivateKey 1 2 3,
              encryptKey = C.PublicKey $ R.PublicKey 1 2 3,
              signKey = C.PrivateKey 1 2 3,
              status = New
            }
    upgradeRcvConnToDuplex store "conn1" anotherSndQueue
      `throwsError` SEBadConnType CSend
    upgradeSndConnToDuplex store "conn1" rcvQueue1
      `returnsResult` ()
    upgradeRcvConnToDuplex store "conn1" anotherSndQueue
      `throwsError` SEBadConnType CDuplex

testUpgradeSndConnToDuplex :: SpecWith SQLiteStore
testUpgradeSndConnToDuplex = do
  it "should throw error on attempts to add receive queue to ReceiveConnection or DuplexConnection" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    let anotherRcvQueue =
          ReceiveQueue
            { server = SMPServer "smp.simplex.im" (Just "5223") (Just "1234"),
              rcvId = "3456",
              connAlias = "conn1",
              rcvPrivateKey = C.PrivateKey 1 2 3,
              sndId = Just "4567",
              sndKey = Nothing,
              decryptKey = C.PrivateKey 1 2 3,
              verifyKey = Nothing,
              status = New
            }
    upgradeSndConnToDuplex store "conn1" anotherRcvQueue
      `throwsError` SEBadConnType CReceive
    upgradeRcvConnToDuplex store "conn1" sndQueue1
      `returnsResult` ()
    upgradeSndConnToDuplex store "conn1" anotherRcvQueue
      `throwsError` SEBadConnType CDuplex

testSetRcvQueueStatus :: SpecWith SQLiteStore
testSetRcvQueueStatus = do
  it "should update status of receive queue" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCReceive (ReceiveConnection "conn1" rcvQueue1)
    setRcvQueueStatus store rcvQueue1 Confirmed
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCReceive (ReceiveConnection "conn1" rcvQueue1 {status = Confirmed})

testSetSndQueueStatus :: SpecWith SQLiteStore
testSetSndQueueStatus = do
  it "should update status of send queue" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCSend (SendConnection "conn1" sndQueue1)
    setSndQueueStatus store sndQueue1 Confirmed
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCSend (SendConnection "conn1" sndQueue1 {status = Confirmed})

testSetQueueStatusConnDuplex :: SpecWith SQLiteStore
testSetQueueStatusConnDuplex = do
  it "should update statuses of receive and send queues in duplex connection" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    upgradeRcvConnToDuplex store "conn1" sndQueue1
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCDuplex (DuplexConnection "conn1" rcvQueue1 sndQueue1)
    setRcvQueueStatus store rcvQueue1 Secured
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn SCDuplex (DuplexConnection "conn1" rcvQueue1 {status = Secured} sndQueue1)
    setSndQueueStatus store sndQueue1 Confirmed
      `returnsResult` ()
    getConn store "conn1"
      `returnsResult` SomeConn
        SCDuplex
        ( DuplexConnection "conn1" rcvQueue1 {status = Secured} sndQueue1 {status = Confirmed}
        )

testSetNonexistentSendQueueStatus :: SpecWith SQLiteStore
testSetNonexistentSendQueueStatus = do
  it "should throw error on attempt to update status of nonexistent send queue" $ \store -> do
    setSndQueueStatus store sndQueue1 Confirmed
      `throwsError` SEInternal

testSetNonexistentReceiveQueueStatus :: SpecWith SQLiteStore
testSetNonexistentReceiveQueueStatus = do
  it "should throw error on attempt to update status of nonexistent receive queue" $ \store -> do
    setRcvQueueStatus store rcvQueue1 Confirmed
      `throwsError` SEInternal

testCreateMsgRcv :: SpecWith SQLiteStore
testCreateMsgRcv = do
  it "should create a message in RCV direction" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    let msg = A_MSG "hello"
    let msgId = 1
    -- TODO getMsg to check message
    createMsg store "conn1" RCV msgId msg
      `returnsResult` ()

testCreateMsgSnd :: SpecWith SQLiteStore
testCreateMsgSnd = do
  it "should create a message in SND direction" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    let msg = A_MSG "hi"
    let msgId = 1
    -- TODO getMsg to check message
    createMsg store "conn1" SND msgId msg
      `returnsResult` ()

testCreateMsgHello :: SpecWith SQLiteStore
testCreateMsgHello = do
  it "should create a HELLO message" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    let verificationKey = C.PublicKey $ R.PublicKey 1 2 3
    let am = AckMode On
    let msg = HELLO verificationKey am
    let msgId = 1
    -- TODO getMsg to check message
    createMsg store "conn1" RCV msgId msg
      `returnsResult` ()

testCreateMsgReply :: SpecWith SQLiteStore
testCreateMsgReply = do
  it "should create a REPLY message" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    let smpServer = SMPServer "smp.simplex.im" (Just "5223") (Just "1234")
    let senderId = "sender1"
    let encryptionKey = C.PublicKey $ R.PublicKey 1 2 3
    let msg = REPLY $ SMPQueueInfo smpServer senderId encryptionKey
    let msgId = 1
    -- TODO getMsg to check message
    createMsg store "conn1" RCV msgId msg
      `returnsResult` ()

testCreateMsgBadDirectionSnd :: SpecWith SQLiteStore
testCreateMsgBadDirectionSnd = do
  it "should throw error on attempt to create a message in ineligible SND direction" $ \store -> do
    createRcvConn store rcvQueue1
      `returnsResult` ()
    let msg = A_MSG "hello"
    let msgId = 1
    createMsg store "conn1" SND msgId msg
      `throwsError` SEBadQueueDirection

testCreateMsgBadDirectionRcv :: SpecWith SQLiteStore
testCreateMsgBadDirectionRcv = do
  it "should throw error on attempt to create a message in ineligible RCV direction" $ \store -> do
    createSndConn store sndQueue1
      `returnsResult` ()
    let msg = A_MSG "hello"
    let msgId = 1
    createMsg store "conn1" RCV msgId msg
      `throwsError` SEBadQueueDirection
