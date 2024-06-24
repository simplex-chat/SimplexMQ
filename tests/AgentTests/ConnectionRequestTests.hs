{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -fno-warn-ambiguous-fields #-}

module AgentTests.ConnectionRequestTests where

import Data.ByteString (ByteString)
import Network.HTTP.Types (urlEncode)
import Simplex.Messaging.Agent.Protocol
import qualified Simplex.Messaging.Crypto as C
import Simplex.Messaging.Crypto.Ratchet
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (ProtocolServer (..), supportedSMPClientVRange, pattern VersionSMPC)
import Simplex.Messaging.ServiceScheme (ServiceScheme (..))
import Simplex.Messaging.Version
import Test.Hspec

uri :: String
uri = "smp.simplex.im"

srv :: SMPServer
srv = SMPServer "smp.simplex.im" "5223" (C.KeyHash "\215m\248\251")

queueAddr :: SMPQueueAddress
queueAddr =
  SMPQueueAddress
    { smpServer = srv,
      senderId = "\223\142z\251",
      dhPublicKey = testDhKey,
      sndSecure = False
    }

queueAddrNoPort :: SMPQueueAddress
queueAddrNoPort = queueAddr {smpServer = srv {port = ""}}

queue :: SMPQueueUri
queue = SMPQueueUri supportedSMPClientVRange queueAddr

queueV1 :: SMPQueueUri
queueV1 = SMPQueueUri (mkVersionRange (VersionSMPC 1) (VersionSMPC 1)) queueAddr

testDhKey :: C.PublicKeyX25519
testDhKey = "MCowBQYDK2VuAyEAjiswwI3O/NlS8Fk3HJUW870EY2bAwmttMBsvRB9eV3o="

testDhKeyStr :: ByteString
testDhKeyStr = strEncode testDhKey

testDhKeyStrUri :: ByteString
testDhKeyStrUri = urlEncode True testDhKeyStr

connReqData :: ConnReqUriData
connReqData =
  ConnReqUriData
    { crScheme = SSSimplex,
      crAgentVRange = mkVersionRange (VersionSMPA 2) (VersionSMPA 2),
      crSmpQueues = [queueV1],
      crClientData = Nothing
    }

testDhPubKey :: C.PublicKeyX448
testDhPubKey = "MEIwBQYDK2VvAzkAmKuSYeQ/m0SixPDS8Wq8VBaTS1cW+Lp0n0h4Diu+kUpR+qXx4SDJ32YGEFoGFGSbGPry5Ychr6U="

testE2ERatchetParams :: RcvE2ERatchetParamsUri 'C.X448
testE2ERatchetParams = E2ERatchetParamsUri (mkVersionRange (VersionE2E 1) (VersionE2E 1)) testDhPubKey testDhPubKey Nothing

testE2ERatchetParams12 :: RcvE2ERatchetParamsUri 'C.X448
testE2ERatchetParams12 = E2ERatchetParamsUri supportedE2EEncryptVRange testDhPubKey testDhPubKey Nothing

connectionRequest :: AConnectionRequestUri
connectionRequest =
  ACR SCMInvitation $
    CRInvitationUri connReqData testE2ERatchetParams

contactAddress :: AConnectionRequestUri
contactAddress = ACR SCMContact $ CRContactUri connReqData

connectionRequestCurrentRange :: AConnectionRequestUri
connectionRequestCurrentRange =
  ACR SCMInvitation $
    CRInvitationUri
      connReqData {crAgentVRange = supportedSMPAgentVRange, crSmpQueues = [queueV1, queueV1]}
      testE2ERatchetParams12

connectionRequestClientDataEmpty :: AConnectionRequestUri
connectionRequestClientDataEmpty =
  ACR SCMInvitation $
    CRInvitationUri connReqData {crClientData = Just "{}"} testE2ERatchetParams

connectionRequestClientData :: AConnectionRequestUri
connectionRequestClientData =
  ACR SCMInvitation $
    CRInvitationUri connReqData {crClientData = Just "{\"type\":\"group_link\", \"group_link_id\":\"abc\"}"} testE2ERatchetParams

connectionRequestTests :: Spec
connectionRequestTests =
  describe "connection request parsing / serializing" $ do
    it "should serialize SMP queue URIs" $ do
      strEncode (queue :: SMPQueueUri) {queueAddress = queueAddrNoPort}
        `shouldBe` "smp://1234-w==@smp.simplex.im/3456-w==#/?v=2-3&dh=" <> testDhKeyStrUri
      strEncode queue {clientVRange = mkVersionRange (VersionSMPC 1) (VersionSMPC 2)}
        `shouldBe` "smp://1234-w==@smp.simplex.im:5223/3456-w==#/?v=1-2&dh=" <> testDhKeyStrUri
    it "should parse SMP queue URIs" $ do
      strDecode ("smp://1234-w==@smp.simplex.im/3456-w==#/?v=2-3&dh=" <> testDhKeyStr)
        `shouldBe` Right (queue :: SMPQueueUri) {queueAddress = queueAddrNoPort}
      strDecode ("smp://1234-w==@smp.simplex.im/3456-w==#" <> testDhKeyStr)
        `shouldBe` Right (queueV1 :: SMPQueueUri) {queueAddress = queueAddrNoPort}
      strDecode ("smp://1234-w==@smp.simplex.im:5223/3456-w==#" <> testDhKeyStr)
        `shouldBe` Right queueV1
      strDecode ("smp://1234-w==@smp.simplex.im:5223/3456-w==#" <> testDhKeyStr <> "/?v=2-3&extra_param=abc")
        `shouldBe` Right queue
      strDecode ("smp://1234-w==@smp.simplex.im:5223/3456-w==#/?extra_param=abc&v=1&dh=" <> testDhKeyStr)
        `shouldBe` Right queueV1
      strDecode ("smp://1234-w==@smp.simplex.im:5223/3456-w==#" <> testDhKeyStr <> "/?v=1&extra_param=abc")
        `shouldBe` Right queueV1
    it "should serialize connection requests" $ do
      strEncode connectionRequest
        `shouldBe` "simplex:/invitation#/?v=2&smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
          <> urlEncode True testDhKeyStrUri
          <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
      strEncode connectionRequestCurrentRange
        `shouldBe` "simplex:/invitation#/?v=2-6&smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
          <> urlEncode True testDhKeyStrUri
          <> "%2Csmp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
          <> urlEncode True testDhKeyStrUri
          <> "&e2e=v%3D2-3%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
      strEncode connectionRequestClientDataEmpty
        `shouldBe` "simplex:/invitation#/?v=2&smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
          <> urlEncode True testDhKeyStrUri
          <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
          <> "&data=%7B%7D"
      strEncode connectionRequestClientData
        `shouldBe` "simplex:/invitation#/?v=2&smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
          <> urlEncode True testDhKeyStrUri
          <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
          <> "&data=%7B%22type%22%3A%22group_link%22%2C%20%22group_link_id%22%3A%22abc%22%7D"
    it "should parse connection requests" $ do
      strDecode
        ( "https://simplex.chat/contact#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23"
            <> testDhKeyStrUri
            <> "&v=1" -- adjusted to v2
        )
        `shouldBe` Right contactAddress
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23"
            <> testDhKeyStrUri
            <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&v=2"
        )
        `shouldBe` Right connectionRequest
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
            <> testDhKeyStrUri
            <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&v=2"
        )
        `shouldBe` Right connectionRequest
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
            <> testDhKeyStrUri
            <> "&e2e=v%3D1-1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&v=2-2"
        )
        `shouldBe` Right connectionRequest
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26extra_param%3Dabc%26dh%3D"
            <> testDhKeyStrUri
            <> "%2Csmp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
            <> testDhKeyStrUri
            <> "&e2e=extra_key%3Dnew%26v%3D2-3%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&some_new_param=abc"
            <> "&v=2-6"
        )
        `shouldBe` Right connectionRequestCurrentRange
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
            <> testDhKeyStrUri
            <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&data=%7B%7D"
            <> "&v=2-2"
        )
        `shouldBe` Right connectionRequestClientDataEmpty
      strDecode
        ( "https://simplex.chat/invitation#/?smp=smp%3A%2F%2F1234-w%3D%3D%40smp.simplex.im%3A5223%2F3456-w%3D%3D%23%2F%3Fv%3D1%26dh%3D"
            <> testDhKeyStrUri
            <> "&e2e=v%3D1%26x3dh%3DMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D%2CMEIwBQYDK2VvAzkAmKuSYeQ_m0SixPDS8Wq8VBaTS1cW-Lp0n0h4Diu-kUpR-qXx4SDJ32YGEFoGFGSbGPry5Ychr6U%3D"
            <> "&data=%7B%22type%22%3A%22group_link%22%2C%20%22group_link_id%22%3A%22abc%22%7D"
            <> "&v=2"
        )
        `shouldBe` Right connectionRequestClientData
