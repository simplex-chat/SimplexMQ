{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module CoreTests.ProtocolErrorTests where

import GHC.Generics (Generic)
import Generic.Random (genericArbitraryU)
import Simplex.FileTransfer.Transport (XFTPErrorType (..))
import Simplex.Messaging.Agent.Protocol
import qualified Simplex.Messaging.Agent.Protocol as Agent
import Simplex.Messaging.Client (ProxyClientError (..))
import Simplex.Messaging.Encoding
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Protocol (CommandError (..), ErrorType (..))
import qualified Simplex.Messaging.Protocol as SMP
import Simplex.Messaging.Transport (HandshakeError (..), TransportError (..))
import Simplex.RemoteControl.Types (RCErrorType (..))
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

protocolErrorTests :: Spec
protocolErrorTests = modifyMaxSuccess (const 1000) $ do
  describe "errors parsing / serializing" $ do
    it "should parse SMP protocol errors" . property . forAll possibleErrorType $ \err ->
      smpDecode (smpEncode err) == Right err
    it "should parse SMP agent errors" . property . forAll possibleAgentErrorType $ \err ->
      strDecode (strEncode err) == Right err
  where
    possibleErrorType :: Gen ErrorType
    possibleErrorType = arbitrary >>= \e -> if skipErrorType e then discard else pure e
    possibleAgentErrorType :: Gen AgentErrorType
    possibleAgentErrorType =
      arbitrary >>= \case
        BROKER srv _ | hasSpaces srv -> discard
        SMP srv e | hasSpaces srv || skipErrorType e -> discard
        NTF srv e | hasSpaces srv || skipErrorType e -> discard
        XFTP srv _ | hasSpaces srv -> discard
        Agent.PROXY pxy srv _ | hasSpaces pxy || hasSpaces srv -> discard
        Agent.PROXY _ _ (ProxyProtocolError e) | skipErrorType e -> discard
        Agent.PROXY _ _ (ProxyUnexpectedResponse e) | hasUnicode e -> discard
        Agent.PROXY _ _ (ProxyResponseError e) | skipErrorType e -> discard
        ok -> pure ok
    hasSpaces :: String -> Bool
    hasSpaces = any (== ' ')
    hasUnicode :: String -> Bool
    hasUnicode = any (>= '\255')
    skipErrorType = \case
      SMP.PROXY (SMP.PROTOCOL e) -> skipErrorType e
      SMP.PROXY (SMP.BROKER (UNEXPECTED s)) -> hasUnicode s
      SMP.PROXY (SMP.BROKER (RESPONSE s)) -> hasUnicode s
      _ -> False

deriving instance Generic AgentErrorType

deriving instance Generic CommandErrorType

deriving instance Generic ConnectionErrorType

deriving instance Generic ProxyClientError

deriving instance Generic BrokerErrorType

deriving instance Generic SMPAgentError

deriving instance Generic AgentCryptoError

deriving instance Generic ErrorType

deriving instance Generic CommandError

deriving instance Generic SMP.ProxyError

deriving instance Generic TransportError

deriving instance Generic HandshakeError

deriving instance Generic XFTPErrorType

deriving instance Generic RCErrorType

instance Arbitrary AgentErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandErrorType where arbitrary = genericArbitraryU

instance Arbitrary ConnectionErrorType where arbitrary = genericArbitraryU

instance Arbitrary ProxyClientError where arbitrary = genericArbitraryU

instance Arbitrary BrokerErrorType where arbitrary = genericArbitraryU

instance Arbitrary SMPAgentError where arbitrary = genericArbitraryU

instance Arbitrary AgentCryptoError where arbitrary = genericArbitraryU

instance Arbitrary ErrorType where arbitrary = genericArbitraryU

instance Arbitrary CommandError where arbitrary = genericArbitraryU

instance Arbitrary SMP.ProxyError where arbitrary = genericArbitraryU

instance Arbitrary TransportError where arbitrary = genericArbitraryU

instance Arbitrary HandshakeError where arbitrary = genericArbitraryU

instance Arbitrary XFTPErrorType where arbitrary = genericArbitraryU

instance Arbitrary RCErrorType where arbitrary = genericArbitraryU
