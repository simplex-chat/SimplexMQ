{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CoreTests.ProtocolErrorTests where

import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Simplex.Messaging.Agent.Protocol (AgentErrorType (..), BrokerErrorType (..))
import Simplex.Messaging.Encoding.String
import Simplex.Messaging.Parsers (parseAll)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

protocolErrorTests :: Spec
protocolErrorTests = modifyMaxSuccess (const 1000) $ do
  describe "errors parsing / serializing" $ do
    it "should parse SMP protocol errors" . property $ \(err :: AgentErrorType) ->
      errHasSpaces err
        || parseAll strP (strEncode err) == Right err
    it "should parse SMP agent errors" . property $ \(err :: AgentErrorType) ->
      errHasSpaces err
        || parseAll strP (strEncode err) == Right err
  where
    errHasSpaces = \case
      BROKER srv (RESPONSE e) -> hasSpaces srv || hasSpaces e
      BROKER srv _ -> hasSpaces srv
      _ -> False
    hasSpaces s = ' ' `B.elem` encodeUtf8 (T.pack s)
