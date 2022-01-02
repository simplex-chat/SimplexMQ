{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CoreTests.VersionRangeTests where

import GHC.Generics (Generic)
import Generic.Random (genericArbitraryU)
import Simplex.Messaging.Version
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

data V = V1 | V2 | V3 | V4 | V5 deriving (Eq, Enum, Ord, Generic, Show)

instance Arbitrary V where arbitrary = genericArbitraryU

versionRangeTests :: Spec
versionRangeTests = modifyMaxSuccess (const 1000) $ do
  describe "VersionRange construction" $ do
    it "should fail on invalid range" $ do
      vr 1 1 `shouldBe` vr 1 1
      vr 1 2 `shouldBe` vr 1 2
      (pure $! vr 2 1) `shouldThrow` anyErrorCall
  describe "compatible version" $ do
    it "should choose mutually compatible max version" $ do
      (vr 1 1, vr 1 1) `compatible` Just 1
      (vr 1 1, vr 1 2) `compatible` Just 1
      (vr 1 2, vr 1 2) `compatible` Just 2
      (vr 1 2, vr 2 3) `compatible` Just 2
      (vr 1 3, vr 2 3) `compatible` Just 3
      (vr 1 3, vr 2 4) `compatible` Just 3
      (vr 1 2, vr 3 4) `compatible` Nothing
    it "should check if version is compatible" $ do
      isCompatible 1 (vr 1 2) `shouldBe` True
      isCompatible 2 (vr 1 2) `shouldBe` True
      isCompatible 2 (vr 1 1) `shouldBe` False
      isCompatible 1 (vr 2 2) `shouldBe` False
    it "compatibleVersion should pass isCompatible check" . property $
      \((min1, max1) :: (V, V)) ((min2, max2) :: (V, V)) ->
        min1 > max1 || min2 > max2 -- one of ranges is invalid, skip testing it
          || let w = fromIntegral . fromEnum
                 vr1 = mkVersionRange (w min1) (w max1)
                 vr2 = mkVersionRange (w min2) (w max2)
              in case compatibleVersion vr1 vr2 of
                   Just v -> v `isCompatible` vr1 && v `isCompatible` vr2
                   _ -> True
  where
    vr = mkVersionRange
    (vr1, vr2) `compatible` v = do
      compatibleVersion vr1 vr2 `shouldBe` v
      compatibleVersion vr2 vr1 `shouldBe` v
