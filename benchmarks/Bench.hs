{- Benchmark harness

Run with: cabal bench -O2 simplexmq-bench

List cases: cabal bench -O2 simplexmq-bench --benchmark-options "-l"
Pick one or group: cabal bench -O2 simplexmq-bench --benchmark-options "-p TRcvQueues.getDelSessQueues"
-}

module Main where

import Bench.Compression
import Bench.SNTRUP761
import Bench.TRcvQueues
import Test.Tasty.Bench

main :: IO ()
main =
  defaultMain
    [ bgroup "TRcvQueues" benchTRcvQueues,
      bgroup "SNTRUP761" benchSNTRUP761,
      bgroup "Compression" benchCompression
    ]
