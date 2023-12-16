{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Simplex.Messaging.Agent.Batch where

import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class
import Data.Composition ((.:))
import Data.Bifunctor (bimap)
import Data.Either (partitionEithers)
import Data.List (foldl')
import Simplex.Messaging.Agent.Client
import Simplex.Messaging.Agent.Env.SQLite
import Simplex.Messaging.Agent.Protocol (AgentErrorType (..))
import qualified Simplex.Messaging.Agent.Store.SQLite.DB as DB
import Simplex.Messaging.Agent.Store
import UnliftIO

data Batch op e m a
  = BPure (Either e a)
  | BBind (BindCont op e m a)
  | BEffect (EffectCont op e m a)
  | BEffects_ (EffectsCont_ op e m a)

data Evaluated op e m a
  = EPure (Either e a)
  | EEffect (EffectCont op e m a)
  | EEffects_ (EffectsCont_ op e m a)

data BindCont op e m a = forall b. BindCont {bindAction :: m (Batch op e m b), next :: b -> m (Batch op e m a)}

data EffectCont op e m a = forall b. EffectCont {effect :: op m b, next :: b -> m (Batch op e m a)}

data EffectsCont_ op e m a = forall b. EffectsCont_ {effects_ :: [op m b], next_ :: m (Batch op e m a)}

class MonadError e m => BatchEffect op cxt e m | op -> cxt, op -> e where
  execBatchEffects :: cxt -> [op m a] -> m [Either e a]
  batchError :: String -> e

type AgentBatch m a = Batch AgentBatchEff AgentErrorType m a

data AgentBatchEff (m :: * -> *) b = ABDatabase {dbAction :: DB.Connection -> IO (Either StoreError b)}

instance AgentMonad m => BatchEffect AgentBatchEff AgentClient AgentErrorType m where
  execBatchEffects c = mapM (\(ABDatabase a) -> runExceptT $ withStore c a)
  batchError = INTERNAL

runBatch :: forall a op cxt e m. BatchEffect op cxt e m => cxt -> [m (Batch op e m a)] -> m [Either e a]
runBatch c as = mapM batchResult =<< execBatch c as
  where
    batchResult :: Batch op e m a -> m (Either e a)
    batchResult = \case
      BPure r -> pure r
      _ -> throwError $ batchError @op @cxt @e @m "incomplete batch processing"

unBatch :: forall a op cxt e m. BatchEffect op cxt e m => cxt -> m (Batch op e m a) -> m a
unBatch c a = runBatch c [a] >>= oneResult
  where
    -- TODO something smarter than "head" to return error if there is more/less results
    oneResult :: [Either e a] -> m a
    oneResult = liftEither . head

type BRef op e m a = IORef (Batch op e m a)

evaluateB :: forall op e m a. MonadError e m => m (Batch op e m a) -> m (Evaluated op e m a)
evaluateB b = tryEval b evalB
  where
    tryEval :: m (Batch op e m c) -> (Batch op e m c -> m (Evaluated op e m d)) -> m (Evaluated op e m d)
    tryEval b eval = tryError b >>= either evalErr eval
    evalB = \case
      BPure v -> pure $ EPure v
      BBind (BindCont a next) -> evaluateBind a next
      BEffect cont -> pure $ EEffect cont
      BEffects_ cont -> pure $ EEffects_ cont
    evaluateBind :: forall b. m (Batch op e m b) -> (b -> m (Batch op e m a)) -> m (Evaluated op e m a)
    evaluateBind a next = tryEval a evalBind
      where
        evalBind :: Batch op e m b -> m (Evaluated op e m a)
        evalBind = \case
          BPure v -> either evalErr (evaluateB . next) v
          BBind (BindCont a' next') -> evaluateBind a' (next' @>=> next)
          BEffect (EffectCont op next') -> pure $ EEffect $ EffectCont op (next' @>=> next)
          BEffects_ (EffectsCont_ ops next_') -> pure $ EEffects_ $ EffectsCont_ ops (next_' @>>= next)
    evalErr = pure . EPure . Left

execBatch' :: forall a op cxt e m. (MonadIO m, BatchEffect op cxt e m) => cxt -> [m (Batch op e m a)] -> m [Batch op e m a]
execBatch' c as = do
  rs <- replicateM (length as) $ newIORef notEvaluated
  exec . (zipWith (\r -> bimap (r,) (r,)) rs) =<< mapM tryError as
  mapM readIORef rs
  where
    notEvaluated = BPure $ Left $ batchError @op @cxt @e @m "not evaluated"
    exec :: [Either (BRef op e m a, e) (BRef op e m a, Batch op e m a)] -> m ()
    exec bs = do
      let (es, bs') = partitionEithers bs
          (vs, binds, effs, effs_) = foldl' addBatch ([], [], [], []) bs' 
      forM_ es $ \(r, e) -> writeIORef r (BPure $ Left e)
      forM_ vs $ \(r, v) -> writeIORef r (BPure v)
      -- evaluate binds till pure or effect
      -- evaluate batches till pure or effect
      -- let (vs, bs'') = partitionEithers $ map (\case (r, BPure v) -> Left (r, v); b -> Right b) bs
      -- forM_ vs $ \(r, v) -> writeIORef r (BPure v)
      pure ()
    addBatch ::
      ([(BRef op e m a, Either e a)], [(BRef op e m a, BindCont op e m a)], [(BRef op e m a, EffectCont op e m a)], [(BRef op e m a, EffectsCont_ op e m a)]) ->
      (BRef op e m a, Batch op e m a) ->
      ([(BRef op e m a, Either e a)], [(BRef op e m a, BindCont op e m a)], [(BRef op e m a, EffectCont op e m a)], [(BRef op e m a, EffectsCont_ op e m a)])
    addBatch (vs, bs, effs, effs_) = \case
      (r, BPure v) -> ((r, v) : vs, bs, effs, effs_)
      (r, BBind cont) -> (vs, (r, cont) : bs, effs, effs_)
      (r, BEffect cont) -> (vs, bs, (r, cont) : effs, effs_)
      (r, BEffects_ cont) -> (vs, bs, effs, (r, cont) : effs_)

execBatch :: forall a op cxt e m. BatchEffect op cxt e m => cxt -> [m (Batch op e m a)] -> m [Batch op e m a]
execBatch c [a] = run =<< tryError (evaluateB a)
  where
    run = \case
      Left e -> pure [BPure $ Left e]
      Right r -> case r of
        EPure r' -> pure [BPure r']
        EEffect (EffectCont op next) -> execBatchEffects c [op] >>= \case
          Left e : _ -> pure [BPure $ Left e]
          Right r' : _ -> execBatch c [next r']
          _ -> pure [BPure $ Left $ batchError @op @cxt @e @m "not implemented"]
        EEffects_ (EffectsCont_ {effects_, next_}) ->
          execBatchEffects c effects_ >> execBatch c [next_]
execBatch _ _ = throwError $ batchError @op @cxt @e @m "not implemented"

pureB :: Monad m => a -> m (Batch op e m a)
pureB = pure . BPure . Right

infixl 0 @>>=, @>>, @>=>

(@>>=) :: Monad m => m (Batch op e m b) -> (b -> m (Batch op e m a)) -> m (Batch op e m a)
(@>>=) = pure . BBind .: BindCont

(@>>) :: Monad m => m (Batch op e m b) -> m (Batch op e m a) -> m (Batch op e m a)
(@>>) f = (f @>>=) . const

(@>=>) :: Monad m => (c -> m (Batch op e m b)) -> (b -> m (Batch op e m a)) -> c -> m (Batch op e m a)
(@>=>) f g x = f x @>>= g

withStoreB :: Monad m => (DB.Connection -> IO (Either StoreError b)) -> (b -> m (AgentBatch m a)) -> m (AgentBatch m a)
withStoreB f = pure . BEffect . EffectCont (ABDatabase f)

withStoreB' :: Monad m => (DB.Connection -> IO b) -> (b -> m (AgentBatch m a)) -> m (AgentBatch m a)
withStoreB' f = withStoreB (fmap Right . f)

withStoreBatchB' :: Monad m => [DB.Connection -> IO b] -> m (AgentBatch m a) -> m (AgentBatch m a)
withStoreBatchB' fs = pure . BEffects_ . EffectsCont_ (map (ABDatabase . (fmap Right .)) fs)
