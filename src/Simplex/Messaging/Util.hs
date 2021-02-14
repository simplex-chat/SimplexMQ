{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Simplex.Messaging.Util where

import Control.Monad.Except
import Control.Monad.IO.Unlift
import Data.Bifunctor (first)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import UnliftIO.Async
import UnliftIO.Exception (Exception)
import qualified UnliftIO.Exception as E

newtype InternalException e = InternalException {unInternalException :: e}
  deriving (Eq, Show)

instance Exception e => Exception (InternalException e)

instance (MonadUnliftIO m, Exception e) => MonadUnliftIO (ExceptT e m) where
  withRunInIO :: ((forall a. ExceptT e m a -> IO a) -> IO b) -> ExceptT e m b
  withRunInIO exceptToIO =
    withExceptT unInternalException . ExceptT . E.try $
      withRunInIO $ \run ->
        exceptToIO $ run . (either (E.throwIO . InternalException) return <=< runExceptT)

raceAny_ :: MonadUnliftIO m => [m a] -> m ()
raceAny_ = r []
  where
    r as (m : ms) = withAsync m $ \a -> r (a : as) ms
    r as [] = void $ waitAnyCancel as

infixl 4 <$$>

(<$$>) :: (Functor f, Functor g) => (a -> b) -> f (g a) -> f (g b)
(<$$>) = fmap . fmap

bshow :: Show a => a -> ByteString
bshow = B.pack . show

liftIOEither :: (MonadUnliftIO m, MonadError e m) => IO (Either e a) -> m a
liftIOEither a = liftIO a >>= liftEither

liftError :: (MonadUnliftIO m, MonadError e' m) => (e -> e') -> ExceptT e IO a -> m a
liftError f = liftError' f . runExceptT

liftError' :: (MonadUnliftIO m, MonadError e' m) => (e -> e') -> IO (Either e a) -> m a
liftError' f a = liftIOEither (first f <$> a)
