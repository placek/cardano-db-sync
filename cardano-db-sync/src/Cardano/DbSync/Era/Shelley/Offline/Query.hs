{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.DbSync.Era.Shelley.Offline.Query
  ( queryOfflinePoolData
  ) where

import           Cardano.Prelude hiding (from, groupBy, on, retry)

import           Cardano.DbSync.Era.Shelley.Offline.FetchQueue

import           Data.Time (UTCTime)
import           Data.Time.Clock.POSIX (POSIXTime)
import qualified Data.Time.Clock.POSIX as Time

import           Cardano.Db
import           Cardano.DbSync.Types

import           Database.Esqueleto.Legacy (InnerJoin (..), SqlExpr, Value (..), ValueList, desc,
                   from, groupBy, in_, just, max_, notExists, on, orderBy, select, subList_select,
                   where_, (==.), (^.))
import           Database.Persist.Sql (SqlBackend)

import           System.Random.Shuffle (shuffleM)

queryOfflinePoolData :: MonadIO m => POSIXTime -> Int -> ReaderT SqlBackend m [PoolFetchRetry]
queryOfflinePoolData now maxCount = do
  -- Results from the query are shuffles so we don't continuously get the same entries.
  xs <- queryNewPoolFetch now
  if length xs >= maxCount
    then take maxCount <$> liftIO (shuffleM xs)
    else do
      ys <- queryPoolFetchRetry (Time.posixSecondsToUTCTime now)
      take maxCount . (xs ++) <$> liftIO (shuffleM ys)

-- Get pool fetch data for new pools (ie pools that had PoolOfflineData entry and no
-- PoolOfflineFetchError).
queryNewPoolFetch :: MonadIO m => POSIXTime -> ReaderT SqlBackend m [PoolFetchRetry]
queryNewPoolFetch now = do
    res <- select . from $ \(ph `InnerJoin` pmr) -> do
              on (ph ^. PoolHashId ==. pmr ^. PoolMetadataRefPoolId)
              where_ (just (pmr ^. PoolMetadataRefId) `in_` latestRefs)
              where_ (notExists . from $ \pod -> where_ (pod ^. PoolOfflineDataPmrId ==. pmr ^. PoolMetadataRefId))
              where_ (notExists . from $ \pofe -> where_ (pofe ^. PoolOfflineFetchErrorPmrId ==. pmr ^. PoolMetadataRefId))
              pure
                  ( ph ^. PoolHashId
                  , pmr ^. PoolMetadataRefId
                  , pmr ^. PoolMetadataRefUrl
                  , pmr ^. PoolMetadataRefHash
                  )
    pure $ map convert res
  where
    -- This assumes that the autogenerated `id` fiels is a reliable proxy for time, ie, higher
    -- `id` was added later. This is a valid assumption because the primary keys are
    -- monotonically increasing and never reused.
    latestRefs :: SqlExpr (ValueList (Maybe PoolMetadataRefId))
    latestRefs =
      subList_select . from $ \ pmr -> do
        groupBy (pmr ^. PoolMetadataRefPoolId)
        pure $ max_ (pmr ^. PoolMetadataRefId)

    convert
        :: (Value PoolHashId, Value PoolMetadataRefId, Value Text, Value ByteString)
        -> PoolFetchRetry
    convert (Value phId, Value pmrId, Value url, Value pmh) =
      PoolFetchRetry
        { pfrPoolHashId = phId
        , pfrReferenceId = pmrId
        , pfrPoolUrl = PoolUrl url
        , pfrPoolMDHash = PoolMetaHash pmh
        , pfrRetry = newRetry now
        }

-- Get pool fetch data for pools that have previously errored.
queryPoolFetchRetry :: MonadIO m => UTCTime -> ReaderT SqlBackend m [PoolFetchRetry]
queryPoolFetchRetry _now = do
    res <- select . from $ \(pofe `InnerJoin` pmr `InnerJoin` ph) -> do
                on (ph ^. PoolHashId ==. pmr ^. PoolMetadataRefPoolId)
                on (pofe ^. PoolOfflineFetchErrorPmrId ==. pmr ^. PoolMetadataRefId)
                where_ (just (pofe ^. PoolOfflineFetchErrorId) `in_` latestRefs)
                where_ (notExists . from $ \pod -> where_ (pod ^. PoolOfflineDataPmrId ==. pofe ^. PoolOfflineFetchErrorPmrId))
                orderBy [desc (pofe ^. PoolOfflineFetchErrorFetchTime)]
                pure
                    ( pofe ^. PoolOfflineFetchErrorFetchTime
                    , pofe ^. PoolOfflineFetchErrorPmrId
                    , pmr ^. PoolMetadataRefUrl
                    , pmr ^. PoolMetadataRefHash
                    , ph ^. PoolHashId
                    , pofe ^. PoolOfflineFetchErrorRetryCount
                    )
    pure $ map convert res
  where
    -- This assumes that the autogenerated `id` fiels is a reliable proxy for time, ie, higher
    -- `id` was added later. This is a valid assumption because the primary keys are
    -- monotonically increasing and never reused.
    latestRefs :: SqlExpr (ValueList (Maybe PoolOfflineFetchErrorId))
    latestRefs =
      subList_select . from $ \ pofe -> do
        groupBy (pofe ^. PoolOfflineFetchErrorPoolId)
        pure $ max_ (pofe ^. PoolOfflineFetchErrorId)

    convert
        :: (Value UTCTime, Value PoolMetadataRefId, Value Text, Value ByteString, Value PoolHashId, Value Word)
        -> PoolFetchRetry
    convert (Value time, Value pmrId, Value url, Value pmh, Value phId, Value rCount) =
      PoolFetchRetry
        { pfrPoolHashId = phId
        , pfrReferenceId = pmrId
        , pfrPoolUrl = PoolUrl url
        , pfrPoolMDHash = PoolMetaHash pmh
        , pfrRetry = retryAgain (Time.utcTimeToPOSIXSeconds time) rCount
        }
