{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Cardano.DbSync.OffChain (
  insertOffChainPoolResults,
  insertOffChainVoteResults,
  loadOffChainPoolWorkQueue,
  loadOffChainVoteWorkQueue,
  runFetchOffChainThreads,
  fetchOffChainPoolData,
  fetchOffChainVoteData,
) where

import Cardano.BM.Trace (Trace, logInfo)
import qualified Cardano.Db as DB
import Cardano.DbSync.Api
import Cardano.DbSync.Api.Types (InsertOptions (..), SyncEnv (..))
import Cardano.DbSync.OffChain.Http
import Cardano.DbSync.OffChain.Query
import Cardano.DbSync.Types
import Cardano.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (
  StrictTBQueue (..),
  flushTBQueue,
  isEmptyTBQueue,
  readTBQueue,
  writeTBQueue,
 )
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.Time.Clock.POSIX (POSIXTime)
import qualified Data.Time.Clock.POSIX as Time
import Database.Persist.Sql (SqlBackend)
import qualified Network.HTTP.Client as Http
import Network.HTTP.Client.TLS (tlsManagerSettings)

loadOffChainPoolWorkQueue ::
  (MonadBaseControl IO m, MonadIO m) =>
  Trace IO Text ->
  StrictTBQueue IO OffChainPoolWorkQueue ->
  ReaderT SqlBackend m ()
loadOffChainPoolWorkQueue _trce workQueue = do
  -- If we try to write the to queue when it is full it will block. Therefore only add more to
  -- the queue if it is empty.
  whenM (liftIO $ atomically (isEmptyTBQueue workQueue)) $ do
    now <- liftIO Time.getPOSIXTime
    runnablePools <- filter (isRunnable now) <$> aquireOffChainPoolData now 100
    liftIO $ mapM_ queueInsert runnablePools
  where
    isRunnable :: POSIXTime -> OffChainPoolWorkQueue -> Bool
    isRunnable now pfr = retryRetryTime (oPoolWqRetry pfr) <= now

    queueInsert :: OffChainPoolWorkQueue -> IO ()
    queueInsert = atomically . writeTBQueue workQueue

loadOffChainVoteWorkQueue ::
  (MonadBaseControl IO m, MonadIO m) =>
  Trace IO Text ->
  StrictTBQueue IO OffChainVoteWorkQueue ->
  ReaderT SqlBackend m ()
loadOffChainVoteWorkQueue _trce workQueue = do
  -- TODO: Vince - do we just get the data from database here? which seems to be what aquireOffChainPoolData
  --       is doing for Pool data in function above
  whenM (liftIO $ atomically (isEmptyTBQueue workQueue)) $ do
    pure ()

---------------------------------------------------------------------------------------------------------------------------------
-- Insert OffChain
---------------------------------------------------------------------------------------------------------------------------------
insertOffChainPoolResults ::
  (MonadBaseControl IO m, MonadIO m) =>
  Trace IO Text ->
  StrictTBQueue IO OffChainPoolResult ->
  ReaderT SqlBackend m ()
insertOffChainPoolResults trce resultQueue = do
  res <- liftIO . atomically $ flushTBQueue resultQueue
  let resLength = length res
      resErrorsLength = length $ filter isFetchError res
  unless (null res)
    $ liftIO
      . logInfo trce
    $ logInsertOffChainResults "Pool" resLength resErrorsLength

  mapM_ insert res
  where
    insert :: (MonadBaseControl IO m, MonadIO m) => OffChainPoolResult -> ReaderT SqlBackend m ()
    insert = \case
      OffChainPoolResultMetadata md -> void $ DB.insertCheckOffChainPoolData md
      OffChainPoolResultError fe -> void $ DB.insertCheckOffChainPoolFetchError fe

    isFetchError :: OffChainPoolResult -> Bool
    isFetchError = \case
      OffChainPoolResultMetadata {} -> False
      OffChainPoolResultError {} -> True

insertOffChainVoteResults ::
  (MonadBaseControl IO m, MonadIO m) =>
  Trace IO Text ->
  StrictTBQueue IO OffChainVoteResult ->
  ReaderT SqlBackend m ()
insertOffChainVoteResults trce resultQueue = do
  res <- liftIO . atomically $ flushTBQueue resultQueue
  let resLength = length res
      resErrorsLength = length $ filter isFetchError res
  unless (null res)
    $ liftIO
      . logInfo trce
    $ logInsertOffChainResults "Voting Anchor" resLength resErrorsLength
  mapM_ insert res
  where
    insert :: (MonadBaseControl IO m, MonadIO m) => OffChainVoteResult -> ReaderT SqlBackend m ()
    insert = \case
      OffChainVoteResultMetadata md -> void $ DB.insertOffChainVoteData md
      OffChainVoteResultError fe -> void $ DB.insertOffChainVoteFetchError fe

    isFetchError :: OffChainVoteResult -> Bool
    isFetchError = \case
      OffChainVoteResultMetadata {} -> False
      OffChainVoteResultError {} -> True

logInsertOffChainResults ::
  Text -> -- Pool of Vote
  Int -> -- length of tbQueue
  Int -> -- length of errors in tbQueue
  Text
logInsertOffChainResults offChainType resLength resErrorsLength =
  mconcat
    [ offChainType
    , " Offchain "
    , "metadata fetch: "
    , DB.textShow (resLength - resErrorsLength)
    , " results, "
    , DB.textShow resErrorsLength
    , " fetch errors"
    ]

---------------------------------------------------------------------------------------------------------------------------------
-- Run OffChain threads
---------------------------------------------------------------------------------------------------------------------------------
runFetchOffChainThreads :: SyncEnv -> IO ()
runFetchOffChainThreads syncEnv = do
  -- if dissable gov is active then don't run voting anchor thread
  unless (ioGov iopts) $ do
    logInfo trce "Running Offchain Vote Anchor fetch thread"
    forever $ do
      tDelay
      voteq <- blockingFlushTBQueue (envOffChainVoteWorkQueue syncEnv)
      manager <- Http.newManager tlsManagerSettings
      now <- liftIO Time.getPOSIXTime
      mapM_ (queueVoteInsert <=< fetchOffChainVoteData trce manager now) voteq

  when (ioOffChainPoolData iopts) $ do
    logInfo trce "Running Offchain Pool fetch thread"
    forever $ do
      tDelay
      poolq <- blockingFlushTBQueue (envOffChainPoolWorkQueue syncEnv)
      manager <- Http.newManager tlsManagerSettings
      now <- liftIO Time.getPOSIXTime
      mapM_ (queuePoolInsert <=< fetchOffChainPoolData trce manager now) poolq
  where
    trce = getTrace syncEnv
    iopts = getInsertOptions syncEnv

    tDelay = threadDelay 60_000_000 -- 60 second sleep
    queuePoolInsert :: OffChainPoolResult -> IO ()
    queuePoolInsert = atomically . writeTBQueue (envOffChainPoolResultQueue syncEnv)

    queueVoteInsert :: OffChainVoteResult -> IO ()
    queueVoteInsert = atomically . writeTBQueue (envOffChainVoteResultQueue syncEnv)

-- OffChainVoteResultType res -> atomically $ writeTBQueue (envOffChainVoteResultQueue syncEnv) $ OffChainVoteResultType res

-- -------------------------------------------------------------------------------------------------

-- Blocks on an empty queue, but gets all elements in the queue if there is more than one.
blockingFlushTBQueue :: StrictTBQueue IO a -> IO [a]
blockingFlushTBQueue queue = do
  atomically $ do
    x <- readTBQueue queue
    xs <- flushTBQueue queue
    pure $ x : xs

---------------------------------------------------------------------------------------------------------------------------------
-- Fetch OffChain data
---------------------------------------------------------------------------------------------------------------------------------
fetchOffChainPoolData :: Trace IO Text -> Http.Manager -> Time.POSIXTime -> OffChainPoolWorkQueue -> IO OffChainPoolResult
fetchOffChainPoolData _tracer manager time oPoolWorkQ =
  convert <<$>> runExceptT $ do
    let url = oPoolWqUrl oPoolWorkQ
        metaHash = oPoolWqMetaHash oPoolWorkQ
    request <- parseOffChainPoolUrl $ oPoolWqUrl oPoolWorkQ
    httpGetOffChainPoolData manager request (OffChainPoolUrl url) (Just $ OffChainPoolHash metaHash)
  where
    convert :: Either OffChainFetchError SimplifiedOffChainPoolData -> OffChainPoolResult
    convert eres =
      case eres of
        Right sPoolData ->
          OffChainPoolResultMetadata $
            DB.OffChainPoolData
              { DB.offChainPoolDataPoolId = oPoolWqHashId oPoolWorkQ
              , DB.offChainPoolDataTickerName = spodTickerName sPoolData
              , DB.offChainPoolDataHash = spodHash sPoolData
              , DB.offChainPoolDataBytes = spodBytes sPoolData
              , DB.offChainPoolDataJson = spodJson sPoolData
              , DB.offChainPoolDataPmrId = oPoolWqReferenceId oPoolWorkQ
              }
        Left err ->
          OffChainPoolResultError $
            DB.OffChainPoolFetchError
              { DB.offChainPoolFetchErrorPoolId = oPoolWqHashId oPoolWorkQ
              , DB.offChainPoolFetchErrorFetchTime = Time.posixSecondsToUTCTime time
              , DB.offChainPoolFetchErrorPmrId = oPoolWqReferenceId oPoolWorkQ
              , DB.offChainPoolFetchErrorFetchError = show err
              , DB.offChainPoolFetchErrorRetryCount = retryCount (oPoolWqRetry oPoolWorkQ)
              }

fetchOffChainVoteData :: Trace IO Text -> Http.Manager -> Time.POSIXTime -> OffChainVoteWorkQueue -> IO OffChainVoteResult
fetchOffChainVoteData _tracer manager time oVoteWorkQ =
  convert <<$>> runExceptT $ do
    let url = oVoteWqUrl oVoteWorkQ
        metaHash = oVoteWqMetaHash oVoteWorkQ
    request <- parseOffChainVoteUrl $ oVoteWqUrl oVoteWorkQ
    httpGetOffChainVoteData manager request (OffChainVoteUrl url) (Just $ OffChainVoteHash metaHash)
  where
    convert :: Either OffChainFetchError SimplifiedOffChainVoteData -> OffChainVoteResult
    convert eres =
      case eres of
        Right sVoteData ->
          OffChainVoteResultMetadata $
            DB.OffChainVoteData
              { DB.offChainVoteDataBytes = sovaBytes sVoteData
              , DB.offChainVoteDataHash = sovaHash sVoteData
              , DB.offChainVoteDataJson = sovaJson sVoteData
              , DB.offChainVoteDataVotingAnchorId = oVoteWqReferenceId oVoteWorkQ
              }
        Left err ->
          OffChainVoteResultError $
            DB.OffChainVoteFetchError
              { DB.offChainVoteFetchErrorVotingAnchorId = oVoteWqReferenceId oVoteWorkQ
              , DB.offChainVoteFetchErrorFetchError = show err
              , DB.offChainVoteFetchErrorFetchTime = Time.posixSecondsToUTCTime time
              , DB.offChainVoteFetchErrorRetryCount = retryCount (oVoteWqRetry oVoteWorkQ)
              }
