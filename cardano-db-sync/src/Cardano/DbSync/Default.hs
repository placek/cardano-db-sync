{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Cardano.DbSync.Default (
  insertListBlocks,
) where

import Cardano.BM.Trace (logInfo)
import qualified Cardano.Db as DB
import Cardano.DbSync.Api
import Cardano.DbSync.Cache
import Cardano.DbSync.Epoch
import Cardano.DbSync.Era.Byron.Insert (insertByronBlock)
import Cardano.DbSync.Era.Cardano.Insert (insertEpochSyncTime)
import Cardano.DbSync.Era.Shelley.Adjust (adjustEpochRewards)
import qualified Cardano.DbSync.Era.Shelley.Generic as Generic
import Cardano.DbSync.Era.Shelley.Insert (insertShelleyBlock)
import Cardano.DbSync.Era.Shelley.Insert.Epoch (insertPoolDepositRefunds, insertRewards)
import Cardano.DbSync.Era.Shelley.Validate (validateEpochRewards)
import Cardano.DbSync.Error
import Cardano.DbSync.LedgerState (
  ApplyResult (..),
  LedgerEvent (..),
  applyBlockAndSnapshot,
  defaultApplyResult,
 )
import Cardano.DbSync.LocalStateQuery
import Cardano.DbSync.Rollback
import Cardano.DbSync.Types
import Cardano.DbSync.Util
import qualified Cardano.Ledger.Alonzo.Scripts as Ledger
import Cardano.Prelude
import Cardano.Slotting.Slot (EpochNo (..))
import Control.Monad.Logger (LoggingT)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Except.Extra (newExceptT)
import qualified Data.ByteString.Short as SBS
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Strict.Maybe as Strict

-- import qualified Data.Text as Text

import Database.Persist.SqlBackend.Internal
import Database.Persist.SqlBackend.Internal.StatementCache
import Ouroboros.Consensus.Cardano.Block (HardForkBlock (..))
import qualified Ouroboros.Consensus.HardFork.Combinator as Consensus
import Ouroboros.Network.Block (blockHash, blockNo, getHeaderFields)

insertListBlocks ::
  SyncEnv ->
  [CardanoBlock] ->
  IO (Either SyncNodeError ())
insertListBlocks env blocks = do
  backend <- getBackend env
  DB.runDbIohkLogging backend tracer
    . runExceptT
    $ do
      traverse_ (applyAndInsertBlockMaybe env) blocks
  where
    tracer = getTrace env

applyAndInsertBlockMaybe ::
  SyncEnv -> CardanoBlock -> ExceptT SyncNodeError (ReaderT SqlBackend (LoggingT IO)) ()
applyAndInsertBlockMaybe env cblk = do
  (!applyRes, !tookSnapshot) <- liftIO mkApplyResult
  bl <- liftIO $ isConsistent env
  if bl
    then -- In the usual case it will be consistent so we don't need to do any queries. Just insert the block
      insertBlock env cblk applyRes False tookSnapshot
    else do
      blockIsInDbAlready <- lift (isRight <$> DB.queryBlockId (SBS.fromShort . Consensus.getOneEraHash $ blockHash cblk))
      -- If the block is already in db, do nothing. If not, delete all blocks with greater 'BlockNo' or
      -- equal, insert the block and restore consistency between ledger and db.
      unless blockIsInDbAlready $ do
        liftIO . logInfo tracer $
          mconcat
            [ "Received block which is not in the db with "
            , textShow (getHeaderFields cblk)
            , ". Time to restore consistency."
            ]
        rollbackFromBlockNo env (blockNo cblk)
        insertBlock env cblk applyRes True tookSnapshot
        liftIO $ setConsistentLevel env Consistent
  where
    tracer = getTrace env

    mkApplyResult :: IO (ApplyResult, Bool)
    mkApplyResult = do
      if hasLedgerState env
        then applyBlockAndSnapshot (envLedger env) cblk
        else do
          slotDetails <- getSlotDetailsNode (envNoLedgerEnv env) (cardanoBlockSlotNo cblk)
          pure (defaultApplyResult slotDetails, False)

insertBlock ::
  SyncEnv ->
  CardanoBlock ->
  ApplyResult ->
  Bool ->
  Bool ->
  ExceptT SyncNodeError (ReaderT SqlBackend (LoggingT IO)) ()
insertBlock env cblk applyRes firstAfterRollback tookSnapshot = do
  !epochEvents <- liftIO $ atomically $ generateNewEpochEvents env (apSlotDetails applyRes)
  let !applyResult = applyRes {apEvents = sort $ epochEvents <> apEvents applyRes}
  let !details = apSlotDetails applyResult
  let !withinTwoMin = isWithinTwoMin details
  let !withinHalfHour = isWithinHalfHour details
  insertLedgerEvents env (sdEpochNo details) (apEvents applyResult)
  insertEpoch details
  let shouldLog = hasEpochStartEvent (apEvents applyResult) || firstAfterRollback
  let isMember poolId = Set.member poolId (apPoolsRegistered applyResult)
  let insertShelley blk =
        insertShelleyBlock
          env
          shouldLog
          withinTwoMin
          withinHalfHour
          blk
          details
          isMember
          (apNewEpoch applyResult)
          (apStakeSlice applyResult)
  case cblk of
    BlockByron blk ->
      newExceptT $
        insertByronBlock env shouldLog blk details
    BlockShelley blk ->
      newExceptT $
        insertShelley $
          Generic.fromShelleyBlock blk
    BlockAllegra blk ->
      newExceptT $
        insertShelley $
          Generic.fromAllegraBlock blk
    BlockMary blk ->
      newExceptT $
        insertShelley $
          Generic.fromMaryBlock blk
    BlockAlonzo blk ->
      newExceptT $
        insertShelley $
          Generic.fromAlonzoBlock (getPrices applyResult) blk
    BlockBabbage blk ->
      newExceptT $
        insertShelley $
          Generic.fromBabbageBlock (getPrices applyResult) blk
  lift $ commitOrIndexes withinTwoMin withinHalfHour
  where
    tracer = getTrace env

    insertEpoch details =
      when (soptExtended $ envOptions env)
        . newExceptT
        $ epochInsert tracer (BlockDetails cblk details)

    getPrices :: ApplyResult -> Maybe Ledger.Prices
    getPrices applyResult = case apPrices applyResult of
      Strict.Just pr -> Just pr
      Strict.Nothing | hasLedgerState env -> Just $ Ledger.Prices minBound minBound
      Strict.Nothing -> Nothing

    commitOrIndexes :: Bool -> Bool -> ReaderT SqlBackend (LoggingT IO) ()
    commitOrIndexes withinTwoMin withinHalfHour = do
      commited <-
        if withinTwoMin || tookSnapshot
          then do
            DB.transactionCommit
            pure True
          else pure False
      when withinHalfHour $ do
        ranIndexes <- liftIO $ getRanIndexes env
        unless ranIndexes $ do
          unless commited DB.transactionCommit
          liftIO $ runIndexMigrations env

    isWithinTwoMin :: SlotDetails -> Bool
    isWithinTwoMin sd = isSyncedWithinSeconds sd 120 == SyncFollowing

    isWithinHalfHour :: SlotDetails -> Bool
    isWithinHalfHour sd = isSyncedWithinSeconds sd 1800 == SyncFollowing

-- -------------------------------------------------------------------------------------------------

insertLedgerEvents ::
  (MonadBaseControl IO m, MonadIO m) =>
  SyncEnv ->
  EpochNo ->
  [LedgerEvent] ->
  ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertLedgerEvents env currentEpochNo@(EpochNo curEpoch) =
  mapM_ handler
  where
    tracer = getTrace env
    lenv = envLedger env
    cache = envCache env
    ntw = leNetwork lenv

    subFromCurrentEpoch :: Word64 -> EpochNo
    subFromCurrentEpoch m =
      if unEpochNo currentEpochNo >= m
        then EpochNo $ unEpochNo currentEpochNo - m
        else EpochNo 0

    toSyncState :: SyncState -> DB.SyncState
    toSyncState SyncLagging = DB.SyncLagging
    toSyncState SyncFollowing = DB.SyncFollowing

    handler ::
      (MonadBaseControl IO m, MonadIO m) =>
      LedgerEvent ->
      ExceptT SyncNodeError (ReaderT SqlBackend m) ()
    handler ev =
      case ev of
        LedgerNewEpoch en ss -> do
          lift $ do
            insertEpochSyncTime en (toSyncState ss) (envEpochSyncTime env)
          sqlBackend <- lift ask
          persistantCacheSize <- liftIO $ statementCacheSize $ connStmtMap sqlBackend
          liftIO . logInfo tracer $ "Persistant SQL Statement Cache size is " <> textShow persistantCacheSize
          stats <- liftIO $ textShowStats cache
          liftIO . logInfo tracer $ stats
          liftIO . logInfo tracer $ "Starting epoch " <> textShow (unEpochNo en)
        LedgerStartAtEpoch en ->
          -- This is different from the previous case in that the db-sync started
          -- in this epoch, for example after a restart, instead of after an epoch boundary.
          liftIO . logInfo tracer $ "Starting at epoch " <> textShow (unEpochNo en)
        LedgerDeltaRewards _e rwd -> do
          let rewards = Map.toList $ Generic.unRewards rwd
          insertRewards ntw (subFromCurrentEpoch 2) currentEpochNo cache (Map.toList $ Generic.unRewards rwd)
          -- This event is only created when it's not empty, so we don't need to check for null here.
          liftIO . logInfo tracer $ "Inserted " <> show (length rewards) <> " Delta rewards"
        LedgerIncrementalRewards _ rwd -> do
          let rewards = Map.toList $ Generic.unRewards rwd
          insertRewards ntw (subFromCurrentEpoch 1) (EpochNo $ curEpoch + 1) cache rewards
        LedgerRestrainedRewards e rwd creds -> do
          lift $ adjustEpochRewards tracer ntw cache e rwd creds
        LedgerTotalRewards _e rwd -> do
          lift $ validateEpochRewards tracer ntw (subFromCurrentEpoch 2) currentEpochNo rwd
        LedgerAdaPots _ ->
          pure () -- These are handled separately by insertBlock
        LedgerMirDist rwd -> do
          unless (Map.null rwd) $ do
            let rewards = Map.toList rwd
            insertRewards ntw (subFromCurrentEpoch 1) currentEpochNo cache rewards
            liftIO . logInfo tracer $ "Inserted " <> show (length rewards) <> " Mir rewards"
        LedgerPoolReap en drs -> do
          unless (Map.null $ Generic.unRewards drs) $ do
            insertPoolDepositRefunds env en drs

hasEpochStartEvent :: [LedgerEvent] -> Bool
hasEpochStartEvent = any isNewEpoch
  where
    isNewEpoch :: LedgerEvent -> Bool
    isNewEpoch le =
      case le of
        LedgerNewEpoch {} -> True
        LedgerStartAtEpoch {} -> True
        _otherwise -> False
