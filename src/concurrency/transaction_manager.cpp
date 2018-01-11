//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include "catalog/manager.h"
#include "concurrency/transaction_context.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager.h"
#include "settings/settings_manager.h"
#include "statistics/stats_aggregator.h"
#include "storage/tile_group.h"

namespace peloton {
namespace concurrency {

ProtocolType TransactionManager::protocol_ = ProtocolType::TIMESTAMP_ORDERING;
IsolationLevelType TransactionManager::isolation_level_ =
    IsolationLevelType::SERIALIZABLE;
ConflictAvoidanceType TransactionManager::conflict_avoidance_ =
    ConflictAvoidanceType::ABORT;

TransactionContext *TransactionManager::BeginTransaction(
    const size_t thread_id, const IsolationLevelType type) {
  TransactionContext *txn = nullptr;

  if (type == IsolationLevelType::READ_ONLY) {
    // transaction processing with decentralized epoch manager
    cid_t read_id = EpochManagerFactory::GetInstance().EnterEpoch(
        thread_id, TimestampType::SNAPSHOT_READ);
    txn = new TransactionContext(thread_id, type, read_id);

  } else if (type == IsolationLevelType::SNAPSHOT) {
    // transaction processing with decentralized epoch manager
    // the DBMS must acquire
    cid_t read_id = EpochManagerFactory::GetInstance().EnterEpoch(
        thread_id, TimestampType::SNAPSHOT_READ);

    if (protocol_ == ProtocolType::TIMESTAMP_ORDERING) {
      cid_t commit_id = EpochManagerFactory::GetInstance().EnterEpoch(
          thread_id, TimestampType::COMMIT);

      txn = new TransactionContext(thread_id, type, read_id, commit_id);
    } else {
      txn = new TransactionContext(thread_id, type, read_id);
    }

  } else {
    // if the isolation level is set to:
    // - SERIALIZABLE, or
    // - REPEATABLE_READS, or
    // - READ_COMMITTED.
    // transaction processing with decentralized epoch manager
    cid_t read_id = EpochManagerFactory::GetInstance().EnterEpoch(
        thread_id, TimestampType::READ);
    txn = new TransactionContext(thread_id, type, read_id);
  }

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
      settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }

  return txn;
}

void TransactionManager::EndTransaction(TransactionContext *current_txn) {
  // fire all on commit triggers
  if (current_txn->GetResult() == ResultType::SUCCESS) {
    current_txn->ExecOnCommitTriggers();
  }

  if(gc::GCManagerFactory::GetGCType() == GarbageCollectionType::ON) {
    gc::GCManagerFactory::GetInstance().RecycleTransaction(current_txn);
  } else {
    delete current_txn;
  }

  current_txn = nullptr;

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
      settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();

  }
}

// this function checks whether a concurrent transaction is inserting the same
// tuple
// that is to-be-inserted by the current transaction.
bool TransactionManager::IsOccupied(TransactionContext *const current_txn,
                                    const void *position_ptr) {
  ItemPointer &position = *((ItemPointer *)position_ptr);

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(position.block)->GetHeader();
  auto tuple_id = position.offset;

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  // the tuple has already been committed.
  bool activated = (current_txn->GetReadId() >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (current_txn->GetReadId() >= tuple_end_cid);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion/select for update.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      PL_ASSERT(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return true;
    } else if (current_txn->GetRWType(position) == RWType::READ_OWN) {
      // the ownership is from a select-for-update read operation
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // uncommitted version.
        if (tuple_end_cid == INVALID_CID) {
          // dirty delete is invisible
          return false;
        } else {
          // dirty update or insert is visible
          return true;
        }
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }
}

// this function checks whether a version is visible to current transaction.
VisibilityType TransactionManager::IsVisible(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, const VisibilityIdType type) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  oid_t tile_group_id = tile_group_header->GetTileGroup()->GetTileGroupId();

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  cid_t txn_vis_id;

  if (type == VisibilityIdType::READ_ID) {
    txn_vis_id = current_txn->GetReadId();
  } else {
    PL_ASSERT(type == VisibilityIdType::COMMIT_ID);
    txn_vis_id = current_txn->GetCommitId();
  }

  // the tuple has already been committed.
  bool activated = (txn_vis_id >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (txn_vis_id >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    if (activated && !invalidated) {
      // deleted tuple
      return VisibilityType::DELETED;
    } else {
      // aborted tuple
      return VisibilityType::INVISIBLE;
    }
  }

  // there are exactly two versions that can be owned by a transaction,
  // unless it is an insertion/select-for-update
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      PL_ASSERT(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted/updated one.
      return VisibilityType::OK;
    } else if (current_txn->GetRWType(ItemPointer(tile_group_id, tuple_id)) ==
               RWType::READ_OWN) {
      // the ownership is from a select-for-update read operation
      return VisibilityType::OK;
    } else if (tuple_end_cid == INVALID_CID) {
      // tuple being deleted by current txn
      return VisibilityType::DELETED;
    } else {
      // old version of the tuple that is being updated by current txn
      return VisibilityType::INVISIBLE;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // in this protocol, we do not allow cascading abort. so never read an
        // uncommitted version.
        return VisibilityType::INVISIBLE;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return VisibilityType::OK;
        } else {
          return VisibilityType::INVISIBLE;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return VisibilityType::OK;
      } else {
        return VisibilityType::INVISIBLE;
      }
    }
  }
}

}  // namespace concurrency
}  // namespace peloton
