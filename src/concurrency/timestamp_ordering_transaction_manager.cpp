//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager.cpp
//
// Identification: src/concurrency/timestamp_ordering_transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/timestamp_ordering_transaction_manager.h"

#include "catalog/manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager.h"
#include "logging/records/transaction_record.h"

namespace peloton {
namespace concurrency {

// timestamp ordering requires a spinlock field for protecting the atomic access
// to txn_id field and last_reader_cid field.
Spinlock *TimestampOrderingTransactionManager::GetSpinlockField(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                      LOCK_OFFSET);
}

// in timestamp ordering, the last_reader_cid records the timestamp of the last
// transaction
// that reads the tuple.
cid_t TimestampOrderingTransactionManager::GetLastReaderCommitId(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return *(cid_t *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                    LAST_READER_OFFSET);
}

bool TimestampOrderingTransactionManager::SetLastReaderCommitId(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, const cid_t &current_cid) {
  // get the pointer to the last_reader_cid field.
  cid_t *ts_ptr = (cid_t *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                            LAST_READER_OFFSET);

  // cid_t current_cid = current_txn->GetBeginCommitId();

  GetSpinlockField(tile_group_header, tuple_id)->Lock();

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if (tuple_txn_id != INITIAL_TXN_ID) {
    // if the write lock has already been acquired by some concurrent
    // transactions,
    // then return without setting the last_reader_cid.
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return false;
  } else {
    // if current_cid is larger than the current value of last_reader_cid field,
    // then set last_reader_cid to current_cid.
    if (*ts_ptr < current_cid) {
      *ts_ptr = current_cid;
    }

    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return true;
  }
}

// Initiate reserved area of a tuple
void TimestampOrderingTransactionManager::InitTupleReserved(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t tuple_id) {
  auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

  new ((reserved_area + LOCK_OFFSET)) Spinlock();
  *(cid_t *)(reserved_area + LAST_READER_OFFSET) = 0;
}

Transaction *TimestampOrderingTransactionManager::BeginTransaction(const size_t thread_id) {

  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.PrepareLogging();

  Transaction *txn = nullptr;

  // transaction processing with centralized epoch manager
  cid_t begin_cid = EpochManagerFactory::GetInstance().EnterEpoch(thread_id);
  txn = new Transaction(begin_cid, thread_id);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }

  return txn;
}

Transaction *TimestampOrderingTransactionManager::BeginReadonlyTransaction(const size_t thread_id) {
  Transaction *txn = nullptr;

  // transaction processing with centralized epoch manager
  cid_t begin_cid = EpochManagerFactory::GetInstance().EnterEpochRO(thread_id);
  txn = new Transaction(begin_cid, thread_id, true);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }

  return txn;
}

void TimestampOrderingTransactionManager::EndTransaction(
    Transaction *current_txn) {
  
  EpochManagerFactory::GetInstance().ExitEpoch(
    current_txn->GetThreadId(), 
    current_txn->GetBeginCommitId());


  // logging logic
  auto &log_manager = logging::LogManager::GetInstance();

  if (current_txn->GetResult() == ResultType::SUCCESS) {
    if (current_txn->IsGCSetEmpty() != true) {
      gc::GCManagerFactory::GetInstance().
          RecycleTransaction(current_txn->GetGCSetPtr(), current_txn->GetBeginCommitId());
    }
    // Log the transaction's commit
    // For time stamp ordering, every transaction only has one timestamp
    log_manager.LogCommitTransaction(current_txn->GetBeginCommitId());
  } else {
    if (current_txn->IsGCSetEmpty() != true) {
      gc::GCManagerFactory::GetInstance().
          RecycleTransaction(current_txn->GetGCSetPtr(), GetNextCommitId());
    }
    log_manager.DoneLogging();
  }

  delete current_txn;
  current_txn = nullptr;
  
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();
  }
}

void TimestampOrderingTransactionManager::EndReadonlyTransaction(
    Transaction *current_txn) {

  PL_ASSERT(current_txn->IsDeclaredReadOnly() == true);
  
  EpochManagerFactory::GetInstance().ExitEpoch(
    current_txn->GetThreadId(), 
    current_txn->GetBeginCommitId());
  
  delete current_txn;
  current_txn = nullptr;
  
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();
  }
}

TimestampOrderingTransactionManager &
TimestampOrderingTransactionManager::GetInstance() {
  static TimestampOrderingTransactionManager txn_manager;
  return txn_manager;
}

// this function checks whether a concurrent transaction is inserting the same
// tuple
// that is to-be-inserted by the current transaction.
bool TimestampOrderingTransactionManager::IsOccupied(
    Transaction *const current_txn, const void *position_ptr) {
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
  bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

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
VisibilityType TimestampOrderingTransactionManager::IsVisible(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  oid_t tile_group_id = tile_group_header->GetTileGroup()->GetTileGroupId();

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  // the tuple has already been committed.
  bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID || CidIsInDirtyRange(tuple_begin_cid)) {
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

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool TimestampOrderingTransactionManager::IsOwner(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// This method tests whether the current transaction has created this version of
// the tuple
bool TimestampOrderingTransactionManager::IsWritten(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return IsOwner(current_txn, tile_group_header, tuple_id) &&
         tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID;
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TimestampOrderingTransactionManager::IsOwnable(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID &&
         tuple_end_cid > current_txn->GetBeginCommitId();
}

bool TimestampOrderingTransactionManager::AcquireOwnership(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  // to acquire the ownership, we must guarantee that no other transactions that
  // has read
  // the tuple has a larger timestamp than the current transaction.
  GetSpinlockField(tile_group_header, tuple_id)->Lock();
  // change timestamp
  cid_t last_reader_cid = GetLastReaderCommitId(tile_group_header, tuple_id);

  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();

    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();

      return false;
    } else {
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();

      return true;
    }
  }
}

// release write lock on a tuple.
// one example usage of this method is when a tuple is acquired, but operation
// (insert,update,delete) can't proceed, the executor needs to yield the
// ownership before return false to upper layer.
// It should not be called if the tuple is in the write set as commit and abort
// will release the write lock anyway.
void TimestampOrderingTransactionManager::YieldOwnership(
    UNUSED_ATTRIBUTE Transaction *const current_txn, const oid_t &tile_group_id,
    const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TimestampOrderingTransactionManager::PerformRead(
    Transaction *const current_txn, const ItemPointer &location,
    bool acquire_ownership) {
  if (current_txn->IsDeclaredReadOnly() == true) {
    // Ignore read validation for all readonly transactions
    return true;
  }

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  // Check if it's select for update before we check the ownership and modify
  // the
  // last reader tid
  if (acquire_ownership == true &&
      IsOwner(current_txn, tile_group_header, tuple_id) == false) {
    // Acquire ownership if we haven't
    if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
      // Can not own
      return false;
    }
    if (AcquireOwnership(current_txn, tile_group_header, tuple_id) == false) {
      // Can not acquire ownership
      return false;
    }
    // Promote to RWType::READ_OWN
    current_txn->RecordReadOwn(location);
  }

  // if the current transaction has already owned this tuple, then perform read
  // directly.
  if (IsOwner(current_txn, tile_group_header, tuple_id) == true) {
    PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) <=
              current_txn->GetBeginCommitId());
    // Increment table read op stats
    if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementTableReads(
          location.block);
    }
    return true;
  }
  // if the current transaction does not own this tuple, then attemp to set last
  // reader cid.
  if (SetLastReaderCommitId(tile_group_header, tuple_id,
                            current_txn->GetBeginCommitId()) == true) {
    current_txn->RecordRead(location);
    // Increment table read op stats
    if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementTableReads(
          location.block);
    }
    return true;
  } else {
    // if the tuple has been owned by some concurrent transactions, then read
    // fails.
    LOG_TRACE("Transaction read failed");
    return false;
  }
}

void TimestampOrderingTransactionManager::PerformInsert(
    Transaction *const current_txn, const ItemPointer &location,
    ItemPointer *index_entry_ptr) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // check MVCC info
  // the tuple slot must be empty.
  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);

  InitTupleReserved(tile_group_header, tuple_id);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

  // Increment table insert op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableInserts(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    Transaction *const current_txn, const ItemPointer &old_location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Performing Write old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Write new tuple %u %u", new_location.block,
            new_location.offset);

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.
  // Set double linked list
  // old_prev is the version next (newer) to the old version.

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    // once everything is set, we can allow traversing the new version.
    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not updating the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are updating the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    if (index_entry_ptr != nullptr) {

      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-done
      // pointer.
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        new_location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    Transaction *const current_txn, const ItemPointer &location) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    Transaction *const current_txn, const ItemPointer &old_location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Performing Delete");

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  PL_ASSERT(GetLastReaderCommitId(tile_group_header, old_location.offset) <=
            current_txn->GetBeginCommitId());

  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // Set up double linked list

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not deleting the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are deleting the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    // if there's no primary index on a table, then index_entry_ptry == nullptr.
    if (index_entry_ptr != nullptr) {
      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-down
      // pointer
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  current_txn->RecordDelete(old_location);

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        old_location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    Transaction *const current_txn, const ItemPointer &location) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        location.block);
  }
}

ResultType TimestampOrderingTransactionManager::CommitTransaction(
    Transaction *const current_txn) {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsDeclaredReadOnly() == true) {
    EndReadonlyTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  auto &manager = catalog::Manager::GetInstance();
  auto &log_manager = logging::LogManager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();
  log_manager.LogBeginTransaction(end_commit_id);

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  // install everything.
  // 1. install a new version for update operations;
  // 2. install an empty version for delete operations;
  // 3. install a new tuple for insert operations.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_id, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        PL_ASSERT(new_version.IsNull() == false);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = false;

        // add to log manager
        log_manager.LogUpdate(
            end_commit_id, ItemPointer(tile_group_id, tuple_slot), new_version);

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        // we need to recycle both old and new versions.
        // we require the GC to delete tuple from index only once.
        // recycle old version, delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
        // recycle new version (which is an empty version), do not delete from index
        gc_set->operator[](new_version.block)[new_version.offset] = false;

        // add to log manager
        log_manager.LogDelete(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INSERT) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // nothing to be added to gc set.

        // add to log manager
        log_manager.LogInsert(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INS_DEL) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());

        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

        // no log is needed for this case
      }
    }
  }

  ResultType result = current_txn->GetResult();

  EndTransaction(current_txn);

  // Increment # txns committed metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnCommitted(
        database_id);
  }

  return result;
}

ResultType TimestampOrderingTransactionManager::AbortTransaction(
    Transaction *const current_txn) {
  // It's impossible that a pre-declared readonly transaction aborts
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_id, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        // these two fields can be set at any time.
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
          tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);
        } else {
          tile_group_header->SetPrevItemPointer(tuple_slot,
                                                INVALID_ITEMPOINTER);
        }

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
        }

        tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        // delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

      } else if (tuple_entry.second == RWType::INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
      }
    }
  }

  current_txn->SetResult(ResultType::ABORTED);
  EndTransaction(current_txn);

  // Increment # txns aborted metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnAborted(database_id);
  }

  return ResultType::ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
