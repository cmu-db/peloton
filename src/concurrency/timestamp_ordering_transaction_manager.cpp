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

#include <cinttypes>
#include "concurrency/timestamp_ordering_transaction_manager.h"

#include "catalog/manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction_context.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager_factory.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace concurrency {

// timestamp ordering requires a spinlock field for protecting the atomic access
// to txn_id field and last_reader_cid field.
common::synchronization::SpinLatch *TimestampOrderingTransactionManager::GetSpinLatchField(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return (common::synchronization::SpinLatch *)
            (tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);
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
    const oid_t &tuple_id, const cid_t &current_cid, const bool is_owner) {
  // get the pointer to the last_reader_cid field.
  cid_t *ts_ptr = (cid_t *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                            LAST_READER_OFFSET);

  GetSpinLatchField(tile_group_header, tuple_id)->Lock();

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if (is_owner == false && tuple_txn_id != INITIAL_TXN_ID) {
    // if the write lock has already been acquired by some concurrent
    // transactions,
    // then return without setting the last_reader_cid.
    GetSpinLatchField(tile_group_header, tuple_id)->Unlock();
    return false;
  } else {
    // if current_cid is larger than the current value of last_reader_cid field,
    // then set last_reader_cid to current_cid.
    if (*ts_ptr < current_cid) {
      *ts_ptr = current_cid;
    }

    GetSpinLatchField(tile_group_header, tuple_id)->Unlock();
    return true;
  }
}

// Initiate reserved area of a tuple
void TimestampOrderingTransactionManager::InitTupleReserved(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t tuple_id) {
  auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

  new ((reserved_area + LOCK_OFFSET)) common::synchronization::SpinLatch();
  *(cid_t *)(reserved_area + LAST_READER_OFFSET) = 0;
}

TimestampOrderingTransactionManager &
TimestampOrderingTransactionManager::GetInstance(
    const ProtocolType protocol, const IsolationLevelType isolation,
    const ConflictAvoidanceType conflict) {
  static TimestampOrderingTransactionManager txn_manager;

  txn_manager.Init(protocol, isolation, conflict);

  return txn_manager;
}

// check whether the current transaction owns the tuple version.
bool TimestampOrderingTransactionManager::IsOwner(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// check whether any other transaction owns the tuple version.
bool TimestampOrderingTransactionManager::IsOwned(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id != current_txn->GetTransactionId() &&
         tuple_txn_id != INITIAL_TXN_ID;
}

// This method tests whether the current transaction has
// created this version of the tuple
//
// this method is designed for select_for_update.
//
// The DBMS can acquire write locks for a transaction in two cases:
// (1) Every time a transaction updates a tuple, the DBMS creates
//     a new version of the tuple and acquire the locks on both
//     the older and the newer version;
// (2) Every time a transaction executes a select_for_update statement,
//     the DBMS needs to acquire the lock on the corresponding version
//     without creating a new version.
// IsWritten() method is designed for distinguishing these two cases.
bool TimestampOrderingTransactionManager::IsWritten(
    UNUSED_ATTRIBUTE TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);

  return tuple_begin_cid == MAX_CID;
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// the version must be the latest version in the version chain.
bool TimestampOrderingTransactionManager::IsOwnable(
    UNUSED_ATTRIBUTE TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool TimestampOrderingTransactionManager::AcquireOwnership(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  // to acquire the ownership,
  // we must guarantee that no transaction that has read
  // the tuple has a larger timestamp than the current transaction.
  GetSpinLatchField(tile_group_header, tuple_id)->Lock();
  // change timestamp
  cid_t last_reader_cid = GetLastReaderCommitId(tile_group_header, tuple_id);

  // must compare last_reader_cid with a transaction's commit_id
  // (rather than read_id).
  // consider a transaction that is executed under snapshot isolation.
  // in this case, commit_id is not equal to read_id.
  if (last_reader_cid > current_txn->GetCommitId()) {
    GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
      GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

      return false;
    } else {
      GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

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
    UNUSED_ATTRIBUTE TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TimestampOrderingTransactionManager::PerformRead(
    TransactionContext *const current_txn, const ItemPointer &read_location,
    bool acquire_ownership) {
  ItemPointer location = read_location;

  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY) {
    // do not update read set for read-only transactions.
    return true;
  }  // end READ ONLY

  //////////////////////////////////////////////////////////
  //// handle SNAPSHOT
  //////////////////////////////////////////////////////////

  // TODO: what if we want to read a version that we write?
  else if (current_txn->GetIsolationLevel() == IsolationLevelType::SNAPSHOT) {
    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    // Check if it's select for update before we check the ownership
    // and modify the last reader cid
    if (acquire_ownership == true) {
      // get the latest version of this tuple.
      location = *(tile_group_header->GetIndirection(location.offset));

      tile_group_id = location.block;
      tuple_id = location.offset;

      tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // Acquire ownership if we haven't
        if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot own
          return false;
        }
        if (AcquireOwnership(current_txn, tile_group_header, tuple_id) ==
            false) {
          // Cannot acquire ownership
          return false;
        }

        // Record RWType::READ_OWN
        current_txn->RecordReadOwn(location);
      }

      // if we have already owned the version.
      PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);

      // Increment table read op stats
      if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
          StatsType::INVALID) {
        stats::BackendStatsContext::GetInstance()->IncrementTableReads(
            location.block);
      }

      return true;

    } else {
      // if it's not select for update, then update read set and return true.

      current_txn->RecordRead(location);

      // Increment table read op stats
      if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
          StatsType::INVALID) {
        stats::BackendStatsContext::GetInstance()->IncrementTableReads(
            location.block);
      }
      return true;
    }

  }  // end SNAPSHOT

  //////////////////////////////////////////////////////////
  //// handle READ_COMMITTED
  //////////////////////////////////////////////////////////
  else if (current_txn->GetIsolationLevel() ==
           IsolationLevelType::READ_COMMITTED) {
    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    // Check if it's select for update before we check the ownership.
    if (acquire_ownership == true) {
      // acquire ownership.
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // Acquire ownership if we haven't
        if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot own
          return false;
        }
        if (AcquireOwnership(current_txn, tile_group_header, tuple_id) ==
            false) {
          // Cannot acquire ownership
          return false;
        }

        // Record RWType::READ_OWN
        current_txn->RecordReadOwn(location);
      }
      // if we have already owned the version.
      PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      // Increment table read op stats
      if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
          StatsType::INVALID) {
        stats::BackendStatsContext::GetInstance()->IncrementTableReads(
            location.block);
      }
      return true;

    } else {
      // a transaction can never read an uncommitted version.
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        if (IsOwned(current_txn, tile_group_header, tuple_id) == false) {
          current_txn->RecordRead(location);

          // Increment table read op stats
          if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode))
              != StatsType::INVALID) {
            stats::BackendStatsContext::GetInstance()->IncrementTableReads(
                location.block);
          }
          return true;

        } else {
          // if the tuple has been owned by some concurrent transactions,
          // then read fails.
          LOG_TRACE("Transaction read failed");
          return false;
        }

      } else {
        // this version must already be in the read/write set.
        // so no need to update read set.
        // current_txn->RecordRead(location);

        // Increment table read op stats
        if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode))
            != StatsType::INVALID) {
          stats::BackendStatsContext::GetInstance()->IncrementTableReads(
              location.block);
        }
        return true;
      }
    }

  }  // end READ_COMMITTED

  //////////////////////////////////////////////////////////
  //// handle SERIALIZABLE and REPEATABLE_READS
  //////////////////////////////////////////////////////////
  else {
    PL_ASSERT(current_txn->GetIsolationLevel() ==
                  IsolationLevelType::SERIALIZABLE ||
              current_txn->GetIsolationLevel() ==
                  IsolationLevelType::REPEATABLE_READS);

    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    // Check if it's select for update before we check the ownership
    // and modify the last reader cid.
    if (acquire_ownership == true) {
      // acquire ownership.
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // Acquire ownership if we haven't
        if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot own
          return false;
        }
        if (AcquireOwnership(current_txn, tile_group_header, tuple_id) ==
            false) {
          // Cannot acquire ownership
          return false;
        }

        // Record RWType::READ_OWN
        current_txn->RecordReadOwn(location);

        // now we have already obtained the ownership.
        // then attempt to set last reader cid.
        UNUSED_ATTRIBUTE bool ret = SetLastReaderCommitId(
            tile_group_header, tuple_id, current_txn->GetCommitId(), true);

        PL_ASSERT(ret == true);
        // there's no need to maintain read set for timestamp ordering protocol.
        // T/O does not check the read set during commit phase.
      }

      // if we have already owned the version.
      PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) ==
                    current_txn->GetCommitId() ||
                GetLastReaderCommitId(tile_group_header, tuple_id) == 0);
      // Increment table read op stats
      if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
          StatsType::INVALID) {
        stats::BackendStatsContext::GetInstance()->IncrementTableReads(
            location.block);
      }
      return true;

    } else {
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // if the current transaction does not own this tuple,
        // then attempt to set last reader cid.
        if (SetLastReaderCommitId(tile_group_header, tuple_id,
                                  current_txn->GetCommitId(), false) == true) {
          // update read set.
          current_txn->RecordRead(location);

          // Increment table read op stats
          if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode))
              != StatsType::INVALID) {
            stats::BackendStatsContext::GetInstance()->IncrementTableReads(
                location.block);
          }
          return true;
        } else {
          // if the tuple has been owned by some concurrent transactions,
          // then read fails.
          LOG_TRACE("Transaction read failed");
          return false;
        }

      } else {
        // if the current transaction has already owned this tuple,
        // then perform read directly.
        PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) ==
                      current_txn->GetCommitId() ||
                  GetLastReaderCommitId(tile_group_header, tuple_id) == 0);

        // this version must already be in the read/write set.
        // so no need to update read set.
        // current_txn->RecordRead(location);

        // Increment table read op stats
        if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode))
            != StatsType::INVALID) {
          stats::BackendStatsContext::GetInstance()->IncrementTableReads(
              location.block);
        }
        return true;
      }
    }

  }  // end SERIALIZABLE || REPEATABLE_READS
}

void TimestampOrderingTransactionManager::PerformInsert(
    TransactionContext *const current_txn, const ItemPointer &location,
    ItemPointer *index_entry_ptr) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

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
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableInserts(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    TransactionContext *const current_txn, const ItemPointer &location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

  ItemPointer old_location = location;

  LOG_TRACE("Performing Update old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Update new tuple %u %u", new_location.block,
            new_location.offset);

  auto &manager = catalog::Manager::GetInstance();

  auto tile_group_header =
      manager.GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header =
      manager.GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(tile_group_header->GetPrevItemPointer(old_location.offset)
                .IsNull() == true);

  // check whether the new version is empty.
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.

  // Set double linked list
  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // we must be updating the latest version.
  // Set the header information for the new version
  ItemPointer *index_entry_ptr =
      tile_group_header->GetIndirection(old_location.offset);

  // if there's no primary index on a table, then index_entry_ptr == nullptr.
  if (index_entry_ptr != nullptr) {
    new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);

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

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);

  // Increment table update op stats
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        new_location.block);
  }
}

// NOTE: this function is deprecated.
void TimestampOrderingTransactionManager::PerformUpdate(
    TransactionContext *const current_txn UNUSED_ATTRIBUTE,
    const ItemPointer &location) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

  oid_t tile_group_id = location.block;
  UNUSED_ATTRIBUTE oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  UNUSED_ATTRIBUTE auto tile_group_header =
      manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // no need to add the older version into the update set.
  // if there exists older version, then the older version must already
  // been added to the update set.
  // if there does not exist an older version, then it means that the
  // transaction
  // is updating a version that is installed by itself.
  // in this case, nothing needs to be performed.

  // Increment table update op stats
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    TransactionContext *const current_txn, const ItemPointer &location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

  ItemPointer old_location = location;

  LOG_TRACE("Performing Delete old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Delete new tuple %u %u", new_location.block,
            new_location.offset);

  auto &manager = catalog::Manager::GetInstance();

  auto tile_group_header =
      manager.GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header =
      manager.GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  PL_ASSERT(GetLastReaderCommitId(tile_group_header, old_location.offset) ==
            current_txn->GetCommitId());

  // if we can perform delete, then we must have already locked the older
  // version.
  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  // we must be deleting the latest version.
  PL_ASSERT(tile_group_header->GetPrevItemPointer(old_location.offset)
                .IsNull() == true);

  // check whether the new version is empty.
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // Set up double linked list
  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // we must be deleting the latest version.
  // Set the header information for the new version
  ItemPointer *index_entry_ptr =
      tile_group_header->GetIndirection(old_location.offset);

  // if there's no primary index on a table, then index_entry_ptr == nullptr.
  if (index_entry_ptr != nullptr) {
    new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);

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

  current_txn->RecordDelete(old_location);

  // Increment table delete op stats
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        old_location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    TransactionContext *const current_txn, const ItemPointer &location) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

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
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        location.block);
  }
}

ResultType TimestampOrderingTransactionManager::CommitTransaction(
    TransactionContext *const current_txn) {
  LOG_TRACE("Committing peloton txn : %" PRId64, current_txn->GetTransactionId());

  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY) {
    EndTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  //////////////////////////////////////////////////////////
  //// handle other isolation levels
  //////////////////////////////////////////////////////////

  auto &manager = catalog::Manager::GetInstance();
  auto &log_manager = logging::LogManager::GetInstance();

  log_manager.StartLogging();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetCommitId();

  auto &rw_set = current_txn->GetReadWriteSet();
  auto &rw_object_set = current_txn->GetCreateDropSet();

  auto gc_set = current_txn->GetGCSetPtr();
  auto gc_object_set = current_txn->GetGCObjectSetPtr();

  for (auto &obj : rw_object_set) {
    auto ddl_type = std::get<3>(obj);
    if (ddl_type == DDLType::CREATE) continue;
    oid_t database_oid = std::get<0>(obj);
    oid_t table_oid = std::get<1>(obj);
    oid_t index_oid = std::get<2>(obj);
    gc_object_set->emplace_back(database_oid, table_oid, index_oid);
  }

  oid_t database_id = 0;
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
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
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;

      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_header, tuple_slot);
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

        // add old version into gc set.
        // may need to delete versions from secondary indexes.
        gc_set->operator[](tile_group_id)[tuple_slot] =
            GCVersionType::COMMIT_UPDATE;

        log_manager.LogUpdate(new_version);

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
        // the gc should be responsible for recycling the newer empty version.
        gc_set->operator[](tile_group_id)[tuple_slot] =
            GCVersionType::COMMIT_DELETE;

        log_manager.LogDelete(ItemPointer(tile_group_id, tuple_slot));

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

        log_manager.LogInsert(ItemPointer(tile_group_id, tuple_slot));

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
        gc_set->operator[](tile_group_id)[tuple_slot] =
            GCVersionType::COMMIT_INS_DEL;

        // no log is needed for this case
      }
    }
  }

  ResultType result = current_txn->GetResult();

  log_manager.LogEnd();

  EndTransaction(current_txn);

  // Increment # txns committed metric
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnCommitted(
        database_id);
  }

  return result;
}

ResultType TimestampOrderingTransactionManager::AbortTransaction(
    TransactionContext *const current_txn) {
  // a pre-declared read-only transaction will never abort.
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

  LOG_TRACE("Aborting peloton txn : %" PRId64, current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetReadWriteSet();
  auto &rw_object_set = current_txn->GetCreateDropSet();

  auto gc_set = current_txn->GetGCSetPtr();
  auto gc_object_set = current_txn->GetGCObjectSetPtr();

  for (int i = rw_object_set.size() - 1; i >= 0; i--) {
    auto &obj = rw_object_set[i];
    auto ddl_type = std::get<3>(obj);
    if (ddl_type == DDLType::DROP) continue;
    oid_t database_oid = std::get<0>(obj);
    oid_t table_oid = std::get<1>(obj);
    oid_t index_oid = std::get<2>(obj);
    gc_object_set->emplace_back(database_oid, table_oid, index_oid);
  }

  oid_t database_id = 0;
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_header, tuple_slot);
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

        // this must be the latest version of a version chain.
        PL_ASSERT(new_tile_group_header->GetPrevItemPointer(new_version.offset)
                      .IsNull() == true);

        PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
        // if we updated the latest version.
        // We must first adjust the head pointer
        // before we unlink the aborted version from version list
        ItemPointer *index_entry_ptr =
            tile_group_header->GetIndirection(tuple_slot);
        UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
            index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
        PL_ASSERT(res == true);
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        tile_group_header->SetPrevItemPointer(tuple_slot, INVALID_ITEMPOINTER);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add the version to gc set.
        // this version has already been unlinked from the version chain.
        // however, the gc should further unlink it from indexes.
        gc_set->operator[](new_version.block)[new_version.offset] =
            GCVersionType::ABORT_UPDATE;

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

        // this must be the latest version of a version chain.
        PL_ASSERT(new_tile_group_header->GetPrevItemPointer(new_version.offset)
                      .IsNull() == true);

        // if we updated the latest version.
        // We must first adjust the head pointer
        // before we unlink the aborted version from version list
        ItemPointer *index_entry_ptr =
            tile_group_header->GetIndirection(tuple_slot);
        UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
            index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
        PL_ASSERT(res == true);
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        tile_group_header->SetPrevItemPointer(tuple_slot, INVALID_ITEMPOINTER);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add the version to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] =
            GCVersionType::ABORT_DELETE;

      } else if (tuple_entry.second == RWType::INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add the version to gc set.
        // delete from index.
        gc_set->operator[](tile_group_id)[tuple_slot] =
            GCVersionType::ABORT_INSERT;

      } else if (tuple_entry.second == RWType::INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] =
            GCVersionType::ABORT_INS_DEL;
      }
    }
  }

  current_txn->SetResult(ResultType::ABORTED);
  EndTransaction(current_txn);

  // Increment # txns aborted metric
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(settings::SettingId::stats_mode)) !=
      StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnAborted(database_id);
  }

  return ResultType::ABORTED;
}

}  // namespace storage
}  // namespace peloton