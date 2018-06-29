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
#include <cinttypes>
#include "storage/storage_manager.h"

#include "catalog/catalog_defaults.h"
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

bool TimestampOrderingTransactionManager::SetLastReaderCommitId(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, const cid_t &current_cid, const bool is_owner) {
  // get the pointer to the last_reader_cid field.
  cid_t read_ts = tile_group_header->GetLastReaderCommitId(tuple_id);

  auto &latch = tile_group_header->GetSpinLatch(tuple_id);

  latch.Lock();

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if (is_owner == false && tuple_txn_id != INITIAL_TXN_ID) {
    // if the write lock has already been acquired by some concurrent
    // transactions,
    // then return without setting the last_reader_cid.
    latch.Unlock();
    return false;
  } else {
    // if current_cid is larger than the current value of last_reader_cid field,
    // then set last_reader_cid to current_cid.
    if (read_ts < current_cid) {
      tile_group_header->SetLastReaderCommitId(tuple_id, current_cid);
    }

    latch.Unlock();
    return true;
  }
}

TimestampOrderingTransactionManager &
TimestampOrderingTransactionManager::GetInstance(
    const ProtocolType protocol, const IsolationLevelType isolation,
    const ConflictAvoidanceType conflict) {
  static TimestampOrderingTransactionManager txn_manager;

  txn_manager.Init(protocol, isolation, conflict);

  return txn_manager;
}

bool TimestampOrderingTransactionManager::IsOwner(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

bool TimestampOrderingTransactionManager::IsOwned(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id != current_txn->GetTransactionId() &&
         tuple_txn_id != INITIAL_TXN_ID;
}

bool TimestampOrderingTransactionManager::IsWritten(
    UNUSED_ATTRIBUTE TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);

  return tuple_begin_cid == MAX_CID;
}

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
  auto &latch = tile_group_header->GetSpinLatch(tuple_id);
  latch.Lock();
  // change timestamp
  cid_t last_reader_cid = tile_group_header->GetLastReaderCommitId(tuple_id);

  // must compare last_reader_cid with a transaction's commit_id
  // (rather than read_id).
  // consider a transaction that is executed under snapshot isolation.
  // in this case, commit_id is not equal to read_id.
  if (last_reader_cid > current_txn->GetCommitId()) {
    latch.Unlock();

    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
      latch.Unlock();

      return false;
    } else {
      latch.Unlock();

      return true;
    }
  }
}

void TimestampOrderingTransactionManager::YieldOwnership(
    UNUSED_ATTRIBUTE TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  PELOTON_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TimestampOrderingTransactionManager::PerformRead(TransactionContext *const current_txn,
                                                      const ItemPointer &read_location,
                                                      storage::TileGroupHeader *tile_group_header,
                                                      bool acquire_ownership) {
  ItemPointer location = read_location;

  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->IsReadOnly()) {
    // do not update read set for read-only transactions.
    return true;
  }  // end READ ONLY

  //////////////////////////////////////////////////////////
  //// handle SNAPSHOT
  //////////////////////////////////////////////////////////

  // TODO: what if we want to read a version that we write?
  else if (current_txn->GetIsolationLevel() == IsolationLevelType::SNAPSHOT) {
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);

    // Check if it's select for update before we check the ownership
    // and modify the last reader cid
    if (acquire_ownership == true) {
      // get the latest version of this tuple.
      location = *(tile_group_header->GetIndirection(location.offset));

      tuple_id = location.offset;

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
      PELOTON_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      return true;

    } else {
      // if it's not select for update, then return true.
      return true;
    }

  }  // end SNAPSHOT

  //////////////////////////////////////////////////////////
  //// handle READ_COMMITTED
  //////////////////////////////////////////////////////////
  else if (current_txn->GetIsolationLevel() ==
           IsolationLevelType::READ_COMMITTED) {
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);

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
      PELOTON_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      return true;

    } else {
      // a transaction can never read an uncommitted version.
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        if (IsOwned(current_txn, tile_group_header, tuple_id) == false) {
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
        return true;
      }
    }

  }  // end READ_COMMITTED

  //////////////////////////////////////////////////////////
  //// handle SERIALIZABLE and REPEATABLE_READS
  //////////////////////////////////////////////////////////
  else {
    PELOTON_ASSERT(current_txn->GetIsolationLevel() ==
                       IsolationLevelType::SERIALIZABLE ||
                   current_txn->GetIsolationLevel() ==
                       IsolationLevelType::REPEATABLE_READS);

    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
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

        PELOTON_ASSERT(ret == true);
        // there's no need to maintain read set for timestamp ordering protocol.
        // T/O does not check the read set during commit phase.
      }

      // if we have already owned the version.
      PELOTON_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      PELOTON_ASSERT(tile_group_header->GetLastReaderCommitId(tuple_id) ==
                         current_txn->GetCommitId() ||
                     tile_group_header->GetLastReaderCommitId(tuple_id) == 0);
      return true;

    } else {
      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // if the current transaction does not own this tuple,
        // then attempt to set last reader cid.
        if (SetLastReaderCommitId(tile_group_header, tuple_id,
                                  current_txn->GetCommitId(), false) == true) {
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
        PELOTON_ASSERT(tile_group_header->GetLastReaderCommitId(tuple_id) ==
                           current_txn->GetCommitId() ||
                       tile_group_header->GetLastReaderCommitId(tuple_id) == 0);

        // this version must already be in the read/write set.
        // so no need to update read set.
        return true;
      }
    }

  }  // end SERIALIZABLE || REPEATABLE_READS
}

void TimestampOrderingTransactionManager::PerformInsert(
    TransactionContext *const current_txn, const ItemPointer &location,
    ItemPointer *index_entry_ptr) {
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_group_header = storage_manager->GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // check MVCC info
  // the tuple slot must be empty.
  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
                 INVALID_TXN_ID);
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetLastReaderCommitId(tuple_id,
                                           current_txn->GetCommitId());

  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);
}

void TimestampOrderingTransactionManager::PerformUpdate(
    TransactionContext *const current_txn, const ItemPointer &location,
    const ItemPointer &new_location) {
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  ItemPointer old_location = location;

  LOG_TRACE("Performing Update old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Update new tuple %u %u", new_location.block,
            new_location.offset);

  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_group_header =
      storage_manager->GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header =
      storage_manager->GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  PELOTON_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
                 transaction_id);
  PELOTON_ASSERT(
      tile_group_header->GetPrevItemPointer(old_location.offset).IsNull() ==
      true);

  // check whether the new version is empty.
  PELOTON_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
                 INVALID_TXN_ID);
  PELOTON_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
                 MAX_CID);
  PELOTON_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
                 MAX_CID);

  // if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.

  // Set double linked list
  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetLastReaderCommitId(new_location.offset,
                                               current_txn->GetCommitId());

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

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
    PELOTON_ASSERT(res == true);
  }

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);
}

void TimestampOrderingTransactionManager::PerformUpdate(
    TransactionContext *const current_txn UNUSED_ATTRIBUTE,
    const ItemPointer &location) {
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  oid_t tile_group_id = location.block;
  UNUSED_ATTRIBUTE oid_t tuple_id = location.offset;

  auto storage_manager = storage::StorageManager::GetInstance();
  UNUSED_ATTRIBUTE auto tile_group_header =
      storage_manager->GetTileGroup(tile_group_id)->GetHeader();

  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
                 current_txn->GetTransactionId());
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // no need to add the older version into the update set.
  // if there exists older version, then the older version must already
  // been added to the update set.
  // if there does not exist an older version, then it means that the
  // transaction
  // is updating a version that is installed by itself.
  // in this case, nothing needs to be performed.
}

void TimestampOrderingTransactionManager::PerformDelete(
    TransactionContext *const current_txn, const ItemPointer &location,
    const ItemPointer &new_location) {
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  ItemPointer old_location = location;

  LOG_TRACE("Performing Delete old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Delete new tuple %u %u", new_location.block,
            new_location.offset);

  auto storage_manager = storage::StorageManager::GetInstance();

  auto tile_group_header =
      storage_manager->GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header =
      storage_manager->GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  PELOTON_ASSERT(tile_group_header->GetLastReaderCommitId(
                     old_location.offset) == current_txn->GetCommitId());

  // if we can perform delete, then we must have already locked the older
  // version.
  PELOTON_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
                 transaction_id);
  // we must be deleting the latest version.
  PELOTON_ASSERT(
      tile_group_header->GetPrevItemPointer(old_location.offset).IsNull() ==
      true);

  // check whether the new version is empty.
  PELOTON_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
                 INVALID_TXN_ID);
  PELOTON_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
                 MAX_CID);
  PELOTON_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
                 MAX_CID);

  // Set up double linked list
  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetLastReaderCommitId(new_location.offset,
                                               current_txn->GetCommitId());

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

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
    PELOTON_ASSERT(res == true);
  }

  current_txn->RecordDelete(old_location);
}

void TimestampOrderingTransactionManager::PerformDelete(
    TransactionContext *const current_txn, const ItemPointer &location) {
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_group_header = storage_manager->GetTileGroup(tile_group_id)->GetHeader();

  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
                 current_txn->GetTransactionId());
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

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
}

ResultType TimestampOrderingTransactionManager::CommitTransaction(
    TransactionContext *const current_txn) {
  LOG_TRACE("Committing peloton txn : %" PRId64,
            current_txn->GetTransactionId());

  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->IsReadOnly()) {
    EndTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  //////////////////////////////////////////////////////////
  //// handle other isolation levels
  //////////////////////////////////////////////////////////

  auto storage_manager = storage::StorageManager::GetInstance();
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

  // install everything.
  // 1. install a new version for update operations;
  // 2. install an empty version for delete operations;
  // 3. install a new tuple for insert operations.
  // Iterate through each item pointer in the read write set

  oid_t last_tile_group_id = INVALID_OID;
  storage::TileGroupHeader *tile_group_header = nullptr;

  for (const auto &tuple_entry : rw_set) {
    ItemPointer item_ptr = tuple_entry.first;
    oid_t tile_group_id = item_ptr.block;
    oid_t tuple_slot = item_ptr.offset;

    if (tile_group_id != last_tile_group_id) {
      tile_group_header =
          storage_manager->GetTileGroup(tile_group_id)->GetHeader();
      last_tile_group_id = tile_group_id;
    }

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

      PELOTON_ASSERT(new_version.IsNull() == false);

      auto cid = tile_group_header->GetEndCommitId(tuple_slot);
      PELOTON_ASSERT(cid > end_commit_id);
      auto new_tile_group_header =
          storage_manager->GetTileGroup(new_version.block)->GetHeader();
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
      PELOTON_ASSERT(cid > end_commit_id);
      auto new_tile_group_header =
          storage_manager->GetTileGroup(new_version.block)->GetHeader();
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
      PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
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
      PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
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

  ResultType result = current_txn->GetResult();

  log_manager.LogEnd();

  EndTransaction(current_txn);

  return result;
}

ResultType TimestampOrderingTransactionManager::AbortTransaction(
    TransactionContext *const current_txn) {
  // a pre-declared read-only transaction will never abort.
  PELOTON_ASSERT(!current_txn->IsReadOnly());

  LOG_TRACE("Aborting peloton txn : %" PRId64, current_txn->GetTransactionId());
  auto storage_manager = storage::StorageManager::GetInstance();

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

  // Iterate through each item pointer in the read write set

  oid_t last_tile_group_id = INVALID_OID;
  storage::TileGroupHeader *tile_group_header = nullptr;

  for (const auto &tuple_entry : rw_set) {
    ItemPointer item_ptr = tuple_entry.first;
    oid_t tile_group_id = item_ptr.block;
    oid_t tuple_slot = item_ptr.offset;

    if (tile_group_id != last_tile_group_id) {
      tile_group_header =
          storage_manager->GetTileGroup(tile_group_id)->GetHeader();
      last_tile_group_id = tile_group_id;
    }

    if (tuple_entry.second == RWType::READ_OWN) {
      // A read operation has acquired ownership but hasn't done any further
      // update/delete yet
      // Yield the ownership
      YieldOwnership(current_txn, tile_group_header, tuple_slot);
    } else if (tuple_entry.second == RWType::UPDATE) {
      ItemPointer new_version =
          tile_group_header->GetPrevItemPointer(tuple_slot);
      auto new_tile_group_header =
          storage_manager->GetTileGroup(new_version.block)->GetHeader();
      // these two fields can be set at any time.
      new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
      new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

      COMPILER_MEMORY_FENCE;

      // as the aborted version has already been placed in the version chain,
      // we need to unlink it by resetting the item pointers.

      // this must be the latest version of a version chain.
      PELOTON_ASSERT(
          new_tile_group_header->GetPrevItemPointer(new_version.offset)
              .IsNull() == true);

      PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
      // if we updated the latest version.
      // We must first adjust the head pointer
      // before we unlink the aborted version from version list
      ItemPointer *index_entry_ptr =
          tile_group_header->GetIndirection(tuple_slot);
      UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
          index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
      PELOTON_ASSERT(res == true);
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
          storage_manager->GetTileGroup(new_version.block)->GetHeader();

      new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
      new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

      COMPILER_MEMORY_FENCE;

      // as the aborted version has already been placed in the version chain,
      // we need to unlink it by resetting the item pointers.

      // this must be the latest version of a version chain.
      PELOTON_ASSERT(
          new_tile_group_header->GetPrevItemPointer(new_version.offset)
              .IsNull() == true);

      // if we updated the latest version.
      // We must first adjust the head pointer
      // before we unlink the aborted version from version list
      ItemPointer *index_entry_ptr =
          tile_group_header->GetIndirection(tuple_slot);
      UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
          index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
      PELOTON_ASSERT(res == true);
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

  current_txn->SetResult(ResultType::ABORTED);
  EndTransaction(current_txn);

  return ResultType::ABORTED;
}

}  // namespace concurrency
}  // namespace peloton
