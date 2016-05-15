//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eager_write_txn_manager.cpp
//
// Identification: src/backend/concurrency/eager_write_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "eager_write_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace concurrency {

thread_local EagerWriteTxnContext *current_txn_ctx;

EagerWriteTxnManager &EagerWriteTxnManager::GetInstance() {
  static EagerWriteTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
// check whether a tuple is visible to current transaction.
// in this protocol, we require that a transaction cannot see other
// transaction's local copy.
bool EagerWriteTxnManager::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // currently, we do not handle cascading abort. so never read an
        // uncommitted version.
        return false;
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

void EagerWriteTxnManager::RemoveReader() {
  LOG_TRACE("release all reader lock");

  // Remove from the read list of accessed tuples
  auto txn = current_txn;
  auto &rw_set = txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tile_group_id);
    if (tile_group == nullptr) continue;

    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;

      // we don't have reader lock on insert
      if (tuple_entry.second == RW_TYPE_INSERT ||
          tuple_entry.second == RW_TYPE_INS_DEL) {
        continue;
      }

      RemoveReader(tile_group_header, tuple_slot, txn->GetTransactionId());
    }
  }
  LOG_TRACE("release EWreader finish");
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool EagerWriteTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool EagerWriteTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  LOG_TRACE("IsOwnable txnid: %lx end_cid: %lx", tuple_txn_id, tuple_end_cid);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool EagerWriteTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  LOG_TRACE("AcquireOwnership");
  assert(IsOwner(tile_group_header, tuple_id) == false);

  // Try to get write lock
  auto current_tid = current_txn->GetTransactionId();
  auto old_tid = INITIAL_TXN_ID;

  GetEwReaderLock(tile_group_header, tuple_id);

  old_tid =
      tile_group_header->SetAtomicTransactionId(tuple_id, old_tid, current_tid);

  // Check if we successfully get the write lock
  bool res = (old_tid == INITIAL_TXN_ID);
  if (!res) {
    LOG_TRACE("Fail to acquire write lock. Set txn failure.");
    ReleaseEwReaderLock(tile_group_header, tuple_id);
    return false;
  }

  // Install wait for dependency on all reader txn
  auto ptr = GetEwReaderList(tile_group_header, tuple_id);
  while (ptr->next != nullptr) {
    // try to install a wait for in every reader
    auto reader_tid = ptr->next->txn_id_;
    // skip my self
    if (reader_tid == current_tid) {
      ptr = ptr->next;
      continue;
    }

    // Lock the txn bucket
    {
      std::lock_guard<std::mutex> lock(running_txn_map_mutex_);
      if (running_txn_map_.count(reader_tid) != 0) {
        // Add myself to the wait_for set of reader
        auto reader_ctx = running_txn_map_[reader_tid];
        LOG_TRACE("Add dependency to %lu", reader_tid);
        if (reader_ctx->wait_list_.insert(current_tid).second == true) {
          // New dependency
          current_txn_ctx->wait_for_counter_++;
        }
      } else {
        assert(false);
      }
    }
    ptr = ptr->next;
  }

  ReleaseEwReaderLock(tile_group_header, tuple_id);
  // Atomically increase the wait_for counter
  assert(current_txn_ctx->wait_for_counter_ >= 0);

  return true;
}

bool EagerWriteTxnManager::PerformRead(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("Perform read");
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  auto &rw_set = current_txn->GetRWSet();
  auto tuple_map = rw_set.find(tile_group_id);
  if (tuple_map != rw_set.end()) {
    if (tuple_map->second.find(tuple_id) != tuple_map->second.end()) {
      // It was already accessed, don't acquire read lock again
      return true;
    }
  }

  if (IsOwner(tile_group_header, tuple_id)) {
    LOG_TRACE("It's already the owner");
    return true;
  }

  GetEwReaderLock(tile_group_header, tuple_id);

  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if (old_txn_id != INITIAL_TXN_ID) {
    // there is a writer
    // so reader (myself) should be blocked
    LOG_TRACE("Own by others: %lu", old_txn_id);
    ReleaseEwReaderLock(tile_group_header, tuple_id);
    return false;
  }

  AddReader(tile_group_header, tuple_id);
  ReleaseEwReaderLock(tile_group_header, tuple_id);
  current_txn->RecordRead(location);

  return true;
}

bool EagerWriteTxnManager::PerformInsert(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("Perform insert");

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  assert(tile_group_header->GetTransactionId(tuple_id) == 0);
  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // SetOwnership(tile_group_id, tuple_id);
  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);
  InitTupleReserved(tile_group_id, tuple_id);
  return true;
}

void EagerWriteTxnManager::PerformUpdate(const ItemPointer &old_location,
                                         const ItemPointer &new_location) {
  LOG_TRACE("Performing Write %u %u", old_location.block, old_location.offset);

  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  // if we can perform update, then we must have already locked the older
  // version.
  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // The write lock must have been acquired
  // Notice: if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.
  // Set double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  InitTupleReserved(new_location.block, new_location.offset);

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);
}

void EagerWriteTxnManager::PerformUpdate(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("Performing Inplace Write %u %u", tile_group_id, tuple_id);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }
}

void EagerWriteTxnManager::PerformDelete(const ItemPointer &old_location,
                                         const ItemPointer &new_location) {
  LOG_TRACE("Performing Delete %u %u", old_location.block, old_location.offset);
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set up double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);
  InitTupleReserved(new_location.block, new_location.offset);

  current_txn->RecordDelete(old_location);
}

void EagerWriteTxnManager::PerformDelete(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("Performing Inplace Delete %u %u", tile_group_id, tuple_id);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // delete an inserted version
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }
}

Result EagerWriteTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  //*****************************************************
  // we can optimize read-only transaction.
  if (current_txn->IsReadOnly() == true) {
    // no dependency
    assert(current_txn_ctx->wait_for_counter_ == 0);
    LOG_TRACE("Read Only txn: %lu ", current_txn->GetTransactionId());
    // is it always true???
    Result ret = current_txn->GetResult();

    EndTransaction();
    return ret;
  }
  //*****************************************************

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();
  current_txn->SetEndCommitId(end_commit_id);

  // Check if we cause dead lock
  if (CauseDeadLock()) {
    // Abort
    return AbortTransaction();
  }

  // Wait for all dependencies to finish
  LOG_TRACE("Start waiting");
  LOG_TRACE("Current wait for counter = %d",
           current_txn_ctx->wait_for_counter_ - 0);
  while (current_txn_ctx->wait_for_counter_ != 0) {
    //    std::chrono::microseconds sleep_time(1);
    //    std::this_thread::sleep_for(sleep_time);
  }
  LOG_TRACE("End waiting");



  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.LogBeginTransaction(end_commit_id);

  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer old_version(tile_group_id, tuple_slot);

        // logging.
        log_manager.LogUpdate(end_commit_id, old_version, new_version);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer delete_location(tile_group_id, tuple_slot);

        // logging.
        log_manager.LogDelete(end_commit_id, delete_location);

        // we do not change begin cid for old tuple.
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        // set the begin commit id to persist insert
        ItemPointer insert_location(tile_group_id, tuple_slot);
        log_manager.LogInsert(end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }
  log_manager.LogCommitTransaction(end_commit_id);

  EndTransaction();

  return Result::RESULT_SUCCESS;
}

Result EagerWriteTxnManager::AbortTransaction() {
  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // reset the item pointers.
        tile_group_header->SetNextItemPointer(tuple_slot, INVALID_ITEMPOINTER);
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        // AtomicSetOnlyTxnId(tile_group_header, tuple_slot, INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // reset the item pointers.
        tile_group_header->SetNextItemPointer(tuple_slot, INVALID_ITEMPOINTER);
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        // AtomicSetOnlyTxnId(tile_group_header, tuple_slot, INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }

  EndTransaction();

  return Result::RESULT_ABORTED;
}

bool EagerWriteTxnManager::CauseDeadLock() {
  LOG_TRACE("Detecting dead lock");
  std::lock_guard<std::mutex> lock(running_txn_map_mutex_);

  // Always acquire the running_txn_map_mutex_ before acquiring the per context
  // lock
  std::unordered_set<txn_id_t> visited;
  auto current_tid = current_txn->GetTransactionId();

  std::queue<txn_id_t> traverse;

  // init
  for (auto tid : current_txn_ctx->wait_list_) {
    LOG_TRACE("visit %lu", tid);
    traverse.push(tid);
  }

  // BFS start from current txn to detect ring
  while (!traverse.empty()) {
    auto tid = traverse.front();
    traverse.pop();
    if (running_txn_map_.count(tid) == 0) {
      continue;
    }
    if (tid == current_tid) {
      LOG_TRACE("Find dead lock");
      return true;
    }
    for (auto ttid : running_txn_map_[tid]->wait_list_) {
      if (visited.count(ttid) != 0) {
        continue;
      }
      traverse.push(ttid);
      visited.insert(ttid);
    }
  }

  return false;
}
}
}
