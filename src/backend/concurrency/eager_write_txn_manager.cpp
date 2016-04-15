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

thread_local std::unordered_map<oid_t, std::unordered_set<oid_t>>
    eager_write_released_rdlock;
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

  if (EXTRACT_TXNID(tuple_txn_id) == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  bool own = (current_txn->GetTransactionId() == EXTRACT_TXNID(tuple_txn_id));

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
    if (EXTRACT_TXNID(tuple_txn_id) != INITIAL_TXN_ID) {
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
  LOG_INFO("release all reader lock");

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
  LOG_INFO("release EWreader finish");
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool EagerWriteTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return EXTRACT_TXNID(tuple_txn_id) == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool EagerWriteTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  LOG_INFO("IsOwnable txnid: %lx end_cid: %lx", tuple_txn_id, tuple_end_cid);
  // FIXME: actually when read count is not 0 this tuple is not accessable
  return EXTRACT_TXNID(tuple_txn_id) == INITIAL_TXN_ID &&
         tuple_end_cid == MAX_CID;
}

bool EagerWriteTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  LOG_INFO("AcquireOwnership");
  assert(IsOwner(tile_group_header, tuple_id) == false);

  // Try to get write lock
  auto current_tid = current_txn->GetTransactionId();
  auto old_tid = tile_group_header->GetTransactionId(tuple_id);

  while (EXTRACT_TXNID(old_tid) == INITIAL_TXN_ID) {
    auto read_count = EXTRACT_READ_COUNT(old_tid);
    auto new_tid = PACK_TXNID(current_tid, read_count);
    auto real_tid =
        tile_group_header->SetAtomicTransactionId(tuple_id, old_tid, new_tid);

    if (real_tid != old_tid) {
      old_tid = real_tid;
    }
  }

  // Check if we successfully get the write lock
  bool res = (EXTRACT_TXNID(old_tid) == current_tid);

  if (res) {
    // Install wait for dependency on all reader txn
    GetEwReaderLock(tile_group_header, tuple_id);
    {
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
            LOG_INFO("Add dependency to %lu", reader_tid);
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
    }
    ReleaseEwReaderLock(tile_group_header, tuple_id);

    // Atomically increase the wait_for counter
    assert(current_txn_ctx->wait_for_counter_ >= 0);

    return true;
  } else {
    LOG_INFO("Fail to acquire write lock. Set txn failure.");
    // SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
}

void EagerWriteTxnManager::DecreaseReaderCount(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  LOG_INFO("ReleaseReadLock on %lx", old_txn_id);

  // Decrease read count
  while (true) {
    assert(EXTRACT_READ_COUNT(old_txn_id) != 0);
    LOG_INFO("ReleaseReadLock inside %lx", old_txn_id);
    auto new_read_count = EXTRACT_READ_COUNT(old_txn_id) - 1;
    auto new_txn_id = PACK_TXNID(EXTRACT_TXNID(old_txn_id), new_read_count);

    txn_id_t real_txn_id = tile_group_header->SetAtomicTransactionId(
        tuple_id, old_txn_id, new_txn_id);

    if (real_txn_id != old_txn_id) {
      old_txn_id = real_txn_id;
    } else {
      LOG_INFO("ReleaseReadLock end %lx -> %lx", old_txn_id, new_txn_id);
      break;
    }
  }
}

bool EagerWriteTxnManager::PerformRead(const oid_t &tile_group_id,
                                       const oid_t &tuple_id) {
  LOG_INFO("Perform read");
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
    LOG_INFO("It's already the owner");
    return true;
  }

  // Try to acquire read lock.
  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);
  // No one is holding the write lock
  if (EXTRACT_TXNID(old_txn_id) == INITIAL_TXN_ID) {
    LOG_INFO("No one holding the lock");
    while (true) {
      LOG_INFO("Current read count is %lu", EXTRACT_READ_COUNT(old_txn_id));
      auto new_read_count = EXTRACT_READ_COUNT(old_txn_id) + 1;

      // Try add read count
      auto new_txn_id = PACK_TXNID(INITIAL_TXN_ID, new_read_count);
      LOG_INFO("New txn id %lx", new_txn_id);

      txn_id_t real_txn_id = tile_group_header->SetAtomicTransactionId(
          tuple_id, old_txn_id, new_txn_id);

      if (real_txn_id != old_txn_id) {
        // See if there's writer
        if (EXTRACT_TXNID(real_txn_id) != INITIAL_TXN_ID) {
          return false;
        }
        old_txn_id = real_txn_id;
      } else {
        break;
      }
    }
  } else {
    LOG_INFO("Own by others: %lu", EXTRACT_TXNID(old_txn_id));
    return false;
  }

  AddReader(tile_group_header, tuple_id);
  current_txn->RecordRead(tile_group_id, tuple_id);

  return true;
}

void EagerWriteTxnManager::SetOwnership(const oid_t &tile_group_id,
                                        const oid_t &tuple_id) {
  LOG_INFO("Set ownership");
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  assert(EXTRACT_READ_COUNT(tile_group_header->GetTransactionId(tuple_id)) ==
         0);
  tile_group_header->SetTransactionId(tuple_id, transaction_id);
}

bool EagerWriteTxnManager::PerformInsert(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  LOG_INFO("Perform insert");
  SetOwnership(tile_group_id, tuple_id);
  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(tile_group_id, tuple_id);
  InitTupleReserved(tile_group_id, tuple_id);
  return true;
}

bool EagerWriteTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                         const oid_t &tuple_id,
                                         const ItemPointer &new_location) {
  LOG_INFO("Performing Write %lu %lu", tile_group_id, tuple_id);

  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();
  // Decrease reader count
  DecreaseReaderCount(tile_group_header, tuple_id);

  // if we can perform update, then we must have already locked the older
  // version.
  assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_id)) ==
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
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  InitTupleReserved(new_location.block, new_location.offset);

  // Add the old tuple into the update set
  current_txn->RecordUpdate(tile_group_id, tuple_id);
  return true;
}

void EagerWriteTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  LOG_INFO("Performing Inplace Write %lu %lu", tile_group_id, tuple_id);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  // Set MVCC info
  assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_id)) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location.block, old_location.offset);
  }
}

bool EagerWriteTxnManager::PerformDelete(const oid_t &tile_group_id,
                                         const oid_t &tuple_id,
                                         const ItemPointer &new_location) {
  LOG_INFO("Performing Delete %lu %lu", tile_group_id, tuple_id);
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();
  // Decrease reader count
  DecreaseReaderCount(tile_group_header, tuple_id);

  assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_id)) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set up double linked list
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);
  InitTupleReserved(new_location.block, new_location.offset);

  current_txn->RecordDelete(tile_group_id, tuple_id);
  return true;
}

void EagerWriteTxnManager::PerformDelete(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  LOG_INFO("Performing Inplace Delete %lu %lu", tile_group_id, tuple_id);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_id)) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // delete an inserted version
    current_txn->RecordDelete(old_location.block, old_location.offset);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(tile_group_id, tuple_id);
  }
}

Result EagerWriteTxnManager::CommitTransaction() {
  LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  //*****************************************************
  // we can optimize read-only transaction.
  if (current_txn->IsReadOnly() == true) {
    // no dependency
    assert(current_txn_ctx->wait_for_counter_ == 0);
    LOG_INFO("Read Only txn: %lu ", current_txn->GetTransactionId());

    // validate read set.
    for (auto &tile_group_entry : rw_set) {
      oid_t tile_group_id = tile_group_entry.first;
      auto tile_group = manager.GetTileGroup(tile_group_id);
      auto tile_group_header = tile_group->GetHeader();
      for (auto &tuple_entry : tile_group_entry.second) {
        auto tuple_slot = tuple_entry.first;
        // if this tuple is not newly inserted.
        if (tuple_entry.second == RW_TYPE_READ) {
          // Release read locks
          if (eager_write_released_rdlock.find(tile_group_id) ==
                  eager_write_released_rdlock.end() ||
              eager_write_released_rdlock[tile_group_id].find(tuple_slot) ==
                  eager_write_released_rdlock[tile_group_id].end()) {
            DecreaseReaderCount(tile_group_header, tuple_slot);
            eager_write_released_rdlock[tile_group_id].insert(tuple_slot);
          }
        } else {
          assert(tuple_entry.second == RW_TYPE_INS_DEL);
        }
      }
    }
    // is it always true???
    Result ret = current_txn->GetResult();
    EndTransaction();
    return ret;
  }
  //*****************************************************

  // Check if we cause dead lock
  if (CauseDeadLock()) {
    // Abort
    return AbortTransaction();
  }

  // Wait for all dependencies to finish
  LOG_INFO("Start waiting");
  LOG_INFO("Current wait for counter = %d",
           current_txn_ctx->wait_for_counter_ - 0);
  while (current_txn_ctx->wait_for_counter_ != 0) {
    std::chrono::microseconds sleep_time(1);
    std::this_thread::sleep_for(sleep_time);
  }
  LOG_INFO("End waiting");

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();

  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.LogBeginTransaction(end_commit_id);

  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_READ) {
        // Release read locks
        if (eager_write_released_rdlock.find(tile_group_id) ==
                eager_write_released_rdlock.end() ||
            eager_write_released_rdlock[tile_group_id].find(tuple_slot) ==
                eager_write_released_rdlock[tile_group_id].end()) {
          ReleaseEwReaderLock(tile_group_header, tuple_slot);
          eager_write_released_rdlock[tile_group_id].insert(tuple_slot);
        }
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer old_version(tile_group_id, tuple_slot);

        // logging.
        log_manager.LogUpdate(current_txn, end_commit_id, old_version,
                              new_version);

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

        assert(EXTRACT_READ_COUNT(
            tile_group_header->GetTransactionId(tuple_slot)) == 0);
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
        assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_slot)) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        ItemPointer insert_location(tile_group_id, tuple_slot);
        log_manager.LogInsert(current_txn, end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(EXTRACT_TXNID(tile_group_header->GetTransactionId(tuple_slot)) ==
               current_txn->GetTransactionId());

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

  eager_write_released_rdlock.clear();

  return Result::RESULT_SUCCESS;
}

Result EagerWriteTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_READ) {
        if (eager_write_released_rdlock.find(tile_group_id) ==
                eager_write_released_rdlock.end() ||
            eager_write_released_rdlock[tile_group_id].find(tuple_slot) ==
                eager_write_released_rdlock[tile_group_id].end()) {
          DecreaseReaderCount(tile_group_header, tuple_slot);
          eager_write_released_rdlock[tile_group_id].insert(tuple_slot);
        }
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        AtomicSetOnlyTxnId(tile_group_header, tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        AtomicSetOnlyTxnId(tile_group_header, tuple_slot, INITIAL_TXN_ID);

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

  eager_write_released_rdlock.clear();
  return Result::RESULT_ABORTED;
}

bool EagerWriteTxnManager::CauseDeadLock() {
  LOG_INFO("Detecting dead lock");
  std::lock_guard<std::mutex> lock(running_txn_map_mutex_);

  // Always acquire the running_txn_map_mutex_ before acquiring the per context
  // lock
  std::unordered_set<txn_id_t> dependencies;
  auto current_tid = current_txn->GetTransactionId();

  std::queue<txn_id_t> traverse;

  // init
  dependencies.insert(current_tid);
  for (auto tid : current_txn_ctx->wait_list_) {
    LOG_INFO("visit %lu", tid);
    traverse.push(tid);
  }

  // BFS start from current txn to detect ring
  while (!traverse.empty()) {
    auto tid = traverse.front();
    traverse.pop();
    if (running_txn_map_.count(tid) == 0) {
      // target txn already exit
      continue;
    } else if (dependencies.count(tid) == 0) {
      // find new dependency
      for (auto ttid : running_txn_map_[tid]->wait_list_) {
        traverse.push(ttid);
      }
    } else {
      // find ring
      LOG_INFO("find dead lock %lu", tid);
      assert(tid == current_tid);
      return true;
    }
  }

  return false;
}
}
}
