//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pessimistic_txn_manager.cpp
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

thread_local std::unordered_map<oid_t, std::unordered_map<oid_t, bool>>
    eager_write_released_rdlock;

EagerWriteTxnManager &EagerWriteTxnManager::GetInstance() {
  static EagerWriteTxnManager txn_manager;
  return txn_manager;
}

// // Visibility check
bool EagerWriteTxnManager::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (GET_WRITER_ID(tuple_txn_id) == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  bool own = (GET_WRITER_ID(tuple_txn_id) == current_txn->GetTransactionId());

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
    if (GET_WRITER_ID(tuple_txn_id) != INITIAL_TXN_ID) {
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

bool EagerWriteTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return GET_WRITER_ID(tuple_txn_id) == current_txn->GetTransactionId();
}

// No others own the tuple
bool EagerWriteTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  LOG_INFO("IsOwnable txnid: %lx end_cid: %lx", tuple_txn_id, tuple_end_cid);
  // FIXME: actually when read count is not 0 this tuple is not accessable
  return GET_WRITER_ID(tuple_txn_id) == INITIAL_TXN_ID &&
         tuple_end_cid == MAX_CID;
}

bool EagerWriteTxnManager::ReleaseReadLock(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  LOG_TRACE("ReleaseReadLock on %lx", old_txn_id);

  if (GET_WRITER_ID(old_txn_id) != INITIAL_TXN_ID) {
    assert(false);
  }

  // No writer
  // Decrease read count
  while (true) {
    assert(GET_READER_COUNT(old_txn_id) != 0);
    auto new_read_count = GET_READER_COUNT(old_txn_id) - 1;
    auto new_txn_id = PACK_TXN_ID(INITIAL_TXN_ID, new_read_count);
    auto res = tile_group_header->CASTxnId(tuple_id, new_txn_id, old_txn_id,
                                           &old_txn_id);
    if (!res) {
      // Assert there's no other writer
      assert(GET_WRITER_ID(old_txn_id) == INITIAL_TXN_ID);
    } else {
      break;
    }
  }

  return true;
}

bool EagerWriteTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id, const oid_t &tuple_id) {
  LOG_TRACE("AcquireOwnership");
  // acquire write lock.
  assert(IsOwner(tile_group_header, tuple_id) == false);

  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  // No writer, release read lock that is acquired before
  auto res = ReleaseReadLock(tile_group_header, tuple_id);
  // Must success
  assert(res);
  eager_write_released_rdlock[tile_group_id][tuple_id] = true;

  // Try get write lock
  auto new_txn_id = current_txn->GetTransactionId();
  res = tile_group_header->CASTxnId(tuple_id, new_txn_id, INITIAL_TXN_ID,
                                    &old_txn_id);

  if (res) {
    return true;
  } else {
    // LOG_INFO("Fail to acquire write lock. Set txn failure.");
    // SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
}

bool EagerWriteTxnManager::PerformRead(const oid_t &tile_group_id,
                                       const oid_t &tuple_id) {
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

  if (IsOwner(tile_group->GetHeader(), tuple_id)) return true;

  // Try to acquire read lock.
  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);
  // No one is holding the write lock
  if (GET_WRITER_ID(old_txn_id) == INITIAL_TXN_ID) {
    LOG_TRACE("No one holding the lock");
    while (true) {
      LOG_TRACE("Current read count is %lu", GET_READER_COUNT(old_txn_id));
      auto new_read_count = GET_READER_COUNT(old_txn_id) + 1;
      // Try add read count
      auto new_txn_id = PACK_TXN_ID(INITIAL_TXN_ID, new_read_count);
      LOG_TRACE("New txn id %lx", new_txn_id);
      auto res = tile_group_header->CASTxnId(tuple_id, new_txn_id, old_txn_id,
                                             &old_txn_id);
      if (!res) {
        // See if there's writer
        if (GET_WRITER_ID(old_txn_id) != INITIAL_TXN_ID) return false;
      } else {
        break;
      }
    }
  } else {
    // SetTransactionResult(RESULT_FAILURE);
    return false;
  }

  current_txn->RecordRead(tile_group_id, tuple_id);

  return true;
}

bool EagerWriteTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                         const oid_t &tuple_id,
                                         const ItemPointer &new_location) {
  LOG_INFO("Performing Write %lu %lu", tile_group_id, tuple_id);

  auto &manager = catalog::Manager::GetInstance();
  auto transaction_id = current_txn->GetTransactionId();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // The write lock must have been acquired
  // Notice: if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no
  // one will possibly release the write lock acquired by this txn.
  // Set double linked list
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);
  PerformUpdate(new_location.block, new_location.offset);

  // Add the old tuple into the update set
  current_txn->RecordUpdate(tile_group_id, tuple_id);
  return true;
}

bool EagerWriteTxnManager::PerformInsert(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  LOG_TRACE("Perform insert");
  SetOwnership(tile_group_id, tuple_id);
  current_txn->RecordInsert(tile_group_id, tuple_id);
  return true;
}

bool EagerWriteTxnManager::PerformDelete(const oid_t &tile_group_id,
                                         const oid_t &tuple_id,
                                         const ItemPointer &new_location) {
  LOG_TRACE("Performing Delete");
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();
  auto res = AcquireOwnership(tile_group_header, tile_group_id, tuple_id);

  if (res) {
    auto new_tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(new_location.block)->GetHeader();
    auto transaction_id = current_txn->GetTransactionId();

    // Set up double linked list
    tile_group_header->SetNextItemPointer(tuple_id, new_location);
    new_tile_group_header->SetPrevItemPointer(
        new_location.offset, ItemPointer(tile_group_id, tuple_id));

    new_tile_group_header->SetTransactionId(new_location.offset,
                                            transaction_id);
    new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
    new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

    current_txn->RecordDelete(tile_group_id, tuple_id);
    return true;
  } else {
    return false;
  }
}

Result EagerWriteTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();

  auto &rw_set = current_txn->GetRWSet();

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
          bool ret = ReleaseReadLock(tile_group_header, tuple_slot);
          if (ret == false) {
            assert(false);
          }
          eager_write_released_rdlock[tile_group_id][tuple_slot] = true;
        }
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);
      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);
      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);
      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());

        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }

  Result ret = current_txn->GetResult();

  EndTransaction();

  eager_write_released_rdlock.clear();
  return ret;
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
      if (tuple_entry.second == RW_TYPE_READ) {
        if (eager_write_released_rdlock.find(tile_group_id) ==
                eager_write_released_rdlock.end() ||
            eager_write_released_rdlock[tile_group_id].find(tuple_slot) ==
                eager_write_released_rdlock[tile_group_id].end()) {
          bool ret = ReleaseReadLock(tile_group_header, tuple_slot);
          if (ret == false) {
            assert(false);
          }
          eager_write_released_rdlock[tile_group_id][tuple_slot] = true;
        }
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }

  EndTransaction();

  eager_write_released_rdlock.clear();
  return Result::RESULT_ABORTED;
}

void EagerWriteTxnManager::PerformDelete(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // delete an inserted version
    current_txn->RecordDelete(old_location.block, old_location.offset);
  }
}

void EagerWriteTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                         const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location.block, old_location.offset);
  }
}

void EagerWriteTxnManager::SetOwnership(const oid_t &tile_group_id,
                                        const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);
}
}
}
