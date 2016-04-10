//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pessimistic_txn_manager.cpp
//
// Identification: src/backend/concurrency/ts_order_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ts_order_txn_manager.h"

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

TsOrderTxnManager &TsOrderTxnManager::GetInstance() {
  static TsOrderTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
bool TsOrderTxnManager::IsVisible(
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

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool TsOrderTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TsOrderTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool TsOrderTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  // change timestamp
  cid_t last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);

  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    return false;
  }

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
  return true;
}

void TsOrderTxnManager::SetOwnership(const oid_t &tile_group_id,
                                     const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
}

bool TsOrderTxnManager::PerformRead(const oid_t &tile_group_id,
                                    const oid_t &tuple_id) {
  LOG_TRACE("Perform read");
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  if (IsOwner(tile_group_header, tuple_id)) {
    return true;
  }

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  if (tuple_txn_id == INITIAL_TXN_ID) {
    SetLastReaderCid(tile_group_header, tuple_id,
                     current_txn->GetBeginCommitId());

    current_txn->RecordRead(tile_group_id, tuple_id);

    return true;

  } else {
    // if the version we want to read is uncommitted, then abort.
    return false;
  }
}

bool TsOrderTxnManager::PerformInsert(const oid_t &tile_group_id,
                                      const oid_t &tuple_id) {
  SetOwnership(tile_group_id, tuple_id);
  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(tile_group_id, tuple_id);
  return true;
}

bool TsOrderTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                      const oid_t &tuple_id,
                                      const ItemPointer &new_location) {
  LOG_INFO("Performing Write %lu %lu", tile_group_id, tuple_id);

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  // if we can perform update, then we must have already locked the older
  // version.
  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Notice: if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no
  // one will possibly release the write lock acquired by this txn.
  // Set double linked list
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // Add the old tuple into the update set
  current_txn->RecordUpdate(tile_group_id, tuple_id);
  return true;
}

void TsOrderTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                      const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
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

bool TsOrderTxnManager::PerformDelete(const oid_t &tile_group_id,
                                      const oid_t &tuple_id,
                                      const ItemPointer &new_location) {
  LOG_TRACE("Performing Delete");

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);
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

  current_txn->RecordDelete(tile_group_id, tuple_id);
  return true;
}

void TsOrderTxnManager::PerformDelete(const oid_t &tile_group_id,
                                      const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordDelete(old_location.block, old_location.offset);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(tile_group_id, tuple_id);
  }
}

Result TsOrderTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsReadOnly() == true) {
    Result ret = current_txn->GetResult();

    EndTransaction();

    return ret;
  }

  auto &manager = catalog::Manager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();

  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_READ) {
        continue;
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

  return ret;
}

Result TsOrderTxnManager::AbortTransaction() {
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
        continue;
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
  return Result::RESULT_ABORTED;
}
}
}