//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/speculative_read_txn_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "speculative_read_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

thread_local SpecTxnContext spec_txn_context;

SpeculativeReadTxnManager &SpeculativeReadTxnManager::GetInstance() {
  static SpeculativeReadTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
// when performing scan, it is possible to see two versions of a single tuple.
// If the first version that is visible to us is the older one,
// then it is guaranteed that we see a single visible version.
// however, if the first version that is visible is the newer one,
// then it is possible that we obtain two versions.
// in this case, we rely on validation to abort this transaction.
bool SpeculativeReadTxnManager::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  txn_id_t txn_begin_cid = current_txn->GetBeginCommitId();

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_end_cid == MAX_CID) {
      // a transaction will immediately write ts to the version.
      assert(tuple_begin_cid == txn_begin_cid);
      // the only version that is visible is the newly inserted one.
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    bool activated = (txn_begin_cid >= tuple_begin_cid);
    bool invalidated = (txn_begin_cid >= tuple_end_cid);

    // check visibility.
    if (activated && !invalidated) {
      return true;
    } else {
      return false;
    }
  }
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool SpeculativeReadTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// will be invoked only by deletes and updates.
bool SpeculativeReadTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

// will be invoked only by deletes and updates.
bool SpeculativeReadTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
  return true;
}

void SpeculativeReadTxnManager::SetOwnership(const oid_t &tile_group_id,
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

bool SpeculativeReadTxnManager::PerformRead(const oid_t &tile_group_id,
                                            const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto current_txn_id = current_txn->GetTransactionId();
  // if the tuple is owned by other transaction, then register dependency.
  if (tuple_txn_id != INITIAL_TXN_ID && tuple_txn_id != INVALID_TXN_ID &&
      tuple_txn_id != current_txn_id) {
    bool ret_type = RegisterDependency(tuple_txn_id);
    if (ret_type == false) {
      // we did not find the dependent txn.
      // actually, we can now validate whether this speculative read succeeds.
    }
  }
  current_txn->RecordRead(tile_group_id, tuple_id);
  return true;
}

bool SpeculativeReadTxnManager::PerformInsert(const oid_t &tile_group_id,
                                              const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetBeginCommitId(tuple_id, txn_begin_id);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  // no need to set next item pointer.
  current_txn->RecordInsert(tile_group_id, tuple_id);
  return true;
}

// at any time point, we must guarantee at least one version of a tuple is
// visible.
// this function is invoked when it is the first time to update the tuple.
// the tuple passed into this function is the global version.
bool SpeculativeReadTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                              const oid_t &tuple_id,
                                              const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  new_tile_group_header->SetBeginCommitId(new_location.offset, txn_begin_id);

  // do we need this fence??
  COMPILER_MEMORY_FENCE;

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  COMPILER_MEMORY_FENCE;
  // before linking the new version to the old one,
  // we must guarantee the txn_id and begin_cid has been set.
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  COMPILER_MEMORY_FENCE;

  // we must guarantee that the newer version is ready
  // before changing the end_cid of the older version.
  tile_group_header->SetEndCommitId(tuple_id, txn_begin_id);

  current_txn->RecordUpdate(tile_group_id, tuple_id);
  return true;
}

// this function is invoked when it is NOT the first time to update the tuple.
// the tuple passed into this function is the local version created by this txn.
void SpeculativeReadTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                              const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) ==
         current_txn->GetBeginCommitId());
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordUpdate(old_location.block, old_location.offset);
  }
}

// the logic is the same as PerformUpdate.
bool SpeculativeReadTxnManager::PerformDelete(const oid_t &tile_group_id,
                                              const oid_t &tuple_id,
                                              const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  new_tile_group_header->SetBeginCommitId(new_location.offset, txn_begin_id);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // do we need this fence?? seems that the fence is still needed. see
  // validation logic.
  COMPILER_MEMORY_FENCE;

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetEndCommitId(tuple_id, txn_begin_id);

  current_txn->RecordDelete(tile_group_id, tuple_id);
  return true;
}

void SpeculativeReadTxnManager::PerformDelete(const oid_t &tile_group_id,
                                              const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) ==
         current_txn->GetBeginCommitId());
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

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

Result SpeculativeReadTxnManager::CommitTransaction() {
  LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();

  // validation must be performed. otherwise, deadlock can occur.
  // validate read set.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second != RW_TYPE_INSERT &&
          tuple_entry.second != RW_TYPE_INS_DEL) {
        if (tile_group_header->GetTransactionId(tuple_slot) ==
            current_txn->GetTransactionId()) {
          // the version is owned by the transaction.
          continue;
        } else {
          if (tile_group_header->GetBeginCommitId(tuple_slot) <=
                  end_commit_id &&
              tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
            // the version is still visible.
            continue;
          } else {
            // otherwise, validation fails. abort transaction.
            return AbortTransaction();
          }
        }
      }
    }
  }

  // we do not start installation until the all the dependencies have been
  // cleared.
  if (IsCommittable() == false) {
    return AbortTransaction();
  }
  //////////////////////////////////////////////////////////

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
        // we do not change begin cid for old tuple.
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
        // we do not change begin cid for old tuple.
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
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }

  NotifyCommit();

  Result ret = current_txn->GetResult();

  EndTransaction();

  return ret;
}

Result SpeculativeReadTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        // we do not set begin cid for old tuple.
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

  NotifyAbort();

  EndTransaction();

  return Result::RESULT_ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
