//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/optimistic_transaction_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "speculative_optimistic_transaction_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

SpeculativeOptimisticTransactionManager &SpeculativeOptimisticTransactionManager::GetInstance() {
  static SpeculativeOptimisticTransactionManager txn_manager;
  return txn_manager;
}

// Visibility check
bool SpeculativeOptimisticTransactionManager::IsVisible(const txn_id_t &tuple_txn_id,
                                             const cid_t &tuple_begin_cid,
                                             const cid_t &tuple_end_cid) {
  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  auto current_txn_id = current_txn->GetTransactionId();
  auto txn_begin_cid = current_txn->GetBeginCommitId();
  bool own = (current_txn_id == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  if (own == true) {
    if (tuple_end_cid != INVALID_CID) {
      assert(tuple_begin_cid == txn_begin_cid);
      assert(tuple_end_cid == MAX_CID);
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

bool SpeculativeOptimisticTransactionManager::IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id){
  auto tuple_txn_id = tile_group->GetHeader()->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current transaction.
bool SpeculativeOptimisticTransactionManager::IsAccessable(storage::TileGroup *tile_group, const oid_t &tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool SpeculativeOptimisticTransactionManager::AcquireTuple(storage::TileGroup *tile_group, const oid_t &physical_tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->LockTupleSlot(physical_tuple_id, txn_id) == false){
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
  return true;
}

bool SpeculativeOptimisticTransactionManager::PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto current_txn_id = current_txn->GetTransactionId();
  // if the tuple is owned by other transaction, then register dependency.
  if (tuple_txn_id != INITIAL_TXN_ID && tuple_txn_id != INVALID_TXN_ID && tuple_txn_id != current_txn_id) {
    // if this dependency has not been registered before.
    if (current_txn->CheckDependency(tuple_txn_id) == true) {
      // if registration succeeded.
      if (RegisterDependency(tuple_txn_id, current_txn_id) == true) {
        // record this dependency locally.
        current_txn->RecordDependency(tuple_txn_id);
      }
    }
  }
  current_txn->RecordRead(tile_group_id, tuple_id);
  return true;
}

bool SpeculativeOptimisticTransactionManager::PerformInsert(const oid_t &tile_group_id,
                                                 const oid_t &tuple_id) {
  auto tile_group_header = 
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);
  
  tile_group_header->SetBeginCommitId(tuple_id, txn_begin_id);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  // no need to set next item pointer.
  current_txn->RecordInsert(tile_group_id, tuple_id);
  return true;
}

bool SpeculativeOptimisticTransactionManager::PerformUpdate(
    const oid_t &tile_group_id, const oid_t &tuple_id,
    const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(new_location.block)->GetHeader();
  
  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);

  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) == MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  new_tile_group_header->SetBeginCommitId(new_location.offset, txn_begin_id);

  COMPILER_MEMORY_FENCE;
  
  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetNextItemPointer(tuple_id, new_location);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetEndCommitId(tuple_id, txn_begin_id);

  current_txn->RecordUpdate(tile_group_id, tuple_id);
  return true;
}

bool SpeculativeOptimisticTransactionManager::PerformDelete(
    const oid_t &tile_group_id, const oid_t &tuple_id,
    const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();  
  auto new_tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(new_location.block)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);

  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) == MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);
  
  new_tile_group_header->SetBeginCommitId(new_location.offset, txn_begin_id);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  COMPILER_MEMORY_FENCE;

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  COMPILER_MEMORY_FENCE;
  
  tile_group_header->SetNextItemPointer(tuple_id, new_location);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetEndCommitId(tuple_id, txn_begin_id);

  current_txn->RecordDelete(tile_group_id, tuple_id);
  return true;
}

void SpeculativeOptimisticTransactionManager::SetDeleteVisibility(const oid_t &tile_group_id, const oid_t &tuple_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  assert(tile_group_header->GetBeginCommitId(tuple_id) == txn_begin_id);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetBeginCommitId(tuple_id, txn_begin_id);
  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  
  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

void SpeculativeOptimisticTransactionManager::SetUpdateVisibility(const oid_t &tile_group_id, const oid_t &tuple_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  assert(tile_group_header->GetBeginCommitId(tuple_id) == txn_begin_id);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Set MVCC info
  tile_group_header->SetBeginCommitId(tuple_id, txn_begin_id);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  
  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

void SpeculativeOptimisticTransactionManager::SetInsertVisibility(const oid_t &tile_group_id, const oid_t &tuple_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();
  auto txn_begin_id = current_txn->GetBeginCommitId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetBeginCommitId(tuple_id, txn_begin_id);

  COMPILER_MEMORY_FENCE;

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  //tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  //tile_group_header->SetEndCommitId(tuple_id, MAX_CID);
  
  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

Result SpeculativeOptimisticTransactionManager::CommitTransaction() {
  LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();
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
          if (tile_group_header->GetTransactionId(tuple_slot) ==
                  INITIAL_TXN_ID &&
              tile_group_header->GetBeginCommitId(tuple_slot) <=
                  end_commit_id &&
              tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
            // the version is not locked and still visible.
            continue;
          }
        }
        // otherwise, validation fails. abort transaction.
        return AbortTransaction();
      }
    }
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
        tile_group_header->SetTransactionId(tuple_slot,
                                           INITIAL_TXN_ID);
      }
      else if (tuple_entry.second == RW_TYPE_DELETE) {
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
        tile_group_header->SetTransactionId(tuple_slot,
                                           INITIAL_TXN_ID);
        
      }
      else if (tuple_entry.second == RW_TYPE_INSERT) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);
      }
      else if (tuple_entry.second == RW_TYPE_INS_DEL) {
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

  Result ret = current_txn->GetResult();

  EndTransaction();

  return ret;
}

Result SpeculativeOptimisticTransactionManager::AbortTransaction() {
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
        tile_group_header->SetTransactionId(tuple_slot,
                                           INITIAL_TXN_ID);

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
        tile_group_header->SetTransactionId(tuple_slot,
                                           INITIAL_TXN_ID);
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

}  // End storage namespace
}  // End peloton namespace