//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_txn_manager.cpp
//
// Identification: src/backend/concurrency/optimistic_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimistic_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

OptimisticTxnManager &OptimisticTxnManager::GetInstance() {
  static OptimisticTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
// check whether a tuple is visible to current transaction.
// in this protocol, we require that a transaction cannot see other
// transaction's local copy.
VisibilityType OptimisticTxnManager::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
  bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    if (activated && !invalidated) {
      // deleted tuple
      return VISIBILITY_DELETED;
    } else {
      // aborted tuple
      return VISIBILITY_INVISIBLE;
    }
  }

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted/updated one.
      return VISIBILITY_OK;
    } else if (tuple_end_cid == INVALID_CID) {
      // tuple being deleted by current txn
      return VISIBILITY_DELETED;
    } else {
      // old version of the tuple that is being updated by current txn
      return VISIBILITY_INVISIBLE;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // in this protocol, we do not allow cascading abort. so never read an
        // uncommitted version.
        return VISIBILITY_INVISIBLE;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return VISIBILITY_OK;
        } else {
          return VISIBILITY_INVISIBLE;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {

        return VISIBILITY_OK;
      } else {
        return VISIBILITY_INVISIBLE;
      }
    }
  }
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool OptimisticTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool OptimisticTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

// get write lock on a tuple.
// this is invoked by update/delete executors.
bool OptimisticTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_ERROR("Fail to acquire tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
  return true;
}


// release write lock on a tuple.
// one example usage of this method is when a tuple is acquired, but operation
// (insert,update,delete) can't proceed, the executor needs to yield the 
// ownership before return false to upper layer.
// It should not be called if the tuple is in the write set as commit and abort
// will release the write lock anyway.
void OptimisticTxnManager::YieldOwnership(const oid_t &tile_group_id,
  const oid_t &tuple_id) {

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  assert(IsOwner(tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool OptimisticTxnManager::PerformRead(const ItemPointer &location) {
  LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
  current_txn->RecordRead(location);
  return true;
}

bool OptimisticTxnManager::PerformInsert(const ItemPointer &location) {
  LOG_TRACE("PerformInsert (%u, %u)\n", location.block, location.offset);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);
  return true;
}

// remember to guarantee that at any time point,
// a certain version (either old or new) of a tuple must be visible.
// this function is invoked when it is the first time to update the tuple.
// the tuple passed into this function is the global version.
void OptimisticTxnManager::PerformUpdate(const ItemPointer &old_location,
                                         const ItemPointer &new_location) {
  LOG_TRACE("PerformUpdate (%u, %u)->(%u, %u)\n", old_location.block, old_location.offset,
             new_location.block, new_location.offset );
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // if we can perform update, then we must have already locked the older
  // version.
  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);
}

// this function is invoked when it is NOT the first time to update the tuple.
// the tuple passed into this function is the local version created by this txn.
void OptimisticTxnManager::PerformUpdate(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;
  LOG_TRACE("PerformUpdate (%u, %u)\n", tile_group_id, tuple_id);

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordUpdate(old_location);
  }
}

void OptimisticTxnManager::PerformDelete(const ItemPointer &old_location,
                                         const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();

  LOG_TRACE("PerformDelete (%u, %u)->(%u, %u)\n", old_location.block, old_location.offset,
                                                 new_location.block, new_location.offset );

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // if we can perform update, then we must have already locked the older
  // version.
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

  // Add the old tuple into the delete set
  current_txn->RecordDelete(old_location);
}

void OptimisticTxnManager::PerformDelete(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("PerformDelete (%u, %u)\n", tile_group_id, tuple_id);

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
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }
}

Result OptimisticTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  //*****************************************************
  // we can optimize read-only transaction.
  if (current_txn->IsReadOnly() == true) {
    // validate read set.
    for (auto &tile_group_entry : rw_set) {
      oid_t tile_group_id = tile_group_entry.first;
      auto tile_group = manager.GetTileGroup(tile_group_id);
      auto tile_group_header = tile_group->GetHeader();
      for (auto &tuple_entry : tile_group_entry.second) {
        auto tuple_slot = tuple_entry.first;
        // if this tuple is not newly inserted.
        if (tuple_entry.second == RW_TYPE_READ) {
          if (tile_group_header->GetTransactionId(tuple_slot) ==
                  INITIAL_TXN_ID &&
              tile_group_header->GetBeginCommitId(tuple_slot) <=
                  current_txn->GetBeginCommitId() &&
              tile_group_header->GetEndCommitId(tuple_slot) >=
                  current_txn->GetBeginCommitId()) {
            // the version is not owned by other txns and is still visible.
            continue;
          }
          // otherwise, validation fails. abort transaction.
          return AbortTransaction();
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

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();
  current_txn->SetEndCommitId(end_commit_id);

  // validate read set.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      // if this tuple is not newly inserted.
      if (tuple_entry.second != RW_TYPE_INSERT &&
          tuple_entry.second != RW_TYPE_INS_DEL) {
        // if this tuple is owned by this txn, then it is safe.
        if (tile_group_header->GetTransactionId(tuple_slot) ==
            current_txn->GetTransactionId()) {
          // the version is owned by the transaction.
          continue;
        } else {
          if (tile_group_header->GetTransactionId(tuple_slot) ==
                  INITIAL_TXN_ID && tile_group_header->GetBeginCommitId(
                                        tuple_slot) <= end_commit_id &&
              tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
            // the version is not owned by other txns and is still visible.
            continue;
          }
        }
        LOG_TRACE("transaction id=%lu",
                  tile_group_header->GetTransactionId(tuple_slot));
        LOG_TRACE("begin commit id=%lu",
                  tile_group_header->GetBeginCommitId(tuple_slot));
        LOG_TRACE("end commit id=%lu",
                  tile_group_header->GetEndCommitId(tuple_slot));
        // otherwise, validation fails. abort transaction.
        return AbortTransaction();
      }
    }
  }
  //////////////////////////////////////////////////////////

  // auto &log_manager = logging::LogManager::GetInstance();
  // log_manager.LogBeginTransaction(end_commit_id);
  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        // logging.
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer old_version(tile_group_id, tuple_slot);

        // logging.
        // log_manager.LogUpdate(current_txn, end_commit_id, old_version,
        //                       new_version);

        // we must guarantee that, at any time point, AT LEAST ONE version is
        // visible.
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
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer delete_location(tile_group_id, tuple_slot);

        // logging.
        // log_manager.LogDelete(end_commit_id, delete_location);

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
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        ItemPointer insert_location(tile_group_id, tuple_slot);
        // log_manager.LogInsert(current_txn, end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());

        // set the begin commit id to persist insert
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }
  // log_manager.LogCommitTransaction(end_commit_id);

  EndTransaction();

  return Result::RESULT_SUCCESS;
}

Result OptimisticTxnManager::AbortTransaction() {
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
        // we do not set begin cid for old tuple.
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

        // reset the item pointers.
        tile_group_header->SetNextItemPointer(tuple_slot, INVALID_ITEMPOINTER);
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);

        COMPILER_MEMORY_FENCE;

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

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        // reset the item pointers.
        tile_group_header->SetNextItemPointer(tuple_slot, INVALID_ITEMPOINTER);
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);

        COMPILER_MEMORY_FENCE;

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

}  // End storage namespace
}  // End peloton namespace
