//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pessimistic_txn_manager.cpp
//
// Identification: src/backend/concurrency/ts_order_n2o_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ts_order_n2o_txn_manager.h"

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


Spinlock *TsOrderN2OTxnManager::GetSpinlockField(
    const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
  return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);
}

cid_t TsOrderN2OTxnManager::GetLastReaderCid(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return *(cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
}

void TsOrderN2OTxnManager::SetLastReaderCid(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, const cid_t &last_read_ts) {
  cid_t *ts_ptr = (cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);

  GetSpinlockField(tile_group_header, tuple_id)->Lock();
  
  if (*ts_ptr < last_read_ts) {
    *ts_ptr = last_read_ts;
  }

  GetSpinlockField(tile_group_header, tuple_id)->Unlock();
}

ItemPointer *TsOrderN2OTxnManager::GetHeadPtr(
    const storage::TileGroupHeader *const tile_group_header, 
    const oid_t tuple_id) {
  return *(reinterpret_cast<ItemPointer**>(tile_group_header->GetReservedFieldRef(tuple_id) + ITEM_POINTER_OFFSET));
}

void TsOrderN2OTxnManager::SetHeadPtr(
    const storage::TileGroupHeader *const tile_group_header, 
    const oid_t tuple_id, ItemPointer *item_ptr) {
  *(reinterpret_cast<ItemPointer**>(tile_group_header->GetReservedFieldRef(tuple_id) + ITEM_POINTER_OFFSET)) = item_ptr;
}


TsOrderN2OTxnManager &TsOrderN2OTxnManager::GetInstance() {
  static TsOrderN2OTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
VisibilityType TsOrderN2OTxnManager::IsVisible(
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
bool TsOrderN2OTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TsOrderN2OTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  // if (tuple_end_cid != MAX_CID) {
  //   fprintf(stderr, "tuple_begin_cid: %d, tuple_end_cid: %d, begin_commit_id: %d\n", (int)tile_group_header->GetBeginCommitId(tuple_id), (int)tuple_end_cid, (int)current_txn->GetBeginCommitId());
  // }
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid > current_txn->GetBeginCommitId();
}

bool TsOrderN2OTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  // change timestamp
  cid_t last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);

  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    return false;
  }

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }

  last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);

  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
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
void TsOrderN2OTxnManager::YieldOwnership(const oid_t &tile_group_id,
  const oid_t &tuple_id) {

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  assert(IsOwner(tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TsOrderN2OTxnManager::PerformRead(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  auto last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);
  if (IsOwner(tile_group_header, tuple_id)) {
    assert(last_reader_cid >= current_txn->GetBeginCommitId());
    return true;
  }

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if(tuple_txn_id != INITIAL_TXN_ID && last_reader_cid < current_txn->GetBeginCommitId()) {
    return false;
  }

  SetLastReaderCid(tile_group_header, tuple_id,
                   current_txn->GetBeginCommitId());

  current_txn->RecordRead(location);
  return true;
}

bool TsOrderN2OTxnManager::PerformInsert(const ItemPointer &location, ItemPointer *itemptr_ptr) {
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
  
  InitTupleReserved(tile_group_header, tuple_id);

  SetLastReaderCid(tile_group_header, location.offset, current_txn->GetBeginCommitId());

  // Write down the head pointer's address in tile group header
  SetHeadPtr(tile_group_header, tuple_id, itemptr_ptr);

  return true;
}

void TsOrderN2OTxnManager::PerformUpdate(const ItemPointer &old_location,
                                      const ItemPointer &new_location) {
  LOG_TRACE("Performing Write old tuple %u %u", old_location.block, old_location.offset);
  LOG_TRACE("Performing Write new tuple %u %u", new_location.block, new_location.offset);

  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  assert(GetLastReaderCid(tile_group_header, old_location.offset) == current_txn->GetBeginCommitId());

  // if we can perform update, then we must have already locked the older
  // version.
  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Notice: if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no
  // one will possibly release the write lock acquired by this txn.
  // Set double linked list
  // old_prev is the version next (newer) to the old version.

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  LOG_TRACE("old prev item pointer: %u, %u", old_prev.block, old_prev.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);


  if (old_prev.IsNull() == false){
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_prev.block)->GetHeader();

    COMPILER_MEMORY_FENCE;

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset, new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // set last read
  SetLastReaderCid(new_tile_group_header, new_location.offset, current_txn->GetBeginCommitId());

  // if the transaction is not updating the latest version, 
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are updating the latest version.
    // Set the header information for the new version
    auto head_ptr = GetHeadPtr(tile_group_header, old_location.offset);

    assert(head_ptr != nullptr);
    
    SetHeadPtr(new_tile_group_header, new_location.offset, head_ptr);
    LOG_TRACE("swap pointer: %u, %u", new_location.block, new_location.offset);
    // Set the index header in an atomic way.
    // We do it atomically because we don't want any one to see a half-done pointer.
    // In case of contention, no one can update this pointer when we are updating it
    // because we are holding the write lock. This update should success in its first trial.
    auto res = AtomicUpdateItemPointer(head_ptr, new_location);
    assert(res == true);
    (void) res;
  }

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);
}

void TsOrderN2OTxnManager::PerformUpdate(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }
}

void TsOrderN2OTxnManager::PerformDelete(const ItemPointer &old_location,
                                      const ItemPointer &new_location) {
  LOG_TRACE("Performing Delete");

  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  assert(GetLastReaderCid(tile_group_header, old_location.offset) == current_txn->GetBeginCommitId());

  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set up double linked list
  
  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);
  
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);


  if (old_prev.IsNull() == false){
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_prev.block)->GetHeader();

    COMPILER_MEMORY_FENCE;

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset, new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);
  
  SetLastReaderCid(new_tile_group_header, new_location.offset, current_txn->GetBeginCommitId());

  // if the transaction is not deleting the latest version, 
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are deleting the latest version.
    // Set the header information for the new version
    auto head_ptr = GetHeadPtr(tile_group_header, old_location.offset);
    SetHeadPtr(new_tile_group_header, new_location.offset, head_ptr);

    // Set the index header in an atomic way.
    // We do it atomically because we don't want any one to see a half-down pointer
    // In case of contention, no one can update this pointer when we are updating it
    // because we are holding the write lock. This update should success in its first trial.
    auto res = AtomicUpdateItemPointer(head_ptr, new_location);
    assert(res == true);
    (void) res;
  }
  
  current_txn->RecordDelete(old_location);
}

void TsOrderN2OTxnManager::PerformDelete(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

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

Result TsOrderN2OTxnManager::CommitTransaction() {
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

  // TODO: Add optimization for read only

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
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        assert(new_version.block != INVALID_OID && new_version.offset != INVALID_OID);
        

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        assert(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle.
        RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        assert(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle.
        RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

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

Result TsOrderN2OTxnManager::AbortTransaction() {
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
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        // these two fields can be set at any time.
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        // TODO: I think there's no need to set this fence.
        COMPILER_MEMORY_FENCE;

        //////////////////////////////////////////////////
        // DOUBLE CHECK!!!
        //////////////////////////////////////////////////

        // reset the item pointers.
        auto old_prev = new_tile_group_header->GetPrevItemPointer(new_version.offset);

        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          auto head_ptr = GetHeadPtr(tile_group_header, tuple_slot);
          auto res = AtomicUpdateItemPointer(head_ptr, ItemPointer(tile_group_id, tuple_slot));
          assert(res == true);
          (void) res;
        }
        //////////////////////////////////////////////////

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false){
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
            .GetTileGroup(old_prev.block)->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
        }

        tile_group_header->SetNextItemPointer(tuple_slot, old_prev);

        COMPILER_MEMORY_FENCE;
        
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(new_version.block, new_version.offset);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        //////////////////////////////////////////////////
        // DOUBLE CHECK!!!
        //////////////////////////////////////////////////

        // reset the item pointers.
        auto old_prev = new_tile_group_header->GetPrevItemPointer(new_version.offset);

        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          auto head_ptr = GetHeadPtr(tile_group_header, tuple_slot);
          auto res = AtomicUpdateItemPointer(head_ptr, ItemPointer(tile_group_id, tuple_slot));
          assert(res == true);
          (void) res;
        }
        //////////////////////////////////////////////////

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false){
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
            .GetTileGroup(old_prev.block)->GetHeader();
          old_prev_tile_group_header->SetPrevItemPointer(old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
        }

        tile_group_header->SetNextItemPointer(tuple_slot, old_prev);

        COMPILER_MEMORY_FENCE;
        
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(new_version.block, new_version.offset);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // GC recycle
        //RecycleInvalidTupleSlot(tile_group_id, tuple_slot);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(tile_group_id, tuple_slot);

      }
    }
  }

  EndTransaction();
  return Result::RESULT_ABORTED;
}
}
}
