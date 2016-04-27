//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_rb_txn_manager.cpp
//
// Identification: src/backend/concurrency/optimistic_rb_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimistic_rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

thread_local storage::RollbackSegmentPool *current_segment_pool;

OptimisticRbTxnManager &OptimisticRbTxnManager::GetInstance() {
  static OptimisticRbTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
// check whether a tuple is visible to current transaction.
// in this protocol, we require that a transaction cannot see other
// transaction's local copy.
bool OptimisticRbTxnManager::IsVisible(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    // This is caused by an aborted insertion of another txn
    return false;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (GetDeleteFlag(tile_group_header, tuple_id) == true) {
      // the tuple is deleted by current transaction
      return false;
    } else {
      assert(tuple_end_cid == MAX_CID);
      // the tuple is updated/inserted by current transaction
      return true;
    }
  } else {
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
    if (invalidated)
      return false;

    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // The tuple is inserted
        return false;
      }
    }

    if (GetActivatedEvidence(tile_group_header, tuple_id) != nullptr) {
      return true;
    } else {
      return false;
    }
  }
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool OptimisticRbTxnManager::IsOwner(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool OptimisticRbTxnManager::IsOwnable(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

// get write lock on a tuple.
// this is invoked by update/delete executors.
bool OptimisticRbTxnManager::AcquireOwnership(
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


bool OptimisticRbTxnManager::PerformRead(const ItemPointer &location) {
  current_txn->RecordRead(location);
  return true;
}

bool OptimisticRbTxnManager::PerformInsert(const ItemPointer &location) {
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

  // init the reserved field
  InitTupleReserved(tile_group_header, tuple_id);

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);
  return true;
}

void OptimisticRbTxnManager::PerformUpdateWithRb(const ItemPointer &location, char *new_rb_seg) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;
  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == current_txn->GetTransactionId());
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // new_rb_seg is a new segment
  assert(storage::RollbackSegmentPool::GetNextPtr(new_rb_seg) == nullptr);
  assert(storage::RollbackSegmentPool::GetTimeStamp(new_rb_seg) == MAX_CID);

  // First link it to the old roolback segment
  auto old_rb_seg = GetRbSeg(tile_group_header, tuple_id);
  storage::RollbackSegmentPool::SetNextPtr(new_rb_seg, old_rb_seg);

  COMPILER_MEMORY_FENCE;

  // Modify the head of the segment list
  // Note that since we are holding the write lock, we don't need atomic update here
  SetRbSeg(tile_group_header, tuple_id, new_rb_seg);

  // Add the location to the update set
  current_txn->RecordUpdate(location);
}

void OptimisticRbTxnManager::PerformDelete(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());

  // tuple deleted should be globally visible
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Set the delete flag
  SetDeleteFlag(tile_group_header, tuple_id);

  // Add the old tuple into the delete set
  bool ins_del = current_txn->RecordDelete(location);

  // Check if we have INS_DEL, if so, just delete and reclaim the tuple
  if (ins_del) {
    // Mark as invalid
    tile_group_header->SetTransactionId(tuple_id, INVALID_TXN_ID);
    // recycle the inserted tuple
    // FIXME: need to delete them in index and free the tuple --jiexi
    // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);
  }
}

void OptimisticRbTxnManager::RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group, const oid_t tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto txn_begin_cid = current_txn->GetBeginCommitId();

  auto rb_seg = GetRbSeg(tile_group_header, tuple_id);
  // Follow the RB chain, rollback if needed, stop when first unreadable RB
  while (IsRBVisible(rb_seg, txn_begin_cid) == true) {

    // Copy the content of the rollback segment onto the tuple
    tile_group->ApplyRollbackSegment(rb_seg, tuple_id);

    // Move to next rollback segment
    rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
  }

  COMPILER_MEMORY_FENCE;

  // Set the tile group header's rollback segment header to next rollback segment
  SetRbSeg(tile_group_header, tuple_id, rb_seg);
}

void OptimisticRbTxnManager::InstallRollbackSegments(storage::TileGroupHeader *tile_group_header,
                                                      const oid_t tuple_id, const cid_t end_cid) {
  auto txn_begin_cid = current_txn->GetBeginCommitId();
  auto rb_seg = GetRbSeg(tile_group_header, tuple_id);

  while (IsRBVisible(rb_seg, txn_begin_cid)) {
    storage::RollbackSegmentPool::SetTimeStamp(rb_seg, end_cid);
    rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
  }
}

bool OptimisticRbTxnManager::ValidateRead(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &end_cid) {
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (IsOwner(tile_group_header, tuple_id) == true) {
    // Read tuple is owned by current txn
    return true;
  }

  // The following is essentially to test that begin_cid and end_cid will look at the same
  // version

  if (end_cid >= tuple_end_cid) {
    // Read tuple is invalidated by others
    return false;
  }

  auto evidence = GetActivatedEvidence(tile_group_header, tuple_id);

  if (evidence == tile_group_header->GetReservedFieldRef(tuple_id)) {
    // begin cid is activated on the master version
    // we already know that the end cid is less than the master version's end cid
    // since master end cid > end cid > begin cid >= master version's begin cid, we are ok
    return true;
  }

  // Now the evidence is a rollback segment
  // If the read is valid, the end cid should also be activated by that rollback segment
  return end_cid >= storage::RollbackSegmentPool::GetTimeStamp(evidence);
}

Result OptimisticRbTxnManager::CommitTransaction() {
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
            GetActivatedEvidence(tile_group_header, tuple_slot) &&
              tile_group_header->GetEndCommitId(tuple_slot) >=
              current_txn->GetBeginCommitId()) {
            // the version is not owned by other txns and is still visible.
            continue;
          }
          LOG_INFO("Abort in read only txn");
          // otherwise, validation fails. abort transaction.
          return AbortTransaction();
        } else {
          assert(tuple_entry.second == RW_TYPE_INS_DEL);
          assert(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
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

          if (ValidateRead(tile_group_header, tuple_slot, end_commit_id)) {
            continue;
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

//  auto &log_manager = logging::LogManager::GetInstance();
//  log_manager.LogBeginTransaction(end_commit_id);
  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
//        // logging.
//        ItemPointer new_version =
//          tile_group_header->GetNextItemPointer(tuple_slot);
//        ItemPointer old_version(tile_group_id, tuple_slot);
//
//        // logging.
//        log_manager.LogUpdate(current_txn, end_commit_id, old_version,
//                              new_version);


        // we must guarantee that, at any time point, AT LEAST ONE version is
        // visible.
        // we do not change begin cid for old tuple.

        // First set the timestamp of the updated master copy
        // Since we have the rollback segment, it's safe to do so
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        // Then we mark all rollback segment's timestamp as our end timestamp
        InstallRollbackSegments(tile_group_header, tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        // Finally we release the write lock on the original tuple
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
//        ItemPointer new_version =
//          tile_group_header->GetNextItemPointer(tuple_slot);
//        ItemPointer delete_location(tile_group_id, tuple_slot);
//
//        // logging.
//        log_manager.LogDelete(end_commit_id, delete_location);

        // we do not change begin cid for master copy
        // First set the timestamp of the master copy
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        // we may have updated this tuple before we delete it, roll it back
        RollbackTuple(tile_group, tuple_slot);

        // Reset the deleted bit for safety
        ClearDeleteFlag(tile_group_header, tuple_slot);

        COMPILER_MEMORY_FENCE;

        // Finally we release the write lock
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // recycle the newer version.
        // FIXME: need to delete them in index and free the tuple --jiexi
        // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        ItemPointer insert_location(tile_group_id, tuple_slot);
//        log_manager.LogInsert(current_txn, end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        // Do nothing for INS_DEL
      }
    }
  }
//  log_manager.LogCommitTransaction(end_commit_id);

  current_txn->SetEndCommitId(end_commit_id);
  EndTransaction();

  return Result::RESULT_SUCCESS;
}

Result OptimisticRbTxnManager::AbortTransaction() {
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

        // We do not have new version now, no need to mantain it
        assert(tile_group_header->GetNextItemPointer(tuple_slot).IsNull());

        // The master copy under updating must be a valid version
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);

        COMPILER_MEMORY_FENCE;

        // Rollback the master copy
        RollbackTuple(tile_group, tuple_slot);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);


      } else if (tuple_entry.second == RW_TYPE_DELETE) {

        // We do not have new version now, no need to mantain it
        assert(tile_group_header->GetNextItemPointer(tuple_slot).IsNull());

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // We may have updated this tuple in the same txn, rollback it
        RollbackTuple(tile_group, tuple_slot);

        // Reset the deleted bit before release the write lock
        ClearDeleteFlag(tile_group_header, tuple_slot);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // recycle the newer version.
        // FIXME: need to delete them in index and free the tuple --jiexi
        // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        // Do nothing for INS_DEL
      }
    }
  }

  current_txn->SetResult(RESULT_ABORTED);
  EndTransaction();
  return Result::RESULT_ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
