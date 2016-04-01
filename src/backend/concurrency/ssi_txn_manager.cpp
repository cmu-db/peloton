//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rowo_txn_manager.cpp
//
// Identification: src/backend/concurrency/rowo_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ssi_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

SsiTxnManager &SsiTxnManager::GetInstance() {
  static SsiTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
bool SsiTxnManager::IsVisible(const txn_id_t &tuple_txn_id,
                              const cid_t &tuple_begin_cid,
                              const cid_t &tuple_end_cid) {
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

bool SsiTxnManager::IsOwner(storage::TileGroup *tile_group,
                            const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group->GetHeader()->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transdaction.
// will only be performed by deletes and updates.
bool SsiTxnManager::IsAccessable(storage::TileGroup *tile_group,
                                 const oid_t &tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool SsiTxnManager::AcquireTuple(storage::TileGroup *tile_group,
                                 const oid_t &physical_tuple_id) {
  LOG_INFO("Acquire tuple");
  auto tile_group_header = tile_group->GetHeader();
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->LockTupleSlot(physical_tuple_id, txn_id) == false) {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    // SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(txn_table_mutex_);

    auto txn_id = current_txn->GetTransactionId();
    GetReadLock(tile_group, physical_tuple_id, txn_id);
    ReadList *header = GetReaderList(tile_group, physical_tuple_id);

    bool should_abort = false;
    while (header != nullptr) {
      // For all siread lock on this version
      auto owner = header->txn_id;
      if (owner == txn_id) { header = header->next; continue; }
      auto &ctx = txn_table_.at(owner);
      auto end_cid = ctx.transaction_->GetEndCommitId();

      // Owner is running
      if (end_cid == INVALID_TXN_ID) {
        SetInConflict(txn_id);
        SetOutConflict(owner);  
        LOG_INFO("set %ld in, set %ld out", current_txn->GetTransactionId(),
               owner);
      } else {
        // Owner has commited and ownner commit after I start
        if (end_cid > current_txn->GetBeginCommitId()) {
          should_abort = true;
          LOG_INFO("abort in acquire");
          break;
        }
      }      

      header = header->next;
    }
    ReleaseReadLock(tile_group, physical_tuple_id, txn_id);

    if (should_abort)
      return false;
  }

  return true;
}

bool SsiTxnManager::PerformRead(const oid_t &tile_group_id,
                                const oid_t &tuple_id) {

  LOG_INFO("Perform Read %lu %lu", tile_group_id, tuple_id);
  auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  auto txn_id = current_txn->GetTransactionId();

  auto &rw_set = current_txn->GetRWSet();
  if (rw_set.count(tile_group_id) == 0 ||
      rw_set.at(tile_group_id).count(tuple_id) == 0) {
    LOG_INFO("Not read before");
    // previously, this tuple hasn't been read

    AddSIReader(tile_group.get(), tuple_id);

    auto writer = tile_group_header->GetTransactionId(tuple_id);
    if (writer != INVALID_TXN_ID && writer != INITIAL_TXN_ID && writer != txn_id) {
      SetInConflict(writer);
      SetOutConflict(txn_id);
    }
  }

  // existing SI code
  current_txn->RecordRead(tile_group_id, tuple_id);

  // for each new version
  {
    std::lock_guard<std::mutex> lock(txn_table_mutex_);

    LOG_INFO("SI read phase 2");

    ItemPointer next_item = tile_group_header->GetNextItemPointer(tuple_id);
    while (!next_item.IsNull()) {
      auto tile_group = catalog::Manager::GetInstance().GetTileGroup(next_item.block);
      auto creator = GetCreatorTxnId(tile_group.get(), next_item.block);

      LOG_INFO("%ld %ld creator is %ld", next_item.block, next_item.offset,
               creator);

      if (txn_table_.count(creator) == 0 || creator == txn_id) {
        if (creator == txn_id) LOG_INFO("check in read, escape myself");
        next_item = tile_group->GetHeader()->GetNextItemPointer(next_item.offset);
        continue;
      }

      auto &ctx = txn_table_.at(creator);
      // if creator committed and has out_confict
      if (ctx.transaction_->GetEndCommitId() != INVALID_TXN_ID
        && ctx.out_conflict_) {
        LOG_INFO("abort in read");
        return false;
      }

      // LOG_INFO("set %ld in, set %ld out", creator, current_txn->GetTransactionId());

      SetInConflict(creator);
      SetOutConflict(current_txn->GetTransactionId());

      next_item = tile_group->GetHeader()->GetNextItemPointer(next_item.block);
    }
  }

  return true;
}

bool SsiTxnManager::PerformInsert(const oid_t &tile_group_id,
                                  const oid_t &tuple_id) {
  LOG_INFO("Perform insert %lu %lu", tile_group_id, tuple_id);
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  // no need to set next item pointer.
  current_txn->RecordInsert(tile_group_id, tuple_id);

  InitTupleReserved(transaction_id, tile_group_id, tuple_id);
  return true;
}

bool SsiTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                  const oid_t &tuple_id,
                                  const ItemPointer &new_location) {
  LOG_INFO("Perform Update %lu %lu", tile_group_id, tuple_id);

  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  // if we can perform update, then we must already locked the older version.
  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);

  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  current_txn->RecordUpdate(tile_group_id, tuple_id);

  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  return true;
}

bool SsiTxnManager::PerformDelete(const oid_t &tile_group_id,
                                  const oid_t &tuple_id,
                                  const ItemPointer &new_location) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  current_txn->RecordDelete(tile_group_id, tuple_id);

  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  return true;
}

void SsiTxnManager::SetDeleteVisibility(const oid_t &tile_group_id,
                                        const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

void SsiTxnManager::SetUpdateVisibility(const oid_t &tile_group_id,
                                        const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

void SsiTxnManager::SetInsertVisibility(const oid_t &tile_group_id,
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

  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

Result SsiTxnManager::CommitTransaction() {
  LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();
  auto txn_id = current_txn->GetTransactionId();
  auto &rw_set = current_txn->GetRWSet();

  bool should_abort = false;
  {
    std::lock_guard<std::mutex> lock(txn_table_mutex_);

    if (GetInConflict(txn_id) && GetOutConflict(txn_id))
      should_abort = true;
  }

  if (should_abort) {
    LOG_INFO("Abort because RW conflict");
    return AbortTransaction();
  }

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();
  //////////////////////////////////////////////////////////

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
        // we do not change begin cid for old tuple.
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer old_version(tile_group_id, tuple_slot);
        log_manager.LogUpdate(current_txn, end_commit_id, old_version,
                              new_version);

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
        ItemPointer delete_location(tile_group_id, tuple_slot);
        log_manager.LogDelete(end_commit_id, delete_location);
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
        ItemPointer insert_location(tile_group_id, tuple_slot);
        log_manager.LogInsert(current_txn, end_commit_id, insert_location);

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
  log_manager.LogCommitTransaction(end_commit_id);

  Result ret = current_txn->GetResult();
  if (ret == Result::RESULT_SUCCESS) {
    current_txn->SetEndCommitId(end_commit_id);
  }
  // EndTransaction();

  CleanUp();
  return ret;
}

Result SsiTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  {
    std::lock_guard<std::mutex> lock(txn_table_mutex_);
    auto txn_id = current_txn->GetTransactionId();
    // Remove all read tuples by the current txns
    RemoveReader(txn_id);
    txn_table_.erase(txn_id);
  }

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

  delete current_txn;
  current_txn = nullptr;

  CleanUp();
  // EndTransaction();
  return Result::RESULT_ABORTED;
}

// removeReader should be protected in txn_table_mutex_
void SsiTxnManager::RemoveReader(txn_id_t txn_id) {
  LOG_INFO("release SILock");
  assert(txn_table_.count(txn_id) > 0);

  // remove read lock
  auto &my_ctx = txn_table_.at(txn_id);
  auto &rw_set = my_ctx.transaction_->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group =
        catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    for (auto &tuple_entry : tile_group_entry.second) {

      auto tuple_slot = tuple_entry.first;

      // we don't have reader lock on insert type
      if (tuple_entry.second == RW_TYPE_INSERT) {
        continue;
      }

      RemoveSIReader(tile_group.get(), tuple_slot, txn_id);
    }
  }
  LOG_INFO("release SILock finish");
}

void SsiTxnManager::CleanUp() {
  std::lock_guard<std::mutex> lock(txn_table_mutex_);

  if (txn_table_.empty()) {
    return;
  }

  // find smallest begin cid of the running transaction

  // init it as max() for the case that all transactions are committed
  cid_t min_begin = std::numeric_limits<cid_t>::max();
  for (auto &item : txn_table_) {
    auto &ctx = item.second;
    if (ctx.transaction_->GetEndCommitId() == INVALID_TXN_ID) {
      if (ctx.transaction_->GetBeginCommitId() < min_begin) {
        min_begin = ctx.transaction_->GetBeginCommitId();
      }
    }
  }

  auto itr = txn_table_.begin();
  while (itr != txn_table_.end()) {
    auto &ctx = itr->second;
    auto end_cid = ctx.transaction_->GetEndCommitId();
    if (end_cid == INVALID_TXN_ID) {
      // running transaction
      break;
    }

    if (end_cid < min_begin) {
      // we can safely remove it from table

      // remove its reader mark
      LOG_INFO("remove %ld in table", ctx.transaction_->GetTransactionId());
      RemoveReader(ctx.transaction_->GetTransactionId());

      // delete transaction
      delete ctx.transaction_;

      // remove from table
      itr = txn_table_.erase(itr);
    } else {
      itr++;
    }
  }
}

}  // End storage namespace
}  // End peloton namespace
