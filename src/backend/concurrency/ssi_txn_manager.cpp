//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssi_txn_manager.cpp
//
// Identification: src/backend/concurrency/ssi_txn_manager.cpp
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

#include <set>
namespace peloton {
namespace concurrency {

thread_local SsiTxnContext *current_ssi_txn_ctx;

SsiTxnManager &SsiTxnManager::GetInstance() {
  static SsiTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
bool SsiTxnManager::IsVisible(
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
        // in this protocol, we do not allow cascading abort. so never read an
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

bool SsiTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// will only be performed by deletes and updates.
bool SsiTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool SsiTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();
  LOG_INFO("AcquireOwnership %lu", txn_id);

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    return false;
  }

  {
    GetReadLock(tile_group_header, tuple_id);
    ReadList *header = GetReaderList(tile_group_header, tuple_id);

    bool should_abort = false;
    while (header != nullptr) {
      // For all owner of siread lock on this version
      auto owner_ctx = header->txn_ctx;

      // Lock the transaction context
      owner_ctx->lock_.Lock();

      // Myself, skip
      if (owner_ctx == current_ssi_txn_ctx || owner_ctx->is_abort_ == true) {
        header = header->next;

        // Unlock the transaction context
        owner_ctx->lock_.Unlock();
        continue;
      }

      auto end_cid = owner_ctx->transaction_->GetEndCommitId();

      // Owner is running, then siread lock owner has an out edge to me
      if (end_cid == INVALID_TXN_ID) {
        SetInConflict(current_ssi_txn_ctx);
        SetOutConflict(owner_ctx);
        LOG_INFO("set %ld in, set %ld out", txn_id,
                 owner_ctx->transaction_->GetTransactionId());
      } else {
        // Owner has commited and ownner commit after I start, then I must abort
        if (end_cid > current_txn->GetBeginCommitId() &&
            GetInConflict(owner_ctx)) {
          should_abort = true;
          LOG_INFO("abort in acquire");

          // Unlock the transaction context
          owner_ctx->lock_.Unlock();
          break;
        }
      }

      header = header->next;

      // Unlock the transaction context
      owner_ctx->lock_.Unlock();
    }
    ReleaseReadLock(tile_group_header, tuple_id);

    if (should_abort) return false;
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
    // Previously, this tuple hasn't been read, add the txn to the reader list
    // of the tuple
    AddSIReader(tile_group.get(), tuple_id);

    auto writer = tile_group_header->GetTransactionId(tuple_id);
    // Another transaction is writting this tuple, add an edge
    if (writer != INVALID_TXN_ID && writer != INITIAL_TXN_ID &&
        writer != txn_id) {
      // std::lock_guard<std::mutex> lock(txn_manager_mutex_);
      txn_manager_mutex_.ReadLock();

      if (txn_table_.count(writer) != 0) {
        // The writer have not been removed from the txn table
        LOG_INFO("Writer %lu has no entry in txn table when read %lu", writer,
                 tuple_id);
        SetInConflict(txn_table_.at(writer));
        SetOutConflict(current_ssi_txn_ctx);
      }

      txn_manager_mutex_.Unlock();
    }
  }

  // existing SI code
  current_txn->RecordRead(tile_group_id, tuple_id);

  // For each new version of the tuple
  {
    // This is a potential big overhead for read operations
    txn_manager_mutex_.ReadLock();

    LOG_INFO("SI read phase 2");

    ItemPointer next_item = tile_group_header->GetNextItemPointer(tuple_id);
    while (!next_item.IsNull()) {
      auto tile_group =
          catalog::Manager::GetInstance().GetTileGroup(next_item.block);
      auto creator = GetCreatorTxnId(tile_group.get(), next_item.offset);

      LOG_INFO("%ld %ld creator is %ld", next_item.block, next_item.offset,
               creator);

      // Check creator status, skip if creator has commited before I start
      // or self is creator
      auto should_skip = false;
      if (txn_table_.count(creator) == 0)
        should_skip = true;
      else {
        if (creator == txn_id)
          should_skip = true;
        else {
          auto ctx = txn_table_.at(creator);
          if (ctx->transaction_->GetEndCommitId() != INVALID_TXN_ID &&
              ctx->transaction_->GetEndCommitId() <
                  current_txn->GetBeginCommitId()) {
            should_skip = true;
          }
        }
      }

      if (should_skip) {
        next_item =
            tile_group->GetHeader()->GetNextItemPointer(next_item.offset);
        continue;
      }

      auto creator_ctx = txn_table_.at(creator);
      // Lock the transaction context
      creator_ctx->lock_.Lock();

      if (creator_ctx->is_abort_ == false) {
        // If creator committed and has out_confict, since creator has commited,
        // I must abort
        if (creator_ctx->transaction_->GetEndCommitId() != INVALID_TXN_ID &&
            creator_ctx->out_conflict_) {
          LOG_INFO("abort in read");
          // Unlock the transaction context
          creator_ctx->lock_.Unlock();
          txn_manager_mutex_.Unlock();
          return false;
        }
        // Creator not commited, add an edge
        SetInConflict(creator_ctx);
        SetOutConflict(current_ssi_txn_ctx);
      }

      // Unlock the transaction context
      creator_ctx->lock_.Unlock();

      next_item = tile_group->GetHeader()->GetNextItemPointer(next_item.offset);
    }
    txn_manager_mutex_.Unlock();
  }

  return true;
}

bool SsiTxnManager::PerformInsert(const oid_t &tile_group_id,
                                  const oid_t &tuple_id) {
  LOG_INFO("Perform insert %lu %lu", tile_group_id, tuple_id);
  SetOwnership(tile_group_id, tuple_id);
  // No need to set next item pointer.
  current_txn->RecordInsert(tile_group_id, tuple_id);
  // Init the creator of this tuple
  InitTupleReserved(current_txn->GetTransactionId(), tile_group_id, tuple_id);
  return true;
}

bool SsiTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                  const oid_t &tuple_id,
                                  const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // if we can perform update, then we must already locked the older version.
  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);
  // Set double linked list
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);

  current_txn->RecordUpdate(tile_group_id, tuple_id);

  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  return true;
}

void SsiTxnManager::PerformUpdate(const oid_t &tile_group_id,
                                  const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // Update an inserted version
    current_txn->RecordUpdate(old_location.block, old_location.offset);
  }
}

bool SsiTxnManager::PerformDelete(const oid_t &tile_group_id,
                                  const oid_t &tuple_id,
                                  const ItemPointer &new_location) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // Set up double linked list
  tile_group_header->SetNextItemPointer(tuple_id, new_location);
  new_tile_group_header->SetPrevItemPointer(
      new_location.offset, ItemPointer(tile_group_id, tuple_id));

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // Add the old tuple into the delete set
  current_txn->RecordDelete(tile_group_id, tuple_id);
  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  return true;
}

void SsiTxnManager::PerformDelete(const oid_t &tile_group_id,
                                  const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
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

void SsiTxnManager::SetOwnership(const oid_t &tile_group_id,
                                 const oid_t &tuple_id) {
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);
}

Result SsiTxnManager::CommitTransaction() {
  LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();
  auto &rw_set = current_txn->GetRWSet();
  cid_t end_commit_id = GetNextCommitId();
  Result ret;

  bool should_abort = false;
  {
    current_ssi_txn_ctx->lock_.Lock();
    if (GetInConflict(current_ssi_txn_ctx) &&
        GetOutConflict(current_ssi_txn_ctx)) {
      should_abort = true;
      current_ssi_txn_ctx->is_abort_ = true;
    }

    // generate transaction id.
    ret = current_txn->GetResult();
    if (should_abort == false && ret == Result::RESULT_SUCCESS) {
      current_txn->SetEndCommitId(end_commit_id);
    }
    current_ssi_txn_ctx->lock_.Unlock();
  }

  if (should_abort) {
    LOG_INFO("Abort because RW conflict");
    return AbortTransaction();
  }

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
  current_txn = nullptr;
  current_ssi_txn_ctx->is_finish_ = true;

  return ret;
}

Result SsiTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_ssi_txn_ctx->is_abort_ == false) {
    // Set abort flag
    current_ssi_txn_ctx->lock_.Lock();
    current_ssi_txn_ctx->is_abort_ = true;
    current_ssi_txn_ctx->lock_.Unlock();
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
        LOG_INFO("Txn %lu free %lu", current_txn->GetTransactionId(),
                 tuple_slot);
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

  auto txn_id = current_txn->GetTransactionId();

  // firstly, let's remove reader
  RemoveReader(current_txn);

  // then, we can erase context safely
  {
    txn_manager_mutex_.WriteLock();
    txn_table_.erase(txn_id);
    txn_manager_mutex_.Unlock();
  }

  delete current_ssi_txn_ctx;
  delete current_txn;
  current_txn = nullptr;

  return Result::RESULT_ABORTED;
}

void SsiTxnManager::RemoveReader(Transaction *txn) {
  LOG_INFO("release SILock");

  // Remove from the read list of accessed tuples
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
      if (tuple_entry.second == RW_TYPE_INSERT) {
        continue;
      }
      RemoveSIReader(tile_group_header, tuple_slot, txn->GetTransactionId());
    }
  }
  LOG_INFO("release SILock finish");
}

// Clean obsolete txn record
// Current implementation might be very expensive, consider using dependency
// count
void SsiTxnManager::CleanUp() {
  std::lock_guard<std::mutex> lock(clean_mutex_);

  std::unordered_set<SsiTxnContext *> garbage_ctx;
  {
    txn_manager_mutex_.ReadLock();

    // init it as max() for the case that all transactions are committed
    cid_t min_begin = std::numeric_limits<cid_t>::max();

    for (auto &item : txn_table_) {
      // find smallest begin cid of the running transaction
      auto &ctx = item.second;
      if (ctx->transaction_->GetEndCommitId() == INVALID_TXN_ID) {
        // txn_id_a > txn_id_b --> begin_cid_a > begin_cid_b
        // so the first running transaction's begin_cid must be the smallest
        // see BeginTransaction()
        min_begin = ctx->transaction_->GetBeginCommitId();
        break;
      }
    }

    // remove committed transactions, whose end_cid < min_begin
    for (auto itr = txn_table_.begin(); itr != txn_table_.end(); itr++) {
      auto &ctx = itr->second;
      auto end_cid = ctx->transaction_->GetEndCommitId();
      if (end_cid == INVALID_TXN_ID) {
        // running transaction
        // then we know that the subsequent txn's end_cid > min_begin
        // so just break
        break;
      }

      // record garbage
      if (end_cid < min_begin && ctx->is_finish_) {
        // we can safely remove it from table
        LOG_INFO("remove %ld in table", ctx->transaction_->GetTransactionId());
        garbage_ctx.insert(ctx);
      }
    }
    txn_manager_mutex_.Unlock();
  }

  // remove garbage from table
  {
    txn_manager_mutex_.WriteLock();
    for (auto ctx : garbage_ctx) {
      txn_table_.erase(ctx->transaction_->GetTransactionId());
    }
    txn_manager_mutex_.Unlock();
  }

  // remove txn's reader list firstly
  for (auto ctx : garbage_ctx) {
    RemoveReader(ctx->transaction_);
    delete ctx->transaction_;
    delete ctx;
  }

}

void SsiTxnManager::CleanUpBg() {
  while (!this->stopped || txn_table_.size() != 0) {
    // GC periodically
    std::chrono::milliseconds sleep_time(50);
    std::this_thread::sleep_for(sleep_time);

    CleanUp();
  }  // End of outer while
  cleaned = true;
}

}  // End storage namespace
}  // End peloton namespace
