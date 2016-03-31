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

bool SsiTxnManager::AcquireTuple(
    storage::TileGroup *tile_group, const oid_t &physical_tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->LockTupleSlot(physical_tuple_id, txn_id) == false) {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    // SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    // I should be in the txn tbale
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    auto& my_cxt = running_txns_.at(current_txn->GetTransactionId());

    auto reserved_area = tile_group_header->GetReservedFieldRef(physical_tuple_id);
    ReadList *header = *GetListAddr(reserved_area);
    while (header != nullptr) {
      // if there is a SIREAD lock(rl) on x
      auto owner = header->txnId;
      header = header->next;
      if (owner == txn_id) {
        continue;
      }
      // The owner must also be in the txn table
      assert(running_txns_.count(owner) > 0);
      auto& ctx = running_txns_.at(owner);
      auto end_cid = ctx.transaction_->GetEndCommitId();
      if (end_cid == INVALID_TXN_ID ||
        end_cid > current_txn->GetBeginCommitId()) {
        // if owner is running
        // or if owner commit time > begin cur txn begin time
        LOG_INFO("abort in acquire");
        return false;
      }
      LOG_INFO("set %ld in, set %ld out", current_txn->GetTransactionId(), owner);
      ctx.out_conflict_ = true;
      my_cxt.in_conflict_ = true;
    }

  }

  return true;
}

bool SsiTxnManager::PerformRead(const oid_t &tile_group_id,
                                               const oid_t &tuple_id) {

  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

  auto& rw_set = current_txn->GetRWSet();
  if(rw_set.count(tile_group_id) == 0 ||
    rw_set.at(tile_group_id).count(tuple_id) == 0) {
    // previously, this tuple hasn't been read

    // get read lock
    ReadList *reader = new ReadList(transaction_id);
    txn_id_t *lock_addr = GetLockAddr(reserved_area);
    ReadList **head_addr = GetListAddr(reserved_area);
    {
      LOG_INFO("SI read phase 1 txn %ld group %ld tid %ld", transaction_id, tile_group_id, tuple_id);
      GetReadLock(lock_addr, transaction_id);
      reader->next = *head_addr;
      *head_addr = reader;
      if (reader->next != nullptr) {
        LOG_INFO("reader next is %ld", reader->next->txnId);
      }
      ReleaseReadLock(lock_addr, transaction_id);
    }
  }

  // existing SI code
  current_txn->RecordRead(tile_group_id, tuple_id);

  // for each new version
  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    auto& my_cxt = running_txns_.at(current_txn->GetTransactionId());
    LOG_INFO("SI read phase 2");
    auto tid = tuple_id;
    while (true) {
      ItemPointer next_item = tile_group_header->GetNextItemPointer(tid);
      // if there is no next tuple.
      if (next_item.IsNull() == true) {
        break;
      }
      reserved_area = GetReservedAreaAddr(next_item.block, next_item.offset);
      auto creator = *GetCreatorAddr(reserved_area);
      LOG_INFO("%ld %ld creator is %ld", next_item.block, next_item.offset, creator);

      if(running_txns_.count(creator) == 0 || creator == transaction_id){
        tile_group_header = catalog::Manager::GetInstance().GetTileGroup(next_item.block)->GetHeader();
        tid = next_item.offset;
        if (creator == transaction_id)
          LOG_INFO("check in read, escape myself");
        continue;
      }

      auto& ctx = running_txns_.at(creator);
      if(ctx.transaction_->GetEndCommitId() != INVALID_TXN_ID){
        // if creator committed
        if(ctx.out_conflict_){
          LOG_INFO("abort in read");
          return false;
        }
      }
      LOG_INFO("set %ld in, set %ld out", creator, current_txn->GetTransactionId());
      ctx.in_conflict_ = true;
      my_cxt.out_conflict_ = true;
      tile_group_header = catalog::Manager::GetInstance().GetTileGroup(next_item.block)->GetHeader();
      tid = next_item.offset;
    }
  }
  return true;
}

bool SsiTxnManager::PerformInsert(const oid_t &tile_group_id,
                                                 const oid_t &tuple_id) {
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

bool SsiTxnManager::PerformUpdate(
    const oid_t &tile_group_id, const oid_t &tuple_id,
    const ItemPointer &new_location) {
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

bool SsiTxnManager::PerformDelete(
    const oid_t &tile_group_id, const oid_t &tuple_id,
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

void SsiTxnManager::SetDeleteVisibility(
    const oid_t &tile_group_id, const oid_t &tuple_id) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // tile_group_header->SetInsertCommit(tuple_id, false); // unused
  // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
}

void SsiTxnManager::SetUpdateVisibility(
    const oid_t &tile_group_id, const oid_t &tuple_id) {
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

void SsiTxnManager::SetInsertVisibility(
    const oid_t &tile_group_id, const oid_t &tuple_id) {
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

  auto abort = false;
  auto &rw_set = current_txn->GetRWSet();
  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    auto& my_cxt = running_txns_.at(current_txn->GetTransactionId());
    if(my_cxt.in_conflict_ && my_cxt.out_conflict_){
      // CleanUp();
      abort = true;
    }
  }

  if(abort){
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
        log_manager.LogUpdate(current_txn, end_commit_id, old_version, new_version);

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
  if(ret == Result::RESULT_SUCCESS){
    current_txn->SetEndCommitId(end_commit_id);
  }
  //EndTransaction();

  CleanUp();
  return ret;
}

Result SsiTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    auto txn_id = current_txn->GetTransactionId();
    assert(running_txns_.count(txn_id) > 0);
    // Remove all read tuples by the current txns
    RemoveReader(txn_id);
    running_txns_.erase(txn_id);
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
  //EndTransaction();
  return Result::RESULT_ABORTED;
}

char *SsiTxnManager::GetReservedAreaAddr(const oid_t tile_group_id, const oid_t tuple_id){
  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  return tile_group_header->GetReservedFieldRef(tuple_id);
}
// init reserved area of a tuple
// creator | lock | read list
void SsiTxnManager::InitTupleReserved(const txn_id_t txn_id, const oid_t tile_group_id, const oid_t tuple_id){
  LOG_INFO("init reserved txn %ld, group %ld tid %ld", txn_id, tile_group_id, tuple_id);

  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == txn_id);
  assert(current_txn->GetTransactionId() == txn_id);

  auto reserved_area = GetReservedAreaAddr(tile_group_id, tuple_id);

  *GetCreatorAddr(reserved_area) = txn_id;
  *GetLockAddr(reserved_area) = INITIAL_TXN_ID;
  *GetListAddr(reserved_area) = nullptr;
  assert(*GetCreatorAddr(reserved_area) == txn_id);
}

bool SsiTxnManager::GetReadLock(txn_id_t *lock_addr, txn_id_t who){
  while(true){
    // check cache
    while(*lock_addr != INITIAL_TXN_ID);
    if(atomic_cas(lock_addr, INITIAL_TXN_ID, who)){
      return true;
    }
  }
}

bool SsiTxnManager::ReleaseReadLock(txn_id_t *lock_addr, txn_id_t holder){
  assert(*lock_addr == holder);
  auto res = atomic_cas(lock_addr, holder, INITIAL_TXN_ID);
  assert(res);
  return res;
}


// removeReader should be protected in running_txns_mutex_
void SsiTxnManager::RemoveReader(txn_id_t txn_id) {
  LOG_INFO("release SILock");
  assert(running_txns_.count(txn_id) > 0);

  // remove read lock
  auto& my_ctx = running_txns_.at(txn_id);
  auto &rw_set = my_ctx.transaction_->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {

      auto tuple_slot = tuple_entry.first;

      // we don't have reader lock on insert type
      if (tuple_entry.second == RW_TYPE_INSERT) {
        continue;
      }

      auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_slot);
      // find and delete reader whose txnId == txn_id
      auto lock_addr = GetLockAddr(reserved_area);
      {
        GetReadLock(lock_addr, txn_id);

        ReadList** head_addr = GetListAddr(reserved_area);
        ReadList fake_header;
        fake_header.next = *head_addr;
        auto prev = &fake_header;
        auto next = prev->next;
        bool find = false;

        while(next != nullptr){
          if(next->txnId == txn_id){
            find = true;
            prev->next = next->next;
            LOG_INFO("find in %ld group %ld tuple %ld", next->txnId, tile_group_id, tuple_slot);
            delete next;
            break;
          }
          prev = next;
          next = next->next;
        }

        *head_addr = fake_header.next;
        ReleaseReadLock(lock_addr, txn_id);
        assert(find);
      }
    }
  }
  LOG_INFO("release SILock finish");
}


void SsiTxnManager::CleanUp() {
  std::lock_guard<std::mutex> lock(running_txns_mutex_);

  if(running_txns_.empty()){
    return;
  }

  // find smallest begin cid of the running transaction

  // init it as max() for the case that all transactions are committed
  cid_t min_begin = std::numeric_limits<cid_t >::max();
  for(auto& item : running_txns_){
    auto &ctx = item.second;
    if(ctx.transaction_->GetEndCommitId() == INVALID_TXN_ID){
      if (ctx.transaction_->GetBeginCommitId() < min_begin) {
        min_begin = ctx.transaction_->GetBeginCommitId();  
      }
    }
  }

  auto itr = running_txns_.begin();
  while(itr != running_txns_.end()){
    auto &ctx = itr->second;
    auto end_cid = ctx.transaction_->GetEndCommitId();
    if(end_cid == INVALID_TXN_ID){
      // running transaction
      break;
    }

    if(end_cid < min_begin){
      // we can safely remove it from table

      // remove its reader mark
      LOG_INFO("remove %ld in table", ctx.transaction_->GetTransactionId());
      RemoveReader(ctx.transaction_->GetTransactionId());

      // delete transaction
      delete ctx.transaction_;

      // remove from table
      itr = running_txns_.erase(itr);
    }else{
      itr++;
    }
  }
}

}  // End storage namespace
}  // End peloton namespace
