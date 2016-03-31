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
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    auto& my_cxt = running_txns_.at(current_txn->GetTransactionId());

    auto reserved_area = tile_group_header->GetReservedFieldRef(physical_tuple_id);
    ReadList *header = *GetListAddr(reserved_area);
    header = header->next;
    while(header != nullptr){
      // if there is a SIREAD lock(rl) on x

      auto owner = header->txnId;
      header = header->next;
      if(owner == txn_id){
        continue;
      }
      assert(running_txns_.count(owner) > 0);
      auto& ctx = running_txns_.at(owner);
      auto endCId = ctx.transaction_->GetEndCommitId();
      if(endCId == INVALID_TXN_ID ||
        endCId > current_txn->GetBeginCommitId()){
        // with rl.owner is running
        //               or commit(rl.owner) > begin(T)
        LOG_INFO("abort in write");
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

  // get read lock
  ReadList *reader = new ReadList(transaction_id);
  std::mutex *read_lock = *GetLockAddr(reserved_area);
  ReadList *head = *GetListAddr(reserved_area);
  {
    LOG_INFO("SI read phrase 1 txn %ld group %ld tid %ld", transaction_id, tile_group_id, tuple_id);
    std::lock_guard<std::mutex> lock(*read_lock);
    reader->next = head->next;
    head->next = reader;
    if(reader->next != nullptr){
      LOG_INFO("reader next is %ld", reader->next->txnId);
    }
  }

  // existing SI code
  current_txn->RecordRead(tile_group_id, tuple_id);

  // for each new version
  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    auto& my_cxt = running_txns_.at(current_txn->GetTransactionId());
    LOG_INFO("SI read phrase 2");
    auto tid = tuple_id;
    while (true) {
      ItemPointer next_item = tile_group_header->GetNextItemPointer(tid);
      // if there is no next tuple.
      if (next_item.IsNull() == true) {
        break;
      }
      reserved_area = GetReservedAreaAddr(next_item.block, next_item.offset);
      auto owner = *GetCreatorAddr(reserved_area);
      LOG_INFO("%ld %ld owner is %ld", next_item.block, next_item.offset, owner);
      if(running_txns_.count(owner) == 0){
        tile_group_header = catalog::Manager::GetInstance().GetTileGroup(next_item.block)->GetHeader();
        tid = next_item.offset;
        continue;
      }else if (owner == transaction_id){
        tile_group_header = catalog::Manager::GetInstance().GetTileGroup(next_item.block)->GetHeader();
        tid = next_item.offset;
        LOG_INFO("check in read, escape myself");
        continue;
      }

      assert(running_txns_.count(owner) > 0);
      auto& ctx = running_txns_.at(owner);
      if(ctx.transaction_->GetEndCommitId() != INVALID_TXN_ID){
        // if creator committed
        if(ctx.out_conflict_){
          LOG_INFO("abort in read");
          return false;
        }
      }
      LOG_INFO("set %ld in, set %ld out", owner, current_txn->GetTransactionId());
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

  return ret;
}

Result SsiTxnManager::AbortTransaction() {
  LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  {
    std::lock_guard<std::mutex> lock(running_txns_mutex_);
    assert(running_txns_.count(current_txn->GetTransactionId()) > 0);
    CleanUp();
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
void SsiTxnManager::InitTupleReserved(const txn_id_t t, const oid_t tile_group_id, const oid_t tuple_id){
  LOG_INFO("init reserved txn %ld, group %ld tid %ld", t, tile_group_id, tuple_id);

  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) == t);
  assert(current_txn->GetTransactionId() == t);

  auto reserved_area = GetReservedAreaAddr(tile_group_id, tuple_id);

  *GetCreatorAddr(reserved_area) = t;
  *GetLockAddr(reserved_area) = new std::mutex();
  *GetListAddr(reserved_area) = new ReadList();
  assert(*GetCreatorAddr(reserved_area) == t);
}


// clean up should runs in lock protection
void SsiTxnManager::CleanUp() {
  LOG_INFO("release SILock");
  auto txn_id = current_txn->GetTransactionId();
  assert(running_txns_.count(txn_id) > 0);

  // remove read lock
  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {

      auto tuple_slot = tuple_entry.first;
      LOG_INFO("in group %ld tuple %ld", tile_group_id, tuple_slot);
      if (tuple_entry.second == RW_TYPE_INSERT) {
        continue;
      }

      auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_slot);
      ReadList* prev = *GetListAddr(reserved_area);
      ReadList* next = prev->next;
      auto read_lock = *GetLockAddr(reserved_area);
      {
        std::lock_guard<std::mutex> lock(*read_lock);
        bool find = false;
        while(next != nullptr){
          if(next->txnId == txn_id){
            find = true;
            prev->next = next->next;
            delete next;
            if(prev->next != nullptr){
              LOG_INFO("find! next is %ld", prev->next->txnId);
            }
            break;
          }
          prev = next;
          next = next->next;
        }

        assert(find);
      }
    }
  }
  LOG_INFO("release SILock finish");
  running_txns_.erase(txn_id);
}


}  // End storage namespace
}  // End peloton namespace
