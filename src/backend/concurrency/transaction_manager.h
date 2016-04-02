//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <unordered_map>
#include <list>

#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace concurrency {

// class Transaction;

extern thread_local Transaction *current_txn;

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
  }

  virtual ~TransactionManager() {}

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  cid_t GetNextCommitId() { return next_cid_++; }

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header,
                         const oid_t &tuple_id) = 0;

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id) = 0;

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id) = 0;

  virtual bool AcquireOwnership(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  virtual bool PerformInsert(const oid_t &tile_group_id,
                             const oid_t &tuple_id) = 0;

  virtual bool PerformRead(const oid_t &tile_group_id,
                           const oid_t &tuple_id) = 0;

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location) = 0;

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  virtual void PerformDelete(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  void RecycleTupleSlot(const oid_t &tile_group_id, const oid_t &tuple_id) {
    auto &manager = catalog::Manager::GetInstance();
    storage::TileGroup *tile_group = manager.GetTileGroup(tile_group_id).get();
    expression::ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_id);
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
    auto transaction_id = current_txn->GetTransactionId();
    tile_group_header -> RecycleTupleSlot(0, tile_group->GetTableId(), tile_group_id, tuple_id, transaction_id); // FIXME: get DB ID
  }

  /*
   * Write a virtual function to push deleted and verified (acc to optimistic
   * concurrency control) tuples into possibly free from all underlying
   * concurrency implementations of transactions.
   */


  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  //for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; };

  virtual Transaction *BeginTransaction() {
    Transaction *txn =
        new Transaction(GetNextTransactionId(), GetNextCommitId());
    current_txn = txn;
    {
      std::lock_guard<std::mutex> lock(running_txns_list_mutex_);
      assert(std::find(running_txns_list_.begin(), running_txns_list_.end(), txn->GetTransactionId()) == running_txns_list_.end());
      running_txns_list_.push_back(txn->GetTransactionId());
    }
    return txn;
  }

  virtual void EndTransaction() {
    {
      std::lock_guard<std::mutex> lock(running_txns_list_mutex_);
      assert(std::find(running_txns_list_.begin(), running_txns_list_.end(), current_txn->GetTransactionId()) != running_txns_list_.end());
      running_txns_list_.remove(current_txn->GetTransactionId());
    }
    delete current_txn;
    current_txn = nullptr;
  }

  virtual txn_id_t GetOldestLiveTransaction(){
    {
      std::lock_guard<std::mutex> lock(running_txns_list_mutex_);
      if(running_txns_list_.size() == 0)
        return INVALID_TXN_ID;
      return running_txns_list_.front();
    }
  }

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  void ResetStates() {
    next_txn_id_ = START_TXN_ID;
    next_cid_ = START_CID;
  }

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;
  // records all running transactions.
  std::mutex running_txns_list_mutex_;
  std::list<txn_id_t> running_txns_list_;
};
}  // End storage namespace
}  // End peloton namespace
