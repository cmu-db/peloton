//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/spec_rowo_txn_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {


struct SpecTxnContext {
  SpecTxnContext() : outer_dep_count_(0), is_cascading_abort_(false) {}
  std::unordered_set<txn_id_t> inner_dep_set_;
  std::unordered_set<txn_id_t> outer_dep_set_;
  size_t outer_dep_count_; // default: 0
  bool is_cascading_abort_; // default: false
};


class SpecRowoTxnManager : public TransactionManager {
 public:
  SpecRowoTxnManager() {}

  virtual ~SpecRowoTxnManager() {}

  static SpecRowoTxnManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id);

  virtual bool IsAccessable(storage::TileGroup *tile_group,
                            const oid_t &tuple_id);

  virtual bool AcquireTuple(storage::TileGroup *tile_group,
                            const oid_t &physical_tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                            const ItemPointer &new_location);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void SetDeleteVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void SetUpdateVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual Transaction *BeginTransaction() {
    Transaction *txn = TransactionManager::BeginTransaction();
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn->GetTransactionId()) == running_txns_.end());
      running_txns_[txn->GetTransactionId()];
    }
    return txn;
  }

  virtual void EndTransaction() {
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(current_txn->GetTransactionId()) != running_txns_.end());
      running_txns_.erase(current_txn->GetTransactionId());
    }
    TransactionManager::EndTransaction();
  }

  // we must distinguish why return false.
  // is it because this dependency has been registered before?
  // or the dst txn does not exist?
  bool RegisterDependency(const txn_id_t &dst_txn_id) {
    {
      txn_id_t src_txn_id = current_txn->GetTransactionId();
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(src_txn_id) != running_txns_.end());
      auto &src_txn_dep = running_txns_.at(src_txn_id);
      // if this dependency has been registered before.
      if (src_txn_dep.outer_dep_set_.find(dst_txn_id) != src_txn_dep.outer_dep_set_.end()) {
        return false;
      }
      // the dst txn has been committed.
      if (running_txns_.find(dst_txn_id) == running_txns_.end()) {
        return false;
      }
      auto &dst_txn_dep = running_txns_.at(dst_txn_id);
      dst_txn_dep.inner_dep_set_.insert(src_txn_id);
      src_txn_dep.outer_dep_set_.insert(dst_txn_id);
      src_txn_dep.outer_dep_count_++;
      return true;
    }
  }
  // return 0: decision not made.
  // return -1: abort.
  // return 1: commit. 
  int IsCommittable() {
    {
      txn_id_t txn_id = current_txn->GetTransactionId();
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn_id) != running_txns_.end());
      auto &txn_dep = running_txns_.at(txn_id);
      if (txn_dep.outer_dep_count_ == 0) {
        return 1;
      }
      if (txn_dep.is_cascading_abort_ == true) {
        return -1;
      }
      return 0;
    }
  }

  void NotifyCommit() {
    {
      txn_id_t txn_id = current_txn->GetTransactionId();
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn_id) != running_txns_.end());
      auto &txn_dep = running_txns_.at(txn_id);
      for (auto &child_txn_id : txn_dep.inner_dep_set_) {
        if (running_txns_.find(child_txn_id) == running_txns_.end()) {
          continue;
        }
        auto &child_txn_dep = running_txns_.at(child_txn_id);
        child_txn_dep.outer_dep_count_--;
      }
    }
  }

  void NotifyAbort() {
    {
      txn_id_t txn_id = current_txn->GetTransactionId();
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn_id) != running_txns_.end());
      auto &txn_dep = running_txns_.at(txn_id);
      for (auto &child_txn_id : txn_dep.inner_dep_set_) {
        if (running_txns_.find(child_txn_id) == running_txns_.end()) {
          continue;
        }
        auto &child_txn_dep = running_txns_.at(child_txn_id);
        child_txn_dep.is_cascading_abort_ = true;
      }
    }
  }

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  private:
    // should be changed to libcuckoo.
    std::mutex running_txns_mutex_;
    // records all running transactions.
    std::unordered_map<txn_id_t, SpecTxnContext> running_txns_;

};
}
}