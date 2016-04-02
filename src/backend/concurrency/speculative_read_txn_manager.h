//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/speculative_read_txn_manager.h
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

  void Clear() {
    inner_dep_set_.clear();
    outer_dep_set_.clear();
    outer_dep_count_ = 0;
    is_cascading_abort_ = false;
  }

  std::unordered_set<txn_id_t> inner_dep_set_;
  std::unordered_set<txn_id_t> outer_dep_set_;
  std::atomic<size_t> outer_dep_count_; // default: 0
  std::atomic<bool> is_cascading_abort_; // default: false
};

extern thread_local SpecTxnContext spec_txn_context;

enum RegisterRetType {
  REGISTER_RET_TYPE_DUPLICATE = 0,
  REGISTER_RET_TYPE_SUCCESS = 1,
  REGISTER_RET_TYPE_NOT_FOUND = 2
};

class SpeculativeReadTxnManager : public TransactionManager {
 public:
  SpeculativeReadTxnManager() {}

  virtual ~SpeculativeReadTxnManager() {}

  static SpeculativeReadTxnManager &GetInstance();

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

  virtual bool AcquireOwnership(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);
  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);
  
  virtual void PerformDelete(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual Transaction *BeginTransaction() {
    Transaction *txn = TransactionManager::BeginTransaction();
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn->GetTransactionId()) == running_txns_.end());
      running_txns_[txn->GetTransactionId()] = &spec_txn_context;
    }
    return txn;
  }

  virtual void EndTransaction() {
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(current_txn->GetTransactionId()) != running_txns_.end());
      running_txns_.erase(current_txn->GetTransactionId());
    }
    spec_txn_context.Clear();
    TransactionManager::EndTransaction();
  }

  // is it because this dependency has been registered before?
  // or the dst txn does not exist?
  RegisterRetType RegisterDependency(const txn_id_t &dst_txn_id) {
    txn_id_t src_txn_id = current_txn->GetTransactionId();
    // if this dependency has been registered before, then return.
    if (spec_txn_context.outer_dep_set_.find(dst_txn_id) != 
          spec_txn_context.outer_dep_set_.end()) {
      return REGISTER_RET_TYPE_DUPLICATE;
    }
    
    // critical section.
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      // the dst txn has been committed.
      if (running_txns_.find(dst_txn_id) == running_txns_.end()) {
        return REGISTER_RET_TYPE_NOT_FOUND;
      }
      auto &dst_txn_context = *(running_txns_.at(dst_txn_id));
      dst_txn_context.inner_dep_set_.insert(src_txn_id);
    }

    spec_txn_context.outer_dep_set_.insert(dst_txn_id);
    spec_txn_context.outer_dep_count_++;
    return REGISTER_RET_TYPE_SUCCESS;
  }
 
  bool IsCommittable() {
    while (true) {
      if (spec_txn_context.outer_dep_count_ == 0) {
        return true;
      }
      if (spec_txn_context.is_cascading_abort_ == true) {
        return false;
      }
    }
  }

  void NotifyCommit() {
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      // some other transactions may also modify my inner dep set.
      for (auto &child_txn_id : spec_txn_context.inner_dep_set_) {
        if (running_txns_.find(child_txn_id) == running_txns_.end()) {
          continue;
        }
        auto &child_txn_context = *(running_txns_.at(child_txn_id));
        assert(child_txn_context.outer_dep_count_ > 0);
        child_txn_context.outer_dep_count_--;
      }
    }
  }

  void NotifyAbort() {
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      // some other transactions may also modify my inner dep set.
      for (auto &child_txn_id : spec_txn_context.inner_dep_set_) {
        if (running_txns_.find(child_txn_id) == running_txns_.end()) {
          continue;
        }
        auto &child_txn_context = *(running_txns_.at(child_txn_id));
        child_txn_context.is_cascading_abort_ = true;
      }
    }
  }

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  private:
    // should be changed to libcuckoo.
    std::mutex running_txns_mutex_;
    // records all running transactions.
    std::unordered_map<txn_id_t, SpecTxnContext*> running_txns_;

};
}
}