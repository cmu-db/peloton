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
      running_txns_[txn->GetTransactionId()] = txn;
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

  bool RegisterDependency(const txn_id_t &depend_txn_id, const txn_id_t &current_txn_id) {
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      if (running_txns_.find(depend_txn_id) == running_txns_.end()) {
        return false;
      } else{
        Transaction *txn = running_txns_.at(depend_txn_id);
        txn->RegisterDependency(current_txn_id);
        return true;
      }
    }
  }

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  private:
    // should be changed to libcuckoo.
    std::mutex running_txns_mutex_;
    std::unordered_map<txn_id_t, Transaction*> running_txns_;

};
}
}