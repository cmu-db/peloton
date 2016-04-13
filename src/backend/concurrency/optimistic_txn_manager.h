//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// optimistic concurrency control
//===--------------------------------------------------------------------===//

class OptimisticTxnManager : public TransactionManager {
 public:
  OptimisticTxnManager() {}

  virtual ~OptimisticTxnManager() {}

  static OptimisticTxnManager &GetInstance();

  virtual bool IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetOwnership(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    {
      std::lock_guard<std::mutex> lock(bucket_lock);
      running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;
    }

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();
    {
      std::lock_guard<std::mutex> lock(bucket_lock);
      running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);
    }
    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
    cid_t min_running_cid = MAX_CID;
    for (size_t i = 0; i < RUNNING_TXN_BUCKET_NUM; ++i) {
      {
        LOG_INFO("stuck in for 1.1");
        std::lock_guard<std::mutex> lock(bucket_lock);
        //auto iter = running_txn_buckets_[i].lock_table();
        LOG_INFO("stuck in for 1.2 %lu", running_txn_buckets_[i].size());
        //for (auto &it : iter) {
        for(auto it = running_txn_buckets_[i].begin(); it != running_txn_buckets_[i].end(); it++) {
          LOG_INFO("stuck in for 2.0");
          if (it->second < min_running_cid) {
            LOG_INFO("stuck in for 2");
            min_running_cid = it->second;
          }
        }
        LOG_INFO("got out of for 2");
      }
    }
    LOG_INFO("got out of for 1; min_running_cid = %lu and MAX_CID = %lu", min_running_cid, MAX_CID);
    //assert(min_running_cid > 0 && min_running_cid != MAX_CID);
    if(min_running_cid == MAX_CID) {
      LOG_INFO("Returning MAX_CID");
      return MAX_CID;
    }
    LOG_INFO("returning %lu", min_running_cid - 1);
    return min_running_cid - 1;
  }
  

private:
  std::mutex bucket_lock;
  //cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  std::map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
};
}
}
