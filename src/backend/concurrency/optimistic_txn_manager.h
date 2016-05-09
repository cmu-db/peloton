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
#include "backend/statistics/stats_aggregator.h"
#include "backend/storage/tile_group.h"

extern StatsType peloton_stats_mode;

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// optimistic concurrency control
//===--------------------------------------------------------------------===//

class OptimisticTxnManager : public TransactionManager {
 public:
  OptimisticTxnManager() : last_epoch_(0) {}

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

  virtual bool PerformInsert(const ItemPointer &location);

  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const ItemPointer &location);

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;

    if (peloton_stats_mode != STATS_TYPE_INVALID) {
      peloton::stats::backend_stats_context->GetTxnLatencyMetric().StartTimer();
    }

    return txn;
  }

  virtual void EndTransaction() {
    if (peloton_stats_mode != STATS_TYPE_INVALID) {
      peloton::stats::backend_stats_context->GetTxnLatencyMetric().RecordLatency();
    }

    txn_id_t txn_id = current_txn->GetTransactionId();

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);

    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
    cid_t curr_epoch = EpochManagerFactory::GetInstance().GetEpoch();
    
    if (last_epoch_ != curr_epoch) {  
      last_epoch_ = curr_epoch;

      cid_t min_running_cid = MAX_CID;
      for (size_t i = 0; i < RUNNING_TXN_BUCKET_NUM; ++i) {
        {
          auto iter = running_txn_buckets_[i].lock_table();
          for (auto &it : iter) {
            if (it.second < min_running_cid) {
              min_running_cid = it.second;
            }
          }
        }
      }
      assert(min_running_cid > 0);
      if (min_running_cid != MAX_CID) {
        last_max_commit_cid_ = min_running_cid - 1;
      } else {
      // in this case, there's no running transaction.
        last_max_commit_cid_ = GetNextCommitId() - 1;
      }

    }

    return last_max_commit_cid_;
  }

 private:
  cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  cid_t last_epoch_;
  cid_t last_max_commit_cid_;
};
}
}
