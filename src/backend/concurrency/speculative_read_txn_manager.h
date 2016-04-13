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
  SpecTxnContext()
      : begin_cid_(MAX_CID),
        inner_dep_set_changeable_(true),
        outer_dep_count_(0),
        is_cascading_abort_(false) {}

  void SetBeginCid(const cid_t &begin_cid) {
    assert(begin_cid_ == MAX_CID);
    begin_cid_ = begin_cid;
  }

  void Clear() {
    begin_cid_ = MAX_CID;

    outer_dep_set_.clear();

    inner_dep_set_.clear();
    inner_dep_set_changeable_ = true;

    outer_dep_count_ = 0;
    is_cascading_abort_ = false;
  }

  // the begin commit id of current transaction.
  cid_t begin_cid_;

  // outer_dep_set is thread_local, and is safe to modify it.
  std::unordered_set<txn_id_t> outer_dep_set_;

  // inner_dep_set is modified by other transactions. so lock is required.
  Spinlock inner_dep_set_lock_;
  std::unordered_set<txn_id_t> inner_dep_set_;
  // whether the inner dependent set is still changeable.
  volatile bool inner_dep_set_changeable_;

  std::atomic<size_t> outer_dep_count_;            // default: 0
  volatile std::atomic<bool> is_cascading_abort_;  // default: false
};

extern thread_local SpecTxnContext spec_txn_context;

//===--------------------------------------------------------------------===//
// optimistic concurrency control with speculative reads
//===--------------------------------------------------------------------===//

class SpeculativeReadTxnManager : public TransactionManager {
 public:
  SpeculativeReadTxnManager() {}

  virtual ~SpeculativeReadTxnManager() {}

  static SpeculativeReadTxnManager &GetInstance();

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

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;
    spec_txn_context.SetBeginCid(begin_cid);

    cid_t bucket_id = txn_id % RUNNING_TXN_BUCKET_NUM;
    assert(running_txn_buckets_[bucket_id].contains(txn_id) == false);
    running_txn_buckets_[bucket_id][txn_id] = &spec_txn_context;
    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();

    cid_t bucket_id = txn_id % RUNNING_TXN_BUCKET_NUM;
    bool ret = running_txn_buckets_[bucket_id].erase(txn_id);
    if (ret == false) {
      assert(false);
    }
    spec_txn_context.Clear();

    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
    cid_t min_running_cid = MAX_CID;
    for (size_t i = 0; i < RUNNING_TXN_BUCKET_NUM; ++i) {
      {
        auto iter = running_txn_buckets_[i].lock_table();
        for (auto &it : iter) {
          if (it.second->begin_cid_ < min_running_cid) {
            min_running_cid = it.second->begin_cid_;
          }
        }
      }
    }
    assert(min_running_cid > 0 && min_running_cid != MAX_CID);
    return min_running_cid - 1;
  }

  // is it because this dependency has been registered before?
  // or the dst txn does not exist?
  bool RegisterDependency(const txn_id_t &dst_txn_id) {
    txn_id_t src_txn_id = current_txn->GetTransactionId();
    // if this dependency has been registered before, then return.
    if (spec_txn_context.outer_dep_set_.find(dst_txn_id) !=
        spec_txn_context.outer_dep_set_.end()) {
      return true;
    }

    bool changeable = true;
    bool ret =
        running_txn_buckets_[dst_txn_id % RUNNING_TXN_BUCKET_NUM].update_fn(
            dst_txn_id, [&changeable, &src_txn_id](SpecTxnContext * context) {
      context->inner_dep_set_lock_.Lock();
      if (context->inner_dep_set_changeable_ == true) {
        assert(context->inner_dep_set_.find(src_txn_id) ==
               context->inner_dep_set_.end());
        context->inner_dep_set_.insert(src_txn_id);
      } else {
        changeable = false;
      }
      context->inner_dep_set_lock_.Unlock();
    });

    if (changeable == false || ret == false) {
      return false;
    }
    spec_txn_context.outer_dep_set_.insert(dst_txn_id);
    spec_txn_context.outer_dep_count_++;
    return true;
  }

  bool IsCommittable() {
    while (true) {
      if (spec_txn_context.outer_dep_count_ == 0) {
        return true;
      }
      if (spec_txn_context.is_cascading_abort_ == true) {
        return false;
      }
      _mm_pause();
    }
  }

  void NotifyCommit() {
    // some other transactions may also modify my inner dep set.
    // so lock first.
    spec_txn_context.inner_dep_set_lock_.Lock();
    for (auto &child_txn_id : spec_txn_context.inner_dep_set_) {
      running_txn_buckets_[child_txn_id % RUNNING_TXN_BUCKET_NUM]
          .update_fn(child_txn_id, [](SpecTxnContext * context) {
        assert(context->outer_dep_count_ > 0);
        context->outer_dep_count_--;
      });
    }
    spec_txn_context.inner_dep_set_changeable_ = false;
    spec_txn_context.inner_dep_set_lock_.Unlock();
  }

  void NotifyAbort() {
    // some other transactions may also modify my inner dep set.
    // so lock first.
    spec_txn_context.inner_dep_set_lock_.Lock();
    for (auto &child_txn_id : spec_txn_context.inner_dep_set_) {
      running_txn_buckets_[child_txn_id % RUNNING_TXN_BUCKET_NUM]
          .update_fn(child_txn_id, [](SpecTxnContext * context) {
        assert(context->outer_dep_count_ > 0);
        context->is_cascading_abort_ = true;
      });
    }
    spec_txn_context.inner_dep_set_changeable_ = false;
    spec_txn_context.inner_dep_set_lock_.Unlock();
  }

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

 private:
  // records all running transactions.
  cuckoohash_map<txn_id_t, SpecTxnContext *>
      running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
};
}
}
