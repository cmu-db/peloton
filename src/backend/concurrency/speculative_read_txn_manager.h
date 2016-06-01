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

  virtual VisibilityType IsVisible(
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

  virtual void YieldOwnership(const oid_t &tile_group_id,
    const oid_t &tuple_id);

  virtual bool PerformInsert(const ItemPointer &location);

  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const ItemPointer &location);

  virtual void PerformDelete(const ItemPointer &location);

  virtual Transaction *BeginTransaction();

  virtual void EndTransaction();


  // is it because this dependency has been registered before?
  // or the dst txn does not exist?
  bool RegisterDependency(const txn_id_t &dst_txn_id);

  bool IsCommittable();

  void NotifyCommit();

  void NotifyAbort();

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

 private:
  // records all running transactions.
  cuckoohash_map<txn_id_t, SpecTxnContext *>
      running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
};
}
}
