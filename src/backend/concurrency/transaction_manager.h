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

#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace concurrency {

extern thread_local Transaction *current_txn;

#define RUNNING_TXN_BUCKET_NUM 10

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
  }

  virtual ~TransactionManager() {}

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  cid_t GetNextCommitId() { return next_cid_++; }

  virtual bool IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  bool IsVisbleOrDirty(const storage::Tuple *, const ItemPointer &) {
    return false;
  }

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id) = 0;

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  virtual void SetOwnership(const oid_t &tile_group_id,
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

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }
  
  //for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; }

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    return txn;
  }

  virtual void EndTransaction() = 0;

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  void ResetStates() {
    next_txn_id_ = START_TXN_ID;
    next_cid_ = START_CID;
  }

  // this function generates the maximum commit id of committed transactions.
  // please note that this function only returns a "safe" value instead of a precise value.
  virtual cid_t GetMaxCommittedCid() {
    return 1;
  }

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;
  
};
}  // End storage namespace
}  // End peloton namespace
