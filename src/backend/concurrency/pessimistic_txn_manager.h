//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pessimistic_txn_manager.h
//
// Identification: src/backend/concurrency/pessimistic_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

extern thread_local std::unordered_map<oid_t, std::unordered_set<oid_t>>
    pessimistic_released_rdlock;

//===--------------------------------------------------------------------===//
// pessimistic concurrency control
//===--------------------------------------------------------------------===//
class PessimisticTxnManager : public TransactionManager {
 public:
  PessimisticTxnManager() {}
  virtual ~PessimisticTxnManager() {}

  static PessimisticTxnManager &GetInstance();

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

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);

    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
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
    assert(min_running_cid > 0 && min_running_cid != MAX_CID);
    return min_running_cid - 1;
  }

 private:
#define READ_COUNT_MASK 0xFF
#define TXNID_MASK 0x00FFFFFFFFFFFFFF
  inline txn_id_t PACK_TXNID(txn_id_t txn_id, int read_count) {
    return ((long)(read_count & READ_COUNT_MASK) << 56) | (txn_id & TXNID_MASK);
  }
  inline txn_id_t EXTRACT_TXNID(txn_id_t txn_id) { return txn_id & TXNID_MASK; }
  inline txn_id_t EXTRACT_READ_COUNT(txn_id_t txn_id) {
    return (txn_id >> 56) & READ_COUNT_MASK;
  }

  void ReleaseReadLock(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
};
}
}