//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pessimistic_transaction_manager.h
//
// Identification: src/backend/concurrency/pessimistic_transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

extern thread_local std::unordered_map<oid_t, std::unordered_map<oid_t, bool>>
    released_rdlock;

class PessimisticTransactionManager : public TransactionManager {
 public:
  PessimisticTransactionManager() {
    released_rdlock =
        std::unordered_map<oid_t, std::unordered_map<oid_t, bool>>();
  }

  virtual ~PessimisticTransactionManager() {}

  static PessimisticTransactionManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool IsOwner(storage::TileGroup *tile_group,
                       const txn_id_t &tuple_txn_id);

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

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

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

  bool ReleaseReadLock(storage::TileGroup *tile_group, const oid_t &tuple_id);
};
}
}