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

extern thread_local std::unordered_map<oid_t, std::unordered_map<oid_t, bool>>
    pessimistic_released_rdlock;

//===--------------------------------------------------------------------===//
// pessimistic concurrency control
//===--------------------------------------------------------------------===//
class PessimisticTxnManager : public TransactionManager {
 public:
  PessimisticTxnManager() {}
  virtual ~PessimisticTxnManager() {}

  static PessimisticTxnManager &GetInstance();

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

  virtual bool AcquireOwnership(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetOwnership(const oid_t &tile_group_id,
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

  bool ReleaseReadLock(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);
};
}
}