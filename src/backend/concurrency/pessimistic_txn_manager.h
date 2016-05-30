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
  PessimisticTxnManager(){}
  virtual ~PessimisticTxnManager() {}

  static PessimisticTxnManager &GetInstance();

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

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  // init reserved area of a tuple
  // creator txnid | lock (for read list) | read list head
  // The txn_id could only be the cur_txn's txn id.
  void InitTupleReserved(const oid_t tile_group_id, const oid_t tuple_id) {

    auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(tile_group_id)->GetHeader();

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    *(int *)(reserved_area + COUNTER_OFFSET) = 0;
  }

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);
    LOG_TRACE("Begin txn %lu", txn_id);


    return txn;
  }

  inline Spinlock *GetSpinlockField(const storage::TileGroupHeader *const tile_group_header,
                                    const oid_t &tuple_id) {
    return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);
  }

  inline int* GetReaderCountField(const storage::TileGroupHeader *const tile_group_header,
                                  const oid_t &tuple_id) {
    return (int *)(tile_group_header->GetReservedFieldRef(tuple_id) + COUNTER_OFFSET);
  }

  virtual void EndTransaction() {


    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    delete current_txn;
    current_txn = nullptr;

    pessimistic_released_rdlock.clear();
  }

 private:

 static const int LOCK_OFFSET = 0;
 static const int COUNTER_OFFSET = (LOCK_OFFSET + sizeof(Spinlock));
//#define READ_COUNT_MASK 0xFF
//#define TXNID_MASK 0x00FFFFFFFFFFFFFF
//  inline txn_id_t PACK_TXNID(txn_id_t txn_id, int read_count) {
//    return ((long)(read_count & READ_COUNT_MASK) << 56) | (txn_id & TXNID_MASK);
//  }
//  inline txn_id_t EXTRACT_TXNID(txn_id_t txn_id) { return txn_id & TXNID_MASK; }
//  inline txn_id_t EXTRACT_READ_COUNT(txn_id_t txn_id) {
//    return (txn_id >> 56) & READ_COUNT_MASK;
//  }

  void ReleaseReadLock(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

};
}
}