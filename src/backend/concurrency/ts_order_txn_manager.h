//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ts_order_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_txn_manager.h
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
// timestamp ordering
//===--------------------------------------------------------------------===//

class TsOrderTxnManager : public TransactionManager {
 public:
  TsOrderTxnManager() {}

  virtual ~TsOrderTxnManager() {}

  static TsOrderTxnManager &GetInstance();

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

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    current_txn = txn;

    return txn;
  }

  virtual void EndTransaction() {
    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    delete current_txn;
    current_txn = nullptr;
  }


 private:
  
  static const int LOCK_OFFSET = 0;
  static const int LAST_READER_OFFSET = (LOCK_OFFSET + 8);


  Spinlock *GetSpinlockField(
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id);

  cid_t GetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  bool SetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  // init reserved area of a tuple
  void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    *(cid_t*)(reserved_area + LAST_READER_OFFSET) = 0;
  }


};
}
}