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
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    return txn;
  }

  virtual void EndTransaction() {
    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    delete current_txn;
    current_txn = nullptr;
  }


 private:
  cid_t GetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) {
    char *reserved_field = tile_group_header->GetReservedFieldRef(tuple_id);

    return *((cid_t *)reserved_field);
  }

  // atomic set max cid
  void AtomicMax(char* addr, cid_t last_read_ts);

  void SetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id, const cid_t &last_read_ts);

//   void SetLastReaderCid(
//     const storage::TileGroupHeader *const tile_group_header,
//     const oid_t &tuple_id, const cid_t &last_read_ts) {
//   char *addr = tile_group_header->GetReservedFieldRef(tuple_id);
  
//   while(true) {
//     auto old = *((cid_t*)addr);
//     if(old > last_read_ts) {
//       return;
//     }else if ( __sync_bool_compare_and_swap(addr, old, last_read_ts) ) {
//       return;
//     }
//   }
// }

};
}
}