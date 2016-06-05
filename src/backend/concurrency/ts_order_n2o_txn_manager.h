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

class TsOrderN2OTxnManager : public TransactionManager {
 public:
  TsOrderN2OTxnManager() {}

  virtual ~TsOrderN2OTxnManager() {}

  static TsOrderN2OTxnManager &GetInstance();

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

  virtual bool PerformInsert(const ItemPointer &location __attribute__((unused)))
    {assert(false); return false;}

  // The itemptr_ptr is the address of the head node of the version chain, 
  // which is directly pointed by the primary index.
  bool PerformInsert(const ItemPointer &location, ItemPointer *itemptr_ptr);

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

  ItemPointer *GetHeadPtr(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t tuple_id);

 private:

  static const int LOCK_OFFSET = 0;
  static const int LAST_READER_OFFSET = (LOCK_OFFSET + 8);
  static const int ITEM_POINTER_OFFSET = (LAST_READER_OFFSET + 8);


  Spinlock *GetSpinlockField(
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id);

  cid_t GetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  void SetLastReaderCid(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id, const cid_t &last_read_ts);

  void SetHeadPtr(
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t tuple_id, ItemPointer *item_ptr);

  // Init reserved area of a tuple
  // delete_flag is used to mark that the transaction that owns the tuple
  // has deleted the tuple
  // Primary index header ptr (8 bytes)
  static void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    *(cid_t*)(reserved_area + LAST_READER_OFFSET) = 0;
    *(reinterpret_cast<ItemPointer**>(reserved_area + ITEM_POINTER_OFFSET)) = nullptr;
  }
};
}
}