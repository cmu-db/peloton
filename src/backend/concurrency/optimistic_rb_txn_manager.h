//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_rb_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"

namespace peloton {
namespace concurrency {

extern thread_local storage::RollbackSegmentPool *current_segment_pool;
//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public TransactionManager {
 public:
  OptimisticRbTxnManager() {}

  virtual ~OptimisticRbTxnManager() {}

  static OptimisticRbTxnManager &GetInstance();

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


  virtual bool PerformInsert(const ItemPointer &location);

  virtual bool PerformRead(const ItemPointer &location);

  virtual bool PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const ItemPointer &location);
  virtual void PerformDelete(const ItemPointer &old_location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;
    current_segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);


    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
  // TODO: rewrite this function since we call it after every txn
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

  // init reserved area of a tuple
  // Spinlock (8 bytes) | ColBitmap (16 bytes)
  void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    new (reserved_area + lock_offset) Spinlock();
    new (reserved_area + mask_left_half_offset) ColBitmap();
  }

  // reused the (prev) item pointer field to point to rollback segment
  inline void SetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                          char * const seg_ptr) {
    char **rb_seg_ptr = (char **)tile_group_header->GetPrevItempointerField(tuple_id);
    *rb_seg_ptr = seg_ptr;
  }

  inline const char *GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    char **rb_seg_ptr = (char **)tile_group_header->GetPrevItempointerField(tuple_id);
    return *rb_seg_ptr;
  }

  inline ColBitmap *GetBitMap(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    return (ColBitmap*)(tile_group_header->GetReservedFieldRef(tuple_id) + mask_left_half_offset);
  }

  inline void LockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Lock();
  }

  inline void UnlockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Unlock();
  }

  inline bool IsColSet(const storage::TileGroupHeader *tile_group_header,
                       const oid_t tuple_id, const oid_t col_id) {
    auto col_bitmap = GetBitMap(tile_group_header, tuple_id);
    return col_bitmap->test(col_id);
  }

  inline void SetAllCol(const storage::TileGroupHeader *tile_group_header, const oid_t &tuple_id) {
    auto col_bitmap = GetBitMap(tile_group_header, tuple_id);
    col_bitmap->set();
  }

  inline void SetCol(const storage::TileGroupHeader *tile_group_header, const oid_t &tuple_id,
                     const oid_t col_id) {
    auto col_bitmap = GetBitMap(tile_group_header, tuple_id);
    col_bitmap->set(col_id);
  }



 private:
  static const size_t lock_offset = 0;
  static const size_t mask_left_half_offset = lock_offset + sizeof(oid_t);
  cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> living_txn_buckets_;
};
}
}