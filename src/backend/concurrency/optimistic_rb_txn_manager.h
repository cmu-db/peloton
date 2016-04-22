//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_txn_manager.h
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
// optimistic concurrency control
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public TransactionManager {
  struct ColBitmap {
    oid_t left_half;
    oid_t right_half;
    ColBitmap(): left_half(0), right_half(0){}
    ~ColBitmap(){}
  };

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

  virtual void SetOwnership(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  // Check if an update need to generate a new rollback segment
  virtual bool NeedNewRbSegment(const storage::TileGroupHeader *const tile_group_header,
                                const oid_t &tuple_id,
                                const planner::ProjectInfo::TargetList &target_list);

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

  // init reserved area of a tuple
  // Spinlock (8 bytes) | ColBitmap (16 bytes)
  void InitTupleReserved(const oid_t tile_group_id, const oid_t tuple_id) {

    auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(tile_group_id)->GetHeader();

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    new (reserved_area + lock_offset) Spinlock();
    new (reserved_area + mask_left_half_offset) ColBitmap();
  }

  inline ColBitmap *GetBitMap(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    return (ColBitmap*)(tile_group_header->GetReservedFieldRef(tuple_id) + mask_left_half_offset);
  }

  inline void LockTuple(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Lock();
  }

  inline void UnlockTuple(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Unlock();
  }

  inline bool IsColSet(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t tuple_id, const oid_t col_id) {
    assert(col_id > sizeof(oid_t) * 16);
    auto col_bitmap = GetBitMap(tile_group_header, tuple_id);
    oid_t mask = (0x01u << (col_id % (sizeof(oid_t) * 8)));

    if (col_id > (sizeof(oid_t)*8) ) {
      // left half
      return (col_bitmap->left_half & mask) != 0;
    } else {
      // right half
      return (col_bitmap->right_half & mask) != 0;
    }
  }

  inline void SetCol(ColBitmap *col_bitmap, const oid_t col_id) {
    assert(col_id > sizeof(oid_t) * 16);
    oid_t mask = (0x01u << (col_id % (sizeof(oid_t) * 8)));
    if (col_id > (sizeof(oid_t)*8)) {
      // left half
      col_bitmap->left_half |= mask;
    } else {
      // right half
      col_bitmap->right_half |= mask;
    }
  }

  inline void SetCol(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id,
                     const oid_t col_id) {
    auto col_bitmap = GetBitMap(tile_group_header, tuple_id);
    SetCol(col_bitmap, col_id);
  }

  inline bool IsTargetOverlapped(const planner::ProjectInfo::TargetList &target_list,
                                 const ColBitmap *original_bitmap) {
    ColBitmap target_bitmap;
    for (auto target : target_list) {
      SetCol(&target_bitmap, target.first);
    }

    return (target_bitmap.left_half == original_bitmap->left_half
            && target_bitmap.right_half == original_bitmap->right_half);
  }

 private:
  static const size_t lock_offset = 0;
  static const size_t mask_left_half_offset = lock_offset + sizeof(oid_t);
  static const size_t mask_right_half_offset = mask_left_half_offset + sizeof(oid_t);
  cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
};
}
}