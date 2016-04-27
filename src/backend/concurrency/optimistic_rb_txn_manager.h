//===----------------------------------------------------------------------===//
//
//                         PelotonDB
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

// Each transaction has a RollbackSegmentPool
extern thread_local storage::RollbackSegmentPool *current_segment_pool;
//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public TransactionManager {
  public:
  typedef char* RBSegType;

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

  inline bool IsInserted(
      const storage::TileGroupHeader *const tile_grou_header,
      const oid_t &tuple_id) {
      assert(IsOwner(tile_grou_header, tuple_id));
      return tile_grou_header->GetBeginCommitId(tuple_id) == MAX_CID;
  }

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  bool ReadIsValid( 
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id,
    const cid_t begin_cid,
    const cid_t end_cid);


  virtual bool PerformInsert(const ItemPointer &location);

  /**
   * Deprecated interfaces
   */
  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused))) { assert(false); }

  virtual void PerformDelete(const ItemPointer &old_location  __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused))) { assert(false); }

  virtual void PerformUpdate(const ItemPointer &location  __attribute__((unused))) { assert(false); }

  /**
   * Interfaces for rollback segment
   */

  // Add a new rollback segment to the tuple
  void PerformUpdateWithRb(const ItemPointer &location, char *new_rb_seg);

  // Rollback the master copy of a tuple to the status at the begin of the 
  // current transaction
  void RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group,
                            const oid_t tuple_id);

  // Whe a txn commits, it needs to set an end timestamp to all RBSeg it has
  // created in order to make them invisible to future transactions
  void InstallRollbackSegments(storage::TileGroupHeader *tile_group_header,
                                const oid_t tuple_id, const cid_t end_cid);

  /**
   * @brief Test if a reader with read timestamp @read_ts should follow on the
   * rb chain started from rb_set
   */
  inline bool ShouldReadRB(char *rb_seg, cid_t read_ts) {
    // Check if we actually have a rollback segment
    if (rb_seg == nullptr) {
      return false;
    }

    cid_t rb_ts = storage::RollbackSegmentPool::GetTimeStamp(rb_seg);

    return read_ts < rb_ts;
  }

  // Return nullptr if the tuple is not activated to current txn.
  // Otherwise return the evident that current tuple is activated
  inline char* GetActivatedEvidence(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_slot_id) {
    cid_t txn_begin_cid = current_txn->GetBeginCommitId();
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);

    assert(tuple_begin_cid != MAX_CID);
    // Owner can not call this function
    assert(IsOwner(tile_group_header, tuple_slot_id) == false);

    char *rb_seg = GetRbSeg(tile_group_header, tuple_slot_id);

    if (txn_begin_cid >= tuple_begin_cid && rb_seg == nullptr) {
      // Master copy is activated
      // Return the address of the reserved field to serve as an id
      return tile_group_header->GetReservedFieldRef(tuple_slot_id);
    } else if (rb_seg == nullptr) {
      // The master copy is not activated, and no more rollback segment is found
      return nullptr;
    }

    // Even if the master copy is valid, we still need to go down the
    // segment list to seek for evidence
    rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);

    // Start checking rollback segment
    while(true) {
      if (rb_seg == nullptr) {
        // No more rollback segment
        return rb_seg;
      } else if (txn_begin_cid >= storage::RollbackSegmentPool::GetTimeStamp(rb_seg)) {
        // The previous rollback segment is visible
        return rb_seg;
      }

      rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
    }
  }

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    // Set current transaction
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;
    // Add to running transaction table
    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;
    // Create current transaction poll
    current_segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();

    running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);

    auto result = current_txn->GetResult();
    auto end_cid = current_txn->GetEndCommitId();

    if (result == RESULT_SUCCESS) {
      // Committed
      if (current_txn->IsReadOnly()) {
        // read only txn, just delete the segment pool because it's empty
        delete current_segment_pool;
      } else {
        // It's not read only txn
        current_segment_pool->SetPoolTimestamp(end_cid);
        living_pools_[end_cid] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
      }
    } else {
      // Aborted
      // TODO: Add coperative GC
      current_segment_pool->MarkedAsGarbage();
      garbage_pools_[current_txn->GetBeginCommitId()] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
    }

    delete current_txn;
    current_txn = nullptr;
    current_segment_pool = nullptr;
  }

  // Get the commit id of the latest commited txn
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

  // Init reserved area of a tuple
  // delete_flag is used to mark that the transaction that owns the tuple
  // has deleted the tuple
  // Spinlock (8 bytes) | next_seg_pointer (8 bytes) | delete_flag (1 bytes)
  void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    new (reserved_area + lock_offset) Spinlock();
    SetRbSeg(tile_group_header, tuple_id, nullptr);
    *(reinterpret_cast<bool*>(reserved_area + delete_flag_offset)) = false;
  }

  // Get current segment pool of the transaction manager
  inline storage::RollbackSegmentPool *GetSegmentPool() {return current_segment_pool;}

 private:
  static const size_t lock_offset = 0;
  static const size_t rb_seg_offset  = lock_offset + sizeof(oid_t);
  static const size_t delete_flag_offset = rb_seg_offset + sizeof(char*);
  cuckoohash_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  // TODO: add cooperative GC
  // The RB segment pool that is activlely being used
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> living_pools_;
  // The RB segment pool that has been marked as garbage
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> garbage_pools_;

  inline void SetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                       const RBSegType seg_ptr) {
    const char **rb_seg_ptr = (const char **)(tile_group_header->GetReservedFieldRef(tuple_id) + rb_seg_offset);
    *rb_seg_ptr = seg_ptr;
  }

  inline RBSegType GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    char **rb_seg_ptr = (char **)(tile_group_header->GetReservedFieldRef(tuple_id) + rb_seg_offset);
    return *rb_seg_ptr;
  }

  inline bool GetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    return *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset));
  }

  inline void SetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset)) = true;
  }

  inline void ClearDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset)) = false;
  }

  // Lock a tuple
  inline void LockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Lock();
  }

  // Unlock a tuple
  inline void UnlockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + lock_offset);
    lock->Unlock();
  }
};
}
}