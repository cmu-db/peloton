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

namespace peloton {

namespace storage{
class RollbackSegmentPool;
}

namespace concurrency {

// Each transaction has a RollbackSegmentPool
extern thread_local storage::RollbackSegmentPool *current_segment_pool;
extern thread_local cid_t latest_read_timestamp;
//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public TransactionManager {
  public:
  typedef char* RBSegType;

  OptimisticRbTxnManager() {}

  virtual ~OptimisticRbTxnManager() {}

  static OptimisticRbTxnManager &GetInstance();

  virtual VisibilityType IsVisible(
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
      PL_ASSERT(IsOwner(tile_grou_header, tuple_id));
      return tile_grou_header->GetBeginCommitId(tuple_id) == MAX_CID;
  }

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void YieldOwnership(const oid_t &tile_group_id,
    const oid_t &tuple_id);

  bool ValidateRead( 
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id,
    const cid_t &end_cid);


  virtual bool PerformInsert(const ItemPointer &location);

  // Get the read timestamp of the latest transaction on this thread, it is 
  // either the begin commit time of current transaction of the just committed
  // transaction.
  cid_t GetLatestReadTimestamp() {
    return latest_read_timestamp;
  }

  /**
   * Deprecated interfaces
   */
  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location UNUSED_ATTRIBUTE,
                             const ItemPointer &new_location UNUSED_ATTRIBUTE) { PL_ASSERT(false); }

  virtual void PerformDelete(const ItemPointer &old_location  UNUSED_ATTRIBUTE,
                             const ItemPointer &new_location UNUSED_ATTRIBUTE) { PL_ASSERT(false); }

  virtual void PerformUpdate(const ItemPointer &location  UNUSED_ATTRIBUTE) { PL_ASSERT(false); }

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
  bool IsRBVisible(char *rb_seg, cid_t read_ts);

  // Return nullptr if the tuple is not activated to current txn.
  // Otherwise return the evident that current tuple is activated
  char* GetActivatedEvidence(const storage::TileGroupHeader *tile_group_header,
                             const oid_t tuple_slot_id);

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction();

  virtual void EndTransaction();

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

  inline RBSegType GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    char **rb_seg_ptr = (char **)(tile_group_header->GetReservedFieldRef(tuple_id) + rb_seg_offset);
    return *rb_seg_ptr;
  }

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
