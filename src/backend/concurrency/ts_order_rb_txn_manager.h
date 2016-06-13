//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_rb_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {

namespace concurrency {

// Each transaction has a RollbackSegmentPool
extern thread_local storage::RollbackSegmentPool *to_current_segment_pool;
extern thread_local cid_t to_latest_read_timestamp;
extern thread_local std::unordered_map<ItemPointer, index::RBItemPointer *> to_updated_index_entries;
//===--------------------------------------------------------------------===//
// timestamp ordering with rollback segment
//===--------------------------------------------------------------------===//

class TsOrderRbTxnManager : public TransactionManager {
public:
  typedef char* RBSegType;

  TsOrderRbTxnManager() {}

  virtual ~TsOrderRbTxnManager() {}

  static TsOrderRbTxnManager &GetInstance();

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
      assert(IsOwner(tile_grou_header, tuple_id));
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

  bool PerformInsert(const ItemPointer &location, index::RBItemPointer *rb_item_ptr);

  // Get the read timestamp of the latest transaction on this thread, it is 
  // either the begin commit time of current transaction of the just committed
  // transaction.
  cid_t GetLatestReadTimestamp() {
    return to_latest_read_timestamp;
  }

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

  // Insert a version, basically maintain secondary index
  bool RBInsertVersion(storage::DataTable *target_table,
    const ItemPointer &location, const storage::Tuple *tuple);

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
  inline bool IsRBVisible(char *rb_seg, cid_t read_ts) {
    // Check if we actually have a rollback segment
    if (rb_seg == nullptr) {
      return false;
    }

    cid_t rb_ts = storage::RollbackSegmentPool::GetTimeStamp(rb_seg);

    return read_ts < rb_ts;
  }

  // Return nullptr if the tuple is not activated to current txn.
  // Otherwise return the version that current tuple is activated
  inline char* GetActivatedRB(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_slot_id) {
    cid_t txn_begin_cid = current_txn->GetBeginCommitId();
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);

    assert(tuple_begin_cid != MAX_CID);
    // Owner cannot call this function
    assert(IsOwner(tile_group_header, tuple_slot_id) == false);

    RBSegType rb_seg = GetRbSeg(tile_group_header, tuple_slot_id);
    char *prev_visible;
    bool master_activated = (txn_begin_cid >= tuple_begin_cid);

    if (master_activated)
      prev_visible = tile_group_header->GetReservedFieldRef(tuple_slot_id);
    else
      prev_visible = nullptr;

    while (IsRBVisible(rb_seg, txn_begin_cid)) {
      prev_visible = rb_seg;
      rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
    }

    return prev_visible;
  }

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    // Set current transaction
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();

    LOG_TRACE("Beginning transaction %lu", txn_id);


    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    to_latest_read_timestamp = begin_cid;
    // Create current transaction poll
    to_current_segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);

    return txn;
  }

  virtual void EndTransaction() {
    auto result = current_txn->GetResult();
    auto end_cid = current_txn->GetEndCommitId();

    if (result == RESULT_SUCCESS) {
      // Committed
      if (current_txn->IsReadOnly()) {
        // read only txn, just delete the segment pool because it's empty
        delete to_current_segment_pool;
      } else {
        // It's not read only txn
        to_current_segment_pool->SetPoolTimestamp(end_cid);
        living_pools_[end_cid] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(to_current_segment_pool);
      }
    } else {
      // Aborted
      // TODO: Add coperative GC
      to_current_segment_pool->MarkedAsGarbage();
      garbage_pools_[current_txn->GetBeginCommitId()] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(to_current_segment_pool);
    }

    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    to_updated_index_entries.clear();
    delete current_txn;
    current_txn = nullptr;
    to_current_segment_pool = nullptr;
  }

  // Init reserved area of a tuple
  // delete_flag is used to mark that the transaction that owns the tuple
  // has deleted the tuple
  // next_seg_pointer (8 bytes) | sindex entry ptr |
  // | delete_flag (1 bytes)
  void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    SetRbSeg(tile_group_header, tuple_id, nullptr);
    SetSIndexPtr(tile_group_header, tuple_id, nullptr);
    ClearDeleteFlag(tile_group_header, tuple_id);
    *(reinterpret_cast<bool*>(reserved_area + DELETE_FLAG_OFFSET)) = false;
  }

  // Get current segment pool of the transaction manager
  inline storage::RollbackSegmentPool *GetSegmentPool() {return to_current_segment_pool;}

  inline RBSegType GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    char **rb_seg_ptr = (char **)(tile_group_header->GetReservedFieldRef(tuple_id) + RB_SEG_OFFSET);
    return *rb_seg_ptr;
  }

 private:
  static const size_t RB_SEG_OFFSET = 0;
  static const size_t SINDEX_PTR_OFFSET = RB_SEG_OFFSET + sizeof(char *);
  static const size_t DELETE_FLAG_OFFSET = SINDEX_PTR_OFFSET + sizeof(char *);
  static const size_t LOCK_OFFSET = DELETE_FLAG_OFFSET + 8; // actually the delete flag only occupies one byte.
  static const size_t LAST_READER_OFFSET = LOCK_OFFSET + 8; // actually the lock also only occupies one byte.

  // TODO: add cooperative GC
  // The RB segment pool that is activlely being used
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> living_pools_;
  // The RB segment pool that has been marked as garbage
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> garbage_pools_;

  inline void SetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                       const RBSegType seg_ptr) {
    const char **rb_seg_ptr = (const char **)(tile_group_header->GetReservedFieldRef(tuple_id) + RB_SEG_OFFSET);
    *rb_seg_ptr = seg_ptr;
  }

  inline void SetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                          index::RBItemPointer *ptr) {
    index::RBItemPointer **index_ptr = (index::RBItemPointer **)(tile_group_header->GetReservedFieldRef(tuple_id) + SINDEX_PTR_OFFSET);
    *index_ptr = ptr;
  }

  inline index::RBItemPointer *GetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    index::RBItemPointer **index_ptr = (index::RBItemPointer **)(tile_group_header->GetReservedFieldRef(tuple_id) + SINDEX_PTR_OFFSET);
    return *index_ptr;
  }

  inline bool GetDeleteFlag(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    return *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET));
  }

  inline void SetDeleteFlag(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET)) = true;
  }

  inline void ClearDeleteFlag(const storage::TileGroupHeader *const tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET)) = false;
  }

  inline Spinlock *GetSpinlockField(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);
  }

  inline cid_t GetLastReaderCid(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    return *(cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
  }

  inline bool SetLastReaderCid(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    assert(IsOwner(tile_group_header, tuple_id) == false);

    cid_t *ts_ptr = (cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
    
    cid_t current_cid = current_txn->GetBeginCommitId();

    GetSpinlockField(tile_group_header, tuple_id)->Lock();
    
    txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
    
    if(tuple_txn_id != INITIAL_TXN_ID) {
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();
      return false;
    } else {
      if (*ts_ptr < current_cid) {
        *ts_ptr = current_cid;
      }
      
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();
      return true;
    }
  }
};
}
}