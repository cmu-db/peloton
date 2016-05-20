//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <unordered_map>
#include <list>
#include <utility>

#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {

class ItemPointer;

namespace storage{
class DataTable;
}

namespace concurrency {

class Transaction;

extern thread_local Transaction *current_txn;

#define RUNNING_TXN_BUCKET_NUM 10

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
    maximum_grant_cid_ = ATOMIC_VAR_INIT(MAX_CID);
  }

  virtual ~TransactionManager() {}

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  cid_t GetNextCommitId() {
	  cid_t temp_cid = next_cid_++;
	  // wait if we do not yet have a grant for this commit id
	  while(temp_cid > maximum_grant_cid_.load());
	  return temp_cid;
  }

  cid_t GetCurrentCommitId() { return next_cid_.load(); }

  bool IsOccupied(const ItemPointer &position);

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id) = 0;


  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  // This method is used by executor to yield ownership after the acquired
  // ownership, some of them will not perform update after they acquired the
  // ownership, leaving the write lock on the tuple unreleased.
  virtual void YieldOwnership(const oid_t &tile_group_id __attribute__((unused)),
    const oid_t &tuple_id __attribute__((unused))) {
    // Do nothing, specific txn manager can choose to implement it
  }

  virtual bool PerformInsert(const ItemPointer &location) = 0;

  virtual bool PerformRead(const ItemPointer &location) = 0;

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(const ItemPointer &location) = 0;

  virtual void PerformDelete(const ItemPointer &location) = 0;

  /*
   * Write a virtual function to push deleted and verified (acc to optimistic
   * concurrency control) tuples into possibly free from all underlying
   * concurrency implementations of transactions.
   */

  void RecycleInvalidTupleSlot(const oid_t &tile_group_id, const oid_t &tuple_id) {
    auto& gc_instance = gc::GCManagerFactory::GetInstance();

    auto tile_group =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    gc_instance.RecycleInvalidTupleSlot(
      tile_group->GetTableId(), tile_group_id, tuple_id);
  }

  void RecycleOldTupleSlot(const oid_t &tile_group_id, const oid_t &tuple_id,
                           const cid_t &tuple_end_cid) {

    if(gc::GCManagerFactory::GetGCType() != GC_TYPE_VACUUM) {
      return;
    }

    auto& gc_instance = gc::GCManagerFactory::GetInstance();

    auto tile_group =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    gc_instance.RecycleOldTupleSlot(
      tile_group->GetTableId(), tile_group_id, tuple_id, tuple_end_cid);
  }



  // Txn manager may store related information in TileGroupHeader, so when
  // TileGroup is dropped, txn manager might need to be notified
  virtual void DroppingTileGroup(const oid_t &tile_group_id
                                 UNUSED_ATTRIBUTE) {
    return;
  }

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  //for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; }

  void SetMaxGrantCid(cid_t cid){ maximum_grant_cid_ = cid; }

  virtual Transaction *BeginTransaction() = 0;

  virtual void EndTransaction() = 0;

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  void ResetStates() {
    next_txn_id_ = START_TXN_ID;
    next_cid_ = START_CID;
  }

  // this function generates the maximum commit id of committed transactions.
  // please note that this function only returns a "safe" value instead of a
  // precise value.
  cid_t GetMaxCommittedCid() {
    return EpochManagerFactory::GetInstance().GetMaxDeadTxnCid();
  }

  void SetDirtyRange(std::pair<cid_t, cid_t> dirty_range){
	  this->dirty_range_ = dirty_range;
  }

 protected:


  inline bool CidIsInDirtyRange(cid_t cid){
	  return ((cid > dirty_range_.first) & (cid <= dirty_range_.second));
  }
  // invisible range after failure and recovery;
  // first value is exclusive, last value is inclusive
  std::pair<cid_t, cid_t> dirty_range_ = std::make_pair(INVALID_CID, INVALID_CID);


 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;
  std::atomic<cid_t> maximum_grant_cid_;

};
}  // End storage namespace
}  // End peloton namespace
