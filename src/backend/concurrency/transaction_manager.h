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

#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/tuple.h"
#include "backend/gc/gc_manager_factory.h"
#include "backend/planner/project_info.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {

namespace concurrency {

extern thread_local Transaction *current_txn;

#define RUNNING_TXN_BUCKET_NUM 10

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
    read_abort_ = 0;
    acquire_owner_abort_ = 0;
    no_space_abort_ = 0;
    cannot_own_abort_ = 0;
  }

  virtual ~TransactionManager() {
    printf("read_abort_ = %d, acquire_owner_abort_ = %d, no_space_abort_ = %d, cannot_own_abort_ = %d\n", read_abort_.load(), acquire_owner_abort_.load(), no_space_abort_.load(), cannot_own_abort_.load());
  }

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  cid_t GetNextCommitId() { return next_cid_++; }

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

  // This method is used by executor to yield ownership after the acquired ownership.
  virtual void YieldOwnership(const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

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
    LOG_TRACE("recycle invalid tuple slot: %u, %u", tile_group_id, tuple_id);
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
    LOG_TRACE("recycle old tuple slot: %u, %u", tile_group_id, tuple_id);

    auto& gc_instance = gc::GCManagerFactory::GetInstance();

    auto tile_group =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    gc_instance.RecycleOldTupleSlot(
      tile_group->GetTableId(), tile_group_id, tuple_id, tuple_end_cid);
  }



  // Txn manager may store related information in TileGroupHeader, so when
  // TileGroup is dropped, txn manager might need to be notified
  virtual void DroppingTileGroup(const oid_t &tile_group_id
                                 __attribute__((unused))) {
    return;
  }

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  //for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; }

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

  void AddOneReadAbort() {
    ++read_abort_;
  }

  void AddOneAcquireOwnerAbort() {
    ++acquire_owner_abort_;
  }

  void AddOneNoSpaceAbort() {
    ++no_space_abort_;
  }

  void AddOneCannotOwnAbort() {
    ++cannot_own_abort_;
  }

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;

  std::atomic<int> read_abort_;
  std::atomic<int> acquire_owner_abort_;
  std::atomic<int> no_space_abort_;
  std::atomic<int> cannot_own_abort_;

};
}  // End storage namespace
}  // End peloton namespace
