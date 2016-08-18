//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <atomic>
#include <unordered_map>
#include <list>
#include <utility>

#include "storage/tile_group_header.h"
#include "concurrency/transaction.h"
#include "concurrency/epoch_manager.h"
#include "common/logger.h"

namespace peloton {

class ItemPointer;

namespace storage {
class DataTable;
class TileGroupHeader;
}

namespace catalog {
class Manager;
}

namespace concurrency {

class Transaction;

extern thread_local Transaction *current_txn;

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
    while (temp_cid > maximum_grant_cid_.load())
      ;
    return temp_cid;
  }

  cid_t GetCurrentCommitId() { return next_cid_.load(); }

  // This method is used for avoiding concurrent inserts.
  virtual bool IsOccupied(const ItemPointer &position) = 0;

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method test whether the current transaction is the owner of a tuple.
  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id) = 0;

  // This method tests whether it is possible to obtain the ownership.
  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method is used to acquire the ownership of a tuple for a transaction.
  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) = 0;

  // This method is used by executor to yield ownership after the acquired ownership.
  virtual void YieldOwnership(const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  // The index_entry_ptr is the address of the head node of the version chain, 
  // which is directly pointed by the primary index.
  virtual void PerformInsert(const ItemPointer &location, ItemPointer *index_entry_ptr = nullptr) = 0;

  virtual bool PerformRead(const ItemPointer &location) = 0;

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(const ItemPointer &location) = 0;

  virtual void PerformDelete(const ItemPointer &location) = 0;

  // Txn manager may store related information in TileGroupHeader, so when
  // TileGroup is dropped, txn manager might need to be notified
  virtual void DroppingTileGroup(const oid_t &tile_group_id UNUSED_ATTRIBUTE) {
    return;
  }

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  // for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; }

  void SetMaxGrantCid(cid_t cid) { maximum_grant_cid_ = cid; }

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

  void SetDirtyRange(std::pair<cid_t, cid_t> dirty_range) {
    this->dirty_range_ = dirty_range;
  }

 protected:
  inline bool CidIsInDirtyRange(cid_t cid) {
    return ((cid > dirty_range_.first) & (cid <= dirty_range_.second));
  }
  // invisible range after failure and recovery;
  // first value is exclusive, last value is inclusive
  std::pair<cid_t, cid_t> dirty_range_ =
      std::make_pair(INVALID_CID, INVALID_CID);

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;
  std::atomic<cid_t> maximum_grant_cid_;
};
}  // End storage namespace
}  // End peloton namespace
