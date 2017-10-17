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
#include "concurrency/epoch_manager_factory.h"
#include "common/logger.h"
#include "logging/wal_log_manager.h"

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

class TransactionManager {
 public:
  TransactionManager() {}

  virtual ~TransactionManager() {}

  void Init(const ProtocolType protocol,
            const IsolationLevelType isolation, 
            const ConflictAvoidanceType conflict) {
    protocol_ = protocol;
    isolation_level_ = isolation;
    conflict_avoidance_ = conflict;
  }

  // This method is used for avoiding concurrent inserts.
  bool IsOccupied(
      Transaction *const current_txn, 
      const void *position_ptr);

  VisibilityType IsVisible(
      Transaction *const current_txn, 
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id,
      const VisibilityIdType type = VisibilityIdType::READ_ID);

  // This method test whether the current transaction is the owner of this version.
  virtual bool IsOwner(
      Transaction *const current_txn, 
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method tests whether any other transaction has owned this version.
  virtual bool IsOwned(
      Transaction *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method tests whether the current transaction has created this version of the tuple
  virtual bool IsWritten(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) = 0;

  // This method tests whether it is possible to obtain the ownership.
  virtual bool IsOwnable(
      Transaction *const current_txn, 
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method is used to acquire the ownership of a tuple for a transaction.
  virtual bool AcquireOwnership(
      Transaction *const current_txn, 
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  // This method is used by executor to yield ownership after the acquired ownership.
  virtual void YieldOwnership(
      Transaction *const current_txn, 
      // const oid_t &tile_group_id, 
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  // The index_entry_ptr is the address of the head node of the version chain, 
  // which is directly pointed by the primary index.
  virtual void PerformInsert(Transaction *const current_txn, 
                             const ItemPointer &location, 
                             ItemPointer *index_entry_ptr = nullptr) = 0;

  virtual bool PerformRead(Transaction *const current_txn, 
                           const ItemPointer &location,
                           bool acquire_ownership = false) = 0;

  virtual void PerformUpdate(Transaction *const current_txn, 
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(Transaction *const current_txn, 
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(Transaction *const current_txn, 
                             const ItemPointer &location) = 0;

  virtual void PerformDelete(Transaction *const current_txn, 
                             const ItemPointer &location) = 0;

  void SetTransactionResult(Transaction *const current_txn, const ResultType result) {
    current_txn->SetResult(result);
  }

  Transaction *BeginTransaction(const IsolationLevelType type) {
    return BeginTransaction(0, type);
  }

  Transaction *BeginTransaction(const size_t thread_id = 0, 
                                const IsolationLevelType type = isolation_level_);

  void EndTransaction(Transaction *current_txn);

  virtual ResultType CommitTransaction(Transaction *const current_txn, logging::WalLogManager *log_manager=nullptr) = 0;

  virtual ResultType AbortTransaction(Transaction *const current_txn) = 0;

  // this function generates the maximum commit id of committed transactions.
  // please note that this function only returns a "safe" value instead of a
  // precise value.
  cid_t GetExpiredCid() {
    return EpochManagerFactory::GetInstance().GetExpiredCid();
  }

  IsolationLevelType GetIsolationLevel() {
    return isolation_level_;
  }

 protected:
  static ProtocolType protocol_;
  static IsolationLevelType isolation_level_;
  static ConflictAvoidanceType conflict_avoidance_;

};
}  // namespace storage
}  // namespace peloton
