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
#include "concurrency/transaction_context.h"
#include "concurrency/epoch_manager_factory.h"
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

class TransactionContext;

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
      TransactionContext *const current_txn,
      const void *position_ptr);

  VisibilityType IsVisible(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id,
      const VisibilityIdType type = VisibilityIdType::READ_ID);

  // This method test whether the current transaction is the owner of this version.
  virtual bool IsOwner(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method tests whether any other transaction has owned this version.
  virtual bool IsOwned(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method tests whether the current transaction has created this version of the tuple
  virtual bool IsWritten(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) = 0;

  // This method tests whether it is possible to obtain the ownership.
  virtual bool IsOwnable(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  // This method is used to acquire the ownership of a tuple for a transaction.
  virtual bool AcquireOwnership(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  // This method is used by executor to yield ownership after the acquired ownership.
  virtual void YieldOwnership(
      TransactionContext *const current_txn,
      // const oid_t &tile_group_id, 
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  // The index_entry_ptr is the address of the head node of the version chain, 
  // which is directly pointed by the primary index.
  virtual void PerformInsert(TransactionContext *const current_txn,
                             const ItemPointer &location, 
                             ItemPointer *index_entry_ptr = nullptr) = 0;

  virtual bool PerformRead(TransactionContext *const current_txn,
                           const ItemPointer &location,
                           bool acquire_ownership = false) = 0;

  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &location) = 0;

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &location) = 0;

  void SetTransactionResult(TransactionContext *const current_txn, const ResultType result) {
    current_txn->SetResult(result);
  }

  TransactionContext *BeginTransaction(const IsolationLevelType type) {
    return BeginTransaction(0, type);
  }

  TransactionContext *BeginTransaction(const size_t thread_id = 0,
                                const IsolationLevelType type = isolation_level_);

  void EndTransaction(TransactionContext *current_txn);

  virtual ResultType CommitTransaction(TransactionContext *const current_txn) = 0;

  virtual ResultType AbortTransaction(TransactionContext *const current_txn) = 0;

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
