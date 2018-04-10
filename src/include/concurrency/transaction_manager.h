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

/**
 * @brief      Class for item pointer.
 */
class ItemPointer;

namespace storage {
class DataTable;
class TileGroupHeader;
}

namespace catalog {
class Manager;
}

namespace concurrency {

/**
 * @brief      Class for transaction context.
 */
class TransactionContext;

/**
 * @brief      Class for transaction manager.
 */
class TransactionManager {
 public:
  TransactionManager() {}

  /**
   * @brief      Destroys the object.
   */
  virtual ~TransactionManager() {}

  void Init(const ProtocolType protocol,
            const IsolationLevelType isolation, 
            const ConflictAvoidanceType conflict) {
    protocol_ = protocol;
    isolation_level_ = isolation;
    conflict_avoidance_ = conflict;
  }

  /**
   * Used for avoiding concurrent inserts.
   *
   * @param      current_txn        The current transaction
   * @param[in]  position_ptr  The position pointer
   *
   * @return     True if occupied, False otherwise.
   */
  bool IsOccupied(
      TransactionContext *const current_txn,
      const void *position_ptr);

  /**
   * @brief      Determines if visible.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   * @param[in]  type               The type
   *
   * @return     True if visible, False otherwise.
   */
  VisibilityType IsVisible(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id,
      const VisibilityIdType type = VisibilityIdType::READ_ID);

  /**
   * Test whether the current transaction is the owner of this tuple.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if owner, False otherwise.
   */
  virtual bool IsOwner(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  /**
   * This method tests whether any other transaction has owned this version.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if owned, False otherwise.
   */
  virtual bool IsOwned(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  /**
   * Test whether the current transaction has created this version of the tuple.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if written, False otherwise.
   */
  virtual bool IsWritten(
    TransactionContext *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) = 0;

  /**
   * Test whether it can obtain ownership.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if ownable, False otherwise.
   */
  virtual bool IsOwnable(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  /**
   * Used to acquire ownership of a tuple for a transaction.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if success, False otherwise.
   */
  virtual bool AcquireOwnership(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  /**
   * Used by executor to yield ownership after the acquired it.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   */
  virtual void YieldOwnership(
      TransactionContext *const current_txn,
      // const oid_t &tile_group_id, 
      const storage::TileGroupHeader *const tile_group_header, 
      const oid_t &tuple_id) = 0;

  /**
   * The index_entry_ptr is the address of the head node of the version chain,
   * which is directly pointed by the primary index.
   *
   * @param      current_txn        The current transaction
   * @param[in]  location         The location
   * @param      index_entry_ptr  The index entry pointer
   */
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

  /**
   * @brief      Sets the transaction result.
   *
   * @param      current_txn  The current transaction
   * @param[in]  result       The result
   */
  void SetTransactionResult(TransactionContext *const current_txn, const ResultType result) {
    current_txn->SetResult(result);
  }

  TransactionContext *BeginTransaction(const IsolationLevelType type) {
    return BeginTransaction(0, type);
  }

  TransactionContext *BeginTransaction(const size_t thread_id = 0,
                                const IsolationLevelType type = isolation_level_);

  /**
   * @brief      Ends a transaction.
   *
   * @param      current_txn  The current transaction
   */
  void EndTransaction(TransactionContext *current_txn);

  virtual ResultType CommitTransaction(TransactionContext *const current_txn) = 0;

  virtual ResultType AbortTransaction(TransactionContext *const current_txn) = 0;

  /**
   * This function generates the maximum commit id of committed transactions.
   * please note that this function only returns a "safe" value instead of a
   * precise value.
   *
   * @return     The expired cid.
   */
  cid_t GetExpiredCid() {
    return EpochManagerFactory::GetInstance().GetExpiredCid();
  }

  /**
   * @brief      Gets the isolation level.
   *
   * @return     The isolation level.
   */
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
