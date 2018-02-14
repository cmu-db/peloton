//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager.h
//
// Identification:
// src/include/concurrency/timestamp_ordering_transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_manager.h"
#include "storage/tile_group.h"
#include "statistics/stats_aggregator.h"
#include "common/synchronization/spin_latch.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for timestamp ordering transaction manager.
 */
class TimestampOrderingTransactionManager : public TransactionManager {
 public:
  TimestampOrderingTransactionManager() {}

  /**
   * @brief      Destroys the object.
   */
  virtual ~TimestampOrderingTransactionManager() {}

  /**
   * @brief      Gets the instance.
   *
   * @param[in]  protocol   The protocol
   * @param[in]  isolation  The isolation
   * @param[in]  conflict   The conflict
   *
   * @return     The instance.
   */
  static TimestampOrderingTransactionManager &GetInstance(
      const ProtocolType protocol,
      const IsolationLevelType isolation, 
      const ConflictAvoidanceType conflict);

  /**
   * This method tests whether the current transaction is the owner of this
   * version.
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
      const oid_t &tuple_id);

  /**
   * This method tests whether any other transaction has owned this version.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if owner, False otherwise.
   */
  virtual bool IsOwned(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  /**
   * This method tests whether the current transaction has created this version
   * of the tuple.
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
      const oid_t &tuple_id);

  /**
   * This method tests whether it is possible to obtain the ownership.
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
      const oid_t &tuple_id);

  /**
   * This method is used to acquire the ownership of a tuple for a transaction.
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
      const oid_t &tuple_id);

  /**
   * This method is used by executor to yield ownership after the acquired
   * ownership.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if success, False otherwise.
   */
  virtual void YieldOwnership(
      TransactionContext *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

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
                             ItemPointer *index_entry_ptr = nullptr);

  virtual bool PerformRead(TransactionContext *const current_txn,
                           const ItemPointer &location,
                           bool acquire_ownership = false);

  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &location);

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &location);

  virtual ResultType CommitTransaction(TransactionContext *const current_txn);

  virtual ResultType AbortTransaction(TransactionContext *const current_txn);


private:
  static const int LOCK_OFFSET = 0;
  static const int LAST_READER_OFFSET = (LOCK_OFFSET + 8);

  /**
   * @brief      Gets the spin latch field.
   *
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     The spin latch field.
   */
  common::synchronization::SpinLatch *GetSpinLatchField(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  /**
   * @brief      Gets the last reader commit identifier.
   *
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     The last reader commit identifier.
   */
  cid_t GetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  bool SetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id, 
      const cid_t &current_cid, 
      const bool is_owner);

  /**
   * Initiate reserved area of a tuple.
   *
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   */
  void InitTupleReserved(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t tuple_id);
};
}
}
