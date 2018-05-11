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
   * Test if this transaction can get ownership.
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
   * Test whether any other transaction has owned this version.
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
  * Test whether the current transaction has created this version of
  * the tuple.
  *
  * This method is designed for select_for_update.
  *
  * The DBMS can acquire write locks for a transaction in two cases: (1) Every
  * time a transaction updates a tuple, the DBMS creates a new version of the
  * tuple and acquire the locks on both the older and the newer version; (2)
  * Every time a transaction executes a select_for_update statement, the DBMS
  * needs to acquire the lock on the corresponding version without creating a new
  * version. IsWritten() method is designed for distinguishing these two cases.
  *
  * @param[in]  TransactionContext  The transaction context
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
   * Test if this transaction can get ownership.
   * If the tuple is not owned by any transaction and is visible to current
   * transaction. the version must be the latest version in the version chain.
   *
   * @param[in]  TransactionContext  The transaction context
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
   * Release write lock on a tuple. one example usage of this method is when a
   * tuple is acquired, but operation (insert,update,delete) can't proceed, the
   * executor needs to yield the ownership before return false to upper layer. It
   * should not be called if the tuple is in the write set as commit and abort
   * will release the write lock anyway.
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
   * @param      current_txn      The current transaction
   * @param[in]  location         The location
   * @param      index_entry_ptr  The index entry pointer
   */
  virtual void PerformInsert(TransactionContext *const current_txn,
                             const ItemPointer &location,
                             ItemPointer *index_entry_ptr = nullptr);

  /**
   * @brief      Perform a read operation
   *
   * @param      current_txn        The current transaction
   * @param[in]  location           The location of the tuple to be read
   * @param[in]  acquire_ownership  The acquire ownership
   */
  virtual bool PerformRead(TransactionContext *const current_txn,
                           const ItemPointer &location,
                           bool acquire_ownership = false);

  /**
   * @brief      Perform an update operation
   *
   * @param      current_txn   The current transaction
   * @param[in]  old_location  The location of the old tuple to be updated
   * @param[in]  new_location  The location of the new tuple
   */
  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  /**
   * @brief      Perform a delete operation. Used when the transaction is the
   *             owner of the tuple.
   *
   * @param      current_txn   The current transaction
   * @param[in]  old_location  The location of the old tuple to be deleted
   * @param[in]  new_location  The location of the new tuple
   */
  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  /**
   * @brief      Perform an update operation
   *
   * @param      current_txn  The current transaction
   * @param[in]  location     The location
   */
  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &location);

  /**
   * @brief      Perform a delete operation. Used when the transaction is not 
   *             the owner of the tuple.
   *
   * @param      current_txn  The current transaction
   * @param[in]  location     The location
   */
  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &location);

  /**
   * @brief      Commits a transaction.
   *
   * @param      current_txn  The current transaction
   *
   * @return     The result type
   */
  virtual ResultType CommitTransaction(TransactionContext *const current_txn);

  /**
   * @brief      Abort a transaction
   *
   * @param      current_txn  The current transaction
   *
   * @return     The result type
   */
  virtual ResultType AbortTransaction(TransactionContext *const current_txn);


private:
  static const int LOCK_OFFSET = 0;
  static const int LAST_READER_OFFSET = (LOCK_OFFSET + 8);

  /**
   * @brief      Gets the spin latch field.
   * 
   * Timestamp ordering requires a spinlock field for protecting the atomic access
   * to txn_id field and last_reader_cid field.
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
   * In timestamp ordering, the last_reader_cid records the timestamp of the last
   * transaction that reads the tuple.
   *
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     The last reader commit identifier.
   */
  cid_t GetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  /**
   * @brief      Sets the last reader commit identifier.
   *
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   * @param[in]  current_cid        The current cid
   * @param[in]  is_owner           Indicates if owner
   *
   * @return     True if success, False otherwise
   */
  bool SetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id, 
      const cid_t &current_cid, 
      const bool is_owner);

  /**
   * Initialize reserved area of a tuple.
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
