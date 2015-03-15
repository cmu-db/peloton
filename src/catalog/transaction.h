/*-------------------------------------------------------------------------
 *
 * transaction.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/transaction.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <atomic>
#include <cassert>
#include <vector>
#include <map>

#include "storage/tile.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class Transaction {

  Transaction(Transaction const&) = delete;

 public:

  Transaction()  :
    txn_id(INVALID_TXN_ID),
    visible_cid(INVALID_CID),
    ref_count(1),
    waiting(false),
    next(nullptr) {
  }


  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  // record inserted tuple
  void RecordInsert(const storage::Tile* tile, id_t offset);

  // record deleted tuple
  void RecordDelete(const storage::Tile* tile, id_t offset);

  bool HasInsertedTuples(const storage::Tile* tile) const;

  bool HasDeletedTuples(const storage::Tile* tile) const;

  const std::vector<id_t>& GetInsertedTuples(const storage::Tile* tile);

  const std::vector<id_t>& GetDeletedTuples(const storage::Tile* tile);

  // maintain reference counts for transactions
  void IncrementRefCount();

  void DecrementRefCount();

  // Get a string representation of this txn
  friend std::ostream& operator<<(std::ostream& os, const Transaction& txn);

 protected:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id;

  // visible commit id
  cid_t visible_cid;

  // references
  std::atomic<size_t> ref_count;

  // waiting for commit ?
  std::atomic<bool> waiting;

  // cid context
  Transaction* next __attribute__((aligned(16)));

  // inserted tuples in each tile
  std::map<const storage::Tile*, std::vector<id_t> > inserted_tuples;

  // deleted tuples in each tile
  std::map<const storage::Tile*, std::vector<id_t> > deleted_tuples;

  // synch helpers
  std::mutex txn_mutex;

};

//===--------------------------------------------------------------------===//
// Transaction Manager
//===--------------------------------------------------------------------===//

class TransactionManager {

 public:

  TransactionManager();

  ~TransactionManager();

  // Singleton
  static TransactionManager& getInstance();

  // Get next transaction id
  txn_id_t GetNextTransactionId();

  // Begin a new transaction
  static Transaction BeginTransaction();

  static Transaction& GetTransaction(txn_id_t txn_id);

  static std::vector<Transaction*> CommitTransaction(Transaction txn, bool group_commit = true);

  static void UndoTransaction(Transaction txn);

  static bool IsValid(txn_id_t txn_id);

  static std::vector<Transaction> GetCurrentTransactions();

  // Get the last valid commit id
  txn_id_t GetLastCommitId();

  void Abort();

  void EndTransaction(txn_id_t txn_id, bool sync);

  void Reset();

  void WaitForCurrentTransactions() const;

  void CommitModifications(Transaction& txn, bool sync);

  void CommitPendingTransactions(std::vector<Transaction*>& txns, Transaction *txn);

  Transaction* BeginCommitPhase(Transaction& txn);

  std::vector<Transaction*> EndCommitPhase(Transaction* txns, bool sync);

 private:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<txn_id_t> txn_counter;

  std::atomic<cid_t> next_cid;

  cid_t last_cid __attribute__((aligned(16)));

  Transaction *last_txn __attribute__((aligned(16)));

  // Table tracking all active transactions
  std::map<txn_id_t, Transaction*> txn_table;

  // synch helpers
  std::mutex txn_manager_mutex;
};


} // End catalog namespace
} // End nstore namespace
