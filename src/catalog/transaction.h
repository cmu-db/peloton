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

#include "storage/tile_group.h"
#include "common/exception.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class TransactionManager;

class Transaction {
  friend class TransactionManager;

  Transaction(Transaction const&) = delete;

 public:

  Transaction()  :
    txn_id(INVALID_TXN_ID),
    cid(INVALID_CID),
    last_cid(INVALID_CID),
    ref_count(1),
    waiting_to_commit(false),
    next(nullptr) {
  }

  Transaction(txn_id_t txn_id, cid_t last_cid)  :
    txn_id(txn_id),
    cid(INVALID_CID),
    last_cid(last_cid),
    ref_count(1),
    waiting_to_commit(false),
    next(nullptr) {
  }

  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  // record inserted tuple
  void RecordInsert(const storage::TileGroup* tile, id_t offset);

  // record deleted tuple
  void RecordDelete(const storage::TileGroup* tile, id_t offset);

  bool HasInsertedTuples(const storage::TileGroup* tile) const;

  bool HasDeletedTuples(const storage::TileGroup* tile) const;

  const std::map<const storage::TileGroup*, std::vector<id_t> >& GetInsertedTuples();

  const std::map<const storage::TileGroup*, std::vector<id_t> >& GetDeletedTuples();

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

  // commit id
  cid_t cid;

  // last visible commit id
  cid_t last_cid;

  // references
  std::atomic<size_t> ref_count;

  // waiting for commit ?
  std::atomic<bool> waiting_to_commit;

  // cid context
  Transaction* next __attribute__((aligned(16)));

  // inserted tuples in each tile
  std::map<const storage::TileGroup*, std::vector<id_t> > inserted_tuples;

  // deleted tuples in each tile
  std::map<const storage::TileGroup*, std::vector<id_t> > deleted_tuples;

  // synch helpers
  std::mutex txn_mutex;

};

//===--------------------------------------------------------------------===//
// Transaction Manager
//===--------------------------------------------------------------------===//

class TransactionManager {

 public:

  TransactionManager() {
    next_txn_id = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid = ATOMIC_VAR_INIT(START_CID);

    last_txn = new Transaction(START_TXN_ID, START_CID);
    last_txn->cid = START_CID;
    last_cid = START_CID;
  }

  ~TransactionManager() {

    // delete base txn
    delete last_txn;
  }

  // Get next transaction id
  txn_id_t GetNextTransactionId() {
    if (next_txn_id == MAX_TXN_ID) {
      throw TransactionException("Txn id equals MAX_TXN_ID");
    }

    return next_txn_id++;
  }

  // Get last commit id for visibility checks
  cid_t GetLastCommitId() {
    return last_cid;
  }

  // Get entry in table
  Transaction *GetTransaction(txn_id_t txn_id);

  std::vector<Transaction *> GetCurrentTransactions();

  bool IsValid(txn_id_t txn_id);

  //===--------------------------------------------------------------------===//
  // Transaction processing
  //===--------------------------------------------------------------------===//

  // Begin a new transaction
  Transaction *BeginTransaction();

  // End the transaction
  void EndTransaction(Transaction *txn, bool sync = true);

  // COMMIT

  void BeginCommitPhase(Transaction *txn);

  void CommitModifications(Transaction *txn, bool sync = true);

  void CommitPendingTransactions(std::vector<Transaction*>& txns, Transaction *txn);

  std::vector<Transaction*> EndCommitPhase(Transaction* txn, bool sync = true);

  void CommitTransaction(Transaction *txn, bool sync = true);

  // ABORT

  void AbortTransaction(Transaction *txn);

  void WaitForCurrentTransactions() const;

 private:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<txn_id_t> next_txn_id;

  std::atomic<cid_t> next_cid;

  cid_t last_cid __attribute__((aligned(16)));

  Transaction *last_txn __attribute__((aligned(16)));

  // Table tracking all active transactions
  std::map<txn_id_t, Transaction *> txn_table;

  // synch helpers
  std::mutex txn_manager_mutex;
};


} // End catalog namespace
} // End nstore namespace
