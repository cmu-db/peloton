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

#include "common/types.h"
#include "storage/tile_group.h"
#include "common/exception.h"

namespace nstore {
namespace catalog {

#define BASE_REF_COUNT 1

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
    ref_count(BASE_REF_COUNT),
    waiting_to_commit(false),
    next(nullptr) {
  }

  Transaction(txn_id_t txn_id, cid_t last_cid)  :
    txn_id(txn_id),
    cid(INVALID_CID),
    last_cid(last_cid),
    ref_count(BASE_REF_COUNT),
    waiting_to_commit(false),
    next(nullptr) {
  }

  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  // record inserted tuple
  void RecordInsert(storage::TileGroup* tile_group, oid_t offset);

  // record deleted tuple
  void RecordDelete(storage::TileGroup* tile_group, oid_t offset);

  // check if it has inserted any tuples in given tile group
  bool HasInsertedTuples(storage::TileGroup* tile_group) const;

  // check if it has deleted any tuples in given tile group
  bool HasDeletedTuples(storage::TileGroup* tile_group) const;

  const std::map<storage::TileGroup*, std::vector<oid_t> >& GetInsertedTuples();

  const std::map<storage::TileGroup*, std::vector<oid_t> >& GetDeletedTuples();

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
  Transaction *next __attribute__((aligned(16)));

  // inserted tuples
  std::map<storage::TileGroup*, std::vector<oid_t> > inserted_tuples;

  // deleted tuples
  std::map<storage::TileGroup*, std::vector<oid_t> > deleted_tuples;

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

    // BASE transaction
    // All transactions are based on this transaction
    last_txn = new Transaction(START_TXN_ID, START_CID);
    last_txn->cid = START_CID;
    last_cid = START_CID;
  }

  ~TransactionManager() {
    // delete BASE txn
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

  //===--------------------------------------------------------------------===//
  // Transaction processing
  //===--------------------------------------------------------------------===//

  static TransactionManager& GetInstance();

  // Begin a new transaction
  Transaction *BeginTransaction();

  // End the transaction
  void EndTransaction(Transaction *txn, bool sync = true);

  // Build a transaction
  Transaction *BuildTransaction();

  // Get entry in transaction table
  Transaction *GetTransaction(txn_id_t txn_id);

  // Get the list of current transactions
  std::vector<Transaction *> GetCurrentTransactions();

  // validity checks
  bool IsValid(txn_id_t txn_id);

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
