//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <cassert>
#include <vector>
#include <map>
#include <mutex>

#include "backend/common/types.h"

namespace peloton {
namespace concurrency {

#define BASE_REF_COUNT 1

typedef unsigned int TransactionId;

class Transaction;

//===--------------------------------------------------------------------===//
// Transaction Manager
//===--------------------------------------------------------------------===//

class TransactionManager {
 public:
  TransactionManager();

  ~TransactionManager();

  // Get next transaction id
  txn_id_t GetNextTransactionId();

  // Get last commit id for visibility checks
  cid_t GetLastCommitId() { return last_cid; }

  //===--------------------------------------------------------------------===//
  // Transaction processing
  //===--------------------------------------------------------------------===//

  static TransactionManager &GetInstance();

  // Begin a new transaction
  Transaction *BeginTransaction();

  // Get entry in transaction table
  Transaction *GetTransaction(txn_id_t txn_id);

  // End the transaction
  void EndTransaction(Transaction *txn, bool sync = true);

  // Get the list of current transactions
  std::vector<Transaction *> GetCurrentTransactions();

  // validity checks
  bool IsValid(txn_id_t txn_id);

  // used by recovery testing
  void ResetStates(void);

  // COMMIT

  void BeginCommitPhase(Transaction *txn);

  void CommitModifications(Transaction *txn, bool sync = true);

  void CommitPendingTransactions(std::vector<Transaction *> &txns,
                                 Transaction *txn);

  std::vector<Transaction *> EndCommitPhase(Transaction *txn, bool sync = true);

  void CommitTransaction(Transaction *txn, bool sync = true);

  // ABORT

  void AbortTransaction(Transaction *txn);

  // Store PG Transaction
  Transaction *StartPGTransaction(TransactionId txn_id);

  // Get PG Transaction
  Transaction *GetPGTransaction(TransactionId txn_id);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  std::atomic<txn_id_t> next_txn_id;

  std::atomic<cid_t> next_cid;

  cid_t last_cid __attribute__((aligned(16)));

  Transaction *last_txn;

  // Table tracking all active transactions
  // Our transaction id -> our transaction
  // Sync access with txn_table_mutex
  std::map<txn_id_t, Transaction *> txn_table;

  std::mutex txn_table_mutex;

  // Postgres transaction id -> our transaction
  // Sync access with pg_txn_table_mutex
  std::map<TransactionId, Transaction *> pg_txn_table;

  std::mutex pg_txn_table_mutex;
};

}  // End concurrency namespace
}  // End peloton namespace
