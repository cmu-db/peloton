/*-------------------------------------------------------------------------
 *
 * transaction_manager.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/storage/tile_group.h"

#include <atomic>
#include <cassert>
#include <vector>
#include <map>

namespace nstore {
namespace concurrency {

#define BASE_REF_COUNT 1

class Transaction;
    
//===--------------------------------------------------------------------===//
// Transaction Manager
//===--------------------------------------------------------------------===//
    
class TransactionManager {
    
public:

    TransactionManager();

    ~TransactionManager();

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
    // MEMBERS
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

} // End concurrency namespace
} // End nstore namespace
