//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/concurrency/transaction_manager.h"

#include "backend/concurrency/transaction.h"

namespace peloton {
    namespace concurrency {

        // Current transaction for the backend thread
        thread_local Transaction *current_txn;

        TransactionManager::TransactionManager() {
            next_txn_id = START_TXN_ID;
            next_cid = START_CID;

            last_txn = new Transaction(START_TXN_ID, START_CID);
            last_txn->end_cid = START_CID;
        }

        TransactionManager::~TransactionManager() {
            delete last_txn;
        }

        TransactionManager &TransactionManager::GetInstance() {
            static TransactionManager txn_manager;
            return txn_manager;
        }

        Transaction * TransactionManager::BeginTransaction(){
            Transaction *txn = new Transaction(GetNextTransactionId(), GetNextCommitId());

            current_txn = txn;
            return txn;
        }

        void TransactionManager::EndTransaction(){}

        void TransactionManager::CommitTransaction(){}

        void TransactionManager::AbortTransaction(){


        }

        void TransactionManager::ResetStates() {
            next_txn_id = START_TXN_ID;
            next_cid = START_CID;

            delete last_txn;
            last_txn = new Transaction(START_TXN_ID, START_CID);
            last_txn->end_cid = START_CID;


        }

    }  // End storage namespace
}  // End peloton namespace