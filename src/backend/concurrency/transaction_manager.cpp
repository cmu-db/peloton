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
#include "backend/common/logger.h"

namespace peloton {
    namespace concurrency {

        // Current transaction for the backend thread
        thread_local Transaction *current_txn;

        TransactionManager::TransactionManager() {
            next_txn_id = START_TXN_ID;
            next_cid = START_CID;
        }

        TransactionManager::~TransactionManager() {}

        TransactionManager &TransactionManager::GetInstance() {
            static TransactionManager txn_manager;
            return txn_manager;
        }

        Transaction * TransactionManager::BeginTransaction(){
            Transaction *txn = new Transaction(GetNextTransactionId(), GetNextCommitId());
            current_txn = txn;
            return txn;
        }

        void TransactionManager::CommitTransaction(){
            LOG_INFO("Committing peloton txn : %lu ", current_txn->GetTransactionId());
            // generate transaction id.
            cid_t end_commit_id = GetNextCommitId();

            // validate read set.
            auto read_tuples = current_txn->GetReadTuples();
            for (auto entry : read_tuples){
                oid_t tile_group_id = entry.first;

                for (auto tuple_slot : entry.second){

                }     
            }

            // install write set.
            auto write_tuples = current_txn->GetWriteTuples();
            for (auto entry : write_tuples){
                oid_t tile_group_id = entry.first;

                for (auto tuple_slot : entry.second){

                }
            }
        }

        void TransactionManager::AbortTransaction(){
            LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

        }

        void TransactionManager::ResetStates() {
            next_txn_id = START_TXN_ID;
            next_cid = START_CID;
        }

    }  // End storage namespace
}  // End peloton namespace