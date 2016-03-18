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

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/storage/tile_group.h"

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

            auto &manager = catalog::Manager::GetInstance();

            // generate transaction id.
            cid_t end_commit_id = GetNextCommitId();

            auto read_tuples = current_txn->GetReadTuples();
            for (auto entry : read_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                for (auto tuple_slot : entry.second){
                //    tile_group->CommitInsertedTuple(tuple_slot, current_txn->txn_id, end_commit_id);
                }

            }

            auto written_tuples = current_txn->GetWrittenTuples();
            for (auto entry : written_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                for (auto tuple_slot : entry.second){
                //    tile_group->CommitDeletedTuple(tuple_slot, current_txn->txn_id, end_commit_id);
                }
            }

            // commit insert set.
            auto inserted_tuples = current_txn->GetInsertedTuples();
            for (auto entry : inserted_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                for (auto tuple_slot : entry.second){
                    tile_group->CommitInsertedTuple(tuple_slot, current_txn->txn_id, end_commit_id);
                }
            }

            // commit delete set.
            auto deleted_tuples = current_txn->GetDeletedTuples();
            for (auto entry : deleted_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                for (auto tuple_slot : entry.second){
                    tile_group->CommitDeletedTuple(tuple_slot, current_txn->txn_id, end_commit_id);
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