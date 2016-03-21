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
#include "backend/storage/tile_group_header.h"

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
                auto tile_group_header = tile_group->GetHeader();
                for (auto tuple_slot : entry.second){
                    if (tile_group_header->GetTransactionId(tuple_slot) == current_txn->GetTransactionId()){
                        // the version is owned by the transaction.
                        continue;
                    }
                    else {
                        if (tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
                            continue;
                        }
                    }
                    AbortTransaction();
                    return;
                }
            }

            auto written_tuples = current_txn->GetWrittenTuples();
            for (auto entry : written_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                auto tile_group_header = tile_group->GetHeader();
                for (auto tuple_slot : entry.second) {
                    // TODO: atomic execution.
                    tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
                    ItemPointer new_version = tile_group_header->GetPrevItemPointer(tuple_slot);
                    auto new_tile_group_header = manager.GetTileGroup(new_version.block)->GetHeader();
                    new_tile_group_header->SetTransactionId(new_version.offset, current_txn->GetTransactionId());
                    new_tile_group_header->SetBeginCommitId(new_version.offset, end_commit_id);
                    new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
                    tile_group_header->ReleaseTupleSlot(tuple_slot, current_txn->GetTransactionId());
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
                auto tile_group_header = tile_group->GetHeader();
                for (auto tuple_slot : entry.second) {
                    // TODO: atomic execution.
                    tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
                    ItemPointer new_version = tile_group_header->GetPrevItemPointer(tuple_slot);
                    auto new_tile_group_header = manager.GetTileGroup(new_version.block)->GetHeader();
                    new_tile_group_header->SetTransactionId(new_version.offset, current_txn->GetTransactionId());
                    new_tile_group_header->SetBeginCommitId(new_version.offset, end_commit_id);
                    new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
                    tile_group_header->ReleaseDeleteTupleSlot(tuple_slot, current_txn->GetTransactionId());
                }
            }
            delete current_txn;
        }

        void TransactionManager::AbortTransaction(){
            LOG_INFO("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
            auto &manager = catalog::Manager::GetInstance();
            auto written_tuples = current_txn->GetWrittenTuples();
            for (auto entry : written_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                auto tile_group_header = tile_group->GetHeader();
                for (auto tuple_slot : entry.second) {
                    tile_group_header->ReleaseTupleSlot(tuple_slot, current_txn->GetTransactionId());
                    tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
                    ItemPointer new_version = tile_group_header->GetPrevItemPointer(tuple_slot);
                    auto new_tile_group_header = manager.GetTileGroup(new_version.block)->GetHeader();
                    new_tile_group_header->SetTransactionId(new_version.offset, INVALID_TXN_ID);
                    new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
                    new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
                }
            }

            auto deleted_tuples = current_txn->GetDeletedTuples();
            for (auto entry : deleted_tuples){
                oid_t tile_group_id = entry.first;
                auto tile_group = manager.GetTileGroup(tile_group_id);
                auto tile_group_header = tile_group->GetHeader();
                for (auto tuple_slot : entry.second) {
                    tile_group_header->ReleaseTupleSlot(tuple_slot, current_txn->GetTransactionId());
                    tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
                    ItemPointer new_version = tile_group_header->GetPrevItemPointer(tuple_slot);
                    auto new_tile_group_header = manager.GetTileGroup(new_version.block)->GetHeader();
                    new_tile_group_header->SetTransactionId(new_version.offset, INVALID_TXN_ID);
                    new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
                    new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
                }
            }

            delete current_txn;
        }

        void TransactionManager::ResetStates() {
            next_txn_id = START_TXN_ID;
            next_cid = START_CID;
        }

    }  // End storage namespace
}  // End peloton namespace