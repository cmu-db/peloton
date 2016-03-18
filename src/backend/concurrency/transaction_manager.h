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
#include <unordered_map>

#include "backend/common/platform.h"
#include "backend/common/types.h"


namespace peloton {
    namespace concurrency {

        extern thread_local Transaction *current_txn;

        class TransactionManager {
        public:
            TransactionManager();
            ~TransactionManager();

            static TransactionManager &GetInstance();

            txn_id_t GetNextTransactionId(){
                return next_txn_id++;
            }

            cid_t GetNextCommitId(){
                return next_cid++;
            }

            Transaction *BeginTransaction();

            void CommitTransaction();

            void AbortTransaction();

            void ResetStates();

        private:
            std::atomic<txn_id_t> next_txn_id;
            std::atomic<cid_t> next_cid;
        };
    }  // End storage namespace
}  // End peloton namespace
