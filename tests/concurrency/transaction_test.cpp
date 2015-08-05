//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_test.cpp
//
// Identification: tests/concurrency/transaction_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "harness.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

void TransactionTest(concurrency::TransactionManager *txn_manager) {
  concurrency::Transaction *txn1, *txn2, *txn3;

  uint64_t thread_id = GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 1000; txn_itr++) {
    txn1 = txn_manager->BeginTransaction();
    txn2 = txn_manager->BeginTransaction();
    txn3 = txn_manager->BeginTransaction();

    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    if (txn_itr % 50 != 0) {
      txn_manager->CommitTransaction(txn3);
      txn_manager->CommitTransaction(txn2);
      txn_manager->CommitTransaction(txn1);
    } else {
      txn_manager->AbortTransaction(txn1);
      txn_manager->AbortTransaction(txn3);
      txn_manager->AbortTransaction(txn2);
    }
  }
}

TEST(TransactionTests, TransactionTest) {
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  LaunchParallelTest(8, TransactionTest, &txn_manager);

  std::cout << "Last Commit Id :: " << txn_manager.GetLastCommitId() << "\n";

  std::cout << "Current transactions count :: "
            << txn_manager.GetCurrentTransactions().size() << "\n";
}

}  // End test namespace
}  // End peloton namespace
