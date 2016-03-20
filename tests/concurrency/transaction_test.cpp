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

#include "harness.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionTests : public PelotonTest {};

void TransactionTest(concurrency::TransactionManager *txn_manager) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 1000; txn_itr++) {
    txn_manager->BeginTransaction();

    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    if (txn_itr % 50 != 0) {
      txn_manager->CommitTransaction();
    } else {
      txn_manager->AbortTransaction();
    }
  }
}

TEST_F(TransactionTests, TransactionTest) {
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  LaunchParallelTest(1, TransactionTest, &txn_manager);

  std::cout << "next Commit Id :: " << txn_manager.GetNextCommitId() << "\n";
}

}  // End test namespace
}  // End peloton namespace
