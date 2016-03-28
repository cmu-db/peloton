//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_test.cpp
//
// Identification: tests/concurrency/transaction_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionTests : public PelotonTest {};

static std::vector<ConcurrencyType> TEST_TYPES = {CONCURRENCY_TYPE_OCC,
                                                  CONCURRENCY_TYPE_2PL};

void TransactionTest(concurrency::TransactionManager *txn_manager) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 50; txn_itr++) {
    txn_manager->BeginTransaction();
    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    if (txn_itr % 25 != 0) {
      txn_manager->CommitTransaction();
    } else {
      txn_manager->AbortTransaction();
    }
  }
}

TEST_F(TransactionTests, TransactionTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    LaunchParallelTest(8, TransactionTest, &txn_manager);

    LOG_INFO("next Commit Id :: %lu", txn_manager.GetNextCommitId());
  }
}

TEST_F(TransactionTests, AbortTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    std::unique_ptr<storage::DataTable> table(
        TransactionTestsUtil::CreateTable());
    {
      TransactionScheduler scheduler(2, table.get(), &txn_manager);
      scheduler.AddUpdate(0, 0, 100);
      scheduler.AddAbort(0);
      scheduler.AddRead(1, 0);
      scheduler.AddCommit(1);

      scheduler.Run();

      EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
      EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    }

    {
      TransactionScheduler scheduler(2, table.get(), &txn_manager);
      // scheduler.AddInsert(0, 100, 0);
      scheduler.AddAbort(0);
      scheduler.AddRead(1, 100);
      scheduler.AddCommit(1);

      scheduler.Run();
      EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
      EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(-1, scheduler.schedules[1].results[0]);
    }
  }
}

}  // End test namespace
}  // End peloton namespace
