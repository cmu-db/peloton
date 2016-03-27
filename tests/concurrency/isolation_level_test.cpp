//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_test.cpp
//
// Identification: tests/concurrency/isolation_level_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class IsolationLevelTest : public PelotonTest {};

static std::vector<ConcurrencyType> TEST_TYPES = {
    CONCURRENCY_TYPE_OCC
    // CONCURRENCY_TYPE_2PL
};

void DirtyWriteTest(ConcurrencyType test_type) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(test_type);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    // T1 updates (0, ?) to (0, 1)
    // T2 updates (0, ?) to (0, 2)
    // T1 commits
    // T2 commits
    scheduler.AddUpdate(0, 0, 1);
    scheduler.AddUpdate(1, 0, 2);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    // T1 and T2 can't both succeed
    EXPECT_FALSE(schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_ABORTED) ||
                (schedules[0].txn_result == RESULT_ABORTED &&
                 schedules[1].txn_result == RESULT_SUCCESS));
    schedules.clear();
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);

    scheduler.AddUpdate(0, 0, 1);
    scheduler.AddUpdate(1, 0, 2);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    // T1 and T2 can't both succeed
    EXPECT_FALSE(schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_ABORTED) ||
                (schedules[0].txn_result == RESULT_ABORTED &&
                 schedules[1].txn_result == RESULT_SUCCESS));
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    // T0 delete (0, ?)
    // T1 update (0, ?) to (0, 3)
    // T0 commit
    // T1 commit
    scheduler.AddDelete(0, 0);
    scheduler.AddUpdate(1, 0, 3);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    // T1 and T2 can't both succeed
    EXPECT_FALSE(schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_ABORTED) ||
                (schedules[0].txn_result == RESULT_ABORTED &&
                 schedules[1].txn_result == RESULT_SUCCESS));
    schedules.clear();
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    // T0 delete (1, ?)
    // T1 delete (1, ?)
    // T0 commit
    // T1 commit
    scheduler.AddDelete(0, 1);
    scheduler.AddDelete(1, 1);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    // T1 and T2 can't both succeed
    EXPECT_FALSE(schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == RESULT_SUCCESS &&
                 schedules[1].txn_result == RESULT_ABORTED) ||
                (schedules[0].txn_result == RESULT_ABORTED &&
                 schedules[1].txn_result == RESULT_SUCCESS));
    schedules.clear();
  }
}

void DirtyReadTest(ConcurrencyType TEST_TYPE) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(TEST_TYPE);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);

    // T1 updates (0, ?) to (0, 1)
    // T2 reads (0, ?)
    // T1 commit
    // T2 commit
    scheduler.AddUpdate(0, 0, 1);
    scheduler.AddRead(1, 0);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();

    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);

    // T1 updates (0, ?) to (0, 1)
    // T2 reads (0, ?)
    // T2 commit
    // T1 commit
    scheduler.AddUpdate(0, 0, 1);
    scheduler.AddRead(1, 0);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();

    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);

    // T0 delete (0, ?)
    // T1 read (0, ?)
    // T0 commit
    // T1 commit
    scheduler.AddDelete(0, 0);
    scheduler.AddRead(1, 0);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();

    // FIXME: both can success as long as T1 reads the original value
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
}

void FuzzyReadTest(ConcurrencyType TEST_TYPE) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(TEST_TYPE);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    // T0 read 0
    // T1 update (0, 0) to (0, 1)
    // T1 commit
    // T0 commit
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddUpdate(1, 0, 1);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();

    LOG_TRACE("%lu", scheduler.schedules.size());
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    // T0 read 0
    // T1 update (0, 0) to (0, 1)
    // T0 commit
    // T1 commit
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddUpdate(1, 0, 1);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();

    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    // T0 read 0
    // T1 delete 0
    // T0 commit
    // T1 commit
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddDelete(1, 0);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
}

void PhantomTest(ConcurrencyType TEST_TYPE) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(TEST_TYPE);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddScan(0, 0);
    scheduler.AddInsert(1, 5, 0);
    scheduler.AddScan(0, 0);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();
    size_t original_tuple_count = 10;
    if (scheduler.schedules[0].txn_result == RESULT_SUCCESS && 
      scheduler.schedules[1].txn_result == RESULT_SUCCESS) {
      // Should scan no more tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() == original_tuple_count*2);
    }
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddScan(0, 0);
    scheduler.AddDelete(1, 4);
    scheduler.AddScan(0, 0);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();

    size_t original_tuple_count = 11;
    if (scheduler.schedules[0].txn_result == RESULT_SUCCESS && 
      scheduler.schedules[1].txn_result == RESULT_SUCCESS) {
      // Should scan no less tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() == original_tuple_count*2);
    }
  }
}

void WriteSkewTest(ConcurrencyType TEST_TYPE) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(TEST_TYPE);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddUpdate(0, 1, 1);
    scheduler.AddRead(1, 0);
    scheduler.AddUpdate(1, 1, 2);
    scheduler.AddCommit(0);
    scheduler.AddCommit(1);

    scheduler.Run();

    // Can't all success
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddUpdate(0, 1, 1);
    scheduler.AddRead(1, 0);
    scheduler.AddCommit(0);
    scheduler.AddUpdate(1, 1, 2);
    scheduler.AddCommit(1);

    scheduler.Run();

    // First txn should success
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                RESULT_ABORTED == scheduler.schedules[1].txn_result);
  }
}

void ReadSkewTest(ConcurrencyType test_type) {
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance(test_type);
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());
  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddRead(0, 0);
    scheduler.AddUpdate(1, 0, 1);
    scheduler.AddUpdate(1, 1, 1);
    scheduler.AddCommit(1);
    scheduler.AddRead(0, 1);
    scheduler.AddCommit(0);

    scheduler.Run();

    if (RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[0] == scheduler.schedules[0].results[1]);
    }
  }
}

TEST_F(IsolationLevelTest, SerializableTest) {
  for (auto test_type : TEST_TYPES) {
    DirtyWriteTest(test_type);
    DirtyReadTest(test_type);
    FuzzyReadTest(test_type);
    WriteSkewTest(test_type);
    ReadSkewTest(test_type);
    PhantomTest(test_type);
  }
}

}  // End test namespace
}  // End peloton namespace
