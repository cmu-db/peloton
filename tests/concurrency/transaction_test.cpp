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
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionTests : public PelotonTest {};

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

void DirtyWriteTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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

void DirtyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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

    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[1].txn_result);
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

    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[1].txn_result);
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

    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[1].txn_result);
  }
}

void FuzzyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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

    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
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

    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
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

    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
  }
}

void PhantomTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddScan(0, 0);
    scheduler.AddInsert(1, 5, 0);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();
    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.AddScan(0, 0);
    scheduler.AddDelete(1, 4);
    scheduler.AddCommit(1);
    scheduler.AddCommit(0);

    scheduler.Run();
    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
  }
}

void WriteSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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

void ReadSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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

    EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
  }
}

TEST_F(TransactionTests, TransactionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  LaunchParallelTest(8, TransactionTest, &txn_manager);

  std::cout << "next Commit Id :: " << txn_manager.GetNextCommitId() << "\n";
}

TEST_F(TransactionTests, AbortTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.AddUpdate(0, 0, 100);
  scheduler.AddRead(1, 0);
  scheduler.AddAbort(0);
  scheduler.AddCommit(1);

  scheduler.Run();

  EXPECT_EQ(RESULT_ABORTED, scheduler.schedules[0].txn_result);
  EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
}

TEST_F(TransactionTests, SerializableTest) {
  DirtyWriteTest();
  DirtyReadTest();
  FuzzyReadTest();
  WriteSkewTest();
  ReadSkewTest();
  //  PhantomTes();
}

}  // End test namespace
}  // End peloton namespace
