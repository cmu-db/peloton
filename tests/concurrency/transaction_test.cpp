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
    // 0 ms       T1 updates (0, ?) to (0, 1)
    // 500 ms     T2 updates (0, ?) to (0, 2), and commits
    // 1000 ms    T1 commits

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
}

void FuzzyReadTest() {
  //  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //  std::unique_ptr<storage::DataTable> table(
  //      TransactionTestsUtil::CreateTable());
  //
  //  TransactionSchedules schedules;
  //
  //  // T1 Read at 0 ms, T2 update at 500 ms, T1 commit at 1000 ms, T1 should
  //  fail
  //  TransactionSchedule schedule1;
  //  schedule1.AddRead(0, 0);
  //  schedule1.AddDoNothing(1000);
  //  TransactionSchedule schedule2;
  //  schedule2.AddUpdate(0, 1, 500);
  //
  //  // T1 Read at 0 ms, T2 delete at 500 ms, T1 commit at 1000 ms, T1 should
  //  fail
  //  TransactionSchedule schedule3;
  //  schedule3.AddRead(1, 0);
  //  schedule3.AddDoNothing(1000);
  //  TransactionSchedule schedule4;
  //  schedule4.AddDelete(1, 500);
  //
  //  schedules.AddSchedule(&schedule1);
  //  schedules.AddSchedule(&schedule2);
  //  schedules.AddSchedule(&schedule3);
  //  schedules.AddSchedule(&schedule4);
  //
  //  LaunchParallelTest(4, ThreadExecuteSchedule, &txn_manager, table.get(),
  //                     &schedules);
  //
  //  EXPECT_EQ(RESULT_SUCCESS, schedule2.txn_result);
  //  EXPECT_EQ(RESULT_SUCCESS, schedule4.txn_result);
  //  EXPECT_EQ(RESULT_ABORTED, schedule1.txn_result);
  //  EXPECT_EQ(RESULT_ABORTED, schedule3.txn_result);
}

void PhantomTest() {
  //  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //  std::unique_ptr<storage::DataTable> table(
  //      TransactionTestsUtil::CreateTable());
  //
  //  TransactionSchedules schedules;
  //  // Begin Scan for id>=0 at 0 ms, Insert (5, 0) at 500 ms, Commit at 1000
  //  ms,
  //  // should fail
  //  TransactionSchedule schedule1;
  //  schedule1.AddScan(0, 0);
  //  schedule1.AddDoNothing(1000);
  //  TransactionSchedule schedule2;
  //  schedule2.AddInsert(5, 0, 500);
  //  // Another txn scan again, should see the new val
  //  TransactionSchedule schedule3;
  //  schedule3.AddScan(0, 1500);
  //  TransactionSchedule schedule4;
  //  schedule4.AddScan(0, 0);
  //
  //  // Begin scan for id>=0 at 1500 ms, Delete (5, 0) at 2000 ms, Commit at
  //  2500 ms.
  //  // Should have missing value.
  //  TransactionSchedule schedule5;
  //  TransactionSchedule schedule6;
  //  schedule5.AddScan(0, 1500);
  //  schedule6.AddDelete(5, 2000);
  //  schedule5.AddDoNothing(2500);
  //
  //  schedules.AddSchedule(&schedule1);
  //  schedules.AddSchedule(&schedule2);
  //  schedules.AddSchedule(&schedule3);
  //  schedules.AddSchedule(&schedule4);
  //  schedules.AddSchedule(&schedule5);
  //  schedules.AddSchedule(&schedule6);
  //
  //  LaunchParallelTest(6, ThreadExecuteSchedule, &txn_manager, table.get(),
  //                     &schedules);
  //
  //  EXPECT_EQ(RESULT_ABORTED, schedule1.txn_result);
  //  EXPECT_EQ(RESULT_SUCCESS, schedule2.txn_result);
  //  EXPECT_EQ(RESULT_SUCCESS, schedule3.txn_result);
  //  EXPECT_EQ(RESULT_SUCCESS, schedule4.txn_result);
  //  EXPECT_EQ(RESULT_ABORTED, schedule5.txn_result);
  //  EXPECT_EQ(RESULT_SUCCESS, schedule6.txn_result);
  //  EXPECT_EQ(11, schedule3.results.size());
  //  EXPECT_EQ(10, schedule4.results.size());
}

TEST_F(TransactionTests, TransactionTest) {
  //  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //
  //  LaunchParallelTest(8, TransactionTest, &txn_manager);
  //
  //  std::cout << "next Commit Id :: " << txn_manager.GetNextCommitId() <<
  //  "\n";
}

TEST_F(TransactionTests, AbortTest) {
  //  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //  std::unique_ptr<storage::DataTable> table(
  //      TransactionTestsUtil::CreateTable());
  //  TransactionSchedules schedules;
  //  TransactionSchedule schedule1;
  //  TransactionSchedule schedule2;
  //  // Schedule one thread to update and abort, another thread to read
  //  afterwards
  //  schedule1.AddUpdate(0, 100, 0);
  //  schedule1.AddAbort(50);
  //  schedule2.AddRead(0, 100);
  //  schedules.AddSchedule(&schedule1);
  //  schedules.AddSchedule(&schedule2);
  //  LaunchParallelTest(2, ThreadExecuteSchedule, &txn_manager, table.get(),
  //                     &schedules);
  //
  //  EXPECT_EQ(schedule2.results[0], 0);
}

TEST_F(TransactionTests, SerializableTest) {
  //    DirtyWriteTest();
  DirtyReadTest();
  //  FuzzyReadTest();
  //  PhantomTes();
}

}  // End test namespace
}  // End peloton namespace
