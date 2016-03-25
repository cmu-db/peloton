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

// Execute a schedule, the result for any read will be recorded in
// schedule->results
void ExecuteSchedule(concurrency::TransactionManager *txn_manager,
                     storage::DataTable *table, TransactionSchedule *schedule) {
  // Sleep for the first operation, this gives the opportunity to schedule
  // the start time of a txn
  int last_time = schedule->times[0];
  std::this_thread::sleep_for(std::chrono::milliseconds(last_time));

  auto transaction = txn_manager->BeginTransaction();
  for (int i = 0; i < (int)schedule->operations.size(); i++) {
    // Sleep milliseconds
    std::this_thread::sleep_for(
        std::chrono::milliseconds(schedule->times[i] - last_time));
    last_time = schedule->times[i];

    // Prepare data for operation
    txn_op_t op = schedule->operations[i].op;
    int id = schedule->operations[i].id;
    int value = schedule->operations[i].value;

    // Execute the operation
    switch (op) {
      case TXN_OP_INSERT: {
        LOG_TRACE("Execute Insert");
        TransactionTestsUtil::ExecuteInsert(transaction, table, id, value);
        break;
      }
      case TXN_OP_READ: {
        LOG_TRACE("Execute Read");
        int result = TransactionTestsUtil::ExecuteRead(transaction, table, id);
        schedule->results.push_back(result);
        break;
      }
      case TXN_OP_DELETE: {
        LOG_TRACE("Execute Delete");
        TransactionTestsUtil::ExecuteDelete(transaction, table, id);
        break;
      }
      case TXN_OP_UPDATE: {
        LOG_TRACE("Execute Update");
        TransactionTestsUtil::ExecuteUpdate(transaction, table, id, value);
        break;
      }
      case TXN_OP_ABORT: {
        LOG_TRACE("Abort");
        // Assert last operation
        assert(i == (int)schedule->operations.size() - 1);
        break;
      }
      case TXN_OP_NOTHING: {
        LOG_TRACE("Do nothing");
        // Do nothing
        break;
      }
    }
  }
  schedule->txn_result = true;
  if (schedule->operations.rbegin()->op == TXN_OP_ABORT)
    txn_manager->AbortTransaction();
  else {
    schedule->txn_result = txn_manager->CommitTransaction();
  }

  LOG_TRACE("Txn finished");
}

void ThreadExecuteSchedule(concurrency::TransactionManager *txn_manager,
                           storage::DataTable *table,
                           TransactionSchedules *schedules) {
  int next_schedule = schedules->next_sched.fetch_add(1);
  auto schedule = schedules->schedules[next_schedule];
  ExecuteSchedule(txn_manager, table, schedule);
}

void TransactionTest(concurrency::TransactionManager *txn_manager) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 100; txn_itr++) {
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

void DirtyWriteTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());
  TransactionSchedules schedules;

  // Update (0, ?) to (0, 1) at 0 ms, commit at 1000 ms
  TransactionSchedule schedule1;
  schedule1.AddUpdate(0, 1, 0);
  schedule1.AddDoNothing(1000);
  // Update (0, ?) to (0, 2) at 500 ms
  TransactionSchedule schedule2;
  schedule2.AddUpdate(0, 2, 500);
  // Read (0, ?) at 1500 ms, should read 1
  TransactionSchedule schedule3;
  schedule3.AddRead(0, 1500);

  schedules.AddSchedule(&schedule1);
  schedules.AddSchedule(&schedule2);
  schedules.AddSchedule(&schedule3);

  LaunchParallelTest(3, ThreadExecuteSchedule, &txn_manager, table.get(),
                     &schedules);

  EXPECT_EQ(1, schedule3.results[0]);
  // When to use RESULT_FAILURE, when RESULT_ABORTED
  EXPECT_EQ(false, schedule2.txn_result);
}

void DirtyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  TransactionSchedules schedules;

  // Insert (1, 1) at 500 ms, commit at 1500 ms
  TransactionSchedule schedule1;
  schedule1.AddInsert(10, 1, 500);
  schedule1.AddDoNothing(1500);
  // Start transaction at 0 ms, but read at 2000 ms, should read nothing
  TransactionSchedule schedule2;
  schedule2.AddDoNothing(0);
  schedule2.AddRead(10, 2000);
  // Read 1 at 1000 ms, should not read the uncommitted version
  TransactionSchedule schedule3;
  schedule3.AddRead(10, 1000);
  // Read 1 at 2000 ms, should read the committed version
  TransactionSchedule schedule4;
  schedule4.AddRead(10, 2000);

  schedules.AddSchedule(&schedule1);
  schedules.AddSchedule(&schedule2);
  schedules.AddSchedule(&schedule3);
  schedules.AddSchedule(&schedule4);

  LaunchParallelTest(4, ThreadExecuteSchedule, &txn_manager, table.get(),
                     &schedules);

  EXPECT_EQ(-1, schedule2.results[0]);
  EXPECT_EQ(-1, schedule3.results[0]);
  EXPECT_EQ(1, schedule4.results[0]);
}

void FuzzyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  TransactionSchedules schedules;

  // T1 Read at 0 ms, T2 update at 500 ms, T1 commit at 1000 ms, T1 should fail
  TransactionSchedule schedule1;
  schedule1.AddRead(0, 0);
  schedule1.AddDoNothing(1000);
  TransactionSchedule schedule2;
  schedule2.AddUpdate(0, 1, 500);

  // T1 Read at 0 ms, T2 delete at 500 ms, T1 commit at 1000 ms, T1 should fail
  TransactionSchedule schedule3;
  schedule3.AddRead(1, 0);
  schedule3.AddDoNothing(1000);
  TransactionSchedule schedule4;
  schedule4.AddDelete(1, 500);

  schedules.AddSchedule(&schedule1);
  schedules.AddSchedule(&schedule2);
  schedules.AddSchedule(&schedule3);
  schedules.AddSchedule(&schedule4);

  LaunchParallelTest(4, ThreadExecuteSchedule, &txn_manager, table.get(),
                     &schedules);

  EXPECT_EQ(true, schedule2.txn_result);
  EXPECT_EQ(true, schedule4.txn_result);
  EXPECT_EQ(false, schedule1.txn_result);
  EXPECT_EQ(false, schedule3.txn_result);
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
  TransactionSchedules schedules;
  TransactionSchedule schedule1;
  TransactionSchedule schedule2;
  // Schedule one thread to update and abort, another thread to read afterwards
  schedule1.AddUpdate(0, 100, 0);
  schedule1.AddAbort(50);
  schedule2.AddRead(0, 100);
  schedules.AddSchedule(&schedule1);
  schedules.AddSchedule(&schedule2);
  LaunchParallelTest(2, ThreadExecuteSchedule, &txn_manager, table.get(),
                     &schedules);

  EXPECT_EQ(schedule2.results[0], 0);
}

TEST_F(TransactionTests, SerializableTest) {
  DirtyWriteTest();
  DirtyReadTest();
  FuzzyReadTest();
  // PhantomTest();
}

}  // End test namespace
}  // End peloton namespace
