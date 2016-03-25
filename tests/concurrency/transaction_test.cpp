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
  std::this_thread::sleep_for(std::chrono::microseconds(last_time));

  auto transaction = txn_manager->BeginTransaction();
  for (int i = 0; i < (int)schedule->operations.size(); i++) {
    // Sleep milliseconds
    std::this_thread::sleep_for(
        std::chrono::microseconds(schedule->times[i] - last_time));
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
        int result = TransactionTestsUtil::ExecuteRead(transaction, table, id);
        schedule->results.push_back(result);
        break;
      }
      case TXN_OP_DELETE: {
        TransactionTestsUtil::ExecuteDelete(transaction, table, id);
        break;
      }
      case TXN_OP_UPDATE: {
        TransactionTestsUtil::ExecuteUpdate(transaction, table, id, value);
        break;
      }
      case TXN_OP_ABORT: {
        // Assert last operation
        assert(i == (int)schedule->operations.size() - 1);
        break;
      }
      case TXN_OP_NOTHING: {
        // Do nothing
        break;
      }
    }
  }
  if (schedule->operations.rbegin()->op == TXN_OP_ABORT)
    txn_manager->AbortTransaction();
  else
    txn_manager->CommitTransaction();
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
  schedule1.AddUpdate(0, 1000, 0);
  schedule1.AddAbort(500);
  schedule2.AddRead(0, 1000);
  schedules.AddSchedule(&schedule1);
  schedules.AddSchedule(&schedule2);
  LaunchParallelTest(2, ThreadExecuteSchedule, &txn_manager, table.get(),
                     &schedules);

  EXPECT_EQ(schedule2.results[0], 0);
}

TEST_F(TransactionTests, SnapshotIsolationTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());
  TransactionSchedule schedule1;
  //  TransactionSchedule schedule2;
  //  TransactionSchedule schedule3;

  schedule1.AddInsert(1, 1, 0);
  schedule1.AddRead(1, 0);
  ExecuteSchedule(&txn_manager, table.get(), &schedule1);
  EXPECT_EQ(1, schedule1.results[0]);
}

}  // End test namespace
}  // End peloton namespace
