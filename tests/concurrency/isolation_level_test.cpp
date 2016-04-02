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

static std::vector<ConcurrencyType> TEST_TYPES = {CONCURRENCY_TYPE_OPTIMISTIC,
                                                  CONCURRENCY_TYPE_PESSIMISTIC};

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
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

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

    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

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
    scheduler.Txn(0).Delete(0);
    scheduler.Txn(1).Update(0, 3);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

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
    scheduler.Txn(0).Delete(1);
    scheduler.Txn(1).Delete(1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

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
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

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
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

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
    scheduler.Txn(0).Delete(0);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    // FIXME: both can success as long as T1 reads the original value
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
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
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

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
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

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
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Delete(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
}

void PhantomTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Insert(5, 0);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();
    size_t original_tuple_count = 10;
    if (scheduler.schedules[0].txn_result == RESULT_SUCCESS &&
        scheduler.schedules[1].txn_result == RESULT_SUCCESS) {
      // Should scan no more tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Delete(4);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    size_t original_tuple_count = 11;
    if (scheduler.schedules[0].txn_result == RESULT_SUCCESS &&
        scheduler.schedules[1].txn_result == RESULT_SUCCESS) {
      // Should scan no less tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }
}

void WriteSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(0).Update(1, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(1).Update(1, 2);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    // Can't all success
    EXPECT_FALSE(RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    TransactionScheduler scheduler(2, table.get(), &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(0).Update(1, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Update(1, 2);
    scheduler.Txn(1).Commit();

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
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Update(1, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
        RESULT_SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[0] ==
                  scheduler.schedules[0].results[1]);
    }
  }
}

// Look at the SSI paper (http://drkp.net/papers/ssi-vldb12.pdf).
// This is an anomaly involving three transactions (one of them is a readonly
// transaction).
void SIAnomalyTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable());
  int current_batch_key = 10000;
  {
    TransactionScheduler scheduler(1, table.get(), &txn_manager);
    // Prepare
    scheduler.Txn(0).Insert(current_batch_key, 100);
    scheduler.Txn(0).Commit();
    scheduler.Run();
    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
  }
  {
    TransactionScheduler scheduler(3, table.get(), &txn_manager);
    // Test against anomaly
    scheduler.Txn(1).ReadStore(current_batch_key, 0);
    scheduler.Txn(2).Update(current_batch_key, 100+1);
    scheduler.Txn(2).Commit();
    scheduler.Txn(0).ReadStore(current_batch_key, -1);
    scheduler.Txn(0).Read(TXN_STORED_VALUE);
    scheduler.Txn(1).Insert(TXN_STORED_VALUE, 0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(TXN_STORED_VALUE);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
        RESULT_SUCCESS == scheduler.schedules[1].txn_result &&
        RESULT_SUCCESS == scheduler.schedules[2].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[1] == -1 &&
                  scheduler.schedules[0].results[2] == -1);
    }
  }
}

TEST_F(IsolationLevelTest, SerializableTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(
        test_type, ISOLATION_LEVEL_TYPE_FULL);
    DirtyWriteTest();
    DirtyReadTest();
    FuzzyReadTest();
    WriteSkewTest();
    ReadSkewTest();
    PhantomTest();
    SIAnomalyTest();
  }
}

}  // End test namespace
}  // End peloton namespace
