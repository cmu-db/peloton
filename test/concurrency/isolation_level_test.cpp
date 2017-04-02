//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// isolation_level_test.cpp
//
// Identification: test/concurrency/isolation_level_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "concurrency/testing_transaction_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class IsolationLevelTests : public PelotonTest {};

static std::vector<ConcurrencyType> TEST_TYPES = {
    ConcurrencyType::TIMESTAMP_ORDERING};

void DirtyWriteTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
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
    EXPECT_FALSE(schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::ABORTED) ||
                (schedules[0].txn_result == ResultType::ABORTED &&
                 schedules[1].txn_result == ResultType::SUCCESS));
    schedules.clear();
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);

    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    // T1 and T2 can't both succeed
    EXPECT_FALSE(schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::ABORTED) ||
                (schedules[0].txn_result == ResultType::ABORTED &&
                 schedules[1].txn_result == ResultType::SUCCESS));
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
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
    EXPECT_FALSE(schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::ABORTED) ||
                (schedules[0].txn_result == ResultType::ABORTED &&
                 schedules[1].txn_result == ResultType::SUCCESS));
    schedules.clear();
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
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
    EXPECT_FALSE(schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::SUCCESS);
    // For MVCC, actually one and only one T should succeed?
    EXPECT_TRUE((schedules[0].txn_result == ResultType::SUCCESS &&
                 schedules[1].txn_result == ResultType::ABORTED) ||
                (schedules[0].txn_result == ResultType::ABORTED &&
                 schedules[1].txn_result == ResultType::SUCCESS));
    schedules.clear();
  }
}

void DirtyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();

  {
    TransactionScheduler scheduler(2, table, &txn_manager);

    // T1 updates (0, ?) to (0, 1)
    // T2 reads (0, ?)
    // T1 commit
    // T2 commit
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      // Don't read uncommited value
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    }
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);

    scheduler.Txn(0).Update(1, 1);
    scheduler.Txn(1).Read(1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      // Don't read uncommited value
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    }
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);

    scheduler.Txn(0).Delete(2);
    scheduler.Txn(1).Read(2);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      // Don't read uncommited value
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    }
  }
}

void FuzzyReadTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();

  // The constraints are the value of 0 and 1 should be equal
  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Update(1, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_EQ(0, scheduler.schedules[0].results[0]);
      EXPECT_EQ(0, scheduler.schedules[0].results[1]);
    }
  }

  // The constraints are 0 and 1 should both exist or bot not exist
  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Delete(0);
    scheduler.Txn(1).Delete(1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_EQ(1, scheduler.schedules[0].results[0]);
      EXPECT_EQ(1, scheduler.schedules[0].results[1]);
    }
  }
}

void PhantomTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Insert(5, 0);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();
    size_t original_tuple_count = 10;
    if (scheduler.schedules[0].txn_result == ResultType::SUCCESS &&
        scheduler.schedules[1].txn_result == ResultType::SUCCESS) {
      // Should scan no more tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Delete(4);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    size_t original_tuple_count = 11;
    if (scheduler.schedules[0].txn_result == ResultType::SUCCESS &&
        scheduler.schedules[1].txn_result == ResultType::SUCCESS) {
      // Should scan no less tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }
}

// Can't pass this test!
void WriteSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();

  {
    // Prepare
    TransactionScheduler scheduler(1, table, &txn_manager);
    scheduler.Txn(0).Update(1, 1);
    scheduler.Txn(0).Commit();
    scheduler.Run();
    EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  }
  {
    // the database has tuple (0, 0), (1, 1)
    // T0 will set all 1 to 0
    // T1 will set all 0 to 1
    // The results are either (0, 0), (1, 0) or (0, 1), (1, 1) in serilizable
    // transactions.
    TransactionScheduler scheduler(3, table, &txn_manager);

    scheduler.Txn(0).UpdateByValue(1,
                                   0);  // txn 0 see (1, 1), update it to (1, 0)
    scheduler.Txn(1).UpdateByValue(0,
                                   1);  // txn 1 see (0, 0), update it to (0, 1)
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Read(1);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);
    // Can't all success
    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[2].results[0] ==
                  scheduler.schedules[2].results[1]);
    }
  }
}

void ReadSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();
  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Update(1, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[0] ==
                  scheduler.schedules[0].results[1]);
    }
  }
}

// Look at the SSI paper (http://drkp.net/papers/ssi-vldb12.pdf).
// This is an anomaly involving three transactions (one of them is a readonly
// transaction).
void SIAnomalyTest1() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = TestingTransactionUtil::CreateTable();
  int current_batch_key = 10000;
  {
    TransactionScheduler scheduler(1, table, &txn_manager);
    // Prepare
    scheduler.Txn(0).Insert(current_batch_key, 100);
    scheduler.Txn(0).Update(100, 1);
    scheduler.Txn(0).Commit();
    scheduler.Run();
    EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  }
  {
    TransactionScheduler scheduler(4, table, &txn_manager);
    // Test against anomaly
    scheduler.Txn(1).ReadStore(current_batch_key, 0);
    scheduler.Txn(2).Update(current_batch_key, 100 + 1);
    scheduler.Txn(2).Commit();
    scheduler.Txn(0).ReadStore(current_batch_key, -1);
    scheduler.Txn(0).Read(TXN_STORED_VALUE);
    scheduler.Txn(1).Update(TXN_STORED_VALUE, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Txn(3).ReadStore(current_batch_key, -1);
    scheduler.Txn(3).Read(TXN_STORED_VALUE);
    scheduler.Txn(3).Commit();
    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[2].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[1] ==
                  scheduler.schedules[3].results[1]);
    }
  }
}

TEST_F(IsolationLevelTests, SerializableTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type,
                                                      IsolationLevelType::FULL);
    DirtyWriteTest();
    DirtyReadTest();
    FuzzyReadTest();
    // WriteSkewTest();
    ReadSkewTest();
    PhantomTest();
    SIAnomalyTest1();
  }
}

TEST_F(IsolationLevelTests, StressTest) {
  const int num_txn = 2;  // 16
  const int scale = 1;    // 20
  const int num_key = 2;  // 256
  srand(15721);

  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type,
                                                      IsolationLevelType::FULL);
    storage::DataTable *table = TestingTransactionUtil::CreateTable(num_key);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    TransactionScheduler scheduler(num_txn, table, &txn_manager);
    scheduler.SetConcurrent(true);
    for (int i = 0; i < num_txn; i++) {
      for (int j = 0; j < scale; j++) {
        // randomly select two uniq keys
        int key1 = rand() % num_key;
        int key2 = rand() % num_key;
        int delta = rand() % 1000;
        // Store substracted value
        scheduler.Txn(i).ReadStore(key1, -delta);
        scheduler.Txn(i).Update(key1, TXN_STORED_VALUE);
        LOG_INFO("Txn %d deducts %d from %d", i, delta, key1);
        // Store increased value
        scheduler.Txn(i).ReadStore(key2, delta);
        scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
        LOG_INFO("Txn %d adds %d to %d", i, delta, key2);
      }
      scheduler.Txn(i).Commit();
    }
    scheduler.Run();

    // Read all values
    TransactionScheduler scheduler2(1, table, &txn_manager);
    for (int i = 0; i < num_key; i++) {
      scheduler2.Txn(0).Read(i);
    }
    scheduler2.Txn(0).Commit();
    scheduler2.Run();

    EXPECT_EQ(ResultType::SUCCESS, scheduler2.schedules[0].txn_result);
    // The sum should be zero
    int sum = 0;
    for (auto result : scheduler2.schedules[0].results) {
      LOG_INFO("Table has tuple value: %d", result);
      sum += result;
    }

    EXPECT_EQ(0, sum);

    // stats
    int nabort = 0;
    for (auto &schedule : scheduler.schedules) {
      if (schedule.txn_result == ResultType::ABORTED) nabort += 1;
    }
    LOG_INFO("Abort: %d out of %d", nabort, num_txn);
  }
}

}  // End test namespace
}  // End peloton namespace
