//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_test.cpp
//
// Identification: test/concurrency/transaction_test.cpp
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

class TransactionTests : public PelotonTest {};

static std::vector<ProtocolType> TEST_TYPES = {
    ProtocolType::TIMESTAMP_ORDERING
};

void TransactionTest(concurrency::TransactionManager *txn_manager,
                     UNUSED_ATTRIBUTE uint64_t thread_itr) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 50; txn_itr++) {
    auto txn = txn_manager->BeginTransaction();
    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    if (txn_itr % 25 != 0) {
      txn_manager->CommitTransaction(txn);
    } else {
      txn_manager->AbortTransaction(txn);
    }
  }
}

TEST_F(TransactionTests, TransactionTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    LaunchParallelTest(8, TransactionTest, &txn_manager);
  }
}

TEST_F(TransactionTests, ReadOnlyTransactionTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    storage::DataTable *table = TestingTransactionUtil::CreateTable();
    // Just scan the table
    {
      TransactionScheduler scheduler(1, table, &txn_manager, true);
      scheduler.Txn(0).Scan(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      // Snapshot read can not read the recent insert
      EXPECT_EQ(0, scheduler.schedules[0].results.size());
    }
  }
}

TEST_F(TransactionTests, SingleTransactionTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    storage::DataTable *table = TestingTransactionUtil::CreateTable();
    // Just scan the table
    {
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Scan(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(10, scheduler.schedules[0].results.size());
    }
    // read, read, read, read, update, read, read not exist
    // another txn read
    {
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(100);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Read(0);
      scheduler.Txn(1).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(0, scheduler.schedules[0].results[0]);
      EXPECT_EQ(0, scheduler.schedules[0].results[1]);
      EXPECT_EQ(0, scheduler.schedules[0].results[2]);
      EXPECT_EQ(0, scheduler.schedules[0].results[3]);
      EXPECT_EQ(1, scheduler.schedules[0].results[4]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[5]);
      EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    }

    // // update, update, update, update, read
    {
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Update(0, 2);
      scheduler.Txn(0).Update(0, 3);
      scheduler.Txn(0).Update(0, 4);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(4, scheduler.schedules[0].results[0]);
    }

    // // delete not exist, delete exist, read deleted, update deleted,
    // // read deleted, insert back, update inserted, read newly updated,
    // // delete inserted, read deleted
    {
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Delete(100);
      scheduler.Txn(0).Delete(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Insert(0, 2);
      scheduler.Txn(0).Update(0, 3);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Delete(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
      EXPECT_EQ(3, scheduler.schedules[0].results[2]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[3]);
      LOG_INFO("FINISH THIS");
    }

    // // insert, delete inserted, read deleted, insert again, delete again
    // // read deleted, insert again, read inserted, update inserted, read
    // updated
    {
      TransactionScheduler scheduler(1, table, &txn_manager);

      scheduler.Txn(0).Insert(1000, 0);
      scheduler.Txn(0).Delete(1000);
      scheduler.Txn(0).Read(1000);
      scheduler.Txn(0).Insert(1000, 1);
      scheduler.Txn(0).Delete(1000);
      scheduler.Txn(0).Read(1000);
      scheduler.Txn(0).Insert(1000, 2);
      scheduler.Txn(0).Read(1000);
      scheduler.Txn(0).Update(1000, 3);
      scheduler.Txn(0).Read(1000);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
      EXPECT_EQ(2, scheduler.schedules[0].results[2]);
      EXPECT_EQ(3, scheduler.schedules[0].results[3]);
    }
  }
}

TEST_F(TransactionTests, AbortTest) {
  for (auto test_type : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(test_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    storage::DataTable *table = TestingTransactionUtil::CreateTable();
    {
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).Update(0, 100);
      scheduler.Txn(0).Abort();
      scheduler.Txn(1).Read(0);
      scheduler.Txn(1).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    }

    {
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).Insert(100, 0);
      scheduler.Txn(0).Abort();
      scheduler.Txn(1).Read(100);
      scheduler.Txn(1).Commit();

      scheduler.Run();
      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(-1, scheduler.schedules[1].results[0]);
    }
  }
}

}  // End test namespace
}  // End peloton namespace
