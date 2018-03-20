//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// serializable_transaction_test.cpp
//
// Identification: test/concurrency/serializable_transaction_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "concurrency/testing_transaction_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Serializable TransactionContext Tests
//===--------------------------------------------------------------------===//

class SerializableTransactionTests : public PelotonTest {};

static std::vector<ProtocolType> PROTOCOL_TYPES = {
    ProtocolType::TIMESTAMP_ORDERING
};

static IsolationLevelType ISOLATION_LEVEL_TYPE = 
    IsolationLevelType::SERIALIZABLE;

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

TEST_F(SerializableTransactionTests, TransactionTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    LaunchParallelTest(8, TransactionTest, &txn_manager);
  }
}

// test predeclared read-only transaction
TEST_F(SerializableTransactionTests, ReadOnlyTransactionTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type, ISOLATION_LEVEL_TYPE);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // Just scan the table
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
      
      TransactionScheduler scheduler(1, table, &txn_manager, true);
      scheduler.Txn(0).Scan(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      // Snapshot read cannot read the recent insert
      EXPECT_EQ(0, scheduler.schedules[0].results.size());
    }
  }
}

// test with single transaction
TEST_F(SerializableTransactionTests, SingleTransactionTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type, ISOLATION_LEVEL_TYPE);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // Just scan the table
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
    
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Scan(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(10, scheduler.schedules[0].results.size());
    }

    // read, read, read, read, update, read, read not exist
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
    
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Read(100);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(0, scheduler.schedules[0].results[0]);
      EXPECT_EQ(0, scheduler.schedules[0].results[1]);
      EXPECT_EQ(0, scheduler.schedules[0].results[2]);
      EXPECT_EQ(0, scheduler.schedules[0].results[3]);
      EXPECT_EQ(1, scheduler.schedules[0].results[4]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[5]);
    }

    // update, update, update, update, read
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
    
      TransactionScheduler scheduler(1, table, &txn_manager);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 2);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 3);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Update(0, 4);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(1, scheduler.schedules[0].results[0]);
      EXPECT_EQ(2, scheduler.schedules[0].results[1]);
      EXPECT_EQ(3, scheduler.schedules[0].results[2]);
      EXPECT_EQ(4, scheduler.schedules[0].results[3]);
    }

    // delete not exist, delete exist, read deleted, update deleted,
    // read deleted, insert back, update inserted, read newly updated,
    // delete inserted, read deleted
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
    
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
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
    
      TransactionScheduler scheduler(1, table, &txn_manager);

      scheduler.Txn(0).Insert(1000, 0);
      scheduler.Txn(0).Read(1000);
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
      EXPECT_EQ(0, scheduler.schedules[0].results[0]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
      EXPECT_EQ(-1, scheduler.schedules[0].results[2]);
      EXPECT_EQ(2, scheduler.schedules[0].results[3]);
      EXPECT_EQ(3, scheduler.schedules[0].results[4]);
    }
  }
}

// test with concurrent transactions
TEST_F(SerializableTransactionTests, ConcurrentTransactionsTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type, ISOLATION_LEVEL_TYPE);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
      
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).Insert(100, 1);
      scheduler.Txn(1).Read(100);
      scheduler.Txn(0).Read(100);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Read(100);
      scheduler.Txn(1).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      
      EXPECT_EQ(1, scheduler.schedules[0].results[0]);
      EXPECT_EQ(-1, scheduler.schedules[1].results[0]);
      // TODO: phantom problem. 
      // In fact, txn 1 should not see the inserted tuple.
      EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    }

    {
      concurrency::EpochManagerFactory::GetInstance().Reset();
      storage::DataTable *table = TestingTransactionUtil::CreateTable();
      
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(1).Read(0);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Read(0);
      scheduler.Txn(1).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
      
      EXPECT_EQ(1, scheduler.schedules[0].results[0]);
    }
  }
}


TEST_F(SerializableTransactionTests, AbortTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type);
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

}  // namespace test
}  // namespace peloton
