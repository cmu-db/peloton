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
      //same as 'ConcurrentReadOnlyTransactionTest'
      thread_pool.Initialize(0, CONNECTION_THREAD_COUNT + 3);
      concurrency::EpochManagerFactory::GetInstance().Reset();
      concurrency::EpochManagerFactory::GetInstance().StartEpoch();
      gc::GCManagerFactory::Configure();
      gc::GCManagerFactory::GetInstance().StartGC();

      //this consists of 2 txns. 1.catalog creation 2.test table creation
      storage::DataTable *table = TestingTransactionUtil::CreateTable();

      //manually update snapshot epoch number, so later snapshot read must get a larger epoch than table creating txn
      //or it may read nothing
      //wait two epochs. so that global epoch is guaranteed to increase
      std::this_thread::sleep_for(std::chrono::milliseconds(2 * EPOCH_LENGTH));
      concurrency::EpochManagerFactory::GetInstance().GetExpiredEpochId();

      TransactionScheduler scheduler(1, table, &txn_manager, {0});
      scheduler.Txn(0).Scan(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);

      //it should read all the 10 tuples
      EXPECT_EQ(10, scheduler.schedules[0].results.size());

      gc::GCManagerFactory::GetInstance().StopGC();
      concurrency::EpochManagerFactory::GetInstance().StopEpoch();
      thread_pool.Shutdown();
      //reset to default value, other test cases
      gc::GCManagerFactory::Configure(0);
    }
  }
}

// test r/w txn with a read-only txn runs concurrently
TEST_F(SerializableTransactionTests, ConcurrentReadOnlyTransactionTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type, ISOLATION_LEVEL_TYPE);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    Txn #0 | Txn #1
//    ----------------
//    BEGIN  |
//    W(X)   |
//           | BEGIN R/O
//           | R(X)
//    W(X)   |
//    COMMIT |
//           | R(X)
//           | COMMIT

    {
      //if gc manager is active, finishing a txn will make the txn be removed from epoch list as well.
      //epoch manager needs this behavior to find the largest expired txn id.
      //that id is used to determine whether snapshot epoch falls behind and needs update.
      //gc and epoch manager both depend on thread pool
      thread_pool.Initialize(0, CONNECTION_THREAD_COUNT + 3);
      concurrency::EpochManagerFactory::GetInstance().Reset();
      concurrency::EpochManagerFactory::GetInstance().StartEpoch();
      gc::GCManagerFactory::Configure();
      gc::GCManagerFactory::GetInstance().StartGC();

      //this contains 2 txns: 1.create catalog table 2.create test table
      storage::DataTable *table = TestingTransactionUtil::CreateTable();

      //force snapshot epoch to be updated. it should be larger than table creation txn's epoch
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));
      concurrency::EpochManagerFactory::GetInstance().GetExpiredEpochId();

      TransactionScheduler scheduler(2, table, &txn_manager, {1});
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(1).Read(0);
      scheduler.Txn(0).Update(0, 2);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Read(0);
      scheduler.Txn(1).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);

      //read only txn should read the same snapshot that exists after table creation and before update txn commits
      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
      EXPECT_EQ(0, scheduler.schedules[1].results[1]);

      gc::GCManagerFactory::GetInstance().StopGC();
      concurrency::EpochManagerFactory::GetInstance().StopEpoch();
      thread_pool.Shutdown();
      //reset it to default value for test cases
      gc::GCManagerFactory::Configure(0);
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
