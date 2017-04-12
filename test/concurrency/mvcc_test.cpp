//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mvcc_test.cpp
//
// Identification: test/concurrency/mvcc_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

#include "gc/gc_manager_factory.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class MVCCTests : public PelotonTest {};

static std::vector<ConcurrencyType> TEST_TYPES = {
    ConcurrencyType::TIMESTAMP_ORDERING};

TEST_F(MVCCTests, SingleThreadVersionChainTest) {
  LOG_INFO("SingleThreadVersionChainTest");

  for (auto protocol : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(
        protocol, IsolationLevelType::FULL);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    std::unique_ptr<storage::DataTable> table(
        TestingTransactionUtil::CreateTable());
    // read, read, read, read, update, read, read not exist
    // another txn read
    {
      TransactionScheduler scheduler(2, table.get(), &txn_manager);
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
    }

    // update, update, update, update, read
    {
      TransactionScheduler scheduler(1, table.get(), &txn_manager);
      scheduler.Txn(0).Update(0, 1);
      scheduler.Txn(0).Update(0, 2);
      scheduler.Txn(0).Update(0, 3);
      scheduler.Txn(0).Update(0, 4);
      scheduler.Txn(0).Read(0);
      scheduler.Txn(0).Commit();

      scheduler.Run();
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
      TransactionScheduler scheduler(1, table.get(), &txn_manager);

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
    }
  }
}

TEST_F(MVCCTests, AbortVersionChainTest) {
  LOG_INFO("AbortVersionChainTest");

  for (auto protocol : TEST_TYPES) {
    concurrency::TransactionManagerFactory::Configure(
        protocol, IsolationLevelType::FULL);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    std::unique_ptr<storage::DataTable> table(
        TestingTransactionUtil::CreateTable());
    {
      TransactionScheduler scheduler(2, table.get(), &txn_manager);
      scheduler.Txn(0).Update(0, 100);
      scheduler.Txn(0).Abort();
      scheduler.Txn(1).Read(0);
      scheduler.Txn(1).Commit();

      scheduler.Run();
    }

    {
      TransactionScheduler scheduler(2, table.get(), &txn_manager);
      scheduler.Txn(0).Insert(100, 0);
      scheduler.Txn(0).Abort();
      scheduler.Txn(1).Read(100);
      scheduler.Txn(1).Commit();

      scheduler.Run();
    }
  }
}

TEST_F(MVCCTests, VersionChainTest) {
  LOG_INFO("VersionChainTest");

  for (auto protocol : TEST_TYPES) {
    LOG_INFO("Validating %d", static_cast<int>(protocol));
    concurrency::TransactionManagerFactory::Configure(
        protocol, IsolationLevelType::FULL);

    const int num_txn = 2;  // 5
    const int scale = 1;    // 20
    const int num_key = 2;  // 256
    srand(15721);

    std::unique_ptr<storage::DataTable> table(
        TestingTransactionUtil::CreateTable(num_key));
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    TransactionScheduler scheduler(num_txn, table.get(), &txn_manager);
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
        // Store increased value
        scheduler.Txn(i).ReadStore(key2, delta);
        scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
      }
      scheduler.Txn(i).Commit();
    }
    scheduler.Run();

    // Read all values
    TransactionScheduler scheduler2(1, table.get(), &txn_manager);
    for (int i = 0; i < num_key; i++) {
      scheduler2.Txn(0).Read(i);
    }
    scheduler2.Txn(0).Commit();
    scheduler2.Run();
  }
}

}  // End test namespace
}  // End peloton namespace
