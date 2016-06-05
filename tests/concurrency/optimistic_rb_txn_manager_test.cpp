//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_rb_txn_manager_test.cpp
//
// Identification: tests/concurrency/optimistic_rb_txn_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManagerTests : public PelotonTest {};

std::vector<index::RBItemPointer *>ScanKeyHelper(index::Index *index, int key) {
  // Create a key tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(index->GetKeySchema(), true));
  tuple->SetValue(0, ValueFactory::GetIntegerValue(key), testing_pool);

  std::vector<index::RBItemPointer *> results;
  index->ScanKey(tuple.get(), results);
  txn_manager.CommitTransaction();

  return results;
}

TEST_F(OptimisticRbTxnManagerTests, SecondaryIndexTest) {
  concurrency::TransactionManagerFactory::Configure(
      CONCURRENCY_TYPE_OCC_RB);
  // First, generate the table with index
  // this table has 10 rows:
  //  int(primary)  int(unique)
  //  0             0
  //  1             1
  //  2             2
  //  .....
  //  9             9
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
  auto index = data_table->GetIndex(1);
  {
    // Get secondary index
    auto scan_results = ScanKeyHelper(index, 1);
    EXPECT_EQ(1, scan_results.size());
  }

  {
    // Test 1 update
    auto scan_results = ScanKeyHelper(index, 9);
    EXPECT_EQ(1, scan_results.size());

    TransactionScheduler scheduler(1, data_table.get(), &txn_manager);
    scheduler.Txn(0).Update(9, 100);
    scheduler.Txn(0).Commit();
    scheduler.Run();

    scan_results = ScanKeyHelper(index, 9);

    EXPECT_EQ(0, scan_results.size());

    scan_results = ScanKeyHelper(index, 100);
    EXPECT_EQ(1, scan_results.size());
  }

  {
    // Test 2 abort
     auto scan_results = ScanKeyHelper(index, 9);
    EXPECT_EQ(0, scan_results.size());

    TransactionScheduler scheduler(1, data_table.get(), &txn_manager);
    scheduler.Txn(0).Update(9, 9);
    scheduler.Txn(0).Abort();
    scheduler.Run();

    scan_results = ScanKeyHelper(index, 9);
    EXPECT_EQ(0, scan_results.size());
  }
}

}  // End test namespace
}  // End peloton namespace
