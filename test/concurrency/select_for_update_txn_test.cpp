////===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// select_for_udpate_txn_test.cpp
////
//// Identification: test/concurrency/select_for_update_txn_test.cpp
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//
//#include "common/harness.h"
//#include "concurrency/transaction_tests_util.h"
//
// namespace peloton {
//
// namespace test {
//
////===--------------------------------------------------------------------===//
//// Transaction Tests
////===--------------------------------------------------------------------===//
//
// class SelectForUpdateTxnTests : public PelotonTest {};
//
// static std::vector<ConcurrencyType> TEST_TYPES = {
//  ConcurrencyType::TIMESTAMP_ORDERING
//};
//
// TEST_F(SelectForUpdateTxnTests, SingleTransactionTest) {
//  for (auto test_type : TEST_TYPES) {
//    concurrency::TransactionManagerFactory::Configure(test_type);
//    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    std::unique_ptr<storage::DataTable> table(
//      TransactionTestsUtil::CreateTable());
//
//    // update, update, update, update, read
//    {
//      TransactionScheduler scheduler(1, table.get(), &txn_manager);
//      scheduler.Txn(0).Update(0, 1, true);
//      scheduler.Txn(0).Update(0, 2, true);
//      scheduler.Txn(0).Update(0, 3, true);
//      scheduler.Txn(0).Update(0, 4, true);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Commit();
//
//      scheduler.Run();
//
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(4, scheduler.schedules[0].results[0]);
//    }
//
//     // delete not exist, delete exist, read deleted, update deleted,
//     // read deleted, insert back, update inserted, read newly updated,
//     // delete inserted, read deleted
//    {
//      TransactionScheduler scheduler(1, table.get(), &txn_manager);
//      scheduler.Txn(0).Delete(100, true);
//      scheduler.Txn(0).Delete(0, true);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Update(0, 1, true);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Insert(0, 2);
//      scheduler.Txn(0).Update(0, 3, true);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Delete(0, true);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Commit();
//
//      scheduler.Run();
//
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
//      EXPECT_EQ(3, scheduler.schedules[0].results[2]);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[3]);
//      LOG_INFO("FINISH THIS");
//    }
//
//    // insert, delete inserted, read deleted, insert again, delete again
//    // read deleted, insert again, read inserted, update inserted, read
//    // updated
//    {
//      TransactionScheduler scheduler(1, table.get(), &txn_manager);
//
//      scheduler.Txn(0).Insert(1000, 0);
//      scheduler.Txn(0).Delete(1000, true);
//      scheduler.Txn(0).Read(1000, true);
//      scheduler.Txn(0).Insert(1000, 1);
//      scheduler.Txn(0).Delete(1000, true);
//      scheduler.Txn(0).Read(1000, true);
//      scheduler.Txn(0).Insert(1000, 2);
//      scheduler.Txn(0).Read(1000, true);
//      scheduler.Txn(0).Update(1000, 3, true);
//      scheduler.Txn(0).Read(1000, true);
//      scheduler.Txn(0).Commit();
//
//      scheduler.Run();
//
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
//      EXPECT_EQ(2, scheduler.schedules[0].results[2]);
//      EXPECT_EQ(3, scheduler.schedules[0].results[3]);
//    }
//
//  }
//}
//
// TEST_F(SelectForUpdateTxnTests, MultiTransactionTest) {
//  for (auto test_type : TEST_TYPES) {
//    concurrency::TransactionManagerFactory::Configure(test_type);
//    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    std::unique_ptr<storage::DataTable> table(
//      TransactionTestsUtil::CreateTable());
//    // Txn 0: scan + select for update
//    // Txn 1: scan
//    // Txn 1: commit (should failed for timestamp ordering cc)
//    // Txn 2: Scan + select for update
//    // Txn 2: commit (should failed)
//    // Txn 0: commit (success)
//    {
//      TransactionScheduler scheduler(3, table.get(), &txn_manager);
//      scheduler.Txn(0).Scan(0, true);
//      scheduler.Txn(1).Scan(0);
//      scheduler.Txn(1).Commit();
//      scheduler.Txn(2).Scan(0, true);
//      scheduler.Txn(0).Commit();
//      scheduler.Txn(2).Commit();
//
//      scheduler.Run();
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[2].txn_result);
//
//      EXPECT_EQ(10, scheduler.schedules[0].results.size());
//    }
//
//    // Txn 0: scan + select for update
//    // Txn 0: abort
//    // Txn 1: Scan + select for update
//    // Txn 1: commit (should success)
//    {
//      TransactionScheduler scheduler(2, table.get(), &txn_manager);
//      scheduler.Txn(0).Scan(0, true);
//      scheduler.Txn(0).Abort();
//      scheduler.Txn(1).Scan(0, true);
//      scheduler.Txn(1).Commit();
//
//      scheduler.Run();
//      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//
//      EXPECT_EQ(10, scheduler.schedules[1].results.size());
//    }
//
//    // Txn 0: read + select for update
//    // Txn 0: abort
//    // Txn 1: read + select for update
//    // Txn 1: commit (should success)
//    {
//      TransactionScheduler scheduler(2, table.get(), &txn_manager);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Abort();
//      scheduler.Txn(1).Read(0, true);
//      scheduler.Txn(1).Commit();
//
//      scheduler.Run();
//      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//
//      EXPECT_EQ(1, scheduler.schedules[1].results.size());
//    }
//
//    // read, read, read, read, update, read, read not exist
//    // another txn read
//    {
//      TransactionScheduler scheduler(2, table.get(), &txn_manager);
//      scheduler.Txn(0).Read(0);
//      scheduler.Txn(0).Read(0, true);
//      scheduler.Txn(0).Read(0);
//      scheduler.Txn(0).Read(0);
//      scheduler.Txn(0).Update(0, 1);
//      scheduler.Txn(0).Read(0);
//      scheduler.Txn(0).Read(100, true);
//      scheduler.Txn(0).Commit();
//      scheduler.Txn(1).Read(0, true);
//      scheduler.Txn(1).Commit();
//
//      scheduler.Run();
//
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//      EXPECT_EQ(0, scheduler.schedules[0].results[0]);
//      EXPECT_EQ(0, scheduler.schedules[0].results[1]);
//      EXPECT_EQ(0, scheduler.schedules[0].results[2]);
//      EXPECT_EQ(0, scheduler.schedules[0].results[3]);
//      EXPECT_EQ(1, scheduler.schedules[0].results[4]);
//      EXPECT_EQ(-1, scheduler.schedules[0].results[5]);
//      EXPECT_EQ(1, scheduler.schedules[1].results[0]);
//    }
//
//
//    {
//      // Test commit/abort protocol when part of the read-own tuples get
//      updated TransactionScheduler scheduler(3, table.get(), &txn_manager);
//      scheduler.Txn(0).Read(3, true);
//      scheduler.Txn(0).Read(4, true);
//      scheduler.Txn(0).Update(3, 1);
//      scheduler.Txn(0).Abort();
//      scheduler.Txn(1).Read(3, true);
//      scheduler.Txn(1).Read(4, true);
//      scheduler.Txn(1).Update(3, 2);
//      scheduler.Txn(1).Commit();
//      scheduler.Txn(2).Read(3);
//      scheduler.Txn(2).Read(4);
//      scheduler.Txn(2).Commit();
//
//      scheduler.Run();
//      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);
//
//      EXPECT_EQ(0, scheduler.schedules[1].results[0]);
//      EXPECT_EQ(0, scheduler.schedules[1].results[1]);
//      EXPECT_EQ(2, scheduler.schedules[2].results[0]);
//      EXPECT_EQ(0, scheduler.schedules[2].results[1]);
//    }
//
//  }
//}
//
//}  // End test namespace
//}  // End peloton namespace
