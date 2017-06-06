// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // write_behind_logging_test.cpp
// //
// // Identification: test/logging/write_behind_logging_test.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//


// #include "executor/testing_executor_util.h"
// #include "logging/testing_logging_util.h"
// #include "common/harness.h"

// #include "concurrency/transaction_manager_factory.h"
// #include "executor/logical_tile_factory.h"
// #include "executor/executor_context.h"
// #include "logging/loggers/wal_frontend_logger.h"
// #include "logging/logging_util.h"
// #include "storage/data_table.h"
// #include "storage/tile.h"
// #include "storage/table_factory.h"

// #include "executor/mock_executor.h"

// using ::testing::NotNull;
// using ::testing::Return;
// using ::testing::InSequence;

// namespace peloton {
// namespace test {
// class WriteBehindLoggingTests : public PelotonTest {};

// /* TODO: Disabled it due to arbitrary timing constraints
// void grant_thread(concurrency::TransactionManager &txn_manager){
//   for (long i = 6; i <= 20; i++){
//     std::this_thread::sleep_for(std::chrono::milliseconds(10));
//     txn_manager.SetMaxGrantCid(i);
//   }
// }

// // not sure the best way to test this, so I will spawn a new thread to bump up the grant every 10 ms
// // and ensure enough time has passed by the end of the test (we are not prematurely
// // allowing transactions to continue with unsanctioned cids
// TEST_F(WriteBehindLoggingTests, BasicGrantTest) {
//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   txn_manager.SetMaxGrantCid(5);
//   auto begin = std::chrono::high_resolution_clock::now();
//   std::thread granting_thread(grant_thread, std::ref(txn_manager));
//   for (int i = 0; i < 20; i++){
//     txn_manager.GetNextCommitId();
//   }
//   auto end = std::chrono::high_resolution_clock::now();
//   std::chrono::milliseconds min_expected_dur(140);
//   EXPECT_TRUE(end-begin > min_expected_dur);
//   granting_thread.join();


// }
// */


// int SeqScanCount(storage::DataTable *table,
//                  const std::vector<oid_t> &column_ids,
//                  expression::AbstractExpression *predicate) {
//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   std::unique_ptr<executor::ExecutorContext> context(
//       new executor::ExecutorContext(txn));

//   planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
//   executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

//   EXPECT_TRUE(seq_scan_executor.Init());
//   auto tuple_cnt = 0;

//   while (seq_scan_executor.Execute()) {
//     std::unique_ptr<executor::LogicalTile> result_logical_tile(
//         seq_scan_executor.GetOutput());
//     tuple_cnt += result_logical_tile->GetTupleCount();
//   }

//   txn_manager.CommitTransaction(txn);

//   return tuple_cnt;
// }

// //check the visibility
// // TEST_F(WriteBehindLoggingTests, DirtyRangeVisibilityTest) {
// //   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
// //   auto &catalog_manager = catalog::Manager::GetInstance();

// //   ItemPointer *index_entry_ptr = nullptr;

// //   std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());
// //   auto pool = TestingHarness::GetInstance().GetTestingPool();

// //   txn_manager.SetNextCid(1);
// //   auto txn = txn_manager.BeginTransaction();
// //   auto tuple = TestingExecutorUtil::GetTuple(table.get(), 1, pool);
// //   index_entry_ptr = nullptr;
// //   auto visible1 = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
// //   txn_manager.PerformInsert(txn, visible1, index_entry_ptr);
// //   txn_manager.CommitTransaction(txn);

// //   // got cid 2
// //   txn = txn_manager.BeginTransaction();
// //   tuple = TestingExecutorUtil::GetTuple(table.get(), 2, pool);
// //   index_entry_ptr = nullptr;
// //   auto visible2 = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
// //   txn_manager.PerformInsert(txn, visible2, index_entry_ptr);
// //   txn_manager.CommitTransaction(txn);
  
// //   // got cid 3
// //   txn = txn_manager.BeginTransaction();
// //   tuple = TestingExecutorUtil::GetTuple(table.get(), 3, pool);
// //   index_entry_ptr = nullptr;
// //   auto invisible1 = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
// //   txn_manager.PerformInsert(txn, invisible1, index_entry_ptr);
// //   txn_manager.CommitTransaction(txn);
  
// //   // got cid 4
// //   txn = txn_manager.BeginTransaction();
// //   tuple = TestingExecutorUtil::GetTuple(table.get(), 4, pool);
// //   index_entry_ptr = nullptr;
// //   auto invisible2 = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
// //   txn_manager.PerformInsert(txn, invisible2, index_entry_ptr);
// //   txn_manager.CommitTransaction(txn);
  
// //   // got cid 5
// //   txn = txn_manager.BeginTransaction();
// //   tuple = TestingExecutorUtil::GetTuple(table.get(), 5, pool);
// //   index_entry_ptr = nullptr;
// //   auto visible3 = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
// //   txn_manager.PerformInsert(txn, visible3, index_entry_ptr);
// //   txn_manager.CommitTransaction(txn);
  
// //   // got cid 6
// //   std::vector<oid_t> column_ids;
// //   column_ids.push_back(0);
// //   column_ids.push_back(1);
// //   column_ids.push_back(2);
// //   column_ids.push_back(3);

// //   txn = txn_manager.BeginTransaction();
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible1.block)->GetHeader(), visible1.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible2.block)->GetHeader(), visible2.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(invisible1.block)->GetHeader(), invisible1.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(invisible2.block)->GetHeader(), invisible2.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible3.block)->GetHeader(), visible3.offset) == VisibilityType::OK);
// //   txn_manager.AbortTransaction(txn);

// //   txn_manager.SetDirtyRange(std::make_pair(2, 4));

// //   txn = txn_manager.BeginTransaction();
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible1.block)->GetHeader(), visible1.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible2.block)->GetHeader(), visible2.offset) == VisibilityType::OK);
// //   EXPECT_FALSE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(invisible1.block)->GetHeader(), invisible1.offset) == VisibilityType::OK);
// //   EXPECT_FALSE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(invisible2.block)->GetHeader(), invisible2.offset) == VisibilityType::OK);
// //   EXPECT_TRUE(txn_manager.IsVisible(txn, catalog_manager.GetTileGroup(visible3.block)->GetHeader(), visible3.offset) == VisibilityType::OK);
// //   txn_manager.AbortTransaction(txn);
// // }

// }  // End test namespace
// }  // End peloton namespace

