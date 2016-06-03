//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// write_behind_logging_test.cpp
//
// Identification: tests/logging/write_behind_logging_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "concurrency/transaction_manager_factory.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include "logging/loggers/wal_frontend_logger.h"
#include "logging/logging_util.h"
#include "storage/table_factory.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"
#include "logging/logging_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {
class WriteBehindLoggingTests : public PelotonTest {};

/*
void grant_thread(concurrency::TransactionManager &txn_manager){
	for (long i = 6; i <= 20; i++){
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		txn_manager.SetMaxGrantCid(i);
	}
}

// not sure the best way to test this, so I will spawn a new thread to bump up the grant every 10 ms
// and ensure enough time has passed by the end of the test (we are not prematurely
// allowing transactions to continue with unsanctioned cids
TEST_F(WriteBehindLoggingTests, BasicGrantTest) {
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.SetMaxGrantCid(5);
	auto begin = std::chrono::high_resolution_clock::now();
	std::thread granting_thread(grant_thread, std::ref(txn_manager));
	for (int i = 0; i < 20; i++){
		txn_manager.GetNextCommitId();
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::milliseconds min_expected_dur(140);
	EXPECT_TRUE(end-begin > min_expected_dur);
	granting_thread.join();


}

int SeqScanCount(storage::DataTable *table,
                 const std::vector<oid_t> &column_ids,
                 expression::AbstractExpression *predicate) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  auto tuple_cnt = 0;

  while (seq_scan_executor.Execute()) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        seq_scan_executor.GetOutput());
    tuple_cnt += result_logical_tile->GetTupleCount();
  }

  txn_manager.CommitTransaction();

  return tuple_cnt;
}

//check the visibility
TEST_F(WriteBehindLoggingTests, DirtyRangeVisibilityTest) {
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto &catalog_manager = catalog::Manager::GetInstance();

	std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable());\
	std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
	txn_manager.SetNextCid(1);
	txn_manager.BeginTransaction();
	auto tuple = ExecutorTestsUtil::GetTuple(table.get(), 1, pool.get());
	auto visible1 = table->InsertTuple(tuple.get());
	txn_manager.PerformInsert(visible1);
	txn_manager.CommitTransaction();
	// got cid_2

	txn_manager.BeginTransaction();
	tuple = ExecutorTestsUtil::GetTuple(table.get(), 2, pool.get());
	auto visible2 = table->InsertTuple(tuple.get());
	txn_manager.PerformInsert(visible2);
	txn_manager.CommitTransaction();
	//git cid 4

	txn_manager.BeginTransaction();
	tuple = ExecutorTestsUtil::GetTuple(table.get(), 3, pool.get());
	auto invisible1 = table->InsertTuple(tuple.get());
	txn_manager.PerformInsert(invisible1);
	txn_manager.CommitTransaction();
	// got cid 6

	txn_manager.BeginTransaction();
	tuple = ExecutorTestsUtil::GetTuple(table.get(), 4, pool.get());
	auto invisible2 = table->InsertTuple(tuple.get());
	txn_manager.PerformInsert(invisible2);
	txn_manager.CommitTransaction();
	// got cid 8

	txn_manager.BeginTransaction();
	tuple = ExecutorTestsUtil::GetTuple(table.get(), 5, pool.get());
	auto visible3 = table->InsertTuple(tuple.get());
	txn_manager.PerformInsert(visible3);
	txn_manager.CommitTransaction();
	// got cid 10

	std::vector<oid_t> column_ids;
	column_ids.push_back(0);
	column_ids.push_back(1);
	column_ids.push_back(2);
	column_ids.push_back(3);

	txn_manager.BeginTransaction();
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible1.block)->GetHeader(), visible1.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible2.block)->GetHeader(), visible2.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(invisible1.block)->GetHeader(), invisible1.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(invisible2.block)->GetHeader(), invisible2.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible3.block)->GetHeader(), visible3.offset));
	txn_manager.AbortTransaction();

	txn_manager.SetDirtyRange(std::make_pair(4, 9));

	txn_manager.BeginTransaction();
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible1.block)->GetHeader(), visible1.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible2.block)->GetHeader(), visible2.offset));
	EXPECT_FALSE(txn_manager.IsVisible(catalog_manager.GetTileGroup(invisible1.block)->GetHeader(), invisible1.offset));
	EXPECT_FALSE(txn_manager.IsVisible(catalog_manager.GetTileGroup(invisible2.block)->GetHeader(), invisible2.offset));
	EXPECT_TRUE(txn_manager.IsVisible(catalog_manager.GetTileGroup(visible3.block)->GetHeader(), visible3.offset));
	txn_manager.AbortTransaction();
}
*/

}  // End test namespace
}  // End peloton namespace
