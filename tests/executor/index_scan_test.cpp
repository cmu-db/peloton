/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/planner/index_scan_node.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

// Index scan of table with index predicate.
TEST(IndexScanTests, IndexPredicateTest) {

  // First, generate the table with index
  std::unique_ptr<storage::DataTable> data_table(ExecutorTestsUtil::CreateAndPopulateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids( { 0, 1, 3 });

  //===--------------------------------------------------------------------===//
  // Start <= Tuple
  //===--------------------------------------------------------------------===//

  // Set start key
  auto index = data_table->GetIndex(0);
  const catalog::Schema *index_key_schema = index->GetKeySchema();
  std::unique_ptr<storage::Tuple> start_key(new storage::Tuple(index_key_schema, true));

  start_key->SetValue(0, ValueFactory::GetIntegerValue(0));

  std::unique_ptr<storage::Tuple> end_key(nullptr);
  bool start_inclusive = true;
  bool end_inclusive = true;

  // Create plan node.
  planner::IndexScanNode node(
      data_table.get(),
      data_table->GetIndex(0),
      start_key.get(), end_key.get(), start_inclusive, end_inclusive,
      column_ids);

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  int expected_num_tiles = 3;

  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile> > result_tiles;

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 5);

  std::cout << *(result_tiles[0].get());

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);

  //===--------------------------------------------------------------------===//
  // Start <= Tuple <= End
  //===--------------------------------------------------------------------===//

  // Set end key
  end_key.reset(new storage::Tuple(index_key_schema, true));
  end_key->SetValue(0, ValueFactory::GetIntegerValue(20));

  // Create another plan node.
  planner::IndexScanNode node2(
      data_table.get(),
      data_table->GetIndex(0),
      start_key.get(), end_key.get(), start_inclusive, end_inclusive,
      column_ids);

  auto txn2 = txn_manager.BeginTransaction();

  // Run the executor
  executor::IndexScanExecutor executor2(&node2, context.get());
  expected_num_tiles = 1;

  result_tiles.clear();
  EXPECT_TRUE(executor2.Init());

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor2.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor2.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor2.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 3);

  std::cout << *(result_tiles[0].get());

  txn_manager.CommitTransaction(txn2);
  txn_manager.EndTransaction(txn2);

}

} // namespace test
} // namespace peloton
