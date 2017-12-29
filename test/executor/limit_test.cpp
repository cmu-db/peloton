//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_test.cpp
//
// Identification: test/executor/limit_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <set>
#include <string>
#include <vector>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "planner/limit_plan.h"

#include "common/internal_types.h"
#include "type/value.h"
#include "executor/logical_tile.h"
#include "executor/limit_executor.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/mock_executor.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class LimitTests : public PelotonTest {};

namespace {

void RunTest(executor::LimitExecutor &executor, size_t expected_num_tiles,
             oid_t expected_first_oid, size_t expected_num_tuples_returned) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  EXPECT_EQ(expected_num_tiles, result_tiles.size());

  if (result_tiles.size() > 0) {
     EXPECT_EQ(expected_first_oid, *(result_tiles[0]->begin()));
  }

  size_t actual_num_tuples_returned = 0;
  for (auto &tile : result_tiles) {
    actual_num_tuples_returned += tile->GetTupleCount();
  }

  EXPECT_EQ(expected_num_tuples_returned, actual_num_tuples_returned);
}

TEST_F(LimitTests, NonLeafLimitOffsetTest) {
  size_t tile_size = 50;
  size_t offset = tile_size / 2, limit = tile_size;

  // Create the plan node
  planner::LimitPlan node(limit, offset);

  // Create and set up executor
  executor::LimitExecutor executor(&node, nullptr);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true));  // even no need to call the child for 3rd time

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tile_size));
  TestingExecutorUtil::PopulateTable(data_table.get(), tile_size * 3, false,
                                   false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, 2, offset, limit);
}

TEST_F(LimitTests, NonLeafSkipAllTest) {
  size_t tile_size = 50;
  size_t offset = tile_size * 10, limit = tile_size;

  // Create the plan node
  planner::LimitPlan node(limit, offset);

  // Create and set up executor
  executor::LimitExecutor executor(&node, nullptr);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tile_size));
  TestingExecutorUtil::PopulateTable(data_table.get(), tile_size * 3, false,
                                   false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, 0, INVALID_OID, 0);
}

TEST_F(LimitTests, NonLeafReturnAllTest) {
  size_t tile_size = 50;
  size_t offset = 0, limit = tile_size * 10;

  // Create the plan node
  planner::LimitPlan node(limit, offset);

  // Create and set up executor
  executor::LimitExecutor executor(&node, nullptr);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tile_size));
  TestingExecutorUtil::PopulateTable(data_table.get(), tile_size * 3, false,
                                   false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, 2, offset, tile_size * 2);
}

TEST_F(LimitTests, NonLeafHugeLimitTest) {
  size_t tile_size = 50;
  size_t offset = tile_size / 2, limit = tile_size * 10;

  // Create the plan node
  planner::LimitPlan node(limit, offset);

  // Create and set up executor
  executor::LimitExecutor executor(&node, nullptr);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tile_size));
  TestingExecutorUtil::PopulateTable(data_table.get(), tile_size * 3, false,
                                   false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, 2, offset, tile_size * 2 - offset);
}
}

}  // namespace test
}  // namespace peloton
