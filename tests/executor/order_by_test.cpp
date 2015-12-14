//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_test.cpp
//
// Identification: tests/executor/order_by_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/order_by_plan.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/order_by_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

namespace {

void RunTest(executor::OrderByExecutor &executor, size_t expected_num_tuples,
             const std::vector<oid_t> &sort_keys,
             const std::vector<bool> &descend_flags) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  size_t actual_num_tuples_returned = 0;
  for (auto &tile : result_tiles) {
    actual_num_tuples_returned += tile->GetTupleCount();
  }

  EXPECT_EQ(expected_num_tuples, actual_num_tuples_returned);

  EXPECT_GT(sort_keys.size(), 0);
  EXPECT_GT(descend_flags.size(), 0);

  // Verify:
  // Lazy here: just print it out and see it by yourself.
  for (auto &tile : result_tiles) {
    for (oid_t tuple_id : *tile) {
      std::cout << "<";
      for (size_t sk = 0; sk < sort_keys.size(); sk++) {
        std::cout << tile->GetValue(tuple_id, sort_keys[sk]) << ",";
      }
      std::cout << ">";
    }
  }

  std::cout << std::endl;
}

TEST(OrderByTests, IntAscTest) {
  // Create the plan node
  std::vector<oid_t> sort_keys({1});
  std::vector<bool> descend_flags({false});
  std::vector<oid_t> output_columns({0, 1, 2, 3});
  planner::OrderByPlan node(sort_keys, descend_flags, output_columns);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  // Create and set up executor
  executor::OrderByExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  size_t tile_size = 20;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  bool random = true;
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 2, false,
                                   random, false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2, sort_keys, descend_flags);

}

TEST(OrderByTests, IntDescTest) {
  // Create the plan node
  std::vector<oid_t> sort_keys({1});
  std::vector<bool> descend_flags({true});
  std::vector<oid_t> output_columns({0, 1, 2, 3});
  planner::OrderByPlan node(sort_keys, descend_flags, output_columns);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  // Create and set up executor
  executor::OrderByExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  size_t tile_size = 20;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  bool random = true;
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 2, false,
                                   random, false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2, sort_keys, descend_flags);

}

TEST(OrderByTests, StringDescTest) {
  // Create the plan node
  std::vector<oid_t> sort_keys({3});
  std::vector<bool> descend_flags({true});
  std::vector<oid_t> output_columns({0, 1, 2, 3});
  planner::OrderByPlan node(sort_keys, descend_flags, output_columns);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  // Create and set up executor
  executor::OrderByExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  size_t tile_size = 20;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  bool random = true;
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 2, false,
                                   random, false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2, sort_keys, descend_flags);

}

TEST(OrderByTests, IntAscStringDescTest) {
  // Create the plan node
  std::vector<oid_t> sort_keys({1, 3});
  std::vector<bool> descend_flags({false, true});
  std::vector<oid_t> output_columns({0, 1, 2, 3});
  planner::OrderByPlan node(sort_keys, descend_flags, output_columns);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  // Create and set up executor
  executor::OrderByExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  size_t tile_size = 20;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  bool random = true;
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 2, false,
                                   random, false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2, sort_keys, descend_flags);
}

/**
 * Switch the order of sort keys of the previous test case
 */
TEST(OrderByTests, StringDescIntAscTest) {
  // Create the plan node
  std::vector<oid_t> sort_keys({3, 1});
  std::vector<bool> descend_flags({true, false});
  std::vector<oid_t> output_columns({0, 1, 2, 3});
  planner::OrderByPlan node(sort_keys, descend_flags, output_columns);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  // Create and set up executor
  executor::OrderByExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create a table and wrap it in logical tile
  size_t tile_size = 20;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  bool random = true;
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 2, false,
                                   random, false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2, sort_keys, descend_flags);

}
}

}  // namespace test
}  // namespace peloton
