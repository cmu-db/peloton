//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nested_loop_join_test.cpp
//
// Identification: tests/executor/nested_loop_join_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/nested_loop_join_plan.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/nested_loop_join_executor.h"
#include "backend/storage/data_table.h"

#include "mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

// Cartesian Product Test
TEST(NestedLoopJoinTests, CartesianProductTest) {
  // Create plan node.
  auto projection = JoinTestsUtil::CreateProjection();
  planner::NestedLoopJoinPlan node(JOIN_TYPE_INNER, nullptr, projection);

  // Run the executor
  executor::NestedLoopJoinExecutor executor(&node, nullptr);

  MockExecutor left_executor, right_executor;
  executor.AddChild(&left_executor);
  executor.AddChild(&right_executor);

  EXPECT_CALL(left_executor, DInit()).WillOnce(Return(true));
  EXPECT_CALL(right_executor, DInit()).WillOnce(Return(true));

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  // Left table has 3 tiles
  std::unique_ptr<storage::DataTable> left_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(left_table.get(), tile_group_size * 3, false,
  false,
                                   false);
  // Right table has 2 tiles
  std::unique_ptr<storage::DataTable> right_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(right_table.get(), tile_group_size * 2,
  false,
                                   false, false);

  // Wrap the input tables in logical tiles
  std::unique_ptr<executor::LogicalTile> left_tile1(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> left_tile2(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(1)));

  std::unique_ptr<executor::LogicalTile> left_tile3(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(2)));

  std::unique_ptr<executor::LogicalTile> right_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> right_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(1)));

  EXPECT_CALL(left_executor, DExecute())
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(false));

  EXPECT_CALL(right_executor, DExecute())
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(false));

  EXPECT_CALL(left_executor, GetOutput())
    .WillOnce(Return(left_tile1.release()))
    .WillOnce(Return(left_tile2.release()))
    .WillOnce(Return(left_tile3.release()));


  EXPECT_CALL(right_executor, GetOutput())
  .WillOnce(Return(right_tile1.release()))
  .WillOnce(Return(right_tile2.release()));

  // Run the executor
  EXPECT_TRUE(executor.Init());

  for (size_t tile_itr = 0; tile_itr < 6; tile_itr++)
    EXPECT_TRUE(executor.Execute());

  EXPECT_FALSE(executor.Execute());
}

// Join Predicate Test
TEST(NestedLoopJoinTests, JoinPredicateTest) {
  // Create predicate
  auto predicate = JoinTestsUtil::CreateJoinPredicate();
  auto projection = JoinTestsUtil::CreateProjection();

  // Create plan node.
  planner::NestedLoopJoinPlan node(JOIN_TYPE_INNER, predicate, projection);

  // Run the executor
  executor::NestedLoopJoinExecutor executor(&node, nullptr);

  MockExecutor left_executor, right_executor;
  executor.AddChild(&left_executor);
  executor.AddChild(&right_executor);

  EXPECT_CALL(left_executor, DInit())
    .WillOnce(Return(true));

  EXPECT_CALL(right_executor, DInit())
    .WillOnce(Return(true));

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> left_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(left_table.get(), tile_group_size * 3, false,
  false,
                                   false);
  bool mutate_table = true;
  std::unique_ptr<storage::DataTable> right_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(right_table.get(), tile_group_size * 2,
                                   mutate_table, false, false);

//  std::cout << *(left_table.get());
//  std::cout << *(right_table.get());

  std::unique_ptr<executor::LogicalTile> left_tile1(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> left_tile2(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(1)));

  std::unique_ptr<executor::LogicalTile> left_tile3(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(2)));

  std::unique_ptr<executor::LogicalTile> right_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> right_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(1)));


  EXPECT_CALL(left_executor, DExecute())
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(false));

  EXPECT_CALL(right_executor, DExecute())
    .WillOnce(Return(true))
    .WillOnce(Return(true))
    .WillOnce(Return(false));

  EXPECT_CALL(left_executor, GetOutput())
    .WillOnce(Return(left_tile1.release()))
    .WillOnce(Return(left_tile2.release()))
    .WillOnce(Return(left_tile3.release()));


  EXPECT_CALL(right_executor, GetOutput())
  .WillOnce(Return(right_tile1.release()))
  .WillOnce(Return(right_tile2.release()));

  // Run the executor
  EXPECT_TRUE(executor.Init());

  // Only two pairs of input tiles are expected to join:
  // (2, 1) and (3, 1)
  for (size_t tile_itr = 0; tile_itr < 2; tile_itr++) {
    EXPECT_TRUE(executor.Execute());
  }

  EXPECT_FALSE(executor.Execute());
}

}  // namespace test
}  // namespace peloton
