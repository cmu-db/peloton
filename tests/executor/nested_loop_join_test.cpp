/**
 * @brief Test cases for nested loop join node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/nested_loop_join_executor.h"
#include "planner/nested_loop_join_node.h"
#include "storage/data_table.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace nstore {
namespace test {

// Cartesian Product Test
TEST(NestedLoopJoinTests, CartesianProductTest) {

  // Create plan node.
  planner::NestedLoopJoinNode node(nullptr);

  // Run the executor
  executor::NestedLoopJoinExecutor executor(&node);

  MockExecutor left_executor, right_executor;
  executor.AddChild(&left_executor);
  executor.AddChild(&right_executor);

  EXPECT_CALL(left_executor, DInit())
  .WillOnce(Return(true));
  EXPECT_CALL(right_executor, DInit())
  .WillRepeatedly(Return(true));

  EXPECT_CALL(left_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(true))
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  EXPECT_CALL(right_executor, DExecute())
  .WillOnce(Return(true))  // Itr 1
  .WillOnce(Return(true))
  .WillOnce(Return(false))
  .WillOnce(Return(true))  // Itr 2
  .WillOnce(Return(true))
  .WillOnce(Return(false))
  .WillOnce(Return(true))  // Itr 3
  .WillOnce(Return(true))
  .WillOnce(Return(false))
  .WillOnce(Return(true)); // Itr 4

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> left_table(ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(left_table.get(), tile_group_size * 3);
  std::unique_ptr<storage::DataTable> right_table(ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(right_table.get(), tile_group_size * 2);

  std::unique_ptr<executor::LogicalTile> left_logical_tile11(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile12(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile21(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(1)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile22(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(1)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile31(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(2)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile32(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(2)));

  std::unique_ptr<executor::LogicalTile> right_logical_tile11(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile12(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile13(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile21(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(1)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile22(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(1)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile23(
      executor::LogicalTileFactory::WrapTileGroup(right_table->GetTileGroup(1)));

  EXPECT_CALL(left_executor, GetOutput())
  .WillOnce(Return(left_logical_tile11.release()))
  .WillOnce(Return(left_logical_tile12.release()))
  .WillOnce(Return(left_logical_tile21.release()))
  .WillOnce(Return(left_logical_tile22.release()))
  .WillOnce(Return(left_logical_tile31.release()))
  .WillOnce(Return(left_logical_tile32.release()));

  EXPECT_CALL(right_executor, GetOutput())
  .WillOnce(Return(right_logical_tile11.release()))
  .WillOnce(Return(right_logical_tile21.release()))
  .WillOnce(Return(right_logical_tile12.release()))
  .WillOnce(Return(right_logical_tile22.release()))
  .WillOnce(Return(right_logical_tile13.release()))
  .WillOnce(Return(right_logical_tile23.release()));

  // Run the executor
  EXPECT_TRUE(executor.Init());

  for(size_t tile_itr = 0; tile_itr < 6 ; tile_itr++)
    EXPECT_TRUE(executor.Execute());

  EXPECT_FALSE(executor.Execute());
}

} // namespace test
} // namespace nstore
