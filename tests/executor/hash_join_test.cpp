//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_test.cpp
//
// Identification: tests/executor/hash_join_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/planner/hash_plan.h"
#include "backend/storage/data_table.h"

#include "mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

// Basic Test
// Single join clause, multiple tiles with same tuples per tile,
// join on key (i.e. there will have at most one tuple for each different join key
TEST(HashJoinTests, BasicTest) {

  expression::AbstractExpression *right_table_attr_1 =
      new expression::TupleValueExpression(1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression> > hashkeys;
  hashkeys.emplace_back(right_table_attr_1);

  // Create hash plan node
  planner::HashPlan hash_plan_node(hashkeys);

  // Create the hash hash_join_executor
  executor::HashExecutor hash_executor(&hash_plan_node, nullptr);

  // Create hash join plan node.
  auto projection = JoinTestsUtil::CreateProjection();
  planner::HashJoinPlan hash_join_plan_node(nullptr, projection);

  // Create the hash join hash_join_executor
  executor::HashJoinExecutor hash_join_executor(&hash_join_plan_node, nullptr);

  MockExecutor left_executor;
  hash_join_executor.AddChild(&left_executor);
  hash_join_executor.AddChild(&hash_join_executor);

  EXPECT_CALL(left_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(left_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true));

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> left_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(left_table.get(), tile_group_size * 3, false,
                                   false, false);
  std::unique_ptr<storage::DataTable> right_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(right_table.get(), tile_group_size * 2,
                                   false, false, false);

  std::unique_ptr<executor::LogicalTile> left_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> left_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(left_table->GetTileGroup(1)));

  std::unique_ptr<executor::LogicalTile> right_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(0)));
  std::unique_ptr<executor::LogicalTile> right_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          right_table->GetTileGroup(1)));

  EXPECT_CALL(left_executor, GetOutput())
      .WillOnce(Return(left_logical_tile1.release()))
      .WillOnce(Return(left_logical_tile2.release()));

  // Run the hash_join_executor
  EXPECT_TRUE(hash_join_executor.Init());

  for (size_t tile_itr = 0; tile_itr < 2; tile_itr++)
    EXPECT_TRUE(hash_join_executor.Execute());

  EXPECT_FALSE(hash_join_executor.Execute());
}

}  // namespace test
}  // namespace peloton
