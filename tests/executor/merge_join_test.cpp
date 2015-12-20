//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// merge_join_test.cpp
//
// Identification: tests/executor/merge_join_test.cpp
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
#include "backend/executor/merge_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/merge_join_plan.h"
#include "backend/storage/data_table.h"

#include "mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

std::vector<planner::MergeJoinPlan::JoinClause> CreateJoinClauses() {
  std::vector<planner::MergeJoinPlan::JoinClause> join_clauses;
  auto left = expression::TupleValueFactory(0, 0);
  auto right = expression::TupleValueFactory(1, 0);
  bool reversed = false;
  join_clauses.emplace_back(left, right, reversed);
  return join_clauses;
}

// Basic Test
// Single join clause, multiple tiles with same tuples per tile,
// join on key (i.e. there will have at most one tuple for each different join key
TEST(MergeJoinTests, BasicTest) {
  // Create plan node.
  auto projection = JoinTestsUtil::CreateProjection();
  auto join_clauses = CreateJoinClauses();
  planner::MergeJoinPlan node(nullptr, projection, join_clauses);

  node.SetJoinType(JOIN_TYPE_INNER);
  // Run the executor
  executor::MergeJoinExecutor executor(&node, nullptr);

  MockExecutor left_executor, right_executor;
  executor.AddChild(&left_executor);
  executor.AddChild(&right_executor);

  EXPECT_CALL(left_executor, DInit()).WillOnce(Return(true));
  EXPECT_CALL(right_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(left_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(right_executor, DExecute())
      .WillOnce(Return(true))  // Itr 1
      .WillOnce(Return(true))
      .WillOnce(Return(false));

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

  EXPECT_CALL(right_executor, GetOutput())
      .WillOnce(Return(right_logical_tile1.release()))
      .WillOnce(Return(right_logical_tile2.release()));

  // Run the executor
  EXPECT_TRUE(executor.Init());

  for (size_t tile_itr = 0; tile_itr < 2; tile_itr++)
    EXPECT_TRUE(executor.Execute());

  EXPECT_FALSE(executor.Execute());
}

}  // namespace test
}  // namespace peloton
