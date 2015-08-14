//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_test.cpp
//
// Identification: tests/executor/projection_test.cpp
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

#include "backend/planner/projection_plan.h"


#include "backend/expression/expression.h"
#include "backend/expression/expression_util.h"
#include "backend/executor/projection_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

void RunTest(executor::ProjectionExecutor &executor,
             size_t expected_num_tiles) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  EXPECT_EQ(expected_num_tiles, result_tiles.size());
}

TEST(ProjectionTests, BasicTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size, false, false,
                                   false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  // Create the plan node
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // construct schema
  std::vector<catalog::Column> columns;
  auto orig_schema = data_table.get()->GetSchema();
  columns.push_back(orig_schema->GetColumn(0));

  auto schema = new catalog::Schema(columns);

  // direct map
  planner::ProjectInfo::DirectMap direct_map =
      std::make_pair(0, std::make_pair(0, 0));
  direct_map_list.push_back(direct_map);

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::ProjectionPlan node(project_info, schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

TEST(ProjectionTests, TwoColumnTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size, false, false,
                                   false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  // Create the plan node
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 3, 1, 3
  /////////////////////////////////////////////////////////

  // construct schema
  std::vector<catalog::Column> columns;
  auto orig_schema = data_table.get()->GetSchema();
  columns.push_back(orig_schema->GetColumn(3));
  columns.push_back(orig_schema->GetColumn(1));
  columns.push_back(orig_schema->GetColumn(3));

  auto schema = new catalog::Schema(columns);

  // direct map
  planner::ProjectInfo::DirectMap map0 =
      std::make_pair(0, std::make_pair(0, 3));
  planner::ProjectInfo::DirectMap map1 =
      std::make_pair(1, std::make_pair(0, 1));
  planner::ProjectInfo::DirectMap map2 =
      std::make_pair(2, std::make_pair(0, 3));
  direct_map_list.push_back(map0);
  direct_map_list.push_back(map1);
  direct_map_list.push_back(map2);

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::ProjectionPlan node(project_info, schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

TEST(ProjectionTests, BasicTargetTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size, false, false,
                                   false);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  // Create the plan node
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0, TARGET 0 + 20
  /////////////////////////////////////////////////////////

  // construct schema
  std::vector<catalog::Column> columns;
  auto orig_schema = data_table.get()->GetSchema();
  columns.push_back(orig_schema->GetColumn(0));
  columns.push_back(orig_schema->GetColumn(0));
  auto schema = new catalog::Schema(columns);

  // direct map
  planner::ProjectInfo::DirectMap direct_map =
      std::make_pair(0, std::make_pair(0, 0));
  direct_map_list.push_back(direct_map);

  // target list
  auto const_val = new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(20));
  auto tuple_value_expr = expression::TupleValueFactory(0, 0);
  expression::AbstractExpression *expr = expression::OperatorFactory(
      EXPRESSION_TYPE_OPERATOR_PLUS, tuple_value_expr, const_val);

  planner::ProjectInfo::Target target = std::make_pair(1, expr);
  target_list.push_back(target);

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::ProjectionPlan node(project_info, schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

}  // namespace test
}  // namespace peloton
