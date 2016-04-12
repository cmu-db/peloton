//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_test.cpp
//
// Identification: tests/executor/projection_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "harness.h"

#include "backend/planner/projection_plan.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/executor/projection_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ProjectionTests : public PelotonTest {};

void RunTest(executor::ProjectionExecutor &executor,
             size_t expected_num_tiles) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  EXPECT_EQ(expected_num_tiles, result_tiles.size());
}

TEST_F(ProjectionTests, BasicTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tile_size, false,
                                   false, false);
  txn_manager.CommitTransaction();

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

  std::shared_ptr<const catalog::Schema> schema(new catalog::Schema(columns));

  // direct map
  planner::ProjectInfo::DirectMap direct_map =
      std::make_pair(0, std::make_pair(0, 0));
  direct_map_list.push_back(direct_map);

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  planner::ProjectionPlan node(std::move(project_info), schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

TEST_F(ProjectionTests, TwoColumnTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tile_size, false,
                                   false, false);
  txn_manager.CommitTransaction();

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

  std::shared_ptr<const catalog::Schema> schema(new catalog::Schema(columns));

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

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  planner::ProjectionPlan node(std::move(project_info), schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

TEST_F(ProjectionTests, BasicTargetTest) {
  MockExecutor child_executor;
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 5;

  // Create a table and wrap it in logical tile
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tile_size, false,
                                   false, false);
  txn_manager.CommitTransaction();

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
  std::shared_ptr<const catalog::Schema> schema(new catalog::Schema(columns));

  // direct map
  planner::ProjectInfo::DirectMap direct_map =
      std::make_pair(0, std::make_pair(0, 0));
  direct_map_list.push_back(direct_map);

  // target list
  auto const_val = new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(20));
  auto tuple_value_expr = expression::ExpressionUtil::TupleValueFactory(0, 0);
  expression::AbstractExpression *expr =
      expression::ExpressionUtil::OperatorFactory(EXPRESSION_TYPE_OPERATOR_PLUS,
                                                  tuple_value_expr, const_val);

  planner::ProjectInfo::Target target = std::make_pair(1, expr);
  target_list.push_back(target);

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  planner::ProjectionPlan node(std::move(project_info), schema);

  // Create and set up executor
  executor::ProjectionExecutor executor(&node, nullptr);
  executor.AddChild(&child_executor);

  RunTest(executor, 1);
}

}  // namespace test
}  // namespace peloton
