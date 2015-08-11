//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregate_test.cpp
//
// Identification: tests/executor/aggregate_test.cpp
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

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/abstract_plan_node.h"
#include "backend/planner/aggregateV2_node.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

TEST(AggregateTests, DistinctTest) {

  /*
   * SELECT d, a, b, c FROM table GROUP BY a, b, c, d;
   */

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  false,
                                   true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = { 0, 1, 2, 3 };

  // 2) Set up project info
  planner::ProjectInfo::DirectMapList direct_map_list = { { 0, { 0, 3 } }, { 1,
      { 0, 0 } }, { 2, { 0, 1 } }, { 3, { 0, 2 } } };
  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates (empty)
  std::vector<planner::AggregateV2Node::AggTerm> agg_terms;

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 3, 0, 1, 2 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }

  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregateV2Node node(proj_info, predicate, std::move(agg_terms),
                                std::move(group_by_columns),
                                output_table_schema);

  // Create and set up executor
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute()).WillOnce(Return(true)).WillOnce(
      Return(true)).WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput()).WillOnce(
      Return(source_logical_tile1.release())).WillOnce(
      Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);
}



TEST(AggregateTests, SumGroupByTest) {
  /*
   * SELECT a, SUM(b) from table GROUP BY a;
   */
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  false,
                                   true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = { 0 };

  // 2) Set up project info
  planner::ProjectInfo::DirectMapList direct_map_list = { { 0, { 0, 0 } }, { 1,
      { 1, 0 } } };

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates
  std::vector<planner::AggregateV2Node::AggTerm> agg_terms;
  planner::AggregateV2Node::AggTerm sumb = std::make_pair(
      EXPRESSION_TYPE_AGGREGATE_SUM, expression::TupleValueFactory(0, 1));
  agg_terms.push_back(sumb);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregateV2Node node(proj_info, predicate, std::move(agg_terms),
                                std::move(group_by_columns),
                                output_table_schema);

  // Create and set up executor
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute()).WillOnce(Return(true)).WillOnce(
      Return(true)).WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput()).WillOnce(
      Return(source_logical_tile1.release())).WillOnce(
      Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);
}

/*
 TEST(AggregateTests, AggregateTest) {
 const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

 // Create a table and wrap it in logical tiles
 std::unique_ptr<storage::DataTable> data_table(
 ExecutorTestsUtil::CreateTable(tuple_count, false));
 ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
 false, true);

 std::unique_ptr<executor::LogicalTile> source_logical_tile1(
 executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

 std::unique_ptr<executor::LogicalTile> source_logical_tile2(
 executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

 // Setup plan node

 std::vector<oid_t> group_by_columns = {0, 1};
 std::map<oid_t, oid_t> pass_through_columns_map;

 // Input tuple column index -> Output tuple column index
 pass_through_columns_map[0] = 0;
 pass_through_columns_map[1] = 1;

 // Aggregates
 std::vector<oid_t> aggregate_columns = {2, 2};
 std::map<oid_t, oid_t> aggregate_columns_map;

 // Input tuple column index -> Output tuple column index
 aggregate_columns_map[0] = 2;
 aggregate_columns_map[1] = 3;

 std::vector<ExpressionType> aggregate_types;
 aggregate_types.push_back(EXPRESSION_TYPE_AGGREGATE_SUM);
 aggregate_types.push_back(EXPRESSION_TYPE_AGGREGATE_AVG);

 // More complex schema construction
 auto data_table_schema = data_table.get()->GetSchema();
 std::vector<oid_t> set = {0, 1, 2, 2};
 std::vector<catalog::Column> columns;
 for (auto column_index : set) {
 columns.push_back(data_table_schema->GetColumn(column_index));
 }

 auto output_table_schema = new catalog::Schema(columns);

 // Create the plan node
 planner::AggregateNode node(
 aggregate_columns, aggregate_columns_map, group_by_columns, nullptr,
 pass_through_columns_map, aggregate_types, output_table_schema);

 // Create and set up executor
 auto &txn_manager = concurrency::TransactionManager::GetInstance();
 auto txn = txn_manager.BeginTransaction();
 std::unique_ptr<executor::ExecutorContext> context(
 new executor::ExecutorContext(txn));

 executor::AggregateExecutor executor(&node, context.get());
 MockExecutor child_executor;
 executor.AddChild(&child_executor);

 EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

 EXPECT_CALL(child_executor, DExecute())
 .WillOnce(Return(true))
 .WillOnce(Return(true))
 .WillOnce(Return(false));

 EXPECT_CALL(child_executor, GetOutput())
 .WillOnce(Return(source_logical_tile1.release()))
 .WillOnce(Return(source_logical_tile2.release()));

 EXPECT_TRUE(executor.Init());

 EXPECT_TRUE(executor.Execute());

 std::unique_ptr<executor::LogicalTile> logical_tile(executor.GetOutput());
 EXPECT_TRUE(logical_tile.get() != nullptr);

 EXPECT_TRUE(logical_tile.get()->GetValue(0, 2) ==
 ValueFactory::GetDoubleValue(110));
 EXPECT_TRUE(logical_tile.get()->GetValue(1, 2) ==
 ValueFactory::GetDoubleValue(360));
 EXPECT_TRUE(logical_tile.get()->GetValue(0, 3) ==
 ValueFactory::GetDoubleValue(22));
 EXPECT_TRUE(logical_tile.get()->GetValue(1, 3) ==
 ValueFactory::GetDoubleValue(72));

 txn_manager.CommitTransaction(txn);
 }

 */

}  // namespace test
}  // namespace peloton
