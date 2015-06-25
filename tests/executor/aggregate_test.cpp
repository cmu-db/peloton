/**
 * @brief Test cases for aggregate node.
 *
 * Copyright(c) 2015, CMU
 */

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
#include "backend/planner/abstract_plan_node.h"
#include "backend/planner/aggregate_node.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace nstore {
namespace test {

TEST(AggregateTests, DistinctTest){

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(ExecutorTestsUtil::CreateTable(tuple_count));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false, false, true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // Setup plan node

  std::vector<oid_t> aggregate_columns;
  std::vector<oid_t> group_by_columns = { 0 , 1, 2, 4};
  std::map<oid_t, oid_t> pass_through_columns;

  pass_through_columns[0] = 0;
  pass_through_columns[1] = 1;
  pass_through_columns[2] = 2;
  pass_through_columns[3] = 3;

  std::vector<ExpressionType> aggregate_types;

  auto output_table_schema = data_table.get()->GetSchema();

  // Create the plan node
  planner::AggregateNode node(aggregate_columns,
                              group_by_columns,
                              pass_through_columns,
                              aggregate_types,
                              output_table_schema);

  // Create and set up executor
  auto& txn_manager = TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  executor::AggregateExecutor executor(&node, txn);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit())
  .WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput())
  .WillOnce(Return(source_logical_tile1.release()))
  .WillOnce(Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
}

TEST(AggregateTests, GroupByTest){

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(ExecutorTestsUtil::CreateTable(tuple_count));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false, false, true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // Setup plan node

  std::vector<oid_t> aggregate_columns;
  std::vector<oid_t> group_by_columns = { 0 , 1};
  std::map<oid_t, oid_t> pass_through_columns;

  pass_through_columns[0] = 0;
  pass_through_columns[1] = 1;
  pass_through_columns[2] = 2;
  pass_through_columns[3] = 3;

  std::vector<ExpressionType> aggregate_types;

  auto output_table_schema = data_table.get()->GetSchema();

  // Create the plan node
  planner::AggregateNode node(aggregate_columns,
                              group_by_columns,
                              pass_through_columns,
                              aggregate_types,
                              output_table_schema);

  // Create and set up executor
  auto& txn_manager = TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  executor::AggregateExecutor executor(&node, txn);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit())
  .WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput())
  .WillOnce(Return(source_logical_tile1.release()))
  .WillOnce(Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
}

TEST(AggregateTests, AggregateTest){

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(ExecutorTestsUtil::CreateTable(tuple_count));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false, false, true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // Setup plan node

  std::vector<oid_t> aggregate_columns = { 2 };
  std::vector<oid_t> group_by_columns = { 0 , 1};
  std::map<oid_t, oid_t> pass_through_columns;

  pass_through_columns[0] = 0;
  pass_through_columns[1] = 1;

  std::vector<ExpressionType> aggregate_types;
  aggregate_types.push_back(EXPRESSION_TYPE_AGGREGATE_SUM);

  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1, 2};
  std::vector<catalog::ColumnInfo> columns;
  for(auto column_index : set){
    columns.push_back(data_table_schema->GetColumnInfo(column_index));
  }

  std::unique_ptr<catalog::Schema> output_table_schema(new catalog::Schema(columns));

  // Create the plan node
  planner::AggregateNode node(aggregate_columns,
                              group_by_columns,
                              pass_through_columns,
                              aggregate_types,
                              output_table_schema.get());

  // Create and set up executor
  auto& txn_manager = TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  executor::AggregateExecutor executor(&node, txn);
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit())
  .WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput())
  .WillOnce(Return(source_logical_tile1.release()))
  .WillOnce(Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  auto logical_tile = executor.GetOutput();
  EXPECT_TRUE(logical_tile != nullptr);

  EXPECT_TRUE(logical_tile->GetValue(0, 2) == ValueFactory::GetDoubleValue(110));
  EXPECT_TRUE(logical_tile->GetValue(1, 2) == ValueFactory::GetDoubleValue(360));

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
}

} // namespace test
} // namespace nstore
