//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_test.cpp
//
// Identification: test/executor/aggregate_test.cpp
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

#include "common/internal_types.h"
#include "type/value.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/aggregate_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/expression_util.h"
#include "planner/abstract_plan.h"
#include "planner/aggregate_plan.h"
#include "storage/data_table.h"

#include "executor/mock_executor.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class AggregateTests : public PelotonTest {};

TEST_F(AggregateTests, SortedDistinctTest) {
  // SELECT d, a, b, c FROM table GROUP BY a, b, c, d;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0, 1, 2, 3};

  // 2) Set up project info
  DirectMapList direct_map_list = {
      {0, {0, 3}}, {1, {0, 0}}, {2, {0, 1}}, {3, {0, 2}}};
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates (empty)
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {3, 0, 1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }

  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::SORTED);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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
  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  LOG_TRACE("%s", result_tile->GetInfo().c_str());
  ASSERT_TRUE(result_tile->GetTupleCount() > 0);

  type::Value val = (result_tile->GetValue(0, 2));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(1)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 3));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(2)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(5, 2));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(51)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(5, 3));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(52)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

TEST_F(AggregateTests, SortedSumGroupByTest) {
  // SELECT a, SUM(b) from table GROUP BY a;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumb(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1));
  agg_terms.push_back(sumb);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {0, 1};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::SORTED);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(0)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(105)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(1, 0));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(1, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(355)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

TEST_F(AggregateTests, SortedSumMaxGroupByTest) {
  // SELECT a, SUM(b), MAX(c) from table GROUP BY a;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));

  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}, {2, {1, 1}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumb(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1));
  planner::AggregatePlan::AggTerm maxc(
      ExpressionType::AGGREGATE_MAX,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2));
  agg_terms.push_back(sumb);
  agg_terms.push_back(maxc);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {0, 1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::SORTED);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(0)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(105)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(42)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(1, 0));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

TEST_F(AggregateTests, MinMaxTest) {
  // SELECT MIN(b), MAX(b), MIN(c), MAX(c) from table
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  DirectMapList direct_map_list = {
      {0, {1, 0}}, {1, {1, 1}}, {2, {1, 2}}, {3, {1, 3}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm minB(
      ExpressionType::AGGREGATE_MIN,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);
  planner::AggregatePlan::AggTerm maxB(
      ExpressionType::AGGREGATE_MAX,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);
  planner::AggregatePlan::AggTerm minC(
      ExpressionType::AGGREGATE_MIN,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2),
      false);
  planner::AggregatePlan::AggTerm maxC(
      ExpressionType::AGGREGATE_MAX,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2),
      false);
  agg_terms.push_back(minB);
  agg_terms.push_back(maxB);
  agg_terms.push_back(minC);
  agg_terms.push_back(maxC);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {1, 1, 2, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::PLAIN);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(1)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(91)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(2)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 3));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(92)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

TEST_F(AggregateTests, HashDistinctTest) {
  // SELECT d, a, b, c FROM table GROUP BY a, b, c, d;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   true, true,
                                   txn);  // let it be random
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0, 1, 2, 3};

  // 2) Set up project info
  DirectMapList direct_map_list = {
      {0, {0, 3}}, {1, {0, 0}}, {2, {0, 1}}, {3, {0, 2}}};
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates (empty)
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {3, 0, 1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }

  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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
  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);

  // for (auto tuple_id : *result_tile) {
  //  int colA =
  //  type::ValuePeeker::PeekInteger(result_tile->GetValue(tuple_id, 1));
  //  (void)colA;
  //}
}

TEST_F(AggregateTests, HashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {1};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 1}}, {1, {1, 0}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumC(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2));
  agg_terms.push_back(sumC);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  // FIXME This should pass
  //  EXPECT_GE(3, result_tile->GetTupleCount());
}

TEST_F(AggregateTests, HashCountDistinctGroupByTest) {
  // SELECT a, COUNT(b), COUNT(DISTINCT b) from table group by a
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}, {2, {1, 1}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm countB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);  // Flag distinct
  planner::AggregatePlan::AggTerm countDistinctB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      true);  // Flag distinct
  agg_terms.push_back(countB);
  agg_terms.push_back(countDistinctB);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {0, 1, 1};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(0)));
  CmpBool cmp1 =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
  EXPECT_TRUE(cmp == CmpBool::TRUE || cmp1 == CmpBool::TRUE);

  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(5)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);

  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareLessThanEquals(type::ValueFactory::GetIntegerValue(3)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

TEST_F(AggregateTests, PlainSumCountDistinctTest) {
  // SELECT SUM(a), COUNT(b), COUNT(DISTINCT b) from table
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                   true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {1, 0}}, {1, {1, 1}}, {2, {1, 2}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumA(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    0),
      false);
  planner::AggregatePlan::AggTerm countB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);  // Flag distinct
  planner::AggregatePlan::AggTerm countDistinctB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      true);  // Flag distinct
  agg_terms.push_back(sumA);
  agg_terms.push_back(countB);
  agg_terms.push_back(countDistinctB);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {0, 1, 1};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::PLAIN);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(50)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareLessThanEquals(type::ValueFactory::GetIntegerValue(3)));
  EXPECT_TRUE(cmp == CmpBool::TRUE);
}

}  // namespace test
}  // namespace peloton
