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

#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

TEST(AggregateTests, SortedDistinctTest) {

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
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;

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
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_SORTED);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);

  EXPECT_TRUE(
      result_tile->GetValue(0, 2).OpEquals(ValueFactory::GetIntegerValue(1))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 3).OpEquals(ValueFactory::GetDoubleValue(2))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(5, 2).OpEquals(ValueFactory::GetIntegerValue(51))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(5, 3).OpEquals(ValueFactory::GetDoubleValue(52))
          .IsTrue());

}

TEST(AggregateTests, SortedSumGroupByTest) {
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
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumb(EXPRESSION_TYPE_AGGREGATE_SUM,
                                       expression::TupleValueFactory(0, 1));
  agg_terms.push_back(sumb);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_SORTED);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  EXPECT_TRUE(
      result_tile->GetValue(0, 0).OpEquals(ValueFactory::GetIntegerValue(0))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 1).OpEquals(ValueFactory::GetIntegerValue(105))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(1, 0).OpEquals(ValueFactory::GetIntegerValue(10))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(1, 1).OpEquals(ValueFactory::GetIntegerValue(355))
          .IsTrue());
}

TEST(AggregateTests, SortedSumMaxGroupByTest) {
  /*
   * SELECT a, SUM(b), MAX(c) from table GROUP BY a;
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
      { 1, 0 } }, { 2, { 1, 1 } } };

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumb(EXPRESSION_TYPE_AGGREGATE_SUM,
                                       expression::TupleValueFactory(0, 1));
  planner::AggregatePlan::AggTerm maxc(EXPRESSION_TYPE_AGGREGATE_MAX,
                                       expression::TupleValueFactory(0, 2));
  agg_terms.push_back(sumb);
  agg_terms.push_back(maxc);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1, 2 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_SORTED);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  EXPECT_TRUE(
      result_tile->GetValue(0, 0).OpEquals(ValueFactory::GetIntegerValue(0))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 1).OpEquals(ValueFactory::GetIntegerValue(105))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 2).OpEquals(ValueFactory::GetDoubleValue(42))
          .IsTrue());

  EXPECT_TRUE(
      result_tile->GetValue(1, 0).OpEquals(ValueFactory::GetIntegerValue(10))
          .IsTrue());
}

TEST(AggregateTests, HashDistinctTest) {

  /*
   * SELECT d, a, b, c FROM table GROUP BY a, b, c, d;
   */

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  true,
                                   true);  // let it be random

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
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;

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
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_HASH);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);

  for (auto tuple_id : *result_tile) {
    int colA = ValuePeeker::PeekAsInteger(result_tile->GetValue(tuple_id, 1));
    (void) colA;
  }

}

TEST(AggregateTests, HashSumGroupByTest) {
  /*
   * SELECT b, SUM(c) from table GROUP BY b;
   */
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  true,
                                   true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = { 1 };

  // 2) Set up project info
  planner::ProjectInfo::DirectMapList direct_map_list = { { 0, { 0, 1 } }, { 1,
      { 1, 0 } } };

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumC(EXPRESSION_TYPE_AGGREGATE_SUM,
                                       expression::TupleValueFactory(0, 2));
  agg_terms.push_back(sumC);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 1, 2 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_HASH);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  /* FIXME This should pass */
//  EXPECT_GE(3, result_tile->GetTupleCount());
//  std::cout << *result_tile;
}

TEST(AggregateTests, HashCountDistinctGroupByTest) {
  /*
   * SELECT a, COUNT(b), COUNT(DISTINCT b) from table group by a
   */
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  true,
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
      { 1, 0 } }, { 2, { 1, 1 } } };

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm countB(EXPRESSION_TYPE_AGGREGATE_COUNT,
                                         expression::TupleValueFactory(0, 1),
                                         false);  // Flag distinct
  planner::AggregatePlan::AggTerm countDistinctB(
      EXPRESSION_TYPE_AGGREGATE_COUNT, expression::TupleValueFactory(0, 1),
      true);  // Flag distinct
  agg_terms.push_back(countB);
  agg_terms.push_back(countDistinctB);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1, 1 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_HASH);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  EXPECT_TRUE(
      result_tile->GetValue(0, 0).OpEquals(ValueFactory::GetIntegerValue(0))
          .IsTrue()
          || result_tile->GetValue(0, 0).OpEquals(
              ValueFactory::GetIntegerValue(10)).IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 1).OpEquals(ValueFactory::GetIntegerValue(5))
          .IsTrue());

  EXPECT_TRUE(
      result_tile->GetValue(0, 2).OpLessThanOrEqual(
          ValueFactory::GetIntegerValue(3)).IsTrue());

}

TEST(AggregateTests, PlainSumCountDistinctTest) {
  /*
   * SELECT SUM(a), COUNT(b), COUNT(DISTINCT b) from table
   */
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
  true,
                                   true);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  planner::ProjectInfo::DirectMapList direct_map_list = { { 0, { 1, 0 } }, { 1,
      { 1, 1 } }, { 2, { 1, 2 } } };

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumA(EXPRESSION_TYPE_AGGREGATE_SUM,
                                       expression::TupleValueFactory(0, 0),
                                       false);
  planner::AggregatePlan::AggTerm countB(EXPRESSION_TYPE_AGGREGATE_COUNT,
                                         expression::TupleValueFactory(0, 1),
                                         false);  // Flag distinct
  planner::AggregatePlan::AggTerm countDistinctB(
      EXPRESSION_TYPE_AGGREGATE_COUNT, expression::TupleValueFactory(0, 1),
      true);  // Flag distinct
  agg_terms.push_back(sumA);
  agg_terms.push_back(countB);
  agg_terms.push_back(countDistinctB);

  // 4) Set up predicate (empty)
  expression::AbstractExpression* predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = { 0, 1, 1 };
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan node(proj_info, predicate, std::move(agg_terms),
                              std::move(group_by_columns), output_table_schema,
                              AGGREGATE_TYPE_PLAIN);

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

  /* Verify result */
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  EXPECT_TRUE(
      result_tile->GetValue(0, 0).OpEquals(ValueFactory::GetIntegerValue(50))
          .IsTrue());
  EXPECT_TRUE(
      result_tile->GetValue(0, 1).OpEquals(ValueFactory::GetIntegerValue(10))
          .IsTrue());

  EXPECT_TRUE(
      result_tile->GetValue(0, 2).OpLessThanOrEqual(
          ValueFactory::GetIntegerValue(3)).IsTrue());

}

}  // namespace test
}  // namespace peloton
