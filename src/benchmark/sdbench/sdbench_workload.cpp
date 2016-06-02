//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_workload.cpp
//
// Identification: src/backend/benchmark/sdbench/sdbench_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>
#include <thread>
#include <algorithm>

#include "backend/expression/expression_util.h"
#include "backend/brain/clusterer.h"

#include "backend/benchmark/sdbench/sdbench_workload.h"
#include "backend/benchmark/sdbench/sdbench_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/macros.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"


#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/projection_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/nested_loop_join_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"

#include "backend/index/index_factory.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/planner/nested_loop_join_plan.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

// Tuple id counter
oid_t sdbench_tuple_counter = -1000000;

expression::AbstractExpression *CreatePredicate(const int lower_bound) {
  // ATTR0 >= LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0);

  // Second, create constant value expression.
  Value constant_value = ValueFactory::GetIntegerValue(lower_bound);

  expression::AbstractExpression *constant_value_expr =
      expression::ExpressionUtil::ConstantValueFactory(constant_value);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ComparisonFactory(
          EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, tuple_value_expr,
          constant_value_expr);

  return predicate;
}

std::ofstream out("outputfile.summary");

oid_t query_itr;

static void WriteOutput(double duration) {
  // Convert to ms
  duration *= 1000;

  LOG_TRACE("----------------------------------------------------------");
  LOG_TRACE("%d %d %lf %lf %lf %d %d %d :: %lf ms",
           state.layout_mode, state.operator_type,
           state.projectivity, state.selectivity,
           state.write_ratio, state.scale_factor,
           state.column_count,
           state.tuples_per_tilegroup,
           duration);

  out << state.layout_mode << " ";
  out << state.operator_type << " ";
  out << state.selectivity << " ";
  out << state.projectivity << " ";
  out << state.column_count << " ";
  out << state.write_ratio << " ";
  out << state.tuples_per_tilegroup << " ";
  out << query_itr << " ";
  out << state.scale_factor << " ";
  out << duration << "\n";
  out.flush();
}

static int GetLowerBound() {
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  const int lower_bound = (1 - state.selectivity) * tuple_count;

  return lower_bound;
}

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors,
                        std::vector<double> columns_accessed, double cost) {
  Timer<> timer;

  auto txn_count = state.transactions;
  bool status = false;

  timer.Start();

  // Construct sample
  brain::Sample sample(columns_accessed, cost);

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {

    // Increment query counter
    query_itr++;

    // Run all the executors
    for (auto executor : executors) {
      status = executor->Init();
      if (status == false) throw Exception("Init failed");

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

      while (executor->Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_tile(
            executor->GetOutput());
        result_tiles.emplace_back(result_tile.release());
      }

      // Execute stuff
      executor->Execute();
    }

    // Capture fine-grained stats in adapt experiment
    if (state.adapt == true) {
      timer.Stop();
      double time_per_transaction = timer.GetDuration();
      WriteOutput(time_per_transaction);

      // Record sample
      if (state.fsm == true && cost != 0) {
        sdbench_table->RecordSample(sample);
      }

      timer.Start();
    }
  }

  if (state.adapt == false) {
    timer.Stop();
    double time_per_transaction = timer.GetDuration() / txn_count;

    WriteOutput(time_per_transaction);
  }

}

std::vector<double> GetColumnsAccessed(const std::vector<oid_t> &column_ids) {
  std::vector<double> columns_accessed;
  std::map<oid_t, oid_t> columns_accessed_map;

  // Init map
  for (auto col : column_ids) columns_accessed_map[col] = 1;

  for (int column_itr = 0; column_itr < state.column_count; column_itr++) {
    auto location = columns_accessed_map.find(column_itr);
    auto end = columns_accessed_map.end();
    if (location != end)
      columns_accessed.push_back(1);
    else
      columns_accessed.push_back(0);
  }

  return columns_accessed;
}

void RunDirectTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.projectivity * state.column_count;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(sdbench_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for (auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema, physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::vector<Value> values;
  Value insert_val = ValueFactory::GetIntegerValue(++sdbench_tuple_counter);

  TargetList target_list;
  DirectMapList direct_map_list;

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression =
        expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  std::unique_ptr<planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(sdbench_table.get(), std::move(project_info),
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

void RunAggregateTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  // We need all columns because projection can require any column
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count;

  column_ids.push_back(0);
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(sdbench_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // AGGREGATION
  /////////////////////////////////////////////////////////

  // Resize column ids to contain only columns over which we compute aggregates
  column_count = state.projectivity * state.column_count;
  column_ids.resize(column_count);

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  DirectMapList direct_map_list;
  oid_t col_itr = 0;
  oid_t tuple_idx = 1;  // tuple2
  for (col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.push_back({col_itr, {tuple_idx, col_itr}});
    col_itr++;
  }

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(),
                               std::move(direct_map_list)));

  // 3) Set up aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  for (auto column_id : column_ids) {
    planner::AggregatePlan::AggTerm max_column_agg(
        EXPRESSION_TYPE_AGGREGATE_MAX,
        expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0,
                                                      column_id), false);
    agg_terms.push_back(max_column_agg);
  }

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> aggregate_predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = sdbench_table->GetSchema();
  std::vector<catalog::Column> columns;
  for (auto column_id : column_ids) {
    columns.push_back(data_table_schema->GetColumn(column_id));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan aggregation_node(
      std::move(proj_info), std::move(aggregate_predicate), std::move(agg_terms),
      std::move(group_by_columns), output_table_schema, AGGREGATE_TYPE_PLAIN);

  executor::AggregateExecutor aggregation_executor(&aggregation_node,
                                                   context.get());
  aggregation_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  col_itr = 0;
  for (auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "MAX " + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema, physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&aggregation_executor);

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::vector<Value> values;
  Value insert_val = ValueFactory::GetIntegerValue(++sdbench_tuple_counter);

  TargetList target_list;
  direct_map_list.clear();

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression =
        expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  std::unique_ptr<planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(sdbench_table.get(), std::move(project_info),
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

void RunArithmeticTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  // We need all columns because projection can require any column
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count;

  column_ids.push_back(0);
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(sdbench_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // PROJECTION
  /////////////////////////////////////////////////////////

  TargetList target_list;
  DirectMapList direct_map_list;

  // Construct schema of projection
  std::vector<catalog::Column> columns;
  auto orig_schema = sdbench_table->GetSchema();
  columns.push_back(orig_schema->GetColumn(0));
  std::shared_ptr<const catalog::Schema> projection_schema(
      new catalog::Schema(columns));

  // target list
  expression::AbstractExpression *sum_expr = nullptr;
  oid_t projection_column_count = state.projectivity * state.column_count;

  // Resize column ids to contain only columns over which we evaluate the
  // expression
  column_ids.resize(projection_column_count);

  for (oid_t col_itr = 0; col_itr < projection_column_count; col_itr++) {
    auto sdbench_colum_id = hyadapt_column_ids[col_itr];
    auto column_expr = expression::ExpressionUtil::TupleValueFactory(
        VALUE_TYPE_INTEGER, 0, sdbench_colum_id);
    if (sum_expr == nullptr)
      sum_expr = column_expr;
    else {
      sum_expr = expression::ExpressionUtil::OperatorFactory(
          EXPRESSION_TYPE_OPERATOR_PLUS, VALUE_TYPE_INTEGER, sum_expr,
          column_expr);
    }
  }

  Target target = std::make_pair(0, sum_expr);
  target_list.push_back(target);

  std::unique_ptr<planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  planner::ProjectionPlan node(std::move(project_info), projection_schema);

  // Create and set up executor
  executor::ProjectionExecutor projection_executor(&node, nullptr);
  projection_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;

  auto column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "SUM", is_inlined);
  output_columns.push_back(column);
  old_to_new_cols[col_itr] = col_itr;

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema, physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&projection_executor);

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::vector<Value> values;
  Value insert_val = ValueFactory::GetIntegerValue(++sdbench_tuple_counter);

  target_list.clear();
  direct_map_list.clear();

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression =
        expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  project_info.reset(new planner::ProjectInfo(std::move(target_list),
                                              std::move(direct_map_list)));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(sdbench_table.get(), std::move(project_info),
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

void RunJoinTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.projectivity * state.column_count;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto left_table_predicate = CreatePredicate(lower_bound);
  auto right_table_predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan left_table_seq_scan_node(
      sdbench_table.get(), left_table_predicate, column_ids);
  planner::SeqScanPlan right_table_seq_scan_node(
      sdbench_table.get(), right_table_predicate, column_ids);

  executor::SeqScanExecutor left_table_scan_executor(&left_table_seq_scan_node,
                                                     context.get());
  executor::SeqScanExecutor right_table_scan_executor(
      &right_table_seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // JOIN EXECUTOR
  /////////////////////////////////////////////////////////

  auto join_type = JOIN_TYPE_INNER;

  // Create join predicate
  std::unique_ptr<const expression::AbstractExpression> join_predicate(nullptr);
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);
  std::shared_ptr<const catalog::Schema> schema(nullptr);

  planner::NestedLoopJoinPlan nested_loop_join_node(
      join_type, std::move(join_predicate), std::move(project_info), schema);

  // Run the nested loop join executor
  executor::NestedLoopJoinExecutor nested_loop_join_executor(
      &nested_loop_join_node, nullptr);

  // Construct the executor tree
  nested_loop_join_executor.AddChild(&left_table_scan_executor);
  nested_loop_join_executor.AddChild(&right_table_scan_executor);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t join_column_count = column_count * 2;
  for (oid_t col_itr = 0; col_itr < join_column_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(col_itr), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema, physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&nested_loop_join_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

void RunInsertTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::vector<Value> values;
  Value insert_val = ValueFactory::GetIntegerValue(++sdbench_tuple_counter);
  TargetList target_list;
  DirectMapList direct_map_list;
  std::vector<oid_t> column_ids;

  target_list.clear();
  direct_map_list.clear();

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression =
        expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
    column_ids.push_back(col_id);
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(sdbench_table.get(), std::move(project_info),
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 0;
  std::vector<double> columns_accessed;

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

void RunUpdateTest() {
  const int lower_bound = GetLowerBound();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  // We need all columns because projection can require any column
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count;

  column_ids.push_back(0);
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(sdbench_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  // Update
  std::vector<Value> values;

  TargetList target_list;
  DirectMapList direct_map_list;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(sdbench_table.get(), std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  update_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&update_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 0;
  std::vector<double> columns_accessed;

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction();
}

/////////////////////////////////////////////////////////
// EXPERIMENTS
/////////////////////////////////////////////////////////

std::vector<oid_t> column_counts = {50, 500};

std::vector<double> write_ratios = {0, 1.0};

std::vector<LayoutType> layouts = {LAYOUT_ROW, LAYOUT_COLUMN, LAYOUT_HYBRID};

std::vector<OperatorType> operators = {
    OPERATOR_TYPE_DIRECT, OPERATOR_TYPE_AGGREGATE, OPERATOR_TYPE_ARITHMETIC};

std::vector<double> selectivity = {0.2, 0.4, 0.6, 0.8, 1.0};

std::vector<double> projectivity = {0.02, 0.1, 0.5, 1.0};

const oid_t query_repeat_count = 10;

void RunProjectivityExperiment() {
  state.selectivity = 1.0;

  // Go over all column counts
  for (auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for (auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      // Go over all layouts
      for (auto layout : layouts) {
        // Set layout
        state.layout_mode = layout;
        peloton_layout_mode = state.layout_mode;

        for (auto proj : projectivity) {
          // Set proj
          state.projectivity = proj;
          peloton_projectivity = state.projectivity;

          // Load in the table with layout
          CreateAndLoadTable(layout);

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest();

          state.operator_type = OPERATOR_TYPE_AGGREGATE;
          RunAggregateTest();

          // state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          // RunArithmeticTest();
        }
      }
    }
  }

  out.close();
}

void RunSelectivityExperiment() {
  state.projectivity = 0.1;
  peloton_projectivity = state.projectivity;

  // Go over all column counts
  for (auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for (auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      // Go over all layouts
      for (auto layout : layouts) {
        // Set layout
        state.layout_mode = layout;
        peloton_layout_mode = state.layout_mode;

        for (auto select : selectivity) {
          // Set selectivity
          state.selectivity = select;

          // Load in the table with layout
          CreateAndLoadTable(layout);

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest();

          state.operator_type = OPERATOR_TYPE_AGGREGATE;
          RunAggregateTest();

          // state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          // RunArithmeticTest();
        }
      }
    }
  }

  out.close();
}

int op_column_count = 100;

std::vector<double> op_projectivity = {0.01, 0.1, 1.0};

std::vector<double> op_selectivity = {0.1, 0.2, 0.3, 0.4, 0.5,
                                      0.6, 0.7, 0.8, 0.9, 1.0};

void RunOperatorExperiment() {
  state.column_count = op_column_count;

  // Generate sequence
  GenerateSequence(state.column_count);

  // Go over all write ratios
  for (auto write_ratio : write_ratios) {
    state.write_ratio = write_ratio;

    // Go over all layouts
    for (auto layout : layouts) {
      // Set layout
      state.layout_mode = layout;
      peloton_layout_mode = state.layout_mode;

      for (auto projectivity : op_projectivity) {
        // Set projectivity
        state.projectivity = projectivity;
        peloton_projectivity = state.projectivity;

        for (auto selectivity : op_selectivity) {
          // Set selectivity
          state.selectivity = selectivity;

          // Load in the table with layout
          CreateAndLoadTable(layout);

          // Run operator
          state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          RunDirectTest();
        }
      }
    }
  }

  out.close();
}


static void Transform(double theta) {
  // Get column map
  auto table_name = sdbench_table->GetName();

  peloton_projectivity = state.projectivity;

  // TODO: Update period ?
  oid_t update_period = 10;
  oid_t update_itr = 0;

  // Transform
  while (state.fsm == true) {
    auto tile_group_count = sdbench_table->GetTileGroupCount();
    auto tile_group_offset = rand() % tile_group_count;

    sdbench_table->TransformTileGroup(tile_group_offset, theta);

    // Update partitioning periodically
    update_itr++;
    if (update_itr == update_period) {
      sdbench_table->UpdateDefaultPartition();
      update_itr = 0;
    }
  }
}

static void RunAdaptTest() {
  double direct_low_proj = 0.06;
  double insert_write_ratio = 0.05;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = 0.05;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;
}

std::vector<LayoutType> adapt_layouts = {LAYOUT_ROW, LAYOUT_COLUMN,
                                         LAYOUT_HYBRID};

std::vector<oid_t> adapt_column_counts = {column_counts[1]};

void RunAdaptExperiment() {
  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.transactions = 25;

  state.write_ratio = 0.0;
  state.selectivity = 1.0;
  state.adapt = true;
  double theta = 0.0;

  // Go over all column counts
  for (auto column_count : adapt_column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all layouts
    for (auto layout : adapt_layouts) {
      // Set layout
      state.layout_mode = layout;
      peloton_layout_mode = state.layout_mode;

      LOG_TRACE("----------------------------------------- \n");

      state.projectivity = 1.0;
      peloton_projectivity = 1.0;
      CreateAndLoadTable((LayoutType)peloton_layout_mode);

      // Reset query counter
      query_itr = 0;

      // Launch transformer
      if (state.layout_mode == LAYOUT_HYBRID) {
        state.fsm = true;
        peloton_fsm = true;
        transformer = std::thread(Transform, theta);
      }

      RunAdaptTest();

      // Stop transformer
      if (state.layout_mode == LAYOUT_HYBRID) {
        state.fsm = false;
        peloton_fsm = false;
        transformer.join();
      }
    }
  }

  // Reset
  state.transactions = orig_transactions;
  state.adapt = false;
  query_itr = 0;

  out.close();
}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
