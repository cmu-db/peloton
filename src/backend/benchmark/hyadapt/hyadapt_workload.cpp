//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyadapt_workload.cpp
//
// Identification: src/backend/benchmark/hyadapt/hyadapt_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#undef NDEBUG

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>

#include "backend/expression/expression_util.h"
#include "backend/brain/clusterer.h"

#include "backend/benchmark/hyadapt/hyadapt_workload.h"
#include "backend/benchmark/hyadapt/hyadapt_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
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
namespace hyadapt {

// Tuple id counter
oid_t hyadapt_tuple_counter = -1000000;

static void CollectColumnMapStats();

static void Reorg();

expression::AbstractExpression *CreatePredicate(const int lower_bound) {
  // ATTR0 >= LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::ExpressionUtil::TupleValueFactory(0, 0);

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

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %lf %lf %lf %d %d %d %d %lf %lf %d %lf %d :: %lf ms",
           state.layout_mode, state.operator_type,
           state.projectivity, state.selectivity,
           state.write_ratio, state.scale_factor,
           state.column_count, state.subset_experiment_type,
           state.access_num_groups, state.subset_ratio,
           state.theta, state.split_point,
           state.sample_weight, state.tuples_per_tilegroup,
           duration);

  out << state.layout_mode << " ";
  out << state.operator_type << " ";
  out << state.selectivity << " ";
  out << state.projectivity << " ";
  out << state.column_count << " ";
  out << state.write_ratio << " ";
  out << state.subset_experiment_type << " ";
  out << state.access_num_groups << " ";
  out << state.subset_ratio << " ";
  out << state.tuples_per_tilegroup << " ";
  out << query_itr << " ";
  out << state.theta << " ";
  out << state.split_point << " ";
  out << state.sample_weight << " ";
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
    // Reorg mode
    if (state.reorg == true && txn_itr == 4) {
      hyadapt_table->RecordSample(sample);
      Reorg();
    }

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

      if (state.distribution == false) WriteOutput(time_per_transaction);

      // Record sample
      if (state.fsm == true && cost != 0) {
        hyadapt_table->RecordSample(sample);
      }

      timer.Start();
    }
  }

  if (state.adapt == false) {
    timer.Stop();
    double time_per_transaction = timer.GetDuration() / txn_count;

    WriteOutput(time_per_transaction);
  }

  if (state.distribution == true) CollectColumnMapStats();
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
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

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
  Value insert_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

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
  planner::InsertPlan insert_node(hyadapt_table.get(), std::move(project_info),
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
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

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
  planner::ProjectInfo::DirectMapList direct_map_list;
  oid_t col_itr = 0;
  oid_t tuple_idx = 1;  // tuple2
  for (col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.push_back({col_itr, {tuple_idx, col_itr}});
    col_itr++;
  }

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                               std::move(direct_map_list)));

  // 3) Set up aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  for (auto column_id : column_ids) {
    planner::AggregatePlan::AggTerm max_column_agg(
        EXPRESSION_TYPE_AGGREGATE_MAX,
        expression::ExpressionUtil::TupleValueFactory(0, column_id), false);
    agg_terms.push_back(max_column_agg);
  }

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> aggregate_predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = hyadapt_table->GetSchema();
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
  Value insert_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);

  planner::ProjectInfo::TargetList target_list;
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
  planner::InsertPlan insert_node(hyadapt_table.get(), std::move(project_info),
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
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // PROJECTION
  /////////////////////////////////////////////////////////

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // Construct schema of projection
  std::vector<catalog::Column> columns;
  auto orig_schema = hyadapt_table->GetSchema();
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
    auto hyadapt_colum_id = hyadapt_column_ids[col_itr];
    auto column_expr =
        expression::ExpressionUtil::TupleValueFactory(0, hyadapt_colum_id);
    if (sum_expr == nullptr)
      sum_expr = column_expr;
    else {
      sum_expr = expression::ExpressionUtil::OperatorFactory(
          EXPRESSION_TYPE_OPERATOR_PLUS, sum_expr, column_expr);
    }
  }

  planner::ProjectInfo::Target target = std::make_pair(0, sum_expr);
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
  Value insert_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);

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
  planner::InsertPlan insert_node(hyadapt_table.get(), std::move(project_info),
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
      hyadapt_table.get(), left_table_predicate, column_ids);
  planner::SeqScanPlan right_table_seq_scan_node(
      hyadapt_table.get(), right_table_predicate, column_ids);

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

void RunSubsetTest(SubsetType subset_test_type, double fraction,
                   int peloton_num_group) {
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

  switch (subset_test_type) {
    case SUBSET_TYPE_SINGLE_GROUP: {
      oid_t column_count = state.projectivity * state.column_count * fraction;

      for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
        column_ids.push_back(hyadapt_column_ids[col_itr]);
      }
    } break;

    case SUBSET_TYPE_MULTIPLE_GROUP: {
      oid_t column_count = state.projectivity * state.column_count;
      oid_t column_proj = column_count * fraction;
      oid_t tile_column_count = column_count / peloton_num_group;
      oid_t tile_column_proj = column_proj / peloton_num_group;

      for (int tile_group_itr = 0; tile_group_itr < peloton_num_group;
           tile_group_itr++) {
        oid_t column_offset = tile_group_itr * tile_column_count;

        for (oid_t col_itr = 0; col_itr < tile_column_proj; col_itr++) {
          column_ids.push_back(hyadapt_column_ids[column_offset + col_itr]);
        }
      }

    } break;

    case SUBSET_TYPE_INVALID:
    default:
      LOG_ERROR("Unsupported subset experiment type : %d", subset_test_type);
      break;
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

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
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  // Not going to use these stats
  double cost = 0;
  std::vector<double> columns_accessed;

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
  Value insert_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;
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
  planner::InsertPlan insert_node(hyadapt_table.get(), std::move(project_info),
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
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  // Update
  std::vector<Value> values;

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(hyadapt_table.get(), std::move(project_info));

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

std::vector<oid_t> vertical_tuples_per_tilegroup = {10, 100, 1000, 10000,
                                                    100000};

void RunVerticalExperiment() {
  // Cache the original value
  auto orig_tuples_per_tilegroup = state.tuples_per_tilegroup;
  auto orig_tuple_count = state.tuples_per_tilegroup * orig_scale_factor;

  state.projectivity = 0.1;
  peloton_projectivity = state.projectivity;
  state.layout_mode = LAYOUT_HYBRID;
  peloton_layout_mode = state.layout_mode;

  // Go over all column counts
  for (auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for (auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      for (auto select : selectivity) {
        // Set selectivity
        state.selectivity = select;

        for (auto tuples_per_tg : vertical_tuples_per_tilegroup) {
          // Set tuples per tilegroup and scale factor
          state.tuples_per_tilegroup = tuples_per_tg;
          state.scale_factor = orig_tuple_count / tuples_per_tg;

          // Load in the table with layout
          CreateAndLoadTable((LayoutType)peloton_layout_mode);

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest();
        }
      }
    }
  }

  // Reset
  state.tuples_per_tilegroup = orig_tuples_per_tilegroup;
  state.scale_factor = orig_scale_factor;

  out.close();
}

std::vector<double> subset_ratios = {0.2, 0.4, 0.6, 0.8, 1.0};

std::vector<oid_t> access_num_groups = {1, 2, 4, 8, 16};

void RunSubsetExperiment() {
  state.projectivity = 1.0;
  peloton_projectivity = state.projectivity;

  state.column_count = column_counts[1];

  // Generate sequence
  GenerateSequence(state.column_count);

  state.write_ratio = 0.0;

  state.layout_mode = LAYOUT_HYBRID;
  peloton_layout_mode = state.layout_mode;

  /////////////////////////////////////////////////////////
  // SINGLE GROUP
  /////////////////////////////////////////////////////////

  state.subset_experiment_type = SUBSET_TYPE_SINGLE_GROUP;

  // Load in the table with layout
  CreateAndLoadTable((LayoutType)peloton_layout_mode);

  for (auto select : selectivity) {
    // Set selectivity
    state.selectivity = select;

    for (auto subset_ratio : subset_ratios) {
      state.subset_ratio = subset_ratio;

      // Go over all ops
      state.operator_type = OPERATOR_TYPE_DIRECT;
      RunSubsetTest(SUBSET_TYPE_SINGLE_GROUP, subset_ratio, 0);
    }
  }

  /////////////////////////////////////////////////////////
  // MULTIPLE GROUPS
  /////////////////////////////////////////////////////////

  state.subset_experiment_type = SUBSET_TYPE_MULTIPLE_GROUP;

  // Across multiple groups
  peloton_num_groups = 5;
  auto subset_ratio = subset_ratios[0];

  state.subset_ratio = subset_ratio;

  state.projectivity = 0.8;
  peloton_projectivity = state.projectivity;

  // Load in the table with layout
  CreateAndLoadTable((LayoutType)peloton_layout_mode);

  for (auto select : selectivity) {
    // Set selectivity
    state.selectivity = select;

    for (auto access_num_group : access_num_groups) {
      state.access_num_groups = access_num_group;

      // Go over all ops
      state.operator_type = OPERATOR_TYPE_DIRECT;
      RunSubsetTest(SUBSET_TYPE_MULTIPLE_GROUP, subset_ratio, access_num_group);
    }
  }

  // Reset
  peloton_num_groups = 0;
  state.access_num_groups = 1;
  state.subset_ratio = 1.0;
  state.subset_experiment_type = SUBSET_TYPE_INVALID;

  out.close();
}

static std::map<oid_t, oid_t> GetColumnMapStats(
    const peloton::storage::column_map_type &column_map) {
  std::map<oid_t, oid_t> column_map_stats;

  // Cluster per-tile column count
  for (auto entry : column_map) {
    auto tile_id = entry.second.first;
    auto column_map_itr = column_map_stats.find(tile_id);
    if (column_map_itr == column_map_stats.end())
      column_map_stats[tile_id] = 1;
    else
      column_map_stats[tile_id]++;
  }

  return std::move(column_map_stats);
}

static void CollectColumnMapStats() {
  std::map<std::map<oid_t, oid_t>, oid_t> col_map_stats_summary;

  // Go over all tg's
  auto tile_group_count = hyadapt_table->GetTileGroupCount();
  LOG_TRACE("TG Count :: %lu", tile_group_count);

  for (size_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = hyadapt_table->GetTileGroup(tile_group_itr);
    auto col_map = tile_group->GetColumnMap();

    // Get stats
    auto col_map_stats = GetColumnMapStats(col_map);

    // Compare stats
    bool found = false;
    for (auto col_map_stats_summary_itr : col_map_stats_summary) {
      // Compare types
      auto col_map_stats_size = col_map_stats.size();
      auto entry = col_map_stats_summary_itr.first;
      auto entry_size = entry.size();

      // Compare sizes
      if (col_map_stats_size != entry_size) continue;

      // Compare entries
      bool match = true;
      for (size_t entry_itr = 0; entry_itr < entry_size; entry_itr++) {
        if (entry[entry_itr] != col_map_stats[entry_itr]) {
          match = false;
          break;
        }
      }
      if (match == false) continue;

      // Match found
      col_map_stats_summary[col_map_stats]++;
      found = true;
      break;
    }

    // Add new type if not found
    if (found == false) col_map_stats_summary[col_map_stats] = 1;
  }

  oid_t type_itr = 0;
  oid_t type_cnt = 5;
  for (auto col_map_stats_summary_entry : col_map_stats_summary) {
    out << query_itr << " " << type_itr << " "
        << col_map_stats_summary_entry.second << "\n";
    type_itr++;
  }

  // Fillers for other types
  for (; type_itr < type_cnt; type_itr++)
    out << query_itr << " " << type_itr << " " << 0 << "\n";

  out.flush();
}

static void Transform(double theta) {
  // Get column map
  auto table_name = hyadapt_table->GetName();

  peloton_projectivity = state.projectivity;

  // TODO: Update period ?
  oid_t update_period = 10;
  oid_t update_itr = 0;

  // Transform
  while (state.fsm == true) {
    auto tile_group_count = hyadapt_table->GetTileGroupCount();
    auto tile_group_offset = rand() % tile_group_count;

    hyadapt_table->TransformTileGroup(tile_group_offset, theta);

    // Update partitioning periodically
    update_itr++;
    if (update_itr == update_period) {
      hyadapt_table->UpdateDefaultPartition();
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

      LOG_INFO("----------------------------------------- \n");

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

brain::Sample GetSample(double projectivity) {
  double cost = 10;
  auto col_count = projectivity * state.column_count;
  std::vector<oid_t> columns_accessed;

  // Build columns accessed
  for (auto col_itr = 0; col_itr < col_count; col_itr++)
    columns_accessed.push_back(col_itr);

  // Construct sample
  auto columns_accessed_bitmap = GetColumnsAccessed(columns_accessed);
  brain::Sample sample(columns_accessed_bitmap, cost);

  return std::move(sample);
}

std::vector<double> sample_weights = {0.0001, 0.001, 0.01, 0.1};

void RunWeightExperiment() {
  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.column_count = column_counts[1];
  state.layout_mode = LAYOUT_HYBRID;
  peloton_layout_mode = state.layout_mode;

  state.transactions = 1000;
  oid_t num_types = 10;
  std::vector<brain::Sample> queries;

  // Construct sample
  for (oid_t type_itr = num_types; type_itr >= 1; type_itr--) {
    auto type_proj = type_itr * ((double)1.0 / num_types);
    auto query = GetSample(type_proj);
    queries.push_back(query);
  }

  // Go over all layouts
  for (auto sample_weight : sample_weights) {
    state.sample_weight = sample_weight;

    // Reset query counter
    query_itr = 0;

    // Clusterer
    oid_t cluster_count = 4;

    brain::Clusterer clusterer(cluster_count, state.column_count,
                               sample_weight);

    // Go over all query types
    for (auto query : queries) {
      for (oid_t txn_itr = 0; txn_itr < state.transactions; txn_itr++) {
        // Process sample
        clusterer.ProcessSample(query);

        auto default_partition = clusterer.GetPartitioning(2);

        auto col_map = GetColumnMapStats(default_partition);
        auto split_point = col_map.at(0);
        state.split_point = split_point;

        query_itr++;
        WriteOutput(0);
      }
    }
  }

  // Reset query counter
  state.transactions = orig_transactions;
  query_itr = 0;
}

std::vector<oid_t> tile_group_counts = {1000};

static void RunReorgTest() {
  double direct_low_proj = 0.06;
  double direct_high_proj = 0.3;

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.projectivity = direct_high_proj;
  state.operator_type = OPERATOR_TYPE_ARITHMETIC;
  RunArithmeticTest();

  state.projectivity = direct_low_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.projectivity = direct_high_proj;
  state.operator_type = OPERATOR_TYPE_ARITHMETIC;
  RunArithmeticTest();
}

static void Reorg() {
  // Reorg all the tile groups
  auto tile_group_count = hyadapt_table->GetTileGroupCount();
  double theta = 0.0;

  hyadapt_table->UpdateDefaultPartition();

  for (size_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    hyadapt_table->TransformTileGroup(tile_group_itr, theta);
  }
}

std::vector<LayoutType> reorg_layout_modes = {LAYOUT_ROW, LAYOUT_HYBRID};

void RunReorgExperiment() {
  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.transactions = 25;

  state.write_ratio = 0.0;
  state.selectivity = 1.0;
  state.adapt = true;
  double theta = 0.0;

  state.layout_mode = LAYOUT_HYBRID;
  peloton_layout_mode = state.layout_mode;

  state.column_count = column_counts[1];

  // Generate sequence
  GenerateSequence(state.column_count);

  // Go over all tile_group_counts
  for (auto tile_group_count : tile_group_counts) {
    state.scale_factor = tile_group_count;

    // Go over all layouts
    for (auto layout_mode : reorg_layout_modes) {
      // Set layout
      state.layout_mode = layout_mode;
      peloton_layout_mode = state.layout_mode;

      // Enable reorg mode
      if (state.layout_mode != LAYOUT_HYBRID) {
        state.reorg = true;
      }

      LOG_INFO("----------------------------------------- \n");

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

      RunReorgTest();

      // Stop transformer
      if (state.layout_mode == LAYOUT_HYBRID) {
        state.fsm = false;
        peloton_fsm = false;
        transformer.join();
      }

      // Enable reorg mode
      if (state.layout_mode != LAYOUT_HYBRID) {
        state.reorg = false;
      }
    }
  }

  // Reset
  state.transactions = orig_transactions;
  state.adapt = false;
  query_itr = 0;

  out.close();
}

std::vector<LayoutType> distribution_layout_modes = {LAYOUT_HYBRID};

void RunDistributionExperiment() {
  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.distribution = true;
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
    for (auto layout_mode : distribution_layout_modes) {
      // Set layout
      state.layout_mode = layout_mode;
      peloton_layout_mode = state.layout_mode;

      LOG_INFO("----------------------------------------- \n");

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
  state.distribution = false;
  query_itr = 0;

  out.close();
}

void RunJoinExperiment() {
  state.selectivity = 1.0;
  state.write_ratio = 0;

  // Save old values and scale down
  oid_t old_scale_factor = state.scale_factor;
  oid_t old_tuples_per_tilegroup = state.tuples_per_tilegroup;
  state.scale_factor = 20;
  state.tuples_per_tilegroup = 100;

  // Go over all column counts
  for (auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

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

        state.operator_type = OPERATOR_TYPE_JOIN;
        RunJoinTest();
      }
    }
  }

  // Reset old values
  state.scale_factor = old_scale_factor;
  state.tuples_per_tilegroup = old_tuples_per_tilegroup;

  out.close();
}

void RunInsertExperiment() {
  // Set write ratio
  state.write_ratio = 1.0;

  // Go over all column counts
  for (auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all layouts
    for (auto layout : layouts) {
      // Set layout
      state.layout_mode = layout;
      peloton_layout_mode = state.layout_mode;

      // Load in the table with layout
      CreateAndLoadTable(layout);

      state.operator_type = OPERATOR_TYPE_INSERT;
      RunInsertTest();
    }
  }

  out.close();
}

std::vector<oid_t> version_chain_lengths = {10, 100, 1000, 10000, 10000};

void RunVersionExperiment() {
  oid_t tuple_count = version_chain_lengths.back();
  Timer<> timer;
  double version_chain_travesal_time = 0;

  std::unique_ptr<storage::TileGroupHeader> header(
      new storage::TileGroupHeader(BACKEND_TYPE_MM, tuple_count));

  // Create a version chain
  oid_t block_id = 0;
  header->SetNextItemPointer(0, INVALID_ITEMPOINTER);
  header->SetPrevItemPointer(0, INVALID_ITEMPOINTER);

  for (oid_t tuple_itr = 1; tuple_itr < tuple_count; tuple_itr++) {
    header->SetNextItemPointer(tuple_itr, ItemPointer(block_id, tuple_itr - 1));
    header->SetPrevItemPointer(tuple_itr - 1, ItemPointer(block_id, tuple_itr));
  }

  timer.Start();

  // Traverse the version chain
  for (auto version_chain_length : version_chain_lengths) {
    oid_t starting_tuple_offset = version_chain_length - 1;
    oid_t prev_tuple_offset = starting_tuple_offset;
    LOG_INFO("Offset : %lu", starting_tuple_offset);

    auto prev_item_pointer = header->GetNextItemPointer(starting_tuple_offset);
    while (prev_item_pointer.block != INVALID_OID) {
      prev_tuple_offset = prev_item_pointer.offset;
      prev_item_pointer = header->GetNextItemPointer(prev_tuple_offset);
    }

    timer.Stop();
    version_chain_travesal_time = timer.GetDuration();

    WriteOutput(version_chain_travesal_time);
  }

  out.close();
}

std::vector<LayoutType> hyrise_layouts = {LAYOUT_HYBRID, LAYOUT_ROW};

std::vector<oid_t> hyrise_column_counts = {50};

std::vector<double> hyrise_projectivities = {0.9, 0.04, 0.9, 0.04};

static void RunHyriseTest() {
  for (auto hyrise_projectivity : hyrise_projectivities) {
    state.projectivity = hyrise_projectivity;
    peloton_projectivity = state.projectivity;
    state.operator_type = OPERATOR_TYPE_DIRECT;
    RunDirectTest();
  }
}

void RunHyriseExperiment() {
  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.transactions = 100;

  state.write_ratio = 0.0;
  state.selectivity = 1.0;
  state.adapt = true;
  double theta = 0.0;

  // Go over all column counts
  for (auto column_count : hyrise_column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all layouts
    oid_t layout_itr = 0;
    // layout itr == 0 => HYBRID
    // layout itr == 1 => HYRISE
    for (auto layout : hyrise_layouts) {
      // Set layout
      state.layout_mode = layout;
      peloton_layout_mode = state.layout_mode;

      LOG_INFO("----------------------------------------- \n");

      state.projectivity = hyrise_projectivities[0];
      peloton_projectivity = state.projectivity;
      // HYPER
      if (layout == LAYOUT_COLUMN) {
        CreateAndLoadTable((LayoutType)LAYOUT_COLUMN);
      }
      // HYRISE and HYBRID
      else {
        CreateAndLoadTable((LayoutType)LAYOUT_HYBRID);
      }

      // Reset query counter
      query_itr = 0;

      // Launch transformer
      if (state.layout_mode == LAYOUT_HYBRID) {
        state.fsm = true;
        peloton_fsm = true;
        transformer = std::thread(Transform, theta);
      }

      RunHyriseTest();

      // Stop transformer
      if (state.layout_mode == LAYOUT_HYBRID) {
        state.fsm = false;
        peloton_fsm = false;
        transformer.join();
      }

      // Update layout itr
      layout_itr++;
    }
  }

  // Reset
  state.transactions = orig_transactions;
  state.adapt = false;
  query_itr = 0;

  out.close();
}

oid_t scan_ctr = 0;
oid_t insert_ctr = 0;

static void ExecuteConcurrentTest(std::vector<executor::AbstractExecutor *> &executors,
                                  oid_t thread_id, oid_t num_threads,
                                  double scan_ratio) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dis(0, 1);
  Timer<> timer;

  auto txn_count = state.transactions;
  bool status = false;

  timer.Start();

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {
    // Increment query counter
    query_itr++;

    auto dis_sample = dis(gen);
    executor::AbstractExecutor *executor = nullptr;

    // SCAN
    if (dis_sample < scan_ratio) {
      executor = executors[0];
      scan_ctr++;
    }
    // INSERT
    else {
      executor = executors[1];
      insert_ctr++;
    }

    // Run the selected executor
    status = executor->Init();
    if (status == false) throw Exception("Init failed");

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

    while (executor->Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }

    // Execute stuff
    executor->Execute();
  }

  timer.Stop();
  double time_per_transaction = timer.GetDuration() / txn_count;

  if (thread_id == 0) {
    double throughput = (double)num_threads / time_per_transaction;
    WriteOutput(throughput / 1000.0);
  }
}

void RunConcurrentTest(oid_t thread_id, oid_t num_threads, double scan_ratio) {
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
  planner::SeqScanPlan seq_scan_node(hyadapt_table.get(), predicate, column_ids);

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
  Value insert_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression =
        expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  planner::InsertPlan insert_node(hyadapt_table.get(), std::move(project_info));
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  ExecuteConcurrentTest(executors, thread_id, num_threads, scan_ratio);

  txn_manager.CommitTransaction();
}

std::vector<oid_t> num_threads_list = {1, 2, 4, 8, 16, 32};

std::vector<double> scan_ratios = {0, 0.5, 0.9, 1.0};

void RunConcurrencyExperiment() {
  state.selectivity = 0.001;
  state.operator_type = OPERATOR_TYPE_INSERT;

  // Set proj
  state.projectivity = 0.1;
  peloton_projectivity = state.projectivity;

  // Go over all scan ratios
  for (auto scan_ratio : scan_ratios) {
    LOG_INFO("SCAN RATIO : %lf \n\n", scan_ratio);

    // Go over all layouts
    for (auto layout : layouts) {
      // Set layout
      state.layout_mode = layout;
      peloton_layout_mode = state.layout_mode;

      LOG_INFO("LAYOUT : %d", layout);

      // Go over all scale factors
      for (auto num_threads : num_threads_list) {
        // Reuse variables
        state.theta = scan_ratio;
        state.sample_weight = num_threads;

        // Reset
        scan_ctr = 0;
        insert_ctr = 0;

        // Load in the table with layout
        CreateAndLoadTable(state.layout_mode);

        // Set up thread group
        std::vector<std::thread> thread_group;

        auto initial_tg_count = hyadapt_table->GetTileGroupCount();

        // Launch a group of threads
        for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
          thread_group.push_back(std::thread(RunConcurrentTest, thread_itr,
                                             num_threads, scan_ratio));
        }

        // Join the threads with the main thread
        for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
          thread_group[thread_itr].join();
        }

        auto final_tg_count = hyadapt_table->GetTileGroupCount();
        auto diff_tg_count = final_tg_count - initial_tg_count;

        LOG_INFO("Inserted Tile Group Count : %lu", diff_tg_count);

        LOG_INFO("Scan count  : %lu", scan_ctr);
        LOG_INFO("Insert count  : %lu", insert_ctr);
      }
    }
  }
}

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
