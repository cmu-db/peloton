//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/hyadapt/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>

#include "backend/benchmark/hyadapt/loader.h"
#include "backend/benchmark/hyadapt/workload.h"
#include "backend/brain/clusterer.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/concurrency/transaction.h"

#include "backend/executor/abstract_executor.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/projection_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/update_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/index/index_factory.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/projection_plan.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

// Tuple id counter
oid_t hyadapt_tuple_counter = -1000000;

expression::AbstractExpression *CreatePredicate( const int lower_bound) {

  // ATTR0 >= LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::TupleValueFactory(0, 0);

  // Second, create constant value expression.
  Value constant_value = ValueFactory::GetIntegerValue(lower_bound);

  expression::AbstractExpression *constant_value_expr =
      expression::ConstantValueFactory(constant_value);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate =
      expression::ComparisonFactory(EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
                                    tuple_value_expr,
                                    constant_value_expr);

  constant_value.Free();

  return predicate;
}

std::ofstream out("outputfile.summary");

oid_t query_itr;

static void WriteOutput(double duration) {

  // Convert to ms
  duration *= 1000;

  std::cout << "----------------------------------------------------------\n";
  std::cout << state.layout << " "
      << state.operator_type << " "
      << state.projectivity << " "
      << state.selectivity << " "
      << state.write_ratio << " "
      << state.scale_factor << " "
      << state.column_count << " "
      << state.subset_experiment_type << " "
      << state.access_num_groups << " "
      << state.subset_ratio << " "
      << state.theta << " "
      << state.split_point << " "
      << state.sample_weight << " "
      << state.tuples_per_tilegroup << " :: ";
  std::cout << duration << " ms\n";

  out << state.layout << " ";
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
  out << duration << "\n";
  out.flush();

}

static int GetLowerBound() {
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  const int lower_bound = (1 - state.selectivity) * tuple_count;

  return lower_bound;
}

static void ExecuteTest(std::vector<executor::AbstractExecutor*>& executors,
                        std::vector<double> columns_accessed,
                        double cost) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  auto txn_count = state.transactions;
  bool status = false;

  start = std::chrono::system_clock::now();

  // Construct sample
  brain::Sample sample(columns_accessed, cost);

  // Run these many transactions
  for(oid_t txn_itr = 0 ; txn_itr < txn_count ; txn_itr++) {

    // Increment query counter
    query_itr++;

    // Run all the executors
    for(auto executor : executors) {

      status = executor->Init();
      assert(status == true);

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

      while(executor->Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
        result_tiles.emplace_back(result_tile.release());
      }

      status = executor->Execute();
      assert(status == false);

    }

    // Capture fine-grained stats in adapt experiment
    if(state.adapt == true) {
      end = std::chrono::system_clock::now();
      std::chrono::duration<double> elapsed_seconds = end-start;
      double time_per_transaction = ((double)elapsed_seconds.count());

      WriteOutput(time_per_transaction);

      // Record sample
      if(state.fsm == true && cost != 0) {
        hyadapt_table->RecordSample(sample);
      }

      start = std::chrono::system_clock::now();
    }
  }

  if(state.adapt == false) {
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    double time_per_transaction = ((double)elapsed_seconds.count())/txn_count;

    WriteOutput(time_per_transaction);
  }
}

std::vector<double> GetColumnsAccessed(const std::vector<oid_t>& column_ids) {
  std::vector<double> columns_accessed;
  std::map<oid_t, oid_t> columns_accessed_map;

  // Init map
  for(auto col : column_ids)
    columns_accessed_map[col] = 1;

  for(int column_itr = 0 ; column_itr < state.column_count; column_itr++){
    auto location = columns_accessed_map.find(column_itr);
    auto end = columns_accessed_map.end();
    if(location != end)
      columns_accessed.push_back(1);
    else
      columns_accessed.push_back(0);
  }

  return columns_accessed;
}

void RunDirectTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.projectivity * state.column_count;

  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema.release(),
                                        physify_flag);

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
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(hyadapt_table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}

void RunAggregateTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

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
  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table, predicate, column_ids);

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
  oid_t tuple_idx = 1; // tuple2
  for (col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.push_back({col_itr, {tuple_idx, col_itr} });
    col_itr++;
  }

  auto proj_info = new planner::ProjectInfo(planner::ProjectInfo::TargetList(),
                                            std::move(direct_map_list));

  // 3) Set up aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  for (auto column_id : column_ids) {
    planner::AggregatePlan::AggTerm max_column_agg(
        EXPRESSION_TYPE_AGGREGATE_MAX,
        expression::TupleValueFactory(0, column_id),
        false);
    agg_terms.push_back(max_column_agg);
  }

  // 4) Set up predicate (empty)
  expression::AbstractExpression* aggregate_predicate = nullptr;

  // 5) Create output table schema
  auto data_table_schema = hyadapt_table->GetSchema();
  std::vector<catalog::Column> columns;
  for (auto column_id : column_ids) {
    columns.push_back(data_table_schema->GetColumn(column_id));
  }
  auto output_table_schema = new catalog::Schema(columns);

  // OK) Create the plan node
  planner::AggregatePlan aggregation_node(proj_info, aggregate_predicate,
                                          std::move(agg_terms),
                                          std::move(group_by_columns),
                                          output_table_schema,
                                          AGGREGATE_TYPE_PLAIN);

  executor::AggregateExecutor aggregation_executor(&aggregation_node, context.get());
  aggregation_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  col_itr = 0;
  for(auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "MAX " + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema.release(),
                                        physify_flag);

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
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(hyadapt_table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}

void RunArithmeticTest() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

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
  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table, predicate, column_ids);

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
  auto projection_schema = new catalog::Schema(columns);

  // target list
  expression::AbstractExpression *sum_expr = nullptr;
  oid_t projection_column_count = state.projectivity * state.column_count;

  // Resize column ids to contain only columns over which we evaluate the expression
  column_ids.resize(projection_column_count);

  for(oid_t col_itr = 0 ; col_itr < projection_column_count; col_itr++) {
    auto hyadapt_colum_id = hyadapt_column_ids[col_itr];
    auto column_expr = expression::TupleValueFactory(0, hyadapt_colum_id);
    if(sum_expr == nullptr)
      sum_expr = column_expr;
    else {
      sum_expr = expression::OperatorFactory(
          EXPRESSION_TYPE_OPERATOR_PLUS, sum_expr, column_expr);
    }
  }

  planner::ProjectInfo::Target target = std::make_pair(0, sum_expr);
  target_list.push_back(target);

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::ProjectionPlan node(project_info, projection_schema);

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

  auto column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "SUM", is_inlined);
  output_columns.push_back(column);
  old_to_new_cols[col_itr] = col_itr;

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema.release(),
                                        physify_flag);

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
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(hyadapt_table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 10;
  column_ids.push_back(0);
  auto columns_accessed = GetColumnsAccessed(column_ids);

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}


void RunSubsetTest(SubsetType subset_test_type, double fraction, int peloton_num_group) {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;

  switch(subset_test_type) {

    case SUBSET_TYPE_SINGLE_GROUP:
    {
      oid_t column_count = state.projectivity * state.column_count * fraction;

      for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
        column_ids.push_back(hyadapt_column_ids[col_itr]);
      }
    }
    break;

    case SUBSET_TYPE_MULTIPLE_GROUP:
    {
      oid_t column_count = state.projectivity * state.column_count;
      oid_t column_proj = column_count * fraction;
      oid_t tile_column_count = column_count / peloton_num_group;
      oid_t tile_column_proj = column_proj / peloton_num_group;

      for(int tile_group_itr = 0 ; tile_group_itr < peloton_num_group ; tile_group_itr++) {
        oid_t column_offset = tile_group_itr * tile_column_count;

        for(oid_t col_itr = 0 ; col_itr < tile_column_proj; col_itr++) {
          column_ids.push_back(hyadapt_column_ids[column_offset + col_itr]);
        }
      }

    }
    break;

    case SUBSET_TYPE_INVALID:
    default:
      std::cout << "Unsupported subset experiment type : " << subset_test_type << "\n";
      break;
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema.release(),
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  // Not going to use these stats
  double cost = 0;
  std::vector<double> columns_accessed;

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}

void RunInsertTest() {

  auto &txn_manager = concurrency::TransactionManager::GetInstance();

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
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
    column_ids.push_back(col_id);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;

  planner::InsertPlan insert_node(hyadapt_table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&insert_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 0;
  std::vector<double> columns_accessed;

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}

void RunUpdateTest() {
  const int lower_bound = GetLowerBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

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
  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(hyadapt_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  // Update
  std::vector<Value> values;
  Value update_val = ValueFactory::GetIntegerValue(++hyadapt_tuple_counter);

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));
  planner::UpdatePlan update_node(hyadapt_table, project_info);

  executor::UpdateExecutor update_executor(&update_node, context.get());

  update_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&update_executor);

  /////////////////////////////////////////////////////////
  // COLLECT STATS
  /////////////////////////////////////////////////////////
  double cost = 0;
  std::vector<double> columns_accessed;

  ExecuteTest(executors, columns_accessed, cost);

  txn_manager.CommitTransaction(txn);
}

/////////////////////////////////////////////////////////
// EXPERIMENTS
/////////////////////////////////////////////////////////

std::vector<oid_t> column_counts = {50, 200};

std::vector<double> write_ratios = {0, 0.1};

std::vector<LayoutType> layouts = { LAYOUT_ROW, LAYOUT_COLUMN, LAYOUT_HYBRID};

std::vector<OperatorType> operators = { OPERATOR_TYPE_DIRECT, OPERATOR_TYPE_AGGREGATE, OPERATOR_TYPE_ARITHMETIC};

std::vector<double> selectivity = {0.2, 0.4, 0.6, 0.8, 1.0};

std::vector<double> projectivity = {0.02, 0.1, 0.5, 1.0};

void RunProjectivityExperiment() {

  state.selectivity = 1.0;

  // Go over all column counts
  for(auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for(auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      // Go over all layouts
      for(auto layout : layouts) {
        // Set layout
        state.layout = layout;
        peloton_layout = state.layout;

        for(auto proj : projectivity) {
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

          state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          RunArithmeticTest();
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
  for(auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for(auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      // Go over all layouts
      for(auto layout : layouts) {
        // Set layout
        state.layout = layout;
        peloton_layout = state.layout;

        for(auto select : selectivity) {
          // Set selectivity
          state.selectivity = select;

          // Load in the table with layout
          CreateAndLoadTable(layout);

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest();

          state.operator_type = OPERATOR_TYPE_AGGREGATE;
          RunAggregateTest();

          state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          RunArithmeticTest();
        }

      }

    }

  }

  out.close();
}

int op_column_count = 100;

std::vector<double> op_projectivity = {0.01, 0.1, 1.0};

std::vector<double> op_selectivity = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

void RunOperatorExperiment() {

  state.column_count = op_column_count;

  // Generate sequence
  GenerateSequence(state.column_count);

  // Go over all write ratios
  for(auto write_ratio : write_ratios) {
    state.write_ratio = write_ratio;

    // Go over all layouts
    for(auto layout : layouts) {
      // Set layout
      state.layout = layout;
      peloton_layout = state.layout;

      for(auto projectivity : op_projectivity) {
        // Set projectivity
        state.projectivity = projectivity;
        peloton_projectivity = state.projectivity;

        for(auto selectivity : op_selectivity) {
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

std::vector<oid_t> vertical_tuples_per_tilegroup = {10, 100, 1000, 10000, 100000};


void RunVerticalExperiment() {

  // Cache the original value
  auto orig_tuples_per_tilegroup = state.tuples_per_tilegroup;
  auto orig_tuple_count = state.tuples_per_tilegroup * orig_scale_factor;

  state.projectivity = 0.1;
  peloton_projectivity = state.projectivity;
  state.layout = LAYOUT_HYBRID;
  peloton_layout = state.layout;

  // Go over all column counts
  for(auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all write ratios
    for(auto write_ratio : write_ratios) {
      state.write_ratio = write_ratio;

      for(auto select : selectivity) {
        // Set selectivity
        state.selectivity = select;

        for(auto tuples_per_tg : vertical_tuples_per_tilegroup) {
          // Set tuples per tilegroup and scale factor
          state.tuples_per_tilegroup = tuples_per_tg;
          state.scale_factor = orig_tuple_count/tuples_per_tg;

          // Load in the table with layout
          CreateAndLoadTable((LayoutType) peloton_layout);

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

  state.layout = LAYOUT_HYBRID;
  peloton_layout = state.layout;

  /////////////////////////////////////////////////////////
  // SINGLE GROUP
  /////////////////////////////////////////////////////////

  state.subset_experiment_type = SUBSET_TYPE_SINGLE_GROUP;

  // Load in the table with layout
  CreateAndLoadTable((LayoutType) peloton_layout);

  for(auto select : selectivity) {
    // Set selectivity
    state.selectivity = select;

    for(auto subset_ratio : subset_ratios) {
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
  CreateAndLoadTable((LayoutType) peloton_layout);

  for(auto select : selectivity) {
    // Set selectivity
    state.selectivity = select;

    for(auto access_num_group : access_num_groups) {
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
    const peloton::storage::column_map_type& column_map) {
  std::map<oid_t, oid_t> column_map_stats;

  // Cluster per-tile column count
  for(auto entry : column_map){
    auto tile_id = entry.second.first;
    auto column_map_itr = column_map_stats.find(tile_id);
    if(column_map_itr == column_map_stats.end())
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
  std::cout << "TG Count :: " << tile_group_count << "\n";


  for(auto tile_group_itr = 0; tile_group_itr < tile_group_count; tile_group_itr++){

    auto tile_group = hyadapt_table->GetTileGroup(tile_group_itr);
    auto col_map = tile_group->GetColumnMap();

    // Get stats
    auto col_map_stats = GetColumnMapStats(col_map);

    // Compare stats
    bool found = false;
    for(auto col_map_stats_summary_itr : col_map_stats_summary) {

      // Compare types
      auto col_map_stats_size = col_map_stats.size();
      auto entry = col_map_stats_summary_itr.first;
      auto entry_size = entry.size();

      // Compare sizes
      if(col_map_stats_size != entry_size)
        continue;

      // Compare entries
      bool match = true;
      for(auto entry_itr = 0; entry_itr < entry_size; entry_itr++) {
        if(entry[entry_itr] != col_map_stats[entry_itr]) {
          match = false;
          break;
        }
      }
      if(match == false)
        continue;

      // Match found
      col_map_stats_summary[col_map_stats]++;
      found = true;
      break;
    }

    // Add new type if not found
    if(found == false)
      col_map_stats_summary[col_map_stats] = 1;

  }

  oid_t type_itr = 0;
  for(auto col_map_stats_summary_entry : col_map_stats_summary) {
    // First, print col map stats
    std::cout << "Type " << type_itr << " -- ";

    for(auto col_stats_itr : col_map_stats_summary_entry.first)
      std::cout << col_stats_itr.first << " " << col_stats_itr.second << " :: ";

    // Next, print the normalized count
    std::cout << col_map_stats_summary_entry.second << "\n";
    type_itr++;
  }



}

static void Transform(double theta) {

  // Get column map
  auto table_name = hyadapt_table->GetName();
  auto column_count = hyadapt_table->GetSchema()->GetColumnCount();

  peloton_projectivity = state.projectivity;

  // TODO: Update period ?
  oid_t update_period = 10;
  oid_t update_itr = 0;

  // Transform
  while(state.fsm == true) {
    auto tile_group_count = hyadapt_table->GetTileGroupCount();
    auto tile_group_offset = rand() % tile_group_count;

    hyadapt_table->TransformTileGroup(tile_group_offset, theta);

    // Update partitioning periodically
    update_itr++;
    if(update_itr == update_period) {
      hyadapt_table->UpdateDefaultPartition();
      update_itr = 0;
    }

  }

}

static void RunAdaptTest() {
  double direct_low_proj = 0.06;
  double direct_high_proj = 0.7;
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

  state.projectivity = direct_high_proj;
  state.operator_type = OPERATOR_TYPE_DIRECT;
  RunDirectTest();

  state.write_ratio = insert_write_ratio;
  state.operator_type = OPERATOR_TYPE_INSERT;
  RunInsertTest();
  state.write_ratio = 0.0;

  state.projectivity = direct_high_proj;
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

std::vector<LayoutType> adapt_layouts = {  LAYOUT_ROW, LAYOUT_COLUMN, LAYOUT_HYBRID};

void RunAdaptExperiment() {

  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.transactions = 25;

  state.write_ratio = 0.0;
  state.selectivity = 1.0;
  state.adapt = true;
  double theta = 0.0;

  // Go over all column counts
  for(auto column_count : column_counts) {
    state.column_count = column_count;

    // Generate sequence
    GenerateSequence(state.column_count);

    // Go over all layouts
    for(auto layout : adapt_layouts) {
      // Set layout
      state.layout = layout;
      peloton_layout = state.layout;

      std::cout << "----------------------------------------- \n\n";

      state.projectivity = 1.0;
      peloton_projectivity = 1.0;
      CreateAndLoadTable((LayoutType) peloton_layout);

      // Reset query counter
      query_itr = 0;

      // Launch transformer
      if(state.layout == LAYOUT_HYBRID) {
        state.fsm = true;
        peloton_fsm = true;
        transformer = std::thread(Transform, theta);
      }

      RunAdaptTest();

      // Stop transformer
      if(state.layout == LAYOUT_HYBRID) {
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
  for(auto col_itr = 0; col_itr < col_count ; col_itr++)
    columns_accessed.push_back(col_itr);

  // Construct sample
  auto columns_accessed_bitmap = GetColumnsAccessed(columns_accessed);
  brain::Sample sample(columns_accessed_bitmap, cost);

  return std::move(sample);
}

std::vector<double> sample_weights = {0.001, 0.01, 0.1};

void RunWeightExperiment() {

  auto orig_transactions = state.transactions;
  std::thread transformer;

  state.column_count = column_counts[1];
  state.layout = LAYOUT_HYBRID;
  peloton_layout = state.layout;

  state.transactions = 20;
  oid_t num_types = 10;
  std::vector<brain::Sample> queries;

  // Construct sample
  for(oid_t type_itr = 1; type_itr <= num_types; type_itr++) {
    auto type_proj = type_itr * ((double) 1.0/num_types);
    auto query = GetSample(type_proj);
    queries.push_back(query);
  }

  // Go over all layouts
  for(auto sample_weight : sample_weights) {
    state.sample_weight = sample_weight;

    // Reset query counter
    query_itr = 0;

    // Clusterer
    oid_t cluster_count = 4;

    brain::Clusterer clusterer(cluster_count,
                               state.column_count,
                               sample_weight);

    // Go over all query types
    for(auto query : queries) {

      for(oid_t txn_itr = 0 ; txn_itr < state.transactions; txn_itr++) {
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

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
