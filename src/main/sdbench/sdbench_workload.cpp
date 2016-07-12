//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_workload.cpp
//
// Identification: src/main/sdbench/sdbench_workload.cpp
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
#include <chrono>

#include "expression/expression_util.h"
#include "brain/layout_tuner.h"
#include "brain/sample.h"

#include "benchmark/sdbench/sdbench_workload.h"
#include "benchmark/sdbench/sdbench_loader.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/aggregate_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/projection_executor.h"
#include "executor/insert_executor.h"
#include "executor/update_executor.h"
#include "executor/nested_loop_join_executor.h"
#include "executor/hybrid_scan_executor.h"

#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"

#include "index/index_factory.h"

#include "planner/abstract_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/materialization_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/projection_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/hybrid_scan_plan.h"

#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

// Tuple id counter
oid_t sdbench_tuple_counter = -1000000;

std::vector<oid_t> column_counts = {50, 500};

expression::AbstractExpression *CreatePredicate(const int tuple_start_offset,
                                                const int tuple_end_offset) {
  // ATTR0 >= LOWER_BOUND && < UPPER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr_left =
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0);

  // Second, create constant value expression.
  Value constant_value_left = ValueFactory::GetIntegerValue(tuple_start_offset);

  expression::AbstractExpression *constant_value_expr_left =
      expression::ExpressionUtil::ConstantValueFactory(constant_value_left);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate_left =
      expression::ExpressionUtil::ComparisonFactory(
          EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
          tuple_value_expr_left,
          constant_value_expr_left);

  expression::AbstractExpression *tuple_value_expr_right =
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0);

  Value constant_value_right = ValueFactory::GetIntegerValue(tuple_end_offset);

  expression::AbstractExpression *constant_value_expr_right =
      expression::ExpressionUtil::ConstantValueFactory(constant_value_right);

  expression::AbstractExpression *predicate_right =
      expression::ExpressionUtil::ComparisonFactory(
          EXPRESSION_TYPE_COMPARE_LESSTHAN,
          tuple_value_expr_right,
          constant_value_expr_right);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                                     predicate_left,
                                                     predicate_right);

  return predicate;
}

void CreateIndexScanPredicate(std::vector<oid_t>& key_column_ids,
                              std::vector<ExpressionType>& expr_types,
                              std::vector<Value>& values,
                              const int tuple_start_offset,
                              const int tuple_end_offset) {
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO);
  values.push_back(ValueFactory::GetIntegerValue(tuple_start_offset));

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHAN);
  values.push_back(ValueFactory::GetIntegerValue(tuple_end_offset));
}

std::ofstream out("outputfile.summary");

oid_t query_itr;

static void WriteOutput(double duration) {
  // Convert to ms
  duration *= 1000;

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %.3lf %.3lf %.1lf %d %d %d :: %.1lf ms",
           state.layout_mode,
           state.operator_type,
           state.selectivity,
           state.projectivity,
           state.write_ratio,
           state.scale_factor,
           state.column_count,
           state.tuples_per_tilegroup,
           duration);

  auto timestamp = std::time(nullptr);

  out << timestamp << " ";
  out << state.layout_mode << " ";
  out << state.operator_type << " ";
  out << state.selectivity << " ";
  out << state.projectivity << " ";
  out << query_itr << " ";
  out << state.write_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << " ";
  out << state.tuples_per_tilegroup << " ";
  out << duration << "\n";

  out.flush();
}

static int GetLowerBound() {
  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  int predicate_offset = 0.1 * tuple_count;

  LOG_TRACE("Tuple count : %d", tuple_count);

  int lower_bound = predicate_offset;
  return lower_bound;
}

static int GetUpperBound() {
  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  int selected_tuple_count = state.selectivity * tuple_count;
  int predicate_offset = 0.1 * tuple_count;

  int upper_bound = predicate_offset + selected_tuple_count;
  return upper_bound;
}

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors,
                        std::vector<double> columns_accessed, double cost) {
  Timer<> timer;

  auto txn_count = state.transactions;
  bool status = false;

  // Construct sample
  brain::Sample sample(columns_accessed, cost);

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {
    // Increment query counter
    query_itr++;

    // Reset timer
    timer.Reset();
    timer.Start();

    // Run all the executors
    for (auto executor : executors) {
      status = executor->Init();
      if (status == false) {
        throw Exception("Init failed");
      }

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

      while (executor->Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_tile(
            executor->GetOutput());
        result_tiles.emplace_back(result_tile.release());
      }

      // Execute stuff
      executor->Execute();
    }

    // Emit time
    timer.Stop();
    auto time_per_transaction = timer.GetDuration();

    // Record sample
    if (state.fsm == true && cost != 0) {
      sdbench_table->RecordSample(sample);
    }

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
  const int upper_bound = GetUpperBound();

  LOG_TRACE("Lower bound : %d", lower_bound);
  LOG_TRACE("Upper bound : %d", upper_bound);

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
    column_ids.push_back(sdbench_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound, upper_bound);

  auto index_count = sdbench_table->GetIndexCount();
  index::Index *index = nullptr;
  planner::IndexScanPlan::IndexScanDesc index_scan_desc;

  // Check if ad-hoc index exists
  if(index_count != 0) {

    index = sdbench_table->GetIndex(0);

    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    std::vector<Value> values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    CreateIndexScanPredicate(key_column_ids, expr_types, values,
                             lower_bound, upper_bound);

    index_scan_desc = planner::IndexScanPlan::IndexScanDesc(index,
                                                            key_column_ids,
                                                            expr_types,
                                                            values,
                                                            runtime_keys);
  }

  // Determine hybrid scan type
  auto hybrid_scan_type = state.hybrid_scan_type;
  if(state.layout_mode == LAYOUT_TYPE_ROW ||
      state.layout_mode == LAYOUT_TYPE_COLUMN) {
    hybrid_scan_type = HYBRID_SCAN_TYPE_SEQUENTIAL;
  }

  if(state.layout_mode == LAYOUT_TYPE_HYBRID &&
      index_count != 0) {
    hybrid_scan_type = HYBRID_SCAN_TYPE_HYBRID;
  }

  planner::HybridScanPlan hybrid_scan_node(sdbench_table.get(),
                                           predicate,
                                           column_ids,
                                           index_scan_desc,
                                           hybrid_scan_type);

  executor::HybridScanExecutor hybrid_scan_executor(&hybrid_scan_node,
                                                    context.get());

  //planner::SeqScanPlan seq_scan_node(sdbench_table.get(), predicate, column_ids);
  //executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for (auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER,
                        GetTypeSize(VALUE_TYPE_INTEGER),
                        std::to_string(column_id),
                        is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema,
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&hybrid_scan_executor);

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

  LOG_TRACE("Bulk insert count : %lf", bulk_insert_count);
  planner::InsertPlan insert_node(sdbench_table.get(),
                                  std::move(project_info),
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node,
                                           context.get());

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

static void BuildIndex(index::Index *index,
                       storage::DataTable *table) {
  oid_t start_tile_group_count = START_OID;
  oid_t table_tile_group_count = table->GetTileGroupCount();
  auto table_schema = table->GetSchema();
  std::unique_ptr<storage::Tuple> tuple_ptr(new storage::Tuple(table_schema, true));

  while (start_tile_group_count < table_tile_group_count &&
      state.fsm == true) {
    table_tile_group_count = table->GetTileGroupCount();
    auto tile_group = table->GetTileGroup(start_tile_group_count++);
    auto tile_group_id = tile_group->GetTileGroupId();

    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      // Copy over the tuple
      tile_group->CopyTuple(tuple_id, tuple_ptr.get());

      // Set the location
      ItemPointer location(tile_group_id, tuple_id);

      // TODO: Adds an entry in ALL the indexes
      // (should only insert in specific index)
      table->InsertInIndexes(tuple_ptr.get(), location);
    }

    // Update indexed tile group offset (set of tgs indexed)
    index->IncrementIndexedTileGroupOffset();

    // TODO: Sleep a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

}

static void RunAdaptTest() {
  double direct_low_proj = 0.06;
  double insert_write_ratio = 0.01;

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
}

std::vector<LayoutType> adapt_layouts = {LAYOUT_TYPE_ROW, LAYOUT_TYPE_HYBRID};

std::vector<oid_t> adapt_column_counts = {column_counts[1]};

void RunAdaptExperiment() {
  auto orig_transactions = state.transactions;
  std::thread index_builder;

  // Setup layout tuner
  auto& layout_tuner = brain::LayoutTuner::GetInstance();

  state.transactions = 100;   // 25

  state.selectivity = 0.06;
  state.adapt = true;

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

      LOG_INFO("*****************************************************");

      state.projectivity = 1.0;
      peloton_projectivity = state.projectivity;
      CreateAndLoadTable((LayoutType)peloton_layout_mode);

      // Reset query counter
      query_itr = 0;

      if (state.layout_mode == LAYOUT_TYPE_HYBRID) {
        state.fsm = true;
        peloton_fsm = true;

        // Start layout tuner
        layout_tuner.AddTable(sdbench_table.get());
        layout_tuner.Start();

        // Create an ad-hoc index
        CreateIndex();

        // Launch index build
        index_builder = std::thread(BuildIndex,
                                    sdbench_table->GetIndex(0),
                                    sdbench_table.get());
      }

      // Run adapt test
      RunAdaptTest();

      // Stop transformer
      if (state.layout_mode == LAYOUT_TYPE_HYBRID) {
        state.fsm = false;
        peloton_fsm = false;

        // Stop layout tuner
        layout_tuner.Stop();
        layout_tuner.ClearTables();

        index_builder.join();
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
