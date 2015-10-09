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

#include "backend/benchmark/hyadapt/workload.h"
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
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/index/index_factory.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/storage/backend_vm.h"
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

  // ATTR0 > LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::TupleValueFactory(0, 0);

  // Second, create constant value expression.
  Value constant_value = ValueFactory::GetIntegerValue(lower_bound);
  expression::AbstractExpression *constant_value_expr =
      expression::ConstantValueFactory(constant_value);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate =
      expression::ComparisonFactory(EXPRESSION_TYPE_COMPARE_GREATERTHAN,
                                    tuple_value_expr,
                                    constant_value_expr);

  constant_value.Free();

  return predicate;
}

std::ofstream out("outputfile.summary");

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
        << state.tuples_per_tilegroup << " :: ";
  std::cout << duration << " ms\n";

  out << state.layout << " ";
  out << state.operator_type << " ";
  out << state.selectivity << " ";
  out << state.projectivity << " ";
  out << state.column_count << " ";
  out << state.write_ratio << " ";
  out << duration << "\n";
  out.flush();

}

static storage::DataTable* CreateTable() {
  const oid_t col_count = state.column_count + 1;
  const bool is_inlined = true;
  const bool indexes = false;

  // Create schema first
  std::vector<catalog::Column> columns;

  for(oid_t col_itr = 0 ; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("TEST_TABLE");


  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      state.tuples_per_tilegroup,
      own_schema, adapt_table));

  // PRIMARY INDEX
  if (indexes == true) {
    std::vector<oid_t> key_attrs;

    auto tuple_schema = table->GetSchema();
    catalog::Schema *key_schema;
    index::IndexMetadata *index_metadata;
    bool unique;

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = true;

    index_metadata = new index::IndexMetadata(
        "primary_index", 123, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

    index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
    table->AddIndex(pkey_index);
  }

  return table.release();
}

static void LoadTable(storage::DataTable *table) {

  const oid_t col_count = state.column_count + 1;
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;

  auto table_schema = table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(table_schema, allocate);

    for(oid_t col_itr = 0 ; col_itr < col_count; col_itr++) {
      auto value = ValueFactory::GetIntegerValue(populate_value);
      tuple.SetValue(col_itr, value);
    }

    ItemPointer tuple_slot_id = table->InsertTuple(txn, &tuple);
    assert(tuple_slot_id.block != INVALID_OID);
    assert(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction(txn);

}

storage::DataTable *CreateAndLoadTable(LayoutType layout_type) {

  // Initialize settings
  peloton_layout = layout_type;

  auto table = CreateTable();

  LoadTable(table);

  return table;
}

static int GetLowerBound() {
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  const int lower_bound = (1 - state.selectivity) * tuple_count;

  return lower_bound;
}

static void ExecuteTest(std::vector<executor::AbstractExecutor*>& executors) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  auto txn_count = state.transactions;
  bool status = false;
  start = std::chrono::system_clock::now();

  // Run these many transactions
  for(oid_t txn_itr = 0 ; txn_itr < txn_count ; txn_itr++) {

    // Run all the executors
    for(auto executor : executors) {

      status = executor->Init();
      assert(status == true);

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

      while(executor->Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
        assert(result_tile != nullptr);
        result_tiles.emplace_back(result_tile.release());
      }

      status = executor->Execute();
      assert(status == false);

    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start;
  double time_per_transaction = ((double)elapsed_seconds.count())/txn_count;

  WriteOutput(time_per_transaction);
}

void RunDirectTest(storage::DataTable *table) {
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
  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);

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

  for (oid_t col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunAggregateTest(storage::DataTable *table) {
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
  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);

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
  auto data_table_schema = table->GetSchema();
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

  for (oid_t col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunArithmeticTest(storage::DataTable *table) {
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
  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // PROJECTION
  /////////////////////////////////////////////////////////

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // Construct schema of projection
  std::vector<catalog::Column> columns;
  auto orig_schema = table->GetSchema();
  columns.push_back(orig_schema->GetColumn(0));
  auto projection_schema = new catalog::Schema(columns);

  // target list
  expression::AbstractExpression *sum_expr = nullptr;
  oid_t projection_column_count = state.projectivity * state.column_count;

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

  for (oid_t col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bulk_insert_count = state.write_ratio * orig_tuple_count;
  planner::InsertPlan insert_node(table, project_info, bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

/////////////////////////////////////////////////////////
// EXPERIMENTS
/////////////////////////////////////////////////////////

std::vector<oid_t> column_counts = {50, 250};

std::vector<double> write_ratios = {0, 0.5};

std::vector<LayoutType> layouts = { LAYOUT_ROW, LAYOUT_COLUMN, LAYOUT_HYBRID};

std::vector<OperatorType> operators = { OPERATOR_TYPE_DIRECT, OPERATOR_TYPE_AGGREGATE, OPERATOR_TYPE_ARITHMETIC};

std::vector<double> selectivity = {0.2, 0.4, 0.6, 0.8, 1.0};

std::vector<double> projectivity = {0.2, 0.4, 0.6, 0.8, 1.0};

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
          std::unique_ptr<storage::DataTable>table(CreateAndLoadTable(layout));

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest(table.get());

          state.operator_type = OPERATOR_TYPE_AGGREGATE;
          RunAggregateTest(table.get());

          state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          RunArithmeticTest(table.get());
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

        // Load in the table with layout
        std::unique_ptr<storage::DataTable>table(CreateAndLoadTable(layout));

        for(auto select : selectivity) {
          // Set selectivity
          state.selectivity = select;

          // Go over all ops
          state.operator_type = OPERATOR_TYPE_DIRECT;
          RunDirectTest(table.get());

          state.operator_type = OPERATOR_TYPE_AGGREGATE;
          RunAggregateTest(table.get());

          state.operator_type = OPERATOR_TYPE_ARITHMETIC;
          RunArithmeticTest(table.get());
        }

      }

    }

  }

  out.close();
}

std::vector<double> op_selectivity = {0.1, 0.5, 1.0};

std::vector<double> op_projectivity = {0.2, 0.4, 0.6, 0.8, 1.0};

void RunOperatorExperiment() {

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

        for(auto selectivity : op_selectivity) {
          // Set selectivity
          state.selectivity = selectivity;

          for(auto projectivity : op_projectivity) {
            // Set projectivity
            state.projectivity = projectivity;
            peloton_projectivity = state.projectivity;

            // Load in the table with layout
            std::unique_ptr<storage::DataTable>table(CreateAndLoadTable(layout));

            // Run operator
            state.operator_type = OPERATOR_TYPE_ARITHMETIC;
            RunArithmeticTest(table.get());
          }
        }

      }

    }

  }

  out.close();
}


}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
