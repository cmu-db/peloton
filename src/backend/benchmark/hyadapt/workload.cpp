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
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/index/index_factory.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

std::vector<oid_t> hyadapt_column_ids = {
    198, 206, 169, 119, 9, 220, 2, 230, 212, 164, 111, 136, 106, 8, 112, 4, 234, 147, 35, 114, 89, 127, 144, 71, 186,
    34, 145, 124, 146, 7, 40, 227, 59, 190, 249, 157, 38, 64, 134, 167, 63, 178, 156, 94, 84, 187, 153, 158, 42, 236,
    83, 182, 107, 76, 58, 102, 96, 31, 244, 54, 37, 228, 24, 120, 92, 233, 170, 209, 93, 12, 47, 200, 248, 171, 22,
    166, 213, 101, 97, 29, 237, 149, 49, 142, 181, 196, 75, 188, 208, 218, 183, 250, 151, 189, 60, 226, 214, 174, 128, 239,
    27, 235, 217, 98, 143, 165, 160, 109, 65, 23, 74, 207, 115, 69, 108, 30, 201, 221, 202, 20, 225, 105, 91, 95, 150,
    123, 16, 238, 81, 3, 219, 204, 68, 203, 73, 41, 66, 192, 113, 216, 117, 99, 126, 53, 1, 139, 116, 229, 100, 215,
    48, 10, 86, 211, 17, 224, 122, 51, 103, 85, 110, 50, 162, 129, 243, 67, 133, 138, 193, 141, 232, 118, 159, 199, 39,
    154, 137, 163, 179, 77, 194, 130, 46, 32, 125, 241, 246, 140, 26, 78, 177, 148, 223, 185, 197, 61, 195, 18, 80, 231,
    222, 70, 191, 52, 72, 155, 88, 175, 43, 172, 173, 13, 152, 180, 62, 121, 25, 55, 247, 36, 15, 210, 56, 6, 104,
    14, 132, 135, 168, 176, 28, 245, 11, 184, 131, 161, 5, 21, 242, 87, 44, 45, 205, 57, 19, 33, 90, 240, 79, 82
};

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

static void WriteOutput(double duration) {

  std::cout << "time :: " << duration << " s\n";

  std::ofstream out("outputfile.summary");
  out << duration << "\n";
  out.close();

}

static storage::DataTable* CreateTable() {
  const int tuples_per_tilegroup_count = DEFAULT_TUPLES_PER_TILEGROUP;
  const oid_t col_count = state.column_count;
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
      tuples_per_tilegroup_count,
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

  const int tuples_per_tilegroup_count = DEFAULT_TUPLES_PER_TILEGROUP;
  const int tile_group_count = state.scale_factor;
  const oid_t col_count = state.column_count;

  const int tuple_count = tuples_per_tilegroup_count * tile_group_count;

  auto table_schema = table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();

  std::cout << "tuple count : " << tuple_count << "\n";

  for (int rowid = 0; rowid < tuple_count; rowid++) {
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

static storage::DataTable *CreateAndLoadTable() {

  auto table = CreateTable();

  LoadTable(table);

  return table;
}

static int GetLowerBound() {
  const int tuples_per_tilegroup_count = DEFAULT_TUPLES_PER_TILEGROUP;
  const int tile_group_count = state.scale_factor;

  const int tuple_count = tuples_per_tilegroup_count * tile_group_count;
  const int lower_bound = (1 - state.selectivity) * tuple_count;

  return lower_bound;
}

static void ExecuteTest(executor::MaterializationExecutor& mat_executor) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  auto txn_count = state.transactions;
  bool status = false;
  start = std::chrono::system_clock::now();

  // Run these many transactions
  for(oid_t txn_itr = 0 ; txn_itr < txn_count ; txn_itr++) {
    status = mat_executor.Init();
    assert(status == true);

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

    while(mat_executor.Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(mat_executor.GetOutput());
      assert(result_tile != nullptr);
      result_tiles.emplace_back(result_tile.release());
    }

    status = mat_executor.Execute();
    assert(status == false);
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start;
  double time_per_transaction = ((double)elapsed_seconds.count())/txn_count;

  WriteOutput(time_per_transaction);
}

void RunDirectTest() {
  std::unique_ptr<storage::DataTable> table(CreateAndLoadTable());

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
    column_ids.push_back(hyadapt_column_ids[col_itr] - 1);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(table.get(), predicate, column_ids);

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
  bool status = false;

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  ExecuteTest(mat_executor);

  txn_manager.CommitTransaction(txn);
}

void RunAggregateTest() {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::unique_ptr<storage::DataTable> table(CreateAndLoadTable());

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

  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr] - 1);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(table.get(), predicate, column_ids);

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
  auto data_table_schema = table.get()->GetSchema();
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
  bool status = false;

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  ExecuteTest(mat_executor);

  txn_manager.CommitTransaction(txn);
}

void RunArithmeticTest() {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::unique_ptr<storage::DataTable> table(CreateAndLoadTable());

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

  for(oid_t col_itr = 0 ; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr] - 1);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(table.get(), predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // PROJECTION
  /////////////////////////////////////////////////////////

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // Construct schema of projection
  std::vector<catalog::Column> columns;
  auto orig_schema = table.get()->GetSchema();
  columns.push_back(orig_schema->GetColumn(0));
  auto projection_schema = new catalog::Schema(columns);

  // target list
  expression::AbstractExpression *sum_expr = nullptr;
  oid_t projection_column_count = state.projectivity * state.column_count;

  for(oid_t col_itr = 0 ; col_itr < projection_column_count; col_itr++) {
    auto hyadapt_colum_id = hyadapt_column_ids[col_itr] - 1;
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
  bool status = false;

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  ExecuteTest(mat_executor);

  txn_manager.CommitTransaction(txn);
}

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
