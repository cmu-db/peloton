//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/ycsb/workload.cpp
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

#include "backend/benchmark/ycsb/workload.h"
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
#include "backend/executor/delete_executor.h"
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
#include "backend/planner/delete_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

storage::DataTable *user_table = nullptr;

// Tuple id counter
oid_t ycsb_tuple_counter = -1000000;

oid_t ycsb_max_scan_length = 1000;

oid_t ycsb_bulk_insert_count = 1000;

expression::AbstractExpression *CreatePointPredicate(const int bound) {

  // ATTR0 == BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::TupleValueFactory(0, 0);

  // Second, create constant value expression.
  Value constant_value = ValueFactory::GetIntegerValue(bound);
  expression::AbstractExpression *constant_value_expr =
      expression::ConstantValueFactory(constant_value);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate =
      expression::ComparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                    tuple_value_expr,
                                    constant_value_expr);

  constant_value.Free();

  return predicate;
}

expression::AbstractExpression *CreateScanPredicate(const int bound) {

  // ATTR0 > BOUND && ATTR0 < BOUND + MAX_SCAN_LENGTH

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr_1 =
      expression::TupleValueFactory(0, 0);

  // Second, create constant value expressions.
  Value lower_bound = ValueFactory::GetIntegerValue(bound);
  Value upper_bound = ValueFactory::GetIntegerValue(bound + ycsb_max_scan_length);

  expression::AbstractExpression *lower_bound_expr =
      expression::ConstantValueFactory(lower_bound);
  expression::AbstractExpression *upper_bound_expr =
      expression::ConstantValueFactory(upper_bound);

  // Link them together using an greater than expression.
  expression::AbstractExpression *greater_predicate =
      expression::ComparisonFactory(EXPRESSION_TYPE_COMPARE_GREATERTHAN,
                                    tuple_value_expr_1,
                                    lower_bound_expr);

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr_2 =
      expression::TupleValueFactory(0, 0);

  // Link them together using an lesser than expression.
  expression::AbstractExpression *less_predicate =
      expression::ComparisonFactory(EXPRESSION_TYPE_COMPARE_LESSTHAN,
                                    tuple_value_expr_2,
                                    upper_bound_expr);

  // Link everything together
  expression::AbstractExpression *predicate =
      expression::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                     less_predicate,
                                     greater_predicate);

  lower_bound.Free();
  upper_bound.Free();

  return predicate;
}

std::ofstream out("outputfile.summary");

static void WriteOutput(double duration) {

  // Convert to ms
  duration *= 1000;

  std::cout << "----------------------------------------------------------\n";
  std::cout << state.layout << " "
      << state.operator_type << " "
      << state.scale_factor << " "
      << state.column_count << " :: ";
  std::cout << duration << " ms\n";

  out << state.layout << " ";
  out << state.operator_type << " ";
  out << state.column_count << " ";
  out << duration << "\n";
  out.flush();

}

static std::vector<catalog::Column> GetColumns(){
  const oid_t col_count = state.column_count;
  bool is_inlined;

  // Create schema first
  std::vector<catalog::Column> columns;

  is_inlined = true;
  auto key_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                                    "YCSB_KEY", is_inlined);
  columns.push_back(key_column);

  is_inlined = false;
  for(oid_t col_itr = 0 ; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_VARCHAR, state.value_length,
                        "FIELD" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  return columns;
}

static void CreateTable() {

  auto columns = GetColumns();
  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("USERTABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  user_table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      state.tuples_per_tilegroup,
      own_schema, adapt_table);

}

static void LoadTable() {

  const oid_t col_count = state.column_count;
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;

  auto table_schema = user_table->GetSchema();
  std::string string_value('.', state.value_length);

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

    auto key_value = ValueFactory::GetIntegerValue(rowid);
    tuple.SetValue(0, key_value);

    for(oid_t col_itr = 1 ; col_itr <= col_count; col_itr++) {
      auto value = ValueFactory::GetStringValue(string_value);
      tuple.SetValue(col_itr, value);
    }

    ItemPointer tuple_slot_id = user_table->InsertTuple(txn, &tuple);
    assert(tuple_slot_id.block != INVALID_OID);
    assert(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction(txn);

}

void CreateAndLoadTable(LayoutType layout_type) {

  // Initialize settings
  peloton_layout = layout_type;

  CreateTable();

  LoadTable();
}

static int GetBound() {
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
  auto bound = rand() % (tuple_count - ycsb_max_scan_length);

  return bound;
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

void RunRead() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  for(oid_t col_itr = 0 ; col_itr <= state.column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePointPredicate(bound);
  planner::SeqScanPlan seq_scan_node(user_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;

  output_columns = GetColumns();
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
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

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunScan() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  for(oid_t col_itr = 0 ; col_itr <= state.column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreateScanPredicate(bound);
  planner::SeqScanPlan seq_scan_node(user_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;

  output_columns = GetColumns();
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
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

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunInsert() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::vector<Value> values;

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  std::string string_value('.', state.value_length);

  Value key_val = ValueFactory::GetIntegerValue(++ycsb_tuple_counter);
  auto expression = expression::ConstantValueFactory(key_val);
  target_list.emplace_back(0, expression);

  Value insert_val = ValueFactory::GetStringValue(string_value);
  for (auto col_id = 1; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::InsertPlan insert_node(user_table, project_info, ycsb_bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunUpdate() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  for(oid_t col_itr = 0 ; col_itr <= state.column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePointPredicate(bound);
  planner::SeqScanPlan seq_scan_node(user_table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  std::vector<Value> values;

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));

  std::string string_value('.', state.value_length);

  Value update_val = ValueFactory::GetStringValue(string_value);
  for (auto col_id = 1; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(update_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::UpdatePlan update_node(user_table, project_info);
  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Parent-Child relationship
  update_node.AddChild(&seq_scan_node);
  update_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&update_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunDelete() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  for(oid_t col_itr = 0 ; col_itr <= state.column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePointPredicate(bound);
  planner::SeqScanPlan seq_scan_node(user_table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // DELETE
  /////////////////////////////////////////////////////////

  bool truncate = false;
  planner::DeletePlan delete_node(user_table, truncate);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Parent-Child relationship
  delete_node.AddChild(&seq_scan_node);
  delete_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&delete_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

void RunReadModifyWrite() {
  const int bound = GetBound();
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  for(oid_t col_itr = 0 ; col_itr <= state.column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePointPredicate(bound);
  planner::SeqScanPlan seq_scan_node(user_table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;

  output_columns = GetColumns();
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
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
  // UPDATE
  /////////////////////////////////////////////////////////

  // Create and set up seq scan executor
  predicate = CreatePointPredicate(bound);
  planner::SeqScanPlan seq_scan_node_2(user_table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor_2(&seq_scan_node, context.get());

  std::vector<Value> values;

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));

  std::string string_value('.', state.value_length);

  Value update_val = ValueFactory::GetStringValue(string_value);
  for (auto col_id = 1; col_id <= state.column_count; col_id++) {
    auto expression = expression::ConstantValueFactory(update_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list), std::move(direct_map_list));

  planner::UpdatePlan update_node(user_table, project_info);
  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Parent-Child relationship
  update_node.AddChild(&seq_scan_node_2);
  update_executor.AddChild(&seq_scan_executor_2);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor*> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&update_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
