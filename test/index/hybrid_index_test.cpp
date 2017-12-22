//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hybrid_index_test.cpp
//
// Identification: test/index/hybrid_index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include <cassert>
#include <chrono>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/timer.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "executor/hybrid_scan_executor.h"
#include "executor/insert_executor.h"
#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "index/index_factory.h"
#include "index/index_factory.h"
#include "planner/hybrid_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

namespace peloton {
namespace test {
namespace hybrid_index_test {

class HybridIndexTests : public PelotonTest {};

static double projectivity = 1.0;
static size_t column_count = 4;

static size_t tuples_per_tile_group = TEST_TUPLES_PER_TILEGROUP * 100;
static size_t tile_group_count = 5;
static size_t tuple_count = tile_group_count * tuples_per_tile_group;

static double selectivity = 0.01;
static double predicate_offset = 0.9;

static double tuple_start_offset = predicate_offset * tuple_count;
static double tuple_end_offset = (selectivity + predicate_offset) * tuple_count;

static size_t query_count = 10;

void CreateTable(std::unique_ptr<storage::DataTable> &hyadapt_table,
                 bool build_indexes) {
  const bool is_inlined = true;

  // Create schema first
  std::vector<catalog::Column> columns;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    auto column = catalog::Column(type::TypeId::INTEGER,
                                  type::Type::GetTypeSize(type::TypeId::INTEGER),
                                  std::to_string(col_itr), is_inlined);
    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("HYADAPT_TABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  hyadapt_table.reset(storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tile_group,
      own_schema, adapt_table));

  // PRIMARY INDEX
  if (build_indexes == true) {
    std::vector<oid_t> key_attrs;

    auto tuple_schema = hyadapt_table->GetSchema();
    catalog::Schema *key_schema;
    index::IndexMetadata *index_metadata;
    bool unique;

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);
    unique = true;

    index_metadata = new index::IndexMetadata(
        "primary_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
        IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
        unique);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetIndex(index_metadata));

    hyadapt_table->AddIndex(pkey_index);
  }
}

void LoadTable(std::unique_ptr<storage::DataTable> &hyadapt_table) {
  auto table_schema = hyadapt_table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();

  for (size_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
    storage::Tuple tuple(table_schema, allocate);
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      auto value = type::ValueFactory::GetIntegerValue(tuple_itr);
      tuple.SetValue(col_itr, value, nullptr);
    }

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
        hyadapt_table->InsertTuple(&tuple, txn, &index_entry_ptr);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

    txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
  }

  txn_manager.CommitTransaction(txn);
}

expression::AbstractExpression *GetPredicate() {
  // ATTR0 >= LOWER_BOUND && < UPPER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr_left =
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0, 0);

  // Second, create constant value expression.
  auto constant_value_left =
      type::ValueFactory::GetIntegerValue(tuple_start_offset);

  expression::AbstractExpression *constant_value_expr_left =
      expression::ExpressionUtil::ConstantValueFactory(constant_value_left);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate_left =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_GREATERTHANOREQUALTO, tuple_value_expr_left,
          constant_value_expr_left);

  expression::AbstractExpression *tuple_value_expr_right =
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0, 0);

  auto constant_value_right =
      type::ValueFactory::GetIntegerValue(tuple_end_offset);

  expression::AbstractExpression *constant_value_expr_right =
      expression::ExpressionUtil::ConstantValueFactory(constant_value_right);

  expression::AbstractExpression *predicate_right =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_LESSTHAN, tuple_value_expr_right,
          constant_value_expr_right);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, predicate_left, predicate_right);

  return predicate;
}

void CreateIndexScanPredicate(std::vector<oid_t> &key_column_ids,
                              std::vector<ExpressionType> &expr_types,
                              std::vector<type::Value> &values) {
  key_column_ids.push_back(0);
  expr_types.push_back(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO);
  values.push_back(
      type::ValueFactory::GetIntegerValue(tuple_start_offset).Copy());

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_LESSTHAN);
  values.push_back(
      type::ValueFactory::GetIntegerValue(tuple_end_offset).Copy());
}

void GenerateSequence(std::vector<oid_t> &hyadapt_column_ids,
                      oid_t column_count) {
  // Reset sequence
  hyadapt_column_ids.clear();

  // Generate sequence
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    hyadapt_column_ids.push_back(column_id);
  }
}

void ExecuteTest(executor::AbstractExecutor *executor) {
  Timer<> timer;

  bool status = false;

  status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  timer.Start();

  size_t result_tuple_count = 0;
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
    result_tuple_count += result_tile->GetTupleCount();
  }

  timer.Stop();
  UNUSED_ATTRIBUTE double time_per_transaction = timer.GetDuration();
  LOG_TRACE("%f", time_per_transaction);

  LOG_TRACE("Lower bound        : %.0lf", tuple_start_offset);
  LOG_TRACE("Upper bound        : %.0lf", tuple_end_offset);
  LOG_TRACE("Result tuple count : %lu", result_tuple_count);
  // EXPECT_EQ(result_tuple_count, selectivity * tuple_count);
}

void LaunchSeqScan(std::unique_ptr<storage::DataTable> &hyadapt_table) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  std::vector<oid_t> hyadapt_column_ids;

  oid_t query_column_count = projectivity * column_count;
  GenerateSequence(hyadapt_column_ids, query_column_count);
  for (oid_t col_itr = 0; col_itr < query_column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = GetPredicate();

  planner::IndexScanPlan::IndexScanDesc dummy_index_scan_desc;

  planner::HybridScanPlan hybrid_scan_node(hyadapt_table.get(), predicate,
                                           column_ids, dummy_index_scan_desc,
                                           HybridScanType::SEQUENTIAL);

  executor::HybridScanExecutor hybrid_scan_executor(&hybrid_scan_node,
                                                    context.get());

  ExecuteTest(&hybrid_scan_executor);

  txn_manager.CommitTransaction(txn);
}

void LaunchIndexScan(std::unique_ptr<storage::DataTable> &hyadapt_table) {
  std::vector<oid_t> column_ids;
  std::vector<oid_t> hyadapt_column_ids;

  oid_t query_column_count = projectivity * column_count;
  GenerateSequence(hyadapt_column_ids, query_column_count);

  for (oid_t col_itr = 0; col_itr < query_column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  auto index = hyadapt_table->GetIndex(0);

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  CreateIndexScanPredicate(key_column_ids, expr_types, values);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  auto predicate = GetPredicate();

  planner::HybridScanPlan hybrid_scan_plan(hyadapt_table.get(), predicate,
                                           column_ids, index_scan_desc,
                                           HybridScanType::INDEX);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::HybridScanExecutor hybrid_scan_executor(&hybrid_scan_plan,
                                                    context.get());

  ExecuteTest(&hybrid_scan_executor);

  txn_manager.CommitTransaction(txn);
}

void LaunchHybridScan(std::unique_ptr<storage::DataTable> &hyadapt_table) {
  std::vector<oid_t> column_ids;
  std::vector<oid_t> column_ids_second;
  oid_t query_column_count = projectivity * column_count;
  std::vector<oid_t> hyadapt_column_ids;

  GenerateSequence(hyadapt_column_ids, query_column_count);

  for (oid_t col_itr = 0; col_itr < query_column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
    column_ids_second.push_back(hyadapt_column_ids[col_itr]);
  }

  auto index = hyadapt_table->GetIndex(0);

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  CreateIndexScanPredicate(key_column_ids, expr_types, values);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  auto predicate = GetPredicate();

  planner::HybridScanPlan hybrid_scan_plan(hyadapt_table.get(), predicate,
                                           column_ids_second, index_scan_desc,
                                           HybridScanType::HYBRID);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::HybridScanExecutor hybrid_scan_executor(&hybrid_scan_plan,
                                                    context.get());

  ExecuteTest(&hybrid_scan_executor);

  txn_manager.CommitTransaction(txn);
}

void CopyTuple(const oid_t &tuple_slot_id, storage::Tuple *tuple,
               storage::TileGroup *tile_group, const size_t column_count) {
  PL_ASSERT(tuple->GetColumnCount() == column_count);
  for (oid_t col_id = 0; col_id < column_count; ++col_id) {
    type::Value val = tile_group->GetValue(tuple_slot_id, col_id);
    tuple->SetValue(col_id, val, nullptr);
  }
}

void BuildIndex(std::shared_ptr<index::Index> index,
                storage::DataTable *table) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  oid_t start_tile_group_count = START_OID;
  oid_t table_tile_group_count = table->GetTileGroupCount();

  while (start_tile_group_count < table_tile_group_count) {
    auto tile_group = table->GetTileGroup(start_tile_group_count++);
    auto column_count = table->GetSchema()->GetColumnCount();
    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      std::unique_ptr<storage::Tuple> tuple_ptr(
          new storage::Tuple(table->GetSchema(), true));
      CopyTuple(tuple_id, tuple_ptr.get(), tile_group.get(), column_count);
      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

      ItemPointer *index_entry_ptr = nullptr;
      table->InsertInIndexes(tuple_ptr.get(), location, txn, &index_entry_ptr);
    }
    index->IncrementIndexedTileGroupOffset();
  }

  txn_manager.CommitTransaction(txn);
}

TEST_F(HybridIndexTests, SeqScanTest) {
  std::unique_ptr<storage::DataTable> hyadapt_table;
  CreateTable(hyadapt_table, false);
  LoadTable(hyadapt_table);

  for (size_t query_itr = 0; query_itr < query_count; query_itr++) {
    LaunchSeqScan(hyadapt_table);
  }
}

TEST_F(HybridIndexTests, IndexScanTest) {
  std::unique_ptr<storage::DataTable> hyadapt_table;
  CreateTable(hyadapt_table, true);
  LoadTable(hyadapt_table);

  for (size_t query_itr = 0; query_itr < query_count; query_itr++) {
    LaunchIndexScan(hyadapt_table);
  }
}

TEST_F(HybridIndexTests, HybridScanTest) {
  std::unique_ptr<storage::DataTable> hyadapt_table;
  CreateTable(hyadapt_table, false);
  LoadTable(hyadapt_table);

  std::vector<oid_t> key_attrs;

  auto tuple_schema = hyadapt_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "primary_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));

  hyadapt_table->AddIndex(pkey_index);

  std::thread index_builder =
      std::thread(BuildIndex, pkey_index, hyadapt_table.get());

  for (size_t query_itr = 0; query_itr < query_count; query_itr++) {
    LaunchHybridScan(hyadapt_table);
  }

  index_builder.join();
}

}  // namespace hybrid_index_test
}  // namespace test
}  // namespace peloton
