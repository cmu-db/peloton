//
// Created by Rui Wang on 16-4-30.
//

#include "gtest/gtest.h"
#include "harness.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>

#include "backend/planner/hybrid_scan_plan.h"
#include "backend/executor/hybrid_scan_executor.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/timer.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/index/index_factory.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/planner/index_scan_plan.h"
#include "backend/index/index_factory.h"

namespace peloton {
namespace test {

class HybridIndexTests : public PelotonTest {};

static double projectivity = 1.0;
static int columncount = 4;
static size_t tuples_per_tile_group = 10000;
static size_t tile_group = 100;
static float scalar = 0.9;
static size_t iter = 100;

void CreateTable(std::unique_ptr<storage::DataTable>& hyadapt_table, bool indexes) {
  oid_t column_count = projectivity * columncount;

  const oid_t col_count = column_count + 1;
  const bool is_inlined = true;

  // Create schema first
  std::vector<catalog::Column> columns;

  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    auto column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("HYADAPTTABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  hyadapt_table.reset(storage::TableFactory::GetDataTable(
    INVALID_OID, INVALID_OID, table_schema, table_name,
    tuples_per_tile_group, own_schema, adapt_table));

  // PRIMARY INDEX
  if (indexes == true) {
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
      "primary_index", 123, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

    index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
    hyadapt_table->AddIndex(pkey_index);
  }
}

void LoadTable(std::unique_ptr<storage::DataTable>& hyadapt_table) {
  oid_t column_count = projectivity * columncount;
  const oid_t col_count = column_count + 1;
  const int tuple_count = tile_group * tuples_per_tile_group;

  auto table_schema = hyadapt_table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  //auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  //auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(table_schema, allocate);

    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      auto value = ValueFactory::GetIntegerValue(populate_value);
      tuple.SetValue(col_itr, value, pool.get());
    }

    ItemPointer tuple_slot_id = hyadapt_table->InsertTuple(&tuple);
    assert(tuple_slot_id.block != INVALID_OID);
    assert(tuple_slot_id.offset != INVALID_OID);
    //txn->RecordInsert(tuple_slot_id);
  }

  //txn_manager.CommitTransaction();
}

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

expression::AbstractExpression *CreateTwoPredicate(const int lower_bound, const int higher_bound) {
  // ATTR0 >= LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr_left =
    expression::ExpressionUtil::TupleValueFactory(0, 0);

  // Second, create constant value expression.
  Value constant_value_left = ValueFactory::GetIntegerValue(lower_bound);

  expression::AbstractExpression *constant_value_expr_left =
    expression::ExpressionUtil::ConstantValueFactory(constant_value_left);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate_left =
    expression::ExpressionUtil::ComparisonFactory(
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, tuple_value_expr_left,
      constant_value_expr_left);

  expression::AbstractExpression *tuple_value_expr_right =
    expression::ExpressionUtil::TupleValueFactory(0, 0);
  
  Value constant_value_right = ValueFactory::GetIntegerValue(higher_bound);

  expression::AbstractExpression *constant_value_expr_right =
    expression::ExpressionUtil::ConstantValueFactory(constant_value_right);
   
  expression::AbstractExpression *predicate_right =
    expression::ExpressionUtil::ComparisonFactory(
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, tuple_value_expr_right,
      constant_value_expr_right);
 
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, predicate_left, predicate_right);
 
  return predicate;
}

void GenerateSequence(std::vector<oid_t>& hyadapt_column_ids, oid_t column_count) {
  // Reset sequence
  hyadapt_column_ids.clear();

  // Generate sequence
  for (oid_t column_id = 0; column_id < column_count; column_id++)
    hyadapt_column_ids.push_back(column_id);

  // std::random_shuffle(hyadapt_column_ids.begin(), hyadapt_column_ids.end());
}


void CreateIndexScanPredicate(const int lower,
                              std::vector<ExpressionType>& expr_types,
                              std::vector<Value>& values) {
  expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO);
  values.push_back(ValueFactory::GetIntegerValue(lower));
}

void CreateIndexScanTwoPredicates(const int lower, const int higher,
                                  std::vector<ExpressionType>& expr_types,
                                  std::vector<Value>& values) {
  expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO);
  values.push_back(ValueFactory::GetIntegerValue(lower));
  expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO);
  values.push_back(ValueFactory::GetIntegerValue(higher));

}


void ExecuteTest(executor::AbstractExecutor *executor) {
  Timer<> timer;

  size_t tuple_counts = 0;
  timer.Start();
  bool status = false;

  status = executor->Init();
  if (status == false) throw Exception("Init failed");

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

  while (executor->Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(
        executor->GetOutput());
      tuple_counts += result_tile->GetTupleCount();
      result_tiles.emplace_back(result_tile.release());
  }

  // Execute stuff
  executor->Execute();

  timer.Stop();
  double time_per_transaction = timer.GetDuration();
  LOG_INFO("%f", time_per_transaction);
//  EXPECT_EQ(tuple_counts, (tile_group * tuples_per_tile_group * 0.3) + 1);
  EXPECT_EQ(tuple_counts,
            tile_group * tuples_per_tile_group -
              (tile_group * tuples_per_tile_group * scalar + 0));
}

void LaunchSeqScan(std::unique_ptr<storage::DataTable>& hyadapt_table) {
// const int lower_bound = 30;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = projectivity * columncount;
  std::vector<oid_t> hyadapt_column_ids;

  GenerateSequence(hyadapt_column_ids, column_count);

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(tile_group * tuples_per_tile_group * scalar);
//  auto predicate = CreateTwoPredicate(tile_group * tuples_per_tile_group * scalar,
//                                      tile_group * tuples_per_tile_group * (scalar + 0.3));

  planner::HybridScanPlan hybrid_scan_node(hyadapt_table.get(), predicate, column_ids);

  executor::HybridScanExecutor Hybrid_scan_executor(&hybrid_scan_node, context.get());

  ExecuteTest(&Hybrid_scan_executor);

  txn_manager.CommitTransaction();
}

void LaunchIndexScan(std::unique_ptr<storage::DataTable>& hyadapt_table) {
  std::vector<oid_t> column_ids;
  oid_t column_count = projectivity * columncount;
  std::vector<oid_t> hyadapt_column_ids;

  GenerateSequence(hyadapt_column_ids, column_count);

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
  }

  auto index = hyadapt_table->GetIndex(0);

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  // key_column_ids.push_back(0);
  CreateIndexScanPredicate(tile_group * tuples_per_tile_group * scalar, expr_types, values);
//  CreateIndexScanTwoPredicates(tile_group * tuples_per_tile_group * scalar,
//                               tile_group * tuples_per_tile_group * (scalar + 0.3),
//                               expr_types,
//                               values);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
    index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  planner::HybridScanPlan hybrid_scan_plan(hyadapt_table.get(), predicate, column_ids,
                                           index_scan_desc);

  //const int lower_bound = 30;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));


  executor::HybridScanExecutor Hybrid_scan_executor(&hybrid_scan_plan, context.get());
  
  ExecuteTest(&Hybrid_scan_executor);

  txn_manager.CommitTransaction();
}


void LaunchHybridScan(std::unique_ptr<storage::DataTable>& hyadapt_table) {
  std::vector<oid_t> column_ids;
  std::vector<oid_t> column_ids_second;
  oid_t column_count = projectivity * columncount;
  std::vector<oid_t> hyadapt_column_ids;

  GenerateSequence(hyadapt_column_ids, column_count);

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(hyadapt_column_ids[col_itr]);
    column_ids_second.push_back(hyadapt_column_ids[col_itr]);
  }

  auto index = hyadapt_table->GetIndex(0);

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
 // key_column_ids.push_back(0);
   CreateIndexScanPredicate(tile_group * tuples_per_tile_group * scalar, expr_types, values);
//  CreateIndexScanTwoPredicates(tile_group * tuples_per_tile_group * scalar,
//                               tile_group * tuples_per_tile_group * (scalar + 0.3),
//                               expr_types,
//                                values);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      nullptr, key_column_ids, expr_types, values, runtime_keys);

   expression::AbstractExpression *predicate = CreatePredicate(tile_group * tuples_per_tile_group * scalar);
//  auto predicate = CreateTwoPredicate(tile_group * tuples_per_tile_group * scalar,
//                                      tile_group * tuples_per_tile_group * (scalar + 0.3));

  planner::HybridScanPlan hybrid_scan_plan(index, hyadapt_table.get(), predicate, column_ids_second,
                                           index_scan_desc);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));


  executor::HybridScanExecutor Hybrid_scan_executor(&hybrid_scan_plan, context.get());

  ExecuteTest(&Hybrid_scan_executor);

  txn_manager.CommitTransaction();
}


void BuildIndex(index::Index *index, storage::DataTable *table) {
  oid_t start_tile_group_count = START_OID;
  oid_t table_tile_group_count = table->GetTileGroupCount();

  while (start_tile_group_count < table_tile_group_count) {
    auto tile_group =
      table->GetTileGroup(start_tile_group_count++);
    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      std::unique_ptr<storage::Tuple> tuple_ptr(new storage::Tuple(table->GetSchema(), true));
      tile_group->CopyTuple(tuple_id, tuple_ptr.get());
      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

      table->InsertInIndexes(tuple_ptr.get(), location);
    }
    printf("finsih index %d\n", start_tile_group_count);
    index->IncreamentIndexedTileGroupOff();
    start_tile_group_count++;
  }
}

/*TEST_F(HybridIndexTests, SeqScanTest) {
  std::unique_ptr<storage::DataTable> hyadapt_table;
  CreateTable(hyadapt_table, false);
  LoadTable(hyadapt_table);

  for (size_t i = 0; i < iter; i++)
    LaunchSeqScan(hyadapt_table);

}

TEST_F(HybridIndexTests, IndexScanTest) {
  std::unique_ptr<storage::DataTable> hyadapt_table;
  CreateTable(hyadapt_table, true);
  LoadTable(hyadapt_table);

  for (size_t i = 0; i < iter; i++)
    LaunchIndexScan(hyadapt_table);
}
*/

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
  "primary_index", 123, INDEX_TYPE_BTREE,
  INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  hyadapt_table->AddIndex(pkey_index);

  std::thread index_builder = std::thread(BuildIndex, pkey_index, hyadapt_table.get());

  for (size_t i = 0; i < iter; i++) {
    sleep(2);
    LaunchHybridScan(hyadapt_table);
  }

  index_builder.join();
}

}  // namespace tet
}  // namespace peloton
