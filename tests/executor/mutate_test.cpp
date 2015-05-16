/**
 * @brief Test cases for insert node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "catalog/schema.h"
#include "common/value_factory.h"

#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "executor/logical_tile_factory.h"

#include "expression/expression_util.h"
#include "expression/abstract_expression.h"
#include "expression/expression.h"

#include "planner/delete_node.h"
#include "planner/insert_node.h"
#include "planner/seq_scan_node.h"
#include "planner/update_node.h"

#include "storage/tile_group.h"

#include "harness.h"

#include <atomic>

using ::testing::NotNull;
using ::testing::Return;

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Mutator Tests
//===--------------------------------------------------------------------===//

std::atomic<int> tuple_id;
std::atomic<int> delete_tuple_id;

void InsertTuple(storage::Table *table){

  std::vector<storage::Tuple *> tuples;
  Context context = GetContext();

  for(int tuple_itr = 0 ; tuple_itr < 10 ; tuple_itr++) {
    auto tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
    tuples.push_back(tuple);
  }

  // Bulk insert
  planner::InsertNode node(table, tuples);
  executor::InsertExecutor executor(&node, &context);
  executor.Execute();

  for(auto tuple : tuples) {
    tuple->FreeUninlinedData();
    delete tuple;
  }

  context.Commit();
}

void UpdateTuple(storage::Table *table){

  Context context = GetContext();
  std::vector<storage::Tuple *> tuples;

  // Update

  std::vector<id_t> update_column_ids = { 1 };
  std::vector<Value> values;
  Value update_val = ValueFactory::GetDoubleValue(23.45);
  values.push_back(update_val);

  planner::UpdateNode update_node(table, update_column_ids, values);
  executor::UpdateExecutor update_executor(&update_node, &context);

  // Predicate

  // WHERE ATTR_0 > 40
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, std::string("tablename"), std::string("colname"));
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(40));
  auto predicate =
      new expression::ComparisonExpression<expression::CmpGt>(EXPRESSION_TYPE_COMPARE_EQ, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<id_t> column_ids = { 0 };
  planner::SeqScanNode seq_scan_node(
      table,
      predicate,
      column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, &context);

  // Parent-Child relationship
  update_node.AddChild(&seq_scan_node);
  update_executor.AddChild(&seq_scan_executor);

  update_executor.Execute();

  context.Commit();
}

void DeleteTuple(storage::Table *table){

  Context context = GetContext();
  std::vector<storage::Tuple *> tuples;

  // Delete
  planner::DeleteNode delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, &context);

  // Predicate

  // WHERE ATTR_0 > 40
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, std::string("tablename"), std::string("colname"));
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(40));
  auto predicate =
      new expression::ComparisonExpression<expression::CmpGt>(EXPRESSION_TYPE_COMPARE_EQ, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<id_t> column_ids = { 0 };
  planner::SeqScanNode seq_scan_node(
      table,
      predicate,
      column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, &context);

  // Parent-Child relationship
  delete_node.AddChild(&seq_scan_node);
  delete_executor.AddChild(&seq_scan_executor);

  delete_executor.Execute();

  context.Commit();
}

// Insert a tuple into a table
TEST(InsertTests, BasicTests) {

  // Create insert node for this test.
  storage::Table *table = ExecutorTestsUtil::CreateTable();

  // Pass through insert executor.
  Context context = GetContext();
  storage::Tuple *tuple;
  std::vector<storage::Tuple *> tuples;

  tuple = ExecutorTestsUtil::GetNullTuple(table);
  tuples.push_back(tuple);
  planner::InsertNode node(table, tuples);
  executor::InsertExecutor executor(&node, &context);

  try{
    executor.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  tuple->FreeUninlinedData();
  delete tuple;
  tuples.clear();

  context.Commit();

  tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
  tuples.push_back(tuple);
  planner::InsertNode node2(table, tuples);
  executor::InsertExecutor executor2(&node2, &context);
  executor2.Execute();

  try{
    executor2.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  tuple->FreeUninlinedData();
  delete tuple;
  tuples.clear();

  LaunchParallelTest(1, InsertTuple, table);
  std::cout << (*table);

  //LaunchParallelTest(1, UpdateTuple, table);
  //std::cout << (*table);

  //LaunchParallelTest(1, DeleteTuple, table);
  //std::cout << (*table);

  // PRIMARY KEY
  auto pkey_index = table->GetIndex(0);
  std::vector<catalog::ColumnInfo> columns;

  columns.push_back(ExecutorTestsUtil::GetColumnInfo(0));
  catalog::Schema *key_schema = new catalog::Schema(columns);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);

  key1->SetValue(0, ValueFactory::GetIntegerValue(10));
  key2->SetValue(0, ValueFactory::GetIntegerValue(100));

  auto pkey_list = pkey_index->GetLocationsForKeyBetween(key1, key2);
  std::cout << "PKEY INDEX :: Entries : " << pkey_list.size() << "\n";

  delete key1;
  delete key2;
  delete key_schema;

  // SEC KEY
  auto sec_index = table->GetIndex(1);

  columns.clear();
  columns.push_back(ExecutorTestsUtil::GetColumnInfo(0));
  columns.push_back(ExecutorTestsUtil::GetColumnInfo(1));
  key_schema = new catalog::Schema(columns);

  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);

  key3->SetValue(0, ValueFactory::GetIntegerValue(10));
  key3->SetValue(1, ValueFactory::GetIntegerValue(11));
  key4->SetValue(0, ValueFactory::GetIntegerValue(100));
  key4->SetValue(1, ValueFactory::GetIntegerValue(101));

  auto sec_list = sec_index->GetLocationsForKeyBetween(key3, key4);
  std::cout << "SEC INDEX :: Entries : " << sec_list.size() << "\n";

  delete key3;
  delete key4;
  delete key_schema;

  delete table;
}

} // namespace test
} // namespace nstore
