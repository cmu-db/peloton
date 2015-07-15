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

#include "backend/catalog/schema.h"
#include "backend/common/value_factory.h"

#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression.h"
#include "backend/planner/delete_node.h"
#include "backend/planner/insert_node.h"
#include "backend/planner/seq_scan_node.h"
#include "backend/planner/update_node.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/table_factory.h"

#include "executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

#include <atomic>

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Mutator Tests
//===--------------------------------------------------------------------===//

std::atomic<int> tuple_id;
std::atomic<int> delete_tuple_id;

void InsertTuple(storage::DataTable *table){

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::vector<storage::Tuple *> tuples;

  for(int tuple_itr = 0 ; tuple_itr < 10 ; tuple_itr++) {
    auto tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
    tuples.push_back(tuple);
  }

  // Bulk insert
  planner::InsertNode node(table, tuples);
  executor::InsertExecutor executor(&node, txn);
  executor.Execute();

  for(auto tuple : tuples) {
    tuple->FreeUninlinedData();
    delete tuple;
  }

  txn_manager.CommitTransaction(txn);
}

void UpdateTuple(storage::DataTable *table){

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::vector<storage::Tuple *> tuples;

  // Update

  std::vector<oid_t> update_column_ids = { 2 };
  std::vector<Value> values;
  Value update_val = ValueFactory::GetDoubleValue(23.5);
  values.push_back(update_val);

  planner::UpdateNode update_node(table, update_column_ids, values);
  executor::UpdateExecutor update_executor(&update_node, txn);

  // Predicate

  // WHERE ATTR_0 < 60
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, 0, std::string("tablename"), std::string("colname"));
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(60));
  auto predicate =
      new expression::ComparisonExpression<expression::CmpLt>(EXPRESSION_TYPE_COMPARE_LT, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = { 0 };
  planner::SeqScanNode seq_scan_node(
      table,
      predicate,
      column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, txn);

  // Parent-Child relationship
  update_node.AddChild(&seq_scan_node);
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  EXPECT_TRUE(update_executor.Execute());

  txn_manager.CommitTransaction(txn);
}

void DeleteTuple(storage::DataTable *table){

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::vector<storage::Tuple *> tuples;

  // Delete
  planner::DeleteNode delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, txn);

  // Predicate

  // WHERE ATTR_0 < 90
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, 0, std::string("tablename"), std::string("colname"));
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(90));
  auto predicate =
      new expression::ComparisonExpression<expression::CmpLt>(EXPRESSION_TYPE_COMPARE_LT, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = { 0 };
  planner::SeqScanNode seq_scan_node(
      table,
      predicate,
      column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, txn);

  // Parent-Child relationship
  delete_node.AddChild(&seq_scan_node);
  delete_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(delete_executor.Init());
  EXPECT_TRUE(delete_executor.Execute());

  txn_manager.CommitTransaction(txn);
}

TEST(MutateTests, StressTests) {

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto context = txn_manager.BeginTransaction();

  // Create insert node for this test.
  storage::DataTable *table = ExecutorTestsUtil::CreateTable();

  // Pass through insert executor.
  storage::Tuple *tuple;
  std::vector<storage::Tuple *> tuples;

  tuple = ExecutorTestsUtil::GetNullTuple(table);
  tuples.push_back(tuple);
  planner::InsertNode node(table, tuples);
  executor::InsertExecutor executor(&node, context);

  try{
    executor.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  tuple->FreeUninlinedData();
  delete tuple;
  tuples.clear();

  tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
  tuples.push_back(tuple);
  planner::InsertNode node2(table, tuples);
  executor::InsertExecutor executor2(&node2, context);
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

  txn_manager.CommitTransaction(context);

  std::cout << "Start tests \n";

  LaunchParallelTest(1, InsertTuple, table);
  //std::cout << (*table);

  LaunchParallelTest(1, UpdateTuple, table);
  //std::cout << (*table);

  LaunchParallelTest(1, DeleteTuple, table);
  //std::cout << (*table);

  // PRIMARY KEY
  auto pkey_index = table->GetIndex(0);
  std::vector<catalog::Column> columns;

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

// Insert a logical tile into a table
TEST(MutateTests, InsertTest) {

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // We are going to insert a tile group into a table in this test
  std::unique_ptr<storage::DataTable> data_table(ExecutorTestsUtil::CreateAndPopulateTable());
  const std::vector<storage::Tuple *> tuples;

  EXPECT_EQ(data_table->GetTileGroupCount(), 3);

  planner::InsertNode node(data_table.get(), tuples);
  executor::InsertExecutor executor(&node, txn);

  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  // Uneventful init...
  EXPECT_CALL(child_executor, DInit())
  .WillOnce(Return(true));

  // Will return one tile.
  EXPECT_CALL(child_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  auto physical_tile = data_table->GetTileGroup(0)->GetTile(0);
  std::vector<storage::Tile *> physical_tiles;
  physical_tiles.push_back(physical_tile);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTiles(physical_tiles, false));

  EXPECT_CALL(child_executor, GetOutput())
  .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());
  EXPECT_FALSE(executor.Execute());

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);

  // We have inserted all the tuples in this logical tile
  EXPECT_EQ(data_table->GetTileGroupCount(), 4);
}

} // namespace test
} // namespace peloton
