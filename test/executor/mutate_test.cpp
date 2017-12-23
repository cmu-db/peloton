//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mutate_test.cpp
//
// Identification: test/executor/mutate_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <atomic>

#include "executor/testing_executor_util.h"
#include "executor/drop_executor.h"
#include "common/harness.h"

#include "catalog/schema.h"
#include "type/value_factory.h"
#include "catalog/catalog.h"

#include "executor/executor_context.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "executor/logical_tile_factory.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/abstract_expression.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/table_factory.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/mock_executor.h"

#include "planner/drop_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Mutator Tests
//===--------------------------------------------------------------------===//

class MutateTests : public PelotonTest {};

std::atomic<int> tuple_id;
std::atomic<int> delete_tuple_id;

void InsertTuple(storage::DataTable *table, type::AbstractPool *pool,
                 UNUSED_ATTRIBUTE uint64_t thread_itr) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  for (oid_t tuple_itr = 0; tuple_itr < 10; tuple_itr++) {
    auto tuple = TestingExecutorUtil::GetTuple(table, ++tuple_id, pool);

    planner::InsertPlan node(table, std::move(tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }
  txn_manager.CommitTransaction(txn);
}

void UpdateTuple(storage::DataTable *table,
                 UNUSED_ATTRIBUTE uint64_t thread_itr) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Update
  //std::vector<oid_t> update_column_ids = {2};
  //std::vector<type::Value *> values;
  auto update_val = type::ValueFactory::GetDecimalValue(23.5);

  TargetList target_list;
  DirectMapList direct_map_list;

  planner::DerivedAttribute attribute{
      expression::ExpressionUtil::ConstantValueFactory(update_val)};
  target_list.emplace_back(2, attribute);

  LOG_TRACE("%u", target_list.at(0).first);

  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
  direct_map_list.emplace_back(1, std::pair<oid_t, oid_t>(0, 1));
  direct_map_list.emplace_back(3, std::pair<oid_t, oid_t>(0, 3));

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Predicate

  // WHERE ATTR_0 < 70
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(70));
  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_LESSTHAN, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  // Parent-Child relationship
  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  while (update_executor.Execute())
    ;

  txn_manager.CommitTransaction(txn);
}

void DeleteTuple(storage::DataTable *table,
                 UNUSED_ATTRIBUTE uint64_t thread_itr) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::vector<storage::Tuple *> tuples;

  // Delete
  planner::DeletePlan delete_node(table);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Predicate

  // WHERE ATTR_0 > 60
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(60));
  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHAN, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));

  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  // Parent-Child relationship
  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(delete_executor.Init());
  EXPECT_TRUE(delete_executor.Execute());

  txn_manager.CommitTransaction(txn);
}

TEST_F(MutateTests, StressTests) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  // Create insert node for this test.
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());

  // Pass through insert executor.
  /*
  auto null_tuple = ExecutorTestsUtil::GetNullTuple(table, testing_pool);

  planner::InsertPlan node(table, std::move(null_tuple));
  executor::InsertExecutor executor(&node, context.get());

  try {
    executor.Execute();
  } catch (ConstraintException &ce) {
    LOG_ERROR("%s", ce.what());
  }
  */

  auto non_empty_tuple =
      TestingExecutorUtil::GetTuple(table.get(), ++tuple_id, testing_pool);
  planner::InsertPlan node2(table.get(), std::move(non_empty_tuple));
  executor::InsertExecutor executor2(&node2, context.get());
  executor2.Execute();

  try {
    executor2.Execute();
  } catch (ConstraintException &ce) {
    LOG_ERROR("%s", ce.what());
  }

  txn_manager.CommitTransaction(txn);

  LaunchParallelTest(1, InsertTuple, table.get(), testing_pool);
  LOG_TRACE("%s", table->GetInfo().c_str());

  LOG_TRACE("%s", peloton::GETINFO_SINGLE_LINE.c_str());

  // LaunchParallelTest(1, UpdateTuple, table);
  // LOG_TRACE(table->GetInfo().c_str());

  LOG_TRACE("%s", peloton::GETINFO_SINGLE_LINE.c_str());

  LaunchParallelTest(1, DeleteTuple, table.get());

  LOG_TRACE("%s", table->GetInfo().c_str());

  // PRIMARY KEY
  std::vector<catalog::Column> columns;

  columns.push_back(TestingExecutorUtil::GetColumnInfo(0));
  std::unique_ptr<catalog::Schema> key_schema(new catalog::Schema(columns));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema.get(),
                                                          true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema.get(),
                                                          true));

  key1->SetValue(0, type::ValueFactory::GetIntegerValue(10), nullptr);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), nullptr);

  // SEC KEY
  columns.clear();
  columns.push_back(TestingExecutorUtil::GetColumnInfo(0));
  columns.push_back(TestingExecutorUtil::GetColumnInfo(1));
  key_schema.reset(new catalog::Schema(columns));

  std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema.get(),
                                                          true));
  std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema.get(),
                                                          true));

  key3->SetValue(0, type::ValueFactory::GetIntegerValue(10), nullptr);
  key3->SetValue(1, type::ValueFactory::GetIntegerValue(11), nullptr);
  key4->SetValue(0, type::ValueFactory::GetIntegerValue(100), nullptr);
  key4->SetValue(1, type::ValueFactory::GetIntegerValue(101), nullptr);

  tuple_id = 0;
}

// Insert a logical tile into a table
TEST_F(MutateTests, InsertTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // We are going to insert a tile group into a table in this test
  std::unique_ptr<storage::DataTable> source_data_table(
      TestingExecutorUtil::CreateAndPopulateTable());
  std::unique_ptr<storage::DataTable> dest_data_table(
      TestingExecutorUtil::CreateTable());
  const std::vector<storage::Tuple *> tuples;

  EXPECT_EQ(source_data_table->GetTileGroupCount(), 4);
  EXPECT_EQ(dest_data_table->GetTileGroupCount(), 1);

  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  planner::InsertPlan node(dest_data_table.get());
  executor::InsertExecutor executor(&node, context.get());

  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  // Uneventful init...
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  // Will return one tile.
  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Construct input logical tile
  auto physical_tile_group = source_data_table->GetTileGroup(0);
  auto tile_count = physical_tile_group->GetTileCount();
  std::vector<std::shared_ptr<storage::Tile> > physical_tile_refs;
  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++)
    physical_tile_refs.push_back(
        physical_tile_group->GetTileReference(tile_itr));

  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapTiles(physical_tile_refs));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());
  EXPECT_FALSE(executor.Execute());

  txn_manager.CommitTransaction(txn);

  // We have inserted all the tuples in this logical tile
  EXPECT_EQ(dest_data_table->GetTileGroupCount(), 2);
}

TEST_F(MutateTests, DeleteTest) {
  // We are going to insert a tile group into a table in this test
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  LaunchParallelTest(1, InsertTuple, table.get(), testing_pool);
  LaunchParallelTest(1, DeleteTuple, table.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  // Seq scan
  std::vector<oid_t> column_ids = {0};
  planner::SeqScanPlan seq_scan_node(table.get(), nullptr, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());
  EXPECT_TRUE(seq_scan_executor.Init());

  auto tuple_cnt = 0;
  while (seq_scan_executor.Execute()) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        seq_scan_executor.GetOutput());
    tuple_cnt += result_logical_tile->GetTupleCount();
  }
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(tuple_cnt, 6);
  tuple_id = 0;
}

static int SeqScanCount(storage::DataTable *table,
                        const std::vector<oid_t> &column_ids,
                        expression::AbstractExpression *predicate) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  auto tuple_cnt = 0;

  while (seq_scan_executor.Execute()) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        seq_scan_executor.GetOutput());
    tuple_cnt += result_logical_tile->GetTupleCount();
  }

  txn_manager.CommitTransaction(txn);

  return tuple_cnt;
}

TEST_F(MutateTests, UpdateTest) {
  // We are going to insert a tile group into a table in this test
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  LaunchParallelTest(1, InsertTuple, table.get(), testing_pool);
  LaunchParallelTest(1, UpdateTuple, table.get());

  // Seq scan to check number
  std::vector<oid_t> column_ids = {0};
  auto tuple_cnt = SeqScanCount(table.get(), column_ids, nullptr);
  EXPECT_EQ(tuple_cnt, 10);

  // ATTR = 23.5
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(type::TypeId::DECIMAL, 0, 2);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          type::ValueFactory::GetDecimalValue(23.5));

  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tup_val_exp, const_val_exp);

  tuple_cnt = SeqScanCount(table.get(), column_ids, predicate);
  EXPECT_EQ(tuple_cnt, 6);

  tuple_id = 0;
}

}  // namespace test
}  // namespace peloton
