//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_test.cpp
//
// Identification: test/executor/seq_scan_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "catalog/schema.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/seq_scan_executor.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_factory.h"

#include "common/harness.h"
#include "executor/mock_executor.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class SeqScanTests : public PelotonTest {};

namespace {

/**
 * @brief Set of tuple_ids that will satisfy the predicate in our test cases.
 */
const std::set<oid_t> g_tuple_ids({0, 3});

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *CreateTable() {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());

  // Schema for first tile group. Vertical partition is 2, 2.
  std::vector<catalog::Schema> schemas1(
      {catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                        TestingExecutorUtil::GetColumnInfo(1)}),
       catalog::Schema({TestingExecutorUtil::GetColumnInfo(2),
                        TestingExecutorUtil::GetColumnInfo(3)})});

  // Schema for second tile group. Vertical partition is 1, 3.
  std::vector<catalog::Schema> schemas2(
      {catalog::Schema({TestingExecutorUtil::GetColumnInfo(0)}),
       catalog::Schema({TestingExecutorUtil::GetColumnInfo(1),
                        TestingExecutorUtil::GetColumnInfo(2),
                        TestingExecutorUtil::GetColumnInfo(3)})});

  TestingHarness::GetInstance().GetNextTileGroupId();

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map1;
  column_map1[0] = std::make_pair(0, 0);
  column_map1[1] = std::make_pair(0, 1);
  column_map1[2] = std::make_pair(1, 0);
  column_map1[3] = std::make_pair(1, 1);

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map2;
  column_map2[0] = std::make_pair(0, 0);
  column_map2[1] = std::make_pair(1, 0);
  column_map2[2] = std::make_pair(1, 1);
  column_map2[3] = std::make_pair(1, 2);

  // Create tile groups.
  table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas1, column_map1, tuple_count)));

  table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas2, column_map2, tuple_count)));

  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(0), tuple_count);
  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(1), tuple_count);
  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(2), tuple_count);

  return table.release();
}

/**
 * @brief Convenience method to create predicate for test.
 * @param tuple_ids Set of tuple ids that we want the predicate to match with.
 *
 * The predicate matches any tuple with ids in the specified set.
 * This assumes that the table was populated with PopulatedValue() in
 * ExecutorTestsUtil.
 *
 * Each OR node has an equality node to its right and another OR node to
 * its left. The leftmost leaf is a FALSE constant value expression.
 *
 * In each equality node, we either use (arbitrarily taking reference from the
 * parity of the loop iteration) the first field or last field of the tuple.
 */
expression::AbstractExpression *CreatePredicate(
    const std::set<oid_t> &tuple_ids) {
  PL_ASSERT(tuple_ids.size() >= 1);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(false));

  bool even = false;
  for (oid_t tuple_id : tuple_ids) {
    even = !even;

    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = nullptr;

    tuple_value_expr = even ? expression::ExpressionUtil::TupleValueFactory(
                                  type::TypeId::INTEGER, 0, 0)
                            : expression::ExpressionUtil::TupleValueFactory(
                                  type::TypeId::VARCHAR, 0, 3);

    // Second, create constant value expression.
    expression::AbstractExpression *constant_value_expr;
    if (even) {
      auto constant_value = type::ValueFactory::GetIntegerValue(
          TestingExecutorUtil::PopulatedValue(tuple_id, 0));
      constant_value_expr =
          expression::ExpressionUtil::ConstantValueFactory(constant_value);
    } else {
      auto constant_value = type::ValueFactory::GetVarcharValue(
          std::to_string(TestingExecutorUtil::PopulatedValue(tuple_id, 3)));
      constant_value_expr =
          expression::ExpressionUtil::ConstantValueFactory(constant_value);
    }

    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
        expression::ExpressionUtil::ComparisonFactory(
            ExpressionType::COMPARE_EQUAL, tuple_value_expr,
            constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ExpressionUtil::ConjunctionFactory(
        ExpressionType::CONJUNCTION_OR, predicate, equality_expr);
  }

  return predicate;
}

/**
 * @brief Convenience method to extract next tile from executor.
 * @param executor Executor to be tested.
 *
 * @return Logical tile extracted from executor.
 */
executor::LogicalTile *GetNextTile(executor::AbstractExecutor &executor) {
  EXPECT_TRUE(executor.Execute());
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_THAT(result_tile, NotNull());
  return result_tile.release();
}

/**
 * @brief Runs actual test used by some or all of the test cases below.
 * @param executor Sequential scan executor to be tested.
 * @param expected_num_tiles Expected number of output tiles.
 * @param expected_num_cols Expected number of columns in the output
 *        logical tile(s).
 *
 * There are a lot of contracts between this function and the test cases
 * that use it (especially the part that verifies values). Please be mindful
 * if you're making changes.
 */
void RunTest(executor::SeqScanExecutor &executor, int expected_num_tiles,
             int expected_num_cols) {
  EXPECT_TRUE(executor.Init());
  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  for (int i = 0; i < expected_num_tiles; i++) {
    result_tiles.emplace_back(GetNextTile(executor));
  }
  EXPECT_FALSE(executor.Execute());

  // Check correctness of result tiles.
  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_EQ(expected_num_cols, result_tiles[i]->GetColumnCount());

    // Only two tuples per tile satisfy our predicate.
    EXPECT_EQ(g_tuple_ids.size(), result_tiles[i]->GetTupleCount());

    // Verify values.
    std::set<oid_t> expected_tuples_left(g_tuple_ids);
    for (oid_t new_tuple_id : *(result_tiles[i])) {
      // We divide by 10 because we know how PopulatedValue() computes.
      // Bad style. Being a bit lazy here...

      type::Value value1 = (result_tiles[i]->GetValue(new_tuple_id, 0));
      int old_tuple_id = value1.GetAs<int32_t>() / 10;

      EXPECT_EQ(1, expected_tuples_left.erase(old_tuple_id));

      int val1 = TestingExecutorUtil::PopulatedValue(old_tuple_id, 1);
      type::Value value2 = (result_tiles[i]->GetValue(new_tuple_id, 1));
      EXPECT_EQ(val1, value2.GetAs<int32_t>());
      int val2 = TestingExecutorUtil::PopulatedValue(old_tuple_id, 3);

      // expected_num_cols - 1 is a hacky way to ensure that
      // we are always getting the last column in the original table.
      // For the tile group test case, it'll be 2 (one column is removed
      // during the scan as part of the test case).
      // For the logical tile test case, it'll be 3.
      type::Value string_value =
          (type::ValueFactory::GetVarcharValue(std::to_string(val2)));
      type::Value val =
          (result_tiles[i]->GetValue(new_tuple_id, expected_num_cols - 1));
      EXPECT_TRUE(val.CompareEquals(string_value) == CmpBool::TRUE);
    }
    EXPECT_EQ(0, expected_tuples_left.size());
  }
}

// Sequential scan of table with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// partitioning changes midway.
TEST_F(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  // Create table.
  std::unique_ptr<storage::DataTable> table(CreateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  // Create plan node.
  planner::SeqScanPlan node(table.get(), CreatePredicate(g_tuple_ids),
                            column_ids);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::SeqScanExecutor executor(&node, context.get());
  RunTest(executor, table->GetTileGroupCount(), column_ids.size());

  txn_manager.CommitTransaction(txn);
}

// Sequential scan of logical tile with predicate.
TEST_F(SeqScanTests, NonLeafNodePredicateTest) {
  // No table for this case as seq scan is not a leaf node.
  storage::DataTable *table = nullptr;

  // No column ids as input to executor is another logical tile.
  std::vector<oid_t> column_ids;

  // Create plan node.
  planner::SeqScanPlan node(table, CreatePredicate(g_tuple_ids), column_ids);
  // This table is generated so we can reuse the test data of the test case
  // where seq scan is a leaf node. We only need the data in the tiles.
  std::unique_ptr<storage::DataTable> data_table(CreateTable());

  // Set up executor and its child.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::SeqScanExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  // Uneventful init...
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  // Will return one tile.
  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(2)));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  int expected_column_count = data_table->GetSchema()->GetColumnCount();

  RunTest(executor, 2, expected_column_count);

  txn_manager.CommitTransaction(txn);
}
}

}  // namespace test
}  // namespace peloton
