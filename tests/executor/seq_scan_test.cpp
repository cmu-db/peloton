/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "executor/abstract_executor.h"
#include "executor/seq_scan_executor.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "planner/seq_scan_node.h"
#include "storage/table.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

using ::testing::NotNull;

namespace nstore {
namespace test {

namespace {

/** @brief Convenience method to create table for test. */
storage::Table *CreateTable() {
  const int tile_tuple_count = 20;
  std::unique_ptr<storage::Table> table(
      ExecutorTestsUtil::CreateTable(tile_tuple_count));
  assert(table->GetTileGroupCount() == 2);
  ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(0), tile_tuple_count);
  ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(1), tile_tuple_count);
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
    const std::set<id_t> &tuple_ids) {
  assert(tuple_ids.size() >= 1);
  expression::AbstractExpression *predicate =
    expression::ConstantValueFactory(Value::GetFalse());
  bool even = false;
  for (id_t tuple_id : tuple_ids) {
    even = !even;
    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = even
      ? expression::TupleValueFactory(0)
      : expression::TupleValueFactory(3);
    // Second, create constant value expression.
    Value constant_value = even
      ? ValueFactory::GetIntegerValue(
          ExecutorTestsUtil::PopulatedValue(tuple_id, 0))
      : ValueFactory::GetStringValue(
          std::to_string(ExecutorTestsUtil::PopulatedValue(tuple_id, 3)));
    expression::AbstractExpression *constant_value_expr =
      expression::ConstantValueFactory(constant_value);
    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
      expression::ComparisonFactory(
        EXPRESSION_TYPE_COMPARE_EQ,
        tuple_value_expr,
        constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_OR,
        predicate,
        equality_expr);
  }
  return predicate;
}

/** @brief Convenience method to extract next tile from executor. */
executor::LogicalTile *GetNextTile(executor::AbstractExecutor *executor) {
  EXPECT_TRUE(executor->Execute());
  std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
  EXPECT_THAT(result_tile, NotNull());
  return result_tile.release();
}

} // namespace

// Sequential scan with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// paritioning changes midway.
TEST(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  // Create table.
  std::unique_ptr<storage::Table> table(CreateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<id_t> column_ids({ 0, 1, 3 });

  // Set of tuple_ids that will satisfy the predicate.
  std::set<id_t> tuple_ids({ 0, 3, 5, 7 });

  // Create plan node.
  planner::SeqScanNode node(
    table.get(),
    CreatePredicate(tuple_ids),
    column_ids);

  // Execute sequential scan.
  executor::SeqScanExecutor executor(&node);
  EXPECT_TRUE(executor.Init());
  std::vector<std::unique_ptr<executor::LogicalTile> > result_tiles;
  for (unsigned int i = 0; i < table->GetTileGroupCount(); i++) {
    result_tiles.emplace_back(GetNextTile(&executor));
  }
  EXPECT_FALSE(executor.Execute());

  // Check correctness of result tiles.
  for (unsigned int i = 0 ; i < table->GetTileGroupCount(); i++) {
    EXPECT_EQ(column_ids.size(), result_tiles[i]->NumCols());

    // Only two tuples per tile satisfy our predicate.
    EXPECT_EQ(tuple_ids.size(), result_tiles[i]->NumTuples());

    // Verify values.
    std::set<id_t> expected_tuples_left(tuple_ids);
    for (id_t new_tuple_id : *(result_tiles[i])) {
      // We divide by 10 because we know how PopulatedValue() computes.
      // Bad style. Being a bit lazy here...
      int old_tuple_id = result_tiles[i]->GetValue(new_tuple_id, 0)
        .GetIntegerForTestsOnly() / 10;
      EXPECT_EQ(1, expected_tuples_left.erase(old_tuple_id));
      EXPECT_EQ(
          ExecutorTestsUtil::PopulatedValue(old_tuple_id, 1),
          result_tiles[i]->GetValue(new_tuple_id, 1).GetIntegerForTestsOnly());
      Value string_value(ValueFactory::GetStringValue(std::to_string(
              ExecutorTestsUtil::PopulatedValue(old_tuple_id, 3))));
      EXPECT_EQ(
          string_value,
          result_tiles[i]->GetValue(new_tuple_id, 2));
      string_value.FreeUninlinedData();
    }
    EXPECT_EQ(0, expected_tuples_left.size());
  }
}

} // namespace test
} // namespace nstore
