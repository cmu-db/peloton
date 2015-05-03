/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "catalog/manager.h"
#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "planner/seq_scan_node.h"
#include "storage/table.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

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

/** @brief Convenience method to create predicate for test. */
expression::AbstractExpression *CreateFancyPredicate() {
  std::unique_ptr<expression::AbstractExpression> predicate(
      expression::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_AND,
        expression::ConstantValueFactory(Value::GetTrue()),
        expression::ConstantValueFactory(Value::GetTrue())));
  return predicate.release();
}

} // namespace

// Sequential scan with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// paritioning changes midway.
TEST(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  // Create table.
  std::unique_ptr<storage::Table> table(CreateTable());

  // Set up catalog for this test.
  auto &locator = catalog::Manager::GetInstance().locator;
  const id_t table_oid = 1;
  locator[table_oid] = table.get();

  // Column ids to be added to logical tile after scan.
  std::vector<id_t> column_ids({ 1, 2, 3 });

  // Create plan node.
  planner::SeqScanNode node(
    table_oid,
    CreateFancyPredicate(),
    std::move(column_ids));

  // Execute sequential scan.
  //TODO
}

} // namespace test
} // namespace nstore
