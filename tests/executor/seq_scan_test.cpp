/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "planner/seq_scan_node.h"
#include "storage/table.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"

#include "harness.h"

namespace nstore {
namespace test {

namespace {

/** @brief TODO */
catalog::ColumnInfo GetColumnInfo(int index) {
  const bool allow_null = false;
  const bool is_inlined = true;
  return catalog::ColumnInfo(
      VALUE_TYPE_INTEGER,
      GetTypeSize(VALUE_TYPE_INTEGER),
      std::to_string(index),
      allow_null,
      is_inlined);
}

/** @brief TODO */
storage::Table *CreateFancyTable() {
  // Schema for first tile group. Vertical partition is 2, 2.
  std::vector<catalog::Schema> schemas1({
      catalog::Schema({ GetColumnInfo(0), GetColumnInfo(1) }),
      catalog::Schema({ GetColumnInfo(2), GetColumnInfo(3) })
      });
  // Schema for second tile group. Vertical partition is 1, 3.
  std::vector<catalog::Schema> schemas2({
      catalog::Schema({ GetColumnInfo(0) }),
      catalog::Schema({ GetColumnInfo(1), GetColumnInfo(2), GetColumnInfo(3) })
      });

  // Create table.
  std::unique_ptr<storage::Table> table(storage::TableFactory::GetTable(
    INVALID_OID,
    new catalog::Schema({
      GetColumnInfo(0), GetColumnInfo(1),
      GetColumnInfo(2), GetColumnInfo(3) })));

  // Create tile groups.
  const int tuple_count1 = 10;
  const int tuple_count2 = 20;
  table->AddTileGroup(storage::TileGroupFactory::GetTileGroup(
    INVALID_OID,
    INVALID_OID,
    INVALID_OID,
    table->GetBackend(),
    schemas1,
    tuple_count1));
  table->AddTileGroup(storage::TileGroupFactory::GetTileGroup(
    INVALID_OID,
    INVALID_OID,
    INVALID_OID,
    table->GetBackend(),
    schemas2,
    tuple_count2));

  // Insert tuples.
  const txn_id_t txn_id = GetTransactionId();
  const bool allocate = true;
  // Tile gorup 1.
  for (int row = 0; row < tuple_count1; row++) {
    storage::Tuple tuple(table->GetSchema(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(1, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(2, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(3, ValueFactory::GetIntegerValue(row));
    table->InsertTuple(txn_id, &tuple);
  }
  // Tile group 2.
  for (int row = 0; row < tuple_count2; row++) {
    storage::Tuple tuple(table->GetSchema(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(1, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(2, ValueFactory::GetIntegerValue(row));
    tuple.SetValue(3, ValueFactory::GetIntegerValue(row));
    table->InsertTuple(txn_id, &tuple);
  }

  return table.release();
}

/** @brief TODO */
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
  std::unique_ptr<storage::Table> table(CreateFancyTable());

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
