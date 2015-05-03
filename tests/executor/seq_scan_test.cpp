/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "catalog/schema.h"
#include "common/types.h"
#include "storage/table.h"
#include "storage/tile_group.h"

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
  const int tuple_count1 = 9;
  // Schema for second tile group. Vertical partition is 1, 3.
  std::vector<catalog::Schema> schemas2({
      catalog::Schema({ GetColumnInfo(0) }),
      catalog::Schema({ GetColumnInfo(1), GetColumnInfo(2), GetColumnInfo(3) })
      });
  const int tuple_count2 = 20;

  // Create table.
  storage::Table *table = storage::TableFactory::GetTable(
    INVALID_OID,
    new catalog::Schema({
      GetColumnInfo(0), GetColumnInfo(1),
      GetColumnInfo(2), GetColumnInfo(3) }));

  // Create tile groups.
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
  (void) txn_id;
  //TODO
  //table->InsertTuple(txn_id, tuple);

  return table;
}

} // namespace

// Sequential scan with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// paritioning changes midway.
TEST(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  //TODO Implement.
}

} // namespace test
} // namespace nstore
