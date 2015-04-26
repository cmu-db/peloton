/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/executor_tests_util.h"

#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"

#include "harness.h"

namespace nstore {
namespace test {

/**
 * @brief Creates simple tile group for testing purposes.
 * @param tuple_count Tuple capacity of this tile group.
 *
 * Tile group has two tiles, and each of them has two columns.
 * The first two columns have INTEGER types, the last two have TINYINT
 * and VARCHAR.
 *
 * IMPORTANT: If you modify this function, it is your responsibility to
 *            fix any affected test cases. Test cases may be depending
 *            on things like the specific number of tiles in this group.
 *
 * @return Pointer to tile group.
 */
storage::TileGroup *ExecutorTestsUtil::CreateSimpleTileGroup(
    storage::Backend *backend,
    int tuple_count) {
  std::vector<catalog::ColumnInfo> columns;
  std::vector<catalog::Schema> schemas;

  const bool allow_null = false;
  const bool is_inlined = true;
  catalog::ColumnInfo column1(
      VALUE_TYPE_INTEGER,
      GetTypeSize(VALUE_TYPE_INTEGER),
      "A",
      allow_null,
      is_inlined);
  catalog::ColumnInfo column2(
      VALUE_TYPE_INTEGER,
      GetTypeSize(VALUE_TYPE_INTEGER),
      "B",
      allow_null,
      is_inlined);
  catalog::ColumnInfo column3(
      VALUE_TYPE_TINYINT,
      GetTypeSize(VALUE_TYPE_TINYINT),
      "C",
      allow_null,
      is_inlined);
  catalog::ColumnInfo column4(
      VALUE_TYPE_VARCHAR,
      25, // Column length.
      "D",
      allow_null,
      false); // Not inlined.

  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema schema1(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema schema2(columns);
  schemas.push_back(schema2);

  storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(
      INVALID_OID,
      INVALID_OID,
      INVALID_OID,
      backend,
      schemas,
      tuple_count);

  return tile_group;
}

/**
 * @brief Populates the tiles in the given tile-group in a specific manner.
 * @param tile_group Tile-group to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ExecutorTestsUtil::PopulateTiles(
    storage::TileGroup *tile_group,
    int num_rows) {
  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
    catalog::Schema::AppendSchemaList(tile_schemas));

  // Ensure that the tile group is as expected.
  assert(tile_schemas.size() == 2);
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  const txn_id_t txn_id = GetTransactionId();
  for (int i = 0; i < num_rows; i++) {
    storage::Tuple tuple(schema.get(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(10 * i));
    tuple.SetValue(1, ValueFactory::GetIntegerValue(10 * i + 1));
    tuple.SetValue(2, ValueFactory::GetTinyIntValue(10 * i + 2));
    tuple.SetValue(
        3,
        ValueFactory::GetStringValue(std::to_string(10 * i + 3),
        tile_group->GetTilePool(1)));
    tile_group->InsertTuple(txn_id, &tuple);
  }
}

} // namespace test
} // namespace nstore
