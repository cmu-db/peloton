/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor_tests_util.h"

#include <memory>

#include "catalog/schema.h"
#include "storage/tile_group.h"

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
 * @return Pointer to tile group.
 */
storage::TileGroup *ExecutorTestsUtil::CreateSimpleTileGroup(
    catalog::Manager *manager,
    storage::Backend *backend,
    int tuple_count) {
  std::vector<catalog::ColumnInfo> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;
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

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");
  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");
  column_names.push_back(tile_column_names);

  return storage::TileGroupFactory::GetTileGroup(
      INVALID_OID,
      INVALID_OID,
      INVALID_OID,
      manager,
      backend,
      schemas,
      tuple_count);
}

} // namespace test
} // namespace nstore
