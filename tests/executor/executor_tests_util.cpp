/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor_tests_util.h"

namespace nstore {

namespace test {

/**
 * @brief TODO
 */
storage::Tile *ExecutorTestsUtil::CreateSimplePhysicalTile() {
  std::vector<catalog::ColumnInfo> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Schema*> schemas;

  // Create schema.
  catalog::ColumnInfo column1(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column2(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column3(
      VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *schema1 = new catalog::Schema(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema2 = new catalog::Schema(columns);
  schemas.push_back(schema2);

  catalog::Schema *schema = catalog::Schema::AppendSchema(schema1, schema2);

  // TILES

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");

  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");

  column_names.push_back(tile_column_names);

  return nullptr;
}

} // namespace test
} // namespace nstore
