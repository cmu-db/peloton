/*-------------------------------------------------------------------------
 *
 * data_table_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/tests/storage/data_table_test.cpp
 *
 *-------------------------------------------------------------------------
 */


#include "gtest/gtest.h"

#include "backend/storage/data_table.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Data Table Tests
//===--------------------------------------------------------------------===//

TEST(DataTableTests, TransformTileGroupTest) {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  storage::DataTable *data_table = ExecutorTestsUtil::CreateTable(tuple_count, false);
  ExecutorTestsUtil::PopulateTable(data_table, tuple_count, false,
                                   false, true);


  auto tile_group = data_table->GetTileGroup(0);
  auto tile_group_id = tile_group->GetTileGroupId();

  std::cout << (*tile_group);

  // Create the new column map
  storage::column_name_type column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  // Transform the tile group
  std::unique_ptr<storage::TileGroup> new_tile_group(data_table->TransformTileGroup(tile_group_id,
                                                       column_map));

  std::cout << *(new_tile_group.get());

}

}  // End test namespace
}  // End peloton namespace


