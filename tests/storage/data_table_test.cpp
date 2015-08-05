//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// data_table_test.cpp
//
// Identification: tests/storage/data_table_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tuple_count, false, false,
                                   true);

  auto tile_group = data_table->GetTileGroup(0);
  auto tile_group_id = tile_group->GetTileGroupId();

  std::cout << (*tile_group);

  // Create the new column map
  storage::column_map_type column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  // Transform the tile group
  std::unique_ptr<storage::TileGroup> tile_group1(
      data_table->TransformTileGroup(tile_group_id, column_map, false));

  std::cout << *(tile_group1.get());

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(0, 2);
  column_map[3] = std::make_pair(1, 0);

  std::unique_ptr<storage::TileGroup> tile_group2(
      data_table->TransformTileGroup(tile_group_id, column_map, false));

  std::cout << *(tile_group2.get());

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(1, 0);
  column_map[2] = std::make_pair(1, 1);
  column_map[3] = std::make_pair(1, 2);

  std::unique_ptr<storage::TileGroup> tile_group3(
      data_table->TransformTileGroup(tile_group_id, column_map, false));

  std::cout << *(tile_group3.get());
}

}  // End test namespace
}  // End peloton namespace
