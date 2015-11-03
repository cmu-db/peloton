//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// materialization_test.cpp
//
// Identification: tests/executor/materialization_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"


#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"

using ::testing::NotNull;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Materialization Tests
//===--------------------------------------------------------------------===//

// "Pass-through" test case. There is nothing to materialize as
// there is only one base tile in the logical tile.
TEST(MaterializationTests, SingleBaseTileTest) {
  const int tuple_count = 9;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateTileGroup(tuple_count));

  ExecutorTestsUtil::PopulateTiles(tile_group.get(), tuple_count);

  // Create logical tile from single base tile.
  storage::Tile *source_base_tile = tile_group->GetTile(0);
  const bool own_base_tiles = false;
  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapTiles({source_base_tile},
                                              own_base_tiles));

  // Pass through materialization executor.
  executor::MaterializationExecutor executor(nullptr, nullptr);
  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      ExecutorTestsUtil::ExecuteTile(&executor, source_logical_tile.release()));

  // Verify that logical tile is only made up of a single base tile.
  int num_cols = result_logical_tile->GetColumnCount();
  EXPECT_EQ(2, num_cols);
  storage::Tile *result_base_tile = result_logical_tile->GetBaseTile(0);
  EXPECT_THAT(result_base_tile, NotNull());
  EXPECT_TRUE(source_base_tile != result_base_tile);
  EXPECT_EQ(result_logical_tile->GetBaseTile(1), result_base_tile);

  // Check that the base tile has the correct values.
  for (int i = 0; i < tuple_count; i++) {
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 0)),
        result_base_tile->GetValue(i, 0));
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 1)),
        result_base_tile->GetValue(i, 1));

    // Double check that logical tile is functioning.
    EXPECT_EQ(result_base_tile->GetValue(i, 0),
              result_logical_tile->GetValue(i, 0));
    EXPECT_EQ(result_base_tile->GetValue(i, 1),
              result_logical_tile->GetValue(i, 1));
  }
}

// Materializing logical tile composed of two base tiles.
// The materialized tile's output columns are reordered.
// Also, one of the columns is dropped.
TEST(MaterializationTests, TwoBaseTilesWithReorderTest) {
  const int tuple_count = 9;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateTileGroup(tuple_count));

  ExecutorTestsUtil::PopulateTiles(tile_group.get(), tuple_count);

  // Create logical tile from two base tiles.
  const std::vector<storage::Tile *> source_base_tiles = {
      tile_group->GetTile(0), tile_group->GetTile(1)};
  const bool own_base_tiles = false;
  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapTiles(source_base_tiles,
                                              own_base_tiles));

  // Create materialization node for this test.
  // Construct output schema. We drop column 3 and reorder the others to 3,1,0.
  std::vector<catalog::Column> output_columns;
  // Note that Column 3 in the tile group is column 1 in the second tile.
  output_columns.push_back(source_base_tiles[1]->GetSchema()->GetColumn(1));
  output_columns.push_back(source_base_tiles[0]->GetSchema()->GetColumn(1));
  output_columns.push_back(source_base_tiles[0]->GetSchema()->GetColumn(0));
  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));

  // Construct mapping using the ordering mentioned above.
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  old_to_new_cols[3] = 0;
  old_to_new_cols[1] = 1;
  old_to_new_cols[0] = 2;
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan node(old_to_new_cols, output_schema.release(),
                                    physify_flag);

  // Pass through materialization executor.
  executor::MaterializationExecutor executor(&node, nullptr);
  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      ExecutorTestsUtil::ExecuteTile(&executor, source_logical_tile.release()));

  // Verify that logical tile is only made up of a single base tile.
  int num_cols = result_logical_tile->GetColumnCount();
  EXPECT_EQ(3, num_cols);
  storage::Tile *result_base_tile = result_logical_tile->GetBaseTile(0);
  EXPECT_THAT(result_base_tile, NotNull());
  EXPECT_EQ(result_base_tile, result_logical_tile->GetBaseTile(1));
  EXPECT_EQ(result_base_tile, result_logical_tile->GetBaseTile(2));

  // Check that the base tile has the correct values.
  for (int i = 0; i < tuple_count; i++) {
    // Output column 2.
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 0)),
        result_base_tile->GetValue(i, 2));
    // Output column 1.
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 1)),
        result_base_tile->GetValue(i, 1));
    // Output column 0.
    Value string_value(ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(i, 3))));
    EXPECT_EQ(string_value, result_base_tile->GetValue(i, 0));
    string_value.Free();

    // Double check that logical tile is functioning.
    EXPECT_EQ(result_base_tile->GetValue(i, 0),
              result_logical_tile->GetValue(i, 0));
    EXPECT_EQ(result_base_tile->GetValue(i, 1),
              result_logical_tile->GetValue(i, 1));
    EXPECT_EQ(result_base_tile->GetValue(i, 2),
              result_logical_tile->GetValue(i, 2));
  }
}

}  // namespace test
}  // namespace peloton
