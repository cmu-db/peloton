//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// materialization_test.cpp
//
// Identification: test/executor/materialization_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

#include "executor/mock_executor.h"

using ::testing::NotNull;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Materialization Tests
//===--------------------------------------------------------------------===//

class MaterializationTests : public PelotonTest {};

// "Pass-through" test case. There is nothing to materialize as
// there is only one base tile in the logical tile.
TEST_F(MaterializationTests, SingleBaseTileTest) {
  const int tuple_count = 9;
  std::shared_ptr<storage::TileGroup> tile_group(
      TestingExecutorUtil::CreateTileGroup(tuple_count));

  TestingExecutorUtil::PopulateTiles(tile_group, tuple_count);

  // Create logical tile from single base tile.
  auto source_base_tile = tile_group->GetTileReference(0);

  // Add a reference because we are going to wrap around it and we don't own it
  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapTiles({source_base_tile}));

  // Pass through materialization executor.
  executor::MaterializationExecutor executor(nullptr, nullptr);
  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      TestingExecutorUtil::ExecuteTile(&executor, source_logical_tile.release()));

  // Verify that logical tile is only made up of a single base tile.
  int num_cols = result_logical_tile->GetColumnCount();
  EXPECT_EQ(2, num_cols);
  storage::Tile *result_base_tile = result_logical_tile->GetBaseTile(0);
  EXPECT_THAT(result_base_tile, NotNull());
  EXPECT_TRUE(source_base_tile.get() == result_base_tile);
  EXPECT_EQ(result_logical_tile->GetBaseTile(1), result_base_tile);

  // Check that the base tile has the correct values.
  for (int i = 0; i < tuple_count; i++) {
    type::Value val0 = (result_base_tile->GetValue(i, 0));
    type::Value val1 = (result_base_tile->GetValue(i, 1));
    CmpBool cmp = (val0.CompareEquals(
      type::ValueFactory::GetIntegerValue(TestingExecutorUtil::PopulatedValue(i, 0))));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
    cmp = val1.CompareEquals(type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 1)));
    EXPECT_TRUE(cmp == CmpBool::TRUE);

    // Double check that logical tile is functioning.
    type::Value logic_val0 = (result_logical_tile->GetValue(i, 0));
    type::Value logic_val1 = (result_logical_tile->GetValue(i, 1));
    cmp = (logic_val0.CompareEquals(val0));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
    cmp = (logic_val1.CompareEquals(val1));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
  }
}

// Materializing logical tile composed of two base tiles.
// The materialized tile's output columns are reordered.
// Also, one of the columns is dropped.
TEST_F(MaterializationTests, TwoBaseTilesWithReorderTest) {
  const int tuple_count = 9;
  std::shared_ptr<storage::TileGroup> tile_group(
      TestingExecutorUtil::CreateTileGroup(tuple_count));

  TestingExecutorUtil::PopulateTiles(tile_group, tuple_count);

  // Create logical tile from two base tiles.
  const std::vector<std::shared_ptr<storage::Tile> > source_base_tiles = {
      tile_group->GetTileReference(0), tile_group->GetTileReference(1)};

  // Add a reference because we are going to wrap around it and we don't own it
  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapTiles(source_base_tiles));

  // Create materialization node for this test.
  // Construct output schema. We drop column 3 and reorder the others to 3,1,0.
  std::vector<catalog::Column> output_columns;
  // Note that Column 3 in the tile group is column 1 in the second tile.
  output_columns.push_back(source_base_tiles[1]->GetSchema()->GetColumn(1));
  output_columns.push_back(source_base_tiles[0]->GetSchema()->GetColumn(1));
  output_columns.push_back(source_base_tiles[0]->GetSchema()->GetColumn(0));
  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));

  // Construct mapping using the ordering mentioned above.
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  old_to_new_cols[3] = 0;
  old_to_new_cols[1] = 1;
  old_to_new_cols[0] = 2;
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan node(old_to_new_cols, output_schema,
                                    physify_flag);

  // Pass through materialization executor.
  executor::MaterializationExecutor executor(&node, nullptr);
  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      TestingExecutorUtil::ExecuteTile(&executor, source_logical_tile.release()));

  // Verify that logical tile is only made up of a single base tile.
  int num_cols = result_logical_tile->GetColumnCount();
  EXPECT_EQ(3, num_cols);
  storage::Tile *result_base_tile = result_logical_tile->GetBaseTile(0);
  EXPECT_THAT(result_base_tile, NotNull());
  EXPECT_EQ(result_base_tile, result_logical_tile->GetBaseTile(1));
  EXPECT_EQ(result_base_tile, result_logical_tile->GetBaseTile(2));

  // Check that the base tile has the correct values.
  for (int i = 0; i < tuple_count; i++) {
    type::Value val0(result_base_tile->GetValue(i, 0));
    type::Value val1(result_base_tile->GetValue(i, 1));
    type::Value val2(result_base_tile->GetValue(i, 2));
    // Output column 2.
    CmpBool cmp(val2.CompareEquals(
      type::ValueFactory::GetIntegerValue(TestingExecutorUtil::PopulatedValue(i, 0))));
    EXPECT_TRUE(cmp == CmpBool::TRUE);

    // Output column 1.
    cmp = (val1.CompareEquals(type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 1))));
    EXPECT_TRUE(cmp == CmpBool::TRUE);

    // Output column 0.
    cmp = (val0.CompareEquals(type::ValueFactory::GetVarcharValue(
        std::to_string(TestingExecutorUtil::PopulatedValue(i, 3)))));
    EXPECT_TRUE(cmp == CmpBool::TRUE);

    // Double check that logical tile is functioning.
    type::Value logic_val0 = (result_logical_tile->GetValue(i, 0));
    type::Value logic_val1 = (result_logical_tile->GetValue(i, 1));
    type::Value logic_val2 = (result_logical_tile->GetValue(i, 2));
    cmp = (logic_val0.CompareEquals(val0));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
    cmp = (logic_val1.CompareEquals(val1));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
    cmp = (logic_val2.CompareEquals(val2));
    EXPECT_TRUE(cmp == CmpBool::TRUE);
  }
}

}  // namespace test
}  // namespace peloton
