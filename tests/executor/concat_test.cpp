/**
 * @brief Test cases for concat node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "common/value.h"
#include "common/value_factory.h"
#include "executor/concat_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/concat_node.h"
#include "storage/backend_vm.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

#include "executor/executor_tests_util.h"

namespace nstore {
namespace test {

// Add two columns to existing logical tile.
TEST(ConcatTests, TwoColsAddedTest) {
  storage::VMBackend backend;
  const int tuple_count = 9;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateSimpleTileGroup(
          &backend,
          tuple_count));

  ExecutorTestsUtil::PopulateTiles(tile_group.get(), tuple_count);

  // Create logical tile from single base tile.
  storage::Tile *source_base_tile = tile_group->GetTile(0);
  const bool own_base_tiles = false;
  std::unique_ptr<executor::LogicalTile> source_logical_tile(
      executor::LogicalTileFactory::WrapBaseTiles(
          { source_base_tile },
          own_base_tiles));

  std::cout << (*(source_logical_tile.get()));

  assert(source_logical_tile->NumCols() == 2);

  // Set up catalog to map the base tile oid to the pointer.
  auto &locator = catalog::Manager::GetInstance().locator;
  locator[1] = tile_group->GetTile(1);

  // Create concat node for this test.
  planner::ConcatNode::ColumnPointer cp1;
  cp1.position_list_idx = 0;
  cp1.base_tile_oid = 1;
  cp1.origin_column_id = 0;

  planner::ConcatNode::ColumnPointer cp2;
  cp2.position_list_idx = 0;
  cp2.base_tile_oid = 1;
  cp2.origin_column_id = 1;

  planner::ConcatNode node(
      std::vector<planner::ConcatNode::ColumnPointer>({ cp1, cp2 }));

  // Pass through concat executor.
  executor::ConcatExecutor executor(&node);

  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      ExecutorTestsUtil::ExecuteTile(&executor, source_logical_tile.release()));

  std::cout << (*(result_logical_tile.get()));

  // Verify that logical tile has two new columns and that they have the
  // correct values.
  EXPECT_EQ(4, result_logical_tile->NumCols());
  for (int i = 0; i < tuple_count; i++) {
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 0)),
        result_logical_tile->GetValue(i, 0));
    EXPECT_EQ(
        ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(i, 1)),
        result_logical_tile->GetValue(i, 1));
    EXPECT_EQ(
        ValueFactory::GetTinyIntValue(ExecutorTestsUtil::PopulatedValue(i, 2)),
        result_logical_tile->GetValue(i, 2));
    Value string_value(ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(i, 3))));
    EXPECT_EQ(string_value, result_logical_tile->GetValue(i, 3));
    string_value.FreeUninlinedData();
  }
}

} // namespace test
} // namespace nstore
