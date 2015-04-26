/**
 * @brief Test cases for concat node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

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
}

} // namespace test
} // namespace nstore
