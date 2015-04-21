/**
 * @brief Test cases for materialization node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor_tests_util.h"
#include "harness.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/abstract_plan_node.h"
#include "planner/materialization_node.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "storage/vm_backend.h"

namespace nstore {
namespace test {

TEST(MaterializationTests, SingleBaseTileTest) {
  storage::VMBackend backend;
  const int tuple_count = 9;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateSimpleTileGroup(
        &backend,
        tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
    catalog::Schema::AppendSchemaList(tile_schemas));

  // Insert tuples into tile_group.
  const bool allocate = true;
  const txn_id_t txn_id = GetTransactionId();
  for (int i = 0; i < tuple_count; i++) {
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

  //TODO Everything above here should be refactored into executor tests util.

  // Create logical tile from single base tile.
  storage::Tile *base_tile = tile_group->GetTile(0);
  const bool own_base_tile = false;
  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::WrapBaseTile(base_tile, own_base_tile));

  // Create materialization node.
  std::vector<planner::AbstractPlanNode *> children;
  std::unordered_map<id_t, id_t> old_to_new_cols;
  std::vector<std::string> column_names;
  catalog::Schema *node_schema; //TODO Get schema from base tile.
  (void) children;
  (void) old_to_new_cols;
  (void) column_names;
  (void) node_schema;

  // Pass them through materialization executor.
  // TODO Use GMock.

  // Verify that materialized tile is correct.
  //TODO This should be moved to ExecutorTestsUtil.
}

TEST(MaterializationTests, TwoBaseTilesTest) {
  //TODO Implement test case.
}

} // namespace test
} // namespace nstore
