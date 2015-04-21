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
#include <utility>
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
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "storage/vm_backend.h"

namespace nstore {
namespace test {

TEST(MaterializationTests, SingleBaseTileTest) {
  catalog::Manager manager;
  storage::VMBackend backend;
  const int tuple_count = 9;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateSimpleTileGroup(
        &manager,
        &backend,
        tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
    catalog::Schema::AppendSchemaList(tile_schemas));

  // Ensure that the tile group created by ExecutorTestsUtil is as expected.
  assert(tile_schemas.size() == 2);
  assert(schema->GetColumnCount() == 4);

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
  //TODO This should be deleted soon...
  std::unique_ptr<catalog::Schema> output_schema(catalog::Schema::CopySchema(
      &base_tile->GetSchema()));
  std::unordered_map<id_t, id_t> old_to_new_cols;
  std::vector<std::string> column_names;

  unsigned int column_count = output_schema->GetColumnCount();
  for (id_t col = 0; col < column_count; col++) {
    old_to_new_cols[col] = col;
    // TODO Why do we need to provide column names when it's alr in the schema?
    column_names.push_back(output_schema->GetColumnInfo(col).name);
  }
  planner::MaterializationNode(
      std::move(old_to_new_cols),
      std::move(column_names),
      output_schema.release());

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
