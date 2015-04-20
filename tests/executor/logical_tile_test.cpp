/**
 * @brief Test cases for logical tile.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor_tests_util.h"

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "harness.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"

namespace nstore {
namespace test {

TEST(LogicalTileTests, TileMaterializationTest) {
  catalog::Manager manager;
  storage::VMBackend backend;

  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateSimpleTileGroup(
        &manager,
        &backend,
        4)); // Tuple count.

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  const bool allocate = true;
  storage::Tuple tuple1(schema.get(), allocate);
  storage::Tuple tuple2(schema.get(), allocate);

  tuple1.SetValue(0, ValueFactory::GetIntegerValue(1));
  tuple1.SetValue(1, ValueFactory::GetIntegerValue(1));
  tuple1.SetValue(2, ValueFactory::GetTinyIntValue(1));
  tuple1.SetValue(
      3, ValueFactory::GetStringValue("tuple 1", tile_group->GetTilePool(1)));

  tuple2.SetValue(0, ValueFactory::GetIntegerValue(2));
  tuple2.SetValue(1, ValueFactory::GetIntegerValue(2));
  tuple2.SetValue(2, ValueFactory::GetTinyIntValue(2));
  tuple2.SetValue(
      3, ValueFactory::GetStringValue("tuple 2", tile_group->GetTilePool(1)));

  // TRANSACTION
  txn_id_t txn_id = GetTransactionId();

  tile_group->InsertTuple(txn_id, &tuple1);
  tile_group->InsertTuple(txn_id, &tuple2);
  tile_group->InsertTuple(txn_id, &tuple1);


  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (1 BASE TILE)
  ////////////////////////////////////////////////////////////////

  // Don't transfer ownership of any base tile to logical tile.
  const bool own_base_tile = false;

  storage::Tile *base_tile = tile_group->GetTile(1);

  std::vector<id_t> position_list1 = { 0, 1 };
  std::vector<id_t> position_list2 = { 0, 1 };

  std::unique_ptr<executor::LogicalTile> logical_tile(
    executor::LogicalTileFactory::GetTile());

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));

  assert(tile_schemas.size() == 2);
  catalog::Schema *schema1 = &tile_schemas[0];
  catalog::Schema *schema2 = &tile_schemas[1];
  id_t column_count = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++) {
    logical_tile->AddColumn(base_tile, own_base_tile, column_itr, column_itr);
  }

  std::cout << (*logical_tile) << "\n";

  std::unique_ptr<storage::Tuple> found_tuple1(logical_tile->GetTuple(0, 0));
  std::unique_ptr<storage::Tuple> found_tuple2(logical_tile->GetTuple(0, 1));

  std::cout << (*found_tuple1);
  std::cout << (*found_tuple2);

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (2 BASE TILE)
  ////////////////////////////////////////////////////////////////

  logical_tile.reset(executor::LogicalTileFactory::GetTile());

  storage::Tile *base_tile1 = tile_group->GetTile(0);
  storage::Tile *base_tile2 = tile_group->GetTile(1);

  position_list1 = {0, 1};
  position_list2 = {0, 1};
  std::vector<id_t> position_list3 = {0, 1};
  std::vector<id_t> position_list4 = {0, 1};

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));
  logical_tile->AddPositionList(std::move(position_list3));
  logical_tile->AddPositionList(std::move(position_list4));

  id_t column_count1 = schema1->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count1; column_itr++) {
    logical_tile->AddColumn(base_tile1, own_base_tile, column_itr, column_itr);
  }

  id_t column_count2 = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count2; column_itr++) {
    logical_tile->AddColumn(
        base_tile2,
        own_base_tile,
        column_itr,
        column_count1 + column_itr);
  }

  std::cout << (*logical_tile) << "\n";

  std::unique_ptr<storage::Tuple> found_tuple12(logical_tile->GetTuple(2, 0));
  std::unique_ptr<storage::Tuple> found_tuple22(logical_tile->GetTuple(2, 1));

  EXPECT_EQ((*found_tuple12), (*found_tuple1));
  EXPECT_EQ((*found_tuple22), (*found_tuple2));

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE DISPLAY
  ////////////////////////////////////////////////////////////////

  std::cout << "Value : " << logical_tile->GetValue(0, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 1) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 1) << "\n";
}

} // End test namespace
} // End nstore namespace
