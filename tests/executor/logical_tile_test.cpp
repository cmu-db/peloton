//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logical_tile_test.cpp
//
// Identification: tests/executor/logical_tile_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value_factory.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logical Tile Tests
//===--------------------------------------------------------------------===//

TEST(LogicalTileTests, TileMaterializationTest) {
  const int tuple_count = 4;
  std::unique_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateTileGroup(tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Create tuples and insert them into tile group.
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

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  txn_id_t txn_id = txn->GetTransactionId();

  tile_group->InsertTuple(txn_id, &tuple1);
  tile_group->InsertTuple(txn_id, &tuple2);
  tile_group->InsertTuple(txn_id, &tuple1);

  txn_manager.CommitTransaction();

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (1 BASE TILE)
  ////////////////////////////////////////////////////////////////

  // Don't transfer ownership of any base tile to logical tile.
  storage::Tile *base_tile = tile_group->GetTile(1);

  std::vector<oid_t> position_list1 = {0, 1};
  std::vector<oid_t> position_list2 = {0, 1};

  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::GetTile());

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));

  assert(tile_schemas.size() == 2);
  catalog::Schema *schema1 = &tile_schemas[0];
  catalog::Schema *schema2 = &tile_schemas[1];
  oid_t column_count = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    logical_tile->AddColumn(base_tile, column_itr, column_itr);
  }

  std::cout << (*logical_tile) << "\n";

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (2 BASE TILE)
  ////////////////////////////////////////////////////////////////

  logical_tile.reset(executor::LogicalTileFactory::GetTile());

  storage::Tile *base_tile1 = tile_group->GetTile(0);
  storage::Tile *base_tile2 = tile_group->GetTile(1);

  position_list1 = {0, 1};
  position_list2 = {0, 1};
  std::vector<oid_t> position_list3 = {0, 1};
  std::vector<oid_t> position_list4 = {0, 1};

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));
  logical_tile->AddPositionList(std::move(position_list3));
  logical_tile->AddPositionList(std::move(position_list4));

  oid_t column_count1 = schema1->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count1; column_itr++) {
    logical_tile->AddColumn(base_tile1, column_itr, column_itr);
  }

  oid_t column_count2 = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count2; column_itr++) {
    logical_tile->AddColumn(base_tile2, column_itr,
                            column_count1 + column_itr);
  }

  std::cout << (*logical_tile) << "\n";

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE DISPLAY
  ////////////////////////////////////////////////////////////////

  std::cout << "Value : " << logical_tile->GetValue(0, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 1) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 2) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 3) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 1) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 2) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 3) << "\n";
}

}  // End test namespace
}  // End peloton namespace
