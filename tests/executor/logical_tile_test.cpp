/*-------------------------------------------------------------------------
 *
 * logical_tile_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/catalog/logical_tile_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"

#include "executor/logical_schema.h"
#include "executor/logical_tile.h"
#include "harness.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

#include <memory>

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Logical Tile Tests
//===--------------------------------------------------------------------===//

TEST(LogicalTileTests, TileMaterializationTest) {

  // PHYSICAL TILE

  std::vector<catalog::ColumnInfo> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Schema*> schemas;

  // SCHEMA

  catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *schema1 = new catalog::Schema(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema2 = new catalog::Schema(columns);
  schemas.push_back(schema2);

  catalog::Schema *schema = catalog::Schema::AppendSchema(schema1, schema2);

  // TILES

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");

  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");

  column_names.push_back(tile_column_names);

  // TILE GROUP

  catalog::Catalog *catalog = new catalog::Catalog();

  storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(schemas, 4, column_names, true, catalog);

  // TUPLES

  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);

  tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
  tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
  tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
  tuple1->SetValue(3, ValueFactory::GetStringValue("tuple 1", tile_group->GetTilePool(1)));

  tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
  tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
  tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
  tuple2->SetValue(3, ValueFactory::GetStringValue("tuple 2", tile_group->GetTilePool(1)));

  // TRANSACTION

  txn_id_t txn_id = GetTransactionId();

  tile_group->InsertTuple(txn_id, tuple1);
  tile_group->InsertTuple(txn_id, tuple2);
  tile_group->InsertTuple(txn_id, tuple1);


  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (1 BASE TILE)
  ////////////////////////////////////////////////////////////////


  storage::Tile *base_tile = tile_group->GetTile(1);

  std::unique_ptr<executor::LogicalSchema> logical_schema(
      new executor::LogicalSchema());
  id_t column_count = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    logical_schema->AddColumn(base_tile, column_itr);

  executor::LogicalTile *logical_tile =
      new executor::LogicalTile(std::move(logical_schema));

  std::vector<id_t> position_tuple1 = { 0, 0 };
  std::vector<id_t> position_tuple2 = { 1, 1 };

  logical_tile->AppendPositionTuple(position_tuple1);
  logical_tile->AppendPositionTuple(position_tuple2);

  std::cout << (*logical_tile) << "\n";

  storage::Tuple* found_tuple1 = logical_tile->GetTuple(0, 0);
  storage::Tuple* found_tuple2 = logical_tile->GetTuple(0, 1);

  std::cout << (*found_tuple1);
  std::cout << (*found_tuple2);

  delete logical_tile;

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (2 BASE TILE)
  ////////////////////////////////////////////////////////////////

  storage::Tile *base_tile1 = tile_group->GetTile(0);
  storage::Tile *base_tile2 = tile_group->GetTile(1);

  logical_schema.reset(new executor::LogicalSchema());

  column_count = schema1->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    logical_schema->AddColumn(base_tile1, column_itr);

  column_count = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    logical_schema->AddColumn(base_tile2, column_itr);

  logical_tile = new executor::LogicalTile(std::move(logical_schema));

  position_tuple1 = {0, 0, 0, 0};
  position_tuple2 = {1, 1, 1, 1};

  logical_tile->AppendPositionTuple(position_tuple1);
  logical_tile->AppendPositionTuple(position_tuple2);

  std::cout << (*logical_tile) << "\n";

  storage::Tuple* found_tuple12 = logical_tile->GetTuple(2, 0);
  storage::Tuple* found_tuple22 = logical_tile->GetTuple(2, 1);

  EXPECT_EQ((*found_tuple12), (*found_tuple1));
  EXPECT_EQ((*found_tuple22), (*found_tuple2));

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE DISPLAY
  ////////////////////////////////////////////////////////////////

  std::cout << "Value : " << logical_tile->GetValue(0, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 1) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 1) << "\n";

  delete logical_tile;

  delete tuple1;
  delete tuple2;
  delete schema;

  delete found_tuple1;
  delete found_tuple2;

  delete found_tuple12;
  delete found_tuple22;

  delete tile_group;
  delete catalog;
}

} // End test namespace
} // End nstore namespace

