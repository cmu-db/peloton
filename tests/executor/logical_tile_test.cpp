/*-------------------------------------------------------------------------
 *
 * catalog_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/catalog/catalog_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"

#include "harness.h"
#include "executor/logical_tile.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Logical Tile Tests
//===--------------------------------------------------------------------===//

TEST(LogicalTileTests, BasicTest) {

  // LOGICAL TILE

  catalog::Catalog *catalog = new catalog::Catalog();
  std::vector<catalog::ItemPointer> column_mapping;

  std::vector<catalog::ColumnInfo> columns;

  catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *schema = new catalog::Schema(columns);

  executor::LogicalTile *logical_tile = new executor::LogicalTile(catalog, 2, schema, column_mapping);

  catalog::ItemPointer logical_tuple1 = catalog::ItemPointer(1, 0);
  catalog::ItemPointer logical_tuple2 = catalog::ItemPointer(1, 1);

  std::vector<catalog::ItemPointer> tuple = {logical_tuple1, logical_tuple2};

  logical_tile->AppendTupleSet(tuple);

  std::cout << (*logical_tile) << "\n";

  delete logical_tile;
  delete schema;
  delete catalog;

}

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
  oid_t tile_id = base_tile->GetTileId();

  std::vector<catalog::ItemPointer> column_mapping;

  id_t column_count = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    column_mapping.push_back(catalog::ItemPointer(tile_id, column_itr));

  executor::LogicalTile *logical_tile = new executor::LogicalTile(catalog, 1, schema2, column_mapping);

  catalog::ItemPointer logical_tuple1 = catalog::ItemPointer(tile_id, 0);
  catalog::ItemPointer logical_tuple2 = catalog::ItemPointer(tile_id, 1);

  std::vector<catalog::ItemPointer> tuple_set_1 = {logical_tuple1};
  std::vector<catalog::ItemPointer> tuple_set_2 = {logical_tuple2};

  logical_tile->AppendTupleSet(tuple_set_1);
  logical_tile->AppendTupleSet(tuple_set_2);

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

  oid_t tile_id1 = base_tile1->GetTileId();
  oid_t tile_id2 = base_tile2->GetTileId();

  column_mapping.clear();

  column_count = schema1->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    column_mapping.push_back(catalog::ItemPointer(tile_id1, column_itr));

  column_count = schema2->GetColumnCount();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++)
    column_mapping.push_back(catalog::ItemPointer(tile_id2, column_itr));

  logical_tile = new executor::LogicalTile(catalog, 2, schema, column_mapping);

  catalog::ItemPointer logical_tuple11 = catalog::ItemPointer(tile_id1, 0);
  catalog::ItemPointer logical_tuple12 = catalog::ItemPointer(tile_id2, 0);
  catalog::ItemPointer logical_tuple21 = catalog::ItemPointer(tile_id1, 1);
  catalog::ItemPointer logical_tuple22 = catalog::ItemPointer(tile_id2, 1);

  tuple_set_1 = {logical_tuple11, logical_tuple12};
  tuple_set_2 = {logical_tuple21, logical_tuple22};

  logical_tile->AppendTupleSet(tuple_set_1);
  logical_tile->AppendTupleSet(tuple_set_2);

  std::cout << (*logical_tile) << "\n";

  storage::Tuple* found_tuple21 = logical_tile->GetTuple(1, 0);
  storage::Tuple* found_tuple22 = logical_tile->GetTuple(1, 1);

  EXPECT_EQ((*found_tuple21), (*found_tuple1));
  EXPECT_EQ((*found_tuple22), (*found_tuple2));

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE DISPLAY
  ////////////////////////////////////////////////////////////////

  std::cout << "Value : " << logical_tile->GetValue(0, 0, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(0, 0, 1) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 0, 0) << "\n";
  std::cout << "Value : " << logical_tile->GetValue(1, 0, 1) << "\n";

  delete logical_tile;

  delete tuple1;
  delete tuple2;
  delete schema;

  delete found_tuple1;
  delete found_tuple2;

  delete found_tuple21;
  delete found_tuple22;

  delete tile_group;
  delete catalog;
}

} // End test namespace
} // End nstore namespace

