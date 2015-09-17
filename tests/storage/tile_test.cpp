//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_test.cpp
//
// Identification: tests/storage/tile_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple_iterator.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Tests
//===--------------------------------------------------------------------===//

TEST(TileTests, BasicTest) {
  // Columns
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  catalog::Column column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_VARCHAR, 25, "D", false);
  catalog::Column column5(VALUE_TYPE_VARCHAR, 25, "E", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);
  columns.push_back(column5);

  // Schema
  catalog::Schema *schema = new catalog::Schema(columns);

  // Column Names
  std::vector<std::string> column_names;

  column_names.push_back("COL 1");
  column_names.push_back("COL 2");
  column_names.push_back("COL 3");
  column_names.push_back("COL 4");
  column_names.push_back("COL 5");

  // Allocated Tuple Count
  const int tuple_count = 6;

  storage::AbstractBackend *backend = new storage::VMBackend();
  storage::TileGroupHeader *header =
      new storage::TileGroupHeader(backend, tuple_count);

  storage::Tile *tile = storage::TileFactory::GetTile(
      INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID, header, backend,
      *schema, nullptr, tuple_count);

  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);
  storage::Tuple *tuple3 = new storage::Tuple(schema, true);

  tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
  tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
  tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
  tuple1->SetValue(
      3, ValueFactory::GetStringValue("vivek sengupta", tile->GetPool()));
  tuple1->SetValue(
      4, ValueFactory::GetStringValue("vivek sengupta again", tile->GetPool()));

  tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
  tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
  tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
  tuple2->SetValue(3,
                   ValueFactory::GetStringValue("ming fang", tile->GetPool()));
  tuple2->SetValue(
      4, ValueFactory::GetStringValue("ming fang again", tile->GetPool()));

  tuple3->SetValue(0, ValueFactory::GetIntegerValue(3));
  tuple3->SetValue(1, ValueFactory::GetIntegerValue(3));
  tuple3->SetValue(2, ValueFactory::GetTinyIntValue(3));
  tuple3->SetValue(
      3, ValueFactory::GetStringValue("jinwoong kim", tile->GetPool()));
  tuple3->SetValue(
      4, ValueFactory::GetStringValue("jinwoong kim again", tile->GetPool()));

  tile->InsertTuple(0, tuple1);
  tile->InsertTuple(1, tuple2);
  tile->InsertTuple(2, tuple3);

  delete tuple1;
  delete tuple2;
  delete tuple3;
  delete tile;
  delete header;
  delete schema;
  delete backend;
}

}  // End test namespace
}  // End peloton namespace
