//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_test.cpp
//
// Identification: test/storage/tile_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tuple_iterator.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Tests
//===--------------------------------------------------------------------===//

class TileTests : public PelotonTest {};

TEST_F(TileTests, BasicTest) {
  // Columns
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 25, "D", false);
  catalog::Column column5(type::TypeId::VARCHAR, 25, "E", false);

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

  storage::TileGroupHeader *header =
      new storage::TileGroupHeader(BackendType::MM, tuple_count);

  storage::Tile *tile = storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      header, *schema, nullptr, tuple_count);

  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);
  storage::Tuple *tuple3 = new storage::Tuple(schema, true);
  auto pool = tile->GetPool();

  tuple1->SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
  tuple1->SetValue(3, type::ValueFactory::GetVarcharValue("vivek sengupta"), pool);
  tuple1->SetValue(4, type::ValueFactory::GetVarcharValue("vivek sengupta again"),
                   pool);

  tuple2->SetValue(0, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(1, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(2, type::ValueFactory::GetTinyIntValue(2), pool);
  tuple2->SetValue(3, type::ValueFactory::GetVarcharValue("ming fang"), pool);
  tuple2->SetValue(4, type::ValueFactory::GetVarcharValue("ming fang again"), pool);

  tuple3->SetValue(0, type::ValueFactory::GetIntegerValue(3), pool);
  tuple3->SetValue(1, type::ValueFactory::GetIntegerValue(3), pool);
  tuple3->SetValue(2, type::ValueFactory::GetTinyIntValue(3), pool);
  tuple3->SetValue(3, type::ValueFactory::GetVarcharValue("jinwoong kim"), pool);
  tuple3->SetValue(4, type::ValueFactory::GetVarcharValue("jinwoong kim again"), pool);

  tile->InsertTuple(0, tuple1);
  tile->InsertTuple(1, tuple2);
  tile->InsertTuple(2, tuple3);

  delete tuple1;
  delete tuple2;
  delete tuple3;
  delete tile;
  delete header;
  delete schema;
}

}  // namespace test
}  // namespace peloton
