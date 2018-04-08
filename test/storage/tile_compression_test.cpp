//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_compression_test.cpp
//
// Identification: test/storage/tile_compression_test.cpp
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

class TileCompressionTests : public PelotonTest {};

TEST_F(TileCompressionTests, BasicTest) {
  // Columns
  std::vector<catalog::Column> columns;
  // Schema: Int, Varchar, Int, TinyInt, Varchar
  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::VARCHAR, 25, "B", false);
  catalog::Column column3(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "C", true);
  catalog::Column column4(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "D", true);
  catalog::Column column5(type::TypeId::VARCHAR, 25, "E", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);
  columns.push_back(column5);

  // Schema
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  // Allocated Tuple Count
  const int tuple_count = 6;

  std::unique_ptr<storage::TileGroupHeader> header(
      new storage::TileGroupHeader(BackendType::MM, tuple_count));

  std::unique_ptr<storage::Tile> tile(storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      header.get(), *schema, nullptr, tuple_count));

  std::unique_ptr<storage::Tuple> tuple1(new storage::Tuple(schema.get(),
                                                            true));
  std::unique_ptr<storage::Tuple> tuple2(new storage::Tuple(schema.get(),
                                                            true));
  std::unique_ptr<storage::Tuple> tuple3(new storage::Tuple(schema.get(),
                                                            true));
  auto pool = tile->GetPool();

  tuple1->SetValue(0, type::ValueFactory::GetIntegerValue(11), pool);
  tuple1->SetValue(1, type::ValueFactory::GetVarcharValue("bohan"), pool);
  tuple1->SetValue(2, type::ValueFactory::GetIntegerValue(12), pool);
  tuple1->SetValue(3, type::ValueFactory::GetTinyIntValue(13), pool);
  tuple1->SetValue(4, type::ValueFactory::GetVarcharValue("bohan again"),
                   pool);

  tuple2->SetValue(0, type::ValueFactory::GetIntegerValue(21), pool);
  tuple2->SetValue(1, type::ValueFactory::GetVarcharValue("ssy"), pool);
  tuple2->SetValue(2, type::ValueFactory::GetIntegerValue(22), pool);
  tuple2->SetValue(3, type::ValueFactory::GetTinyIntValue(23), pool);
  tuple2->SetValue(4, type::ValueFactory::GetVarcharValue("ssy again"), pool);

  tuple3->SetValue(0, type::ValueFactory::GetIntegerValue(31), pool);
  tuple3->SetValue(1, type::ValueFactory::GetVarcharValue("tao dai"), pool);
  tuple3->SetValue(2, type::ValueFactory::GetIntegerValue(32), pool);
  tuple3->SetValue(3, type::ValueFactory::GetTinyIntValue(33), pool);
  tuple3->SetValue(4, type::ValueFactory::GetVarcharValue("tao dai again"), pool);

  tile->InsertTuple(0, tuple1.get());
  tile->InsertTuple(1, tuple2.get());
  tile->InsertTuple(2, tuple3.get());
  
  // encode the tile
  tile->DictEncode();
  // decode the tile
  tile->DictDecode();

 
  // Check whether the decoded tile is same as the original tile.
  
  // check tuple 1
  type::Value val0 = (tile->GetValue(0, 0));
  type::Value val1 = (tile->GetValue(0, 1));
  type::Value val2 = (tile->GetValue(0, 2));
  type::Value val3 = (tile->GetValue(0, 3));
  type::Value val4 = (tile->GetValue(0, 4));

  type::Value cmp = type::ValueFactory::GetBooleanValue(
      (val0.CompareEquals(type::ValueFactory::GetIntegerValue(11))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val1.CompareEquals(type::ValueFactory::GetVarcharValue("bohan"))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val2.CompareEquals(type::ValueFactory::GetIntegerValue(12))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val3.CompareEquals(type::ValueFactory::GetTinyIntValue(13))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val4.CompareEquals(type::ValueFactory::GetVarcharValue("bohan again"))));
  EXPECT_TRUE(cmp.IsTrue());

  // check tuple 2
  val0 = (tile->GetValue(1, 0));
  val1 = (tile->GetValue(1, 1));
  val2 = (tile->GetValue(1, 2));
  val3 = (tile->GetValue(1, 3));
  val4 = (tile->GetValue(1, 4));

  cmp = type::ValueFactory::GetBooleanValue(
      (val0.CompareEquals(type::ValueFactory::GetIntegerValue(21))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val1.CompareEquals(type::ValueFactory::GetVarcharValue("ssy"))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val2.CompareEquals(type::ValueFactory::GetIntegerValue(22))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val3.CompareEquals(type::ValueFactory::GetTinyIntValue(23))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val4.CompareEquals(type::ValueFactory::GetVarcharValue("ssy again"))));
  EXPECT_TRUE(cmp.IsTrue());

  // check tuple 3
  val0 = (tile->GetValue(2, 0));
  val1 = (tile->GetValue(2, 1));
  val2 = (tile->GetValue(2, 2));
  val3 = (tile->GetValue(2, 3));
  val4 = (tile->GetValue(2, 4));

  cmp = type::ValueFactory::GetBooleanValue(
      (val0.CompareEquals(type::ValueFactory::GetIntegerValue(31))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val1.CompareEquals(type::ValueFactory::GetVarcharValue("tao dai"))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val2.CompareEquals(type::ValueFactory::GetIntegerValue(32))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val3.CompareEquals(type::ValueFactory::GetTinyIntValue(33))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val4.CompareEquals(type::ValueFactory::GetVarcharValue("tao dai again"))));
  EXPECT_TRUE(cmp.IsTrue());


}

}  // namespace test
}  // namespace peloton
