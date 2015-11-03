//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group_iterator_test.cpp
//
// Identification: tests/storage/tile_group_iterator_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "gtest/gtest.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/tile_group_iterator.h"

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TileGroupIterator Tests
//===--------------------------------------------------------------------===//

TEST(TileGroupIteratorTests, BasicTest) {
  int num_cols = 10;

  std::vector<catalog::Column> columns;
  std::vector<std::string> column_names;

  for (int i = 0; i < num_cols; i++) {
    std::stringstream name;
    name << "COL_" << i;

    catalog::Column col(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "A", true);
    columns.push_back(col);
    column_names.push_back(name.str());
  }  // FOR

  // Schema
  catalog::Schema *schema = new catalog::Schema(columns);

  //     storage::DataTable *table = storage::TableFactory::GetDataTable(
  //         INVALID_OID,
  //         INVALID_OID,
  //         schema,
  //         "XYZ",
  //         1);

  // Allocated Tuple Count
  const int tuple_count = 6;

  storage::TileGroupHeader *header =
      new storage::TileGroupHeader(tuple_count);

  storage::Tile *tile = storage::TileFactory::GetTile(
      INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID, header,
      *schema, nullptr, tuple_count);

  for (int i = 0; i < tuple_count; i++) {
    storage::Tuple *tuple = new storage::Tuple(schema, true);
    tuple->SetValue(0, ValueFactory::GetIntegerValue(i));
    tile->InsertTuple(0, tuple);
    delete tuple;
  }  // FOR

  //     storage::TileGroupIterator itr(table);
  //     std::shared_ptr<storage::TileGroup> itrPtr;
  int found = 0;
  int expected = 0;  // 1;
                     //     while (itr.Next(itrPtr)) {
                     //         if (itrPtr.get() != nullptr) {
                     //             found += 1;
                     //         }
                     //     } // WHILE

  EXPECT_EQ(expected, found);

  delete tile;
  delete header;
  delete schema;
}

}  // End test namespace
}  // End peloton namespace
