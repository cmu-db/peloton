//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_test.cpp
//
// Identification: tests/index/index_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

void PrintSlots(const std::vector<ItemPointer> &slots) {
  std::cout << "SLOTS :: " << slots.size() << "\n";
  for (auto item : slots) std::cout << item.block << " " << item.offset << "\n";
}

TEST(IndexTests, BtreeIndexTest) {
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;

  // SCHEMA

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_VARCHAR, 25, "B", true);
  catalog::Column column3(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "D", true);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0, 1});

  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *tuple_schema = new catalog::Schema(columns);

  // BTREE Multi INDEX
  bool unique_keys = false;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "btree_index", 125, INDEX_TYPE_BTREE, INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, unique_keys);
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  storage::Tuple *key0 = new storage::Tuple(key_schema, true);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);
  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);
  storage::Tuple *keynonce = new storage::Tuple(key_schema, true);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key3->SetValue(0, ValueFactory::GetIntegerValue(400), pool);
  key4->SetValue(0, ValueFactory::GetIntegerValue(500), pool);
  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000), pool);

  key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
  key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
  key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);
  key3->SetValue(1, ValueFactory::GetStringValue("d"), pool);
  key4->SetValue(1, ValueFactory::GetStringValue("e"), pool);
  keynonce->SetValue(1, ValueFactory::GetStringValue("f"), pool);

  ItemPointer item0(120, 5);
  ItemPointer item1(120, 7);
  ItemPointer item2(123, 19);

  index->InsertEntry(key0, item0);

  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item2);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item0);

  index->InsertEntry(key2, item1);
  index->InsertEntry(key3, item1);
  index->InsertEntry(key4, item1);

  auto locations = index->Scan(keynonce);
  EXPECT_EQ(locations.size(), 0);
  locations = index->Scan(key0);
  EXPECT_EQ(locations.size(), 1);

  // EXPECT_EQ(location.block, item0.block);

  LOG_TRACE("Delete \n");

  index->DeleteEntry(key0, item0);
  index->DeleteEntry(key1, item1);
  index->DeleteEntry(key2, item2);
  index->DeleteEntry(key3, item1);
  index->DeleteEntry(key4, item1);

  locations = index->Scan(key0);
  EXPECT_EQ(locations.size(), 0);

  locations = index->Scan(key1);
  EXPECT_EQ(locations.size(), 2);

  locations = index->Scan(key2);
  EXPECT_EQ(locations.size(), 1);

  // EXPECT_EQ(location.block, INVALID_OID);

  delete key0;
  delete key1;
  delete key2;
  delete key3;
  delete key4;
  delete keynonce;

  delete tuple_schema;

  delete index;
}

}  // End test namespace
}  // End peloton namespace
