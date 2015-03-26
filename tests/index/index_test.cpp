/*-------------------------------------------------------------------------
 *
 * index_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/index/index_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"
#include "harness.h"

#include "index/index_factory.h"
#include "index/array_unique_index.h"

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 1000
#define PKEY_ID 100
#define INT_UNIQUE_ID 101
#define INT_MULTI_ID 102
#define INTS_UNIQUE_ID 103
#define INTS_MULTI_ID 104

#define ARRAY_UNIQUE_ID 105

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

TEST(IndexTests, ArrayUniqueIndexTest) {

  std::vector<catalog::ColumnInfo> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Schema*> schemas;

  // SCHEMA

  catalog::ColumnInfo column1(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);

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

  storage::Tuple *tuple0 = new storage::Tuple(schema, true);
  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);

  for(id_t column_itr = 0; column_itr < schema->GetColumnCount(); column_itr++) {
    tuple0->SetValue(column_itr, ValueFactory::GetBigIntValue(100 * (column_itr + 1)));
    tuple1->SetValue(column_itr, ValueFactory::GetBigIntValue(200 * (column_itr + 1)));
    tuple2->SetValue(column_itr, ValueFactory::GetBigIntValue(300 * (column_itr + 1)));
  }

  // TRANSACTION

  txn_id_t txn_id = GetTransactionId();

  tile_group->InsertTuple(txn_id, tuple0);
  tile_group->InsertTuple(txn_id, tuple1);
  tile_group->InsertTuple(txn_id, tuple2);

  EXPECT_EQ(3, tile_group->GetActiveTupleCount());

  std::cout << (*tile_group);

  // ARRAY UNIQUE INDEX

  std::vector<id_t> table_columns_in_key;

  table_columns_in_key.push_back(0);

  index::IndexMetadata *index_metadata = new index::IndexMetadata("array_unique",
                                                                  INDEX_TYPE_ARRAY,
                                                                  catalog, schema,
                                                                  table_columns_in_key,
                                                                  true, true);

  catalog::Schema *key_schema = index_metadata->key_schema;

  index::Index *index = new index::ArrayUniqueIndex(*index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  id_t tile_id = tile_group->GetTileId(0);

  ItemPointer *item0 = new ItemPointer(tile_id, 0);
  ItemPointer *item1 = new ItemPointer(tile_id, 1);
  ItemPointer *item2 = new ItemPointer(tile_id, 2);

  index->AddEntry(item0);
  index->AddEntry(item1);
  index->AddEntry(item2);

  // SEARCH

  storage::Tuple *search_key = new storage::Tuple(key_schema, true);
  search_key->SetValue(0, ValueFactory::GetBigIntValue(static_cast<int32_t>(100)));

  bool found = index->MoveToKey(search_key);
  EXPECT_EQ(found, true);

  storage::Tuple *search_tuple = nullptr;
  int count = 0;
  while ((search_tuple = index->NextValueAtKey()) != nullptr)  {
    ++count;
    EXPECT_TRUE(ValueFactory::GetBigIntValue(100).OpEquals(search_tuple->GetValue(0)).IsTrue());
  }

  EXPECT_EQ(1, count);

  storage::Tuple *search_key2 = new storage::Tuple(key_schema, true);
  search_key2->SetValue(0, ValueFactory::GetBigIntValue(static_cast<int32_t>(150)));
  found = index->MoveToKey(search_key2);

  count = 0;
  while ((search_tuple = index->NextValueAtKey()) != nullptr)  {
    ++count;
  }

  EXPECT_EQ(0, count);

}


} // End test namespace
} // End nstore namespace



