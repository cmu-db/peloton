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

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

TEST(IndexTests, BtreeMultimapIndexTest) {

  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::ColumnInfo> columns;
  std::vector<catalog::Schema*> schemas;

  // SCHEMA

  catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_VARCHAR, 25, "B", false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "C", false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "D", false, true);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *key_schema = new catalog::Schema(columns);

  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *tuple_schema = new catalog::Schema(columns);

  catalog::Manager *catalog = new catalog::Manager();

  // BTREE INDEX

  index::IndexMetadata index_metadata("btree_index", INDEX_TYPE_BTREE_MULTIMAP,
                                      catalog, tuple_schema, key_schema, true);


  storage::VMBackend *backend = new storage::VMBackend();
  Pool *pool = new Pool(backend);

  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  storage::Tuple *key0 = new storage::Tuple(key_schema, true);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);
  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);
  storage::Tuple *keynonce = new storage::Tuple(key_schema, true);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100));
  key1->SetValue(0, ValueFactory::GetIntegerValue(100));
  key2->SetValue(0, ValueFactory::GetIntegerValue(100));
  key3->SetValue(0, ValueFactory::GetIntegerValue(400));
  key4->SetValue(0, ValueFactory::GetIntegerValue(500));
  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000));

  key0->SetValue(1, ValueFactory::GetStringValue("a", pool));
  key1->SetValue(1, ValueFactory::GetStringValue("b", pool));
  key2->SetValue(1, ValueFactory::GetStringValue("c", pool));
  key3->SetValue(1, ValueFactory::GetStringValue("d", pool));
  key4->SetValue(1, ValueFactory::GetStringValue("e", pool));
  keynonce->SetValue(1, ValueFactory::GetStringValue("f", pool));

  ItemPointer item0(120, 5);
  ItemPointer item1(120, 7);
  ItemPointer item2(123, 19);

  index->InsertEntry(key0, item0);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item2);
  index->InsertEntry(key2, item1);
  index->InsertEntry(key3, item1);
  index->InsertEntry(key4, item1);

  EXPECT_EQ(false, index->Exists(keynonce));
  EXPECT_EQ(true, index->Exists(key0));

  index->GetLocationsForKey(key1);
  index->GetLocationsForKey(key0);

  index->GetLocationsForKeyBetween(key1, key3);

  index->GetLocationsForKeyLT(key3);
  index->GetLocationsForKeyLTE(key3);

  index->GetLocationsForKeyGT(key1);
  index->GetLocationsForKeyGTE(key1);

  index->Scan();

  index->DeleteEntry(key0);
  index->DeleteEntry(key1);
  index->DeleteEntry(key2);
  index->DeleteEntry(key3);
  index->DeleteEntry(key4);

  EXPECT_EQ(false, index->Exists(key0));

  index->Scan();

  delete key0;
  delete key1;
  delete key2;
  delete key3;
  delete key4;
  delete keynonce;

  delete pool;
  delete backend;

  delete tuple_schema;
  delete key_schema;
  delete catalog;

  delete index;
}


} // End test namespace
} // End nstore namespace



