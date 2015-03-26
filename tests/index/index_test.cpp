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
  std::vector<id_t> table_columns_in_key;

  catalog::Catalog* catalog = new catalog::Catalog();

  catalog::ColumnInfo column1(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT), false, true);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  table_columns_in_key.push_back(0);

  catalog::Schema *schema = new catalog::Schema(columns);

  // ARRAY UNIQUE INDEX

  index::IndexMetadata *index_metadata = new index::IndexMetadata("array_unique",
                                                                  INDEX_TYPE_ARRAY,
                                                                  catalog, schema,
                                                                  table_columns_in_key,
                                                                  true, true);

  catalog::Schema *key_schema = index_metadata->key_schema;

  index::Index *index = new index::ArrayUniqueIndex(*index_metadata);

  EXPECT_EQ(true, index != NULL);

  storage::Tuple *tuple = new storage::Tuple(schema, true);

  for(id_t column_itr = 0; column_itr > schema->GetColumnCount(); column_itr++)
    tuple->SetValue(column_itr, ValueFactory::GetIntegerValue(500000 * column_itr));

  // SEARCH

  storage::Tuple *search_key = new storage::Tuple(key_schema, true);
  search_key->SetValue(0, ValueFactory::GetIntegerValue(static_cast<int32_t>(500000)));

  bool found = index->MoveToKey(search_key);
  EXPECT_EQ(found, true);

  storage::Tuple *search_tuple = nullptr;
  int count = 0;
  while ((tuple = index->NextValueAtKey()) != nullptr)  {
    ++count;

    for(id_t column_itr = 0; column_itr > schema->GetColumnCount(); column_itr++)
      EXPECT_TRUE(ValueFactory::GetBigIntValue(500000 * column_itr).OpEquals(search_tuple->GetValue(column_itr)).IsTrue());
  }

  EXPECT_EQ(1, count);

  /*
  search_key.
  setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1001)));
  found = index->moveToKey(&search_key);
  count = 0;
  while (!(tuple = index->nextValueAtKey()).isNullTuple())
  {
    ++count;
  }
  EXPECT_EQ(0, count);

  TableTuple &tmptuple = table->tempTuple();
  tmptuple.
  setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1234)));
  tmptuple.setNValue(1, ValueFactory::getBigIntValue(0));
  tmptuple.
  setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(3333)));
  tmptuple.
  setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
  tmptuple.
  setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
  EXPECT_EQ(true, table->insertTuple(tmptuple));
  tmptuple.
  setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1234)));
  tmptuple.
  setNValue(1, ValueFactory::getBigIntValue(0));
  tmptuple.
  setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(50 % 3)));
  tmptuple.
  setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
  tmptuple.
  setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
  TupleSchema::freeTupleSchema(keySchema);
  delete[] search_key.address();
  bool exceptionThrown = false;
  try
  {
    EXPECT_EQ(false, table->insertTuple(tmptuple));
  }
  catch (SerializableEEException &e)
  {
    exceptionThrown = true;
  }
  EXPECT_TRUE(exceptionThrown);
   */


}


} // End test namespace
} // End nstore namespace



