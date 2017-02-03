//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tests_util.cpp
//
// Identification: test/index/index_tests_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/testing_index_util.h"

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "index/index.h"
#include "storage/tuple.h"
#include "type/types.h"

namespace peloton {
namespace test {

std::shared_ptr<ItemPointer> TestingIndexUtil::item0(new ItemPointer(120, 5));
std::shared_ptr<ItemPointer> TestingIndexUtil::item1(new ItemPointer(120, 7));
std::shared_ptr<ItemPointer> TestingIndexUtil::item2(new ItemPointer(123, 19));

index::Index *TestingIndexUtil::BuildIndex(const IndexType index_type,
                                           const bool unique_keys) {
  LOG_DEBUG("Build index type: %s", IndexTypeToString(index_type).c_str());

  catalog::Schema *key_schema = nullptr;
  catalog::Schema *tuple_schema = nullptr;

  // Build tuple and key schema
  std::vector<catalog::Column> column_list;

  // The following key are both in index key and tuple key and they are
  // indexed
  // The size of the key is:
  //   integer 4 + varchar 8 = total 12

  catalog::Column column1(type::Type::INTEGER,
                          type::Type::GetTypeSize(type::Type::INTEGER), "A",
                          true);

  catalog::Column column2(type::Type::VARCHAR, 1024, "B", false);

  // The following two constitutes tuple schema but does not appear in index

  catalog::Column column3(type::Type::DECIMAL,
                          type::Type::GetTypeSize(type::Type::DECIMAL), "C",
                          true);

  catalog::Column column4(type::Type::INTEGER,
                          type::Type::GetTypeSize(type::Type::INTEGER), "D",
                          true);

  // Use the first two columns to build key schema

  column_list.push_back(column1);
  column_list.push_back(column2);

  // This will be copied into the key schema as well as into the IndexMetadata
  // object to identify indexed columns
  std::vector<oid_t> key_attrs = {0, 1};

  key_schema = new catalog::Schema(column_list);
  key_schema->SetIndexedColumns(key_attrs);

  // Use all four columns to build tuple schema

  column_list.push_back(column3);
  column_list.push_back(column4);

  tuple_schema = new catalog::Schema(column_list);

  // Build index metadata
  //
  // NOTE: Since here we use a relatively small key (size = 12)
  // so index_test is only testing with a certain kind of key
  // (most likely, GenericKey)
  //
  // For testing IntsKey and TupleKey we need more test cases
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "test_index", 125,  // Index oid
      INVALID_OID, INVALID_OID, index_type, IndexConstraintType::DEFAULT,
      tuple_schema, key_schema, key_attrs, unique_keys);

  // Build index
  //
  // The type of index key has been chosen inside this function, but we are
  // unable to know the exact type of key it has chosen
  //
  // The index interface always accept tuple key from the external world
  // and transforms into the correct index key format, so the caller
  // do not need to worry about the actual implementation of the index
  // key, and only passing tuple key suffices
  index::Index *index = index::IndexFactory::GetIndex(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);

  return index;
}

void TestingIndexUtil::InsertTest(index::Index *index, type::AbstractPool *pool,
                                  size_t scale_factor,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Insert a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> keynonce(
        new storage::Tuple(key_schema, true));

    key0->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
    key1->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
    key2->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);
    key3->SetValue(0, type::ValueFactory::GetIntegerValue(400 * scale_itr),
                   pool);
    key3->SetValue(1, type::ValueFactory::GetVarcharValue("d"), pool);
    key4->SetValue(0, type::ValueFactory::GetIntegerValue(500 * scale_itr),
                   pool);
    key4->SetValue(
        1, type::ValueFactory::GetVarcharValue(StringUtil::Repeat("e", 1000)),
        pool);
    keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000 * scale_itr),
                       pool);
    keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

    // INSERT
    // key0 1 (100, a)   item0
    // key1 5  (100, b)  item1 2 1 1 0
    // key2 1 (100, c) item 1
    // key3 1 (400, d) item 1
    // key4 1  (500, eeeeee...) item 1
    // no keyonce (1000, f)

    // item0 = 2
    // item1 = 6
    // item2 = 1
    index->InsertEntry(key0.get(), item0.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item2.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item0.get());

    index->InsertEntry(key2.get(), item1.get());
    index->InsertEntry(key3.get(), item1.get());
    index->InsertEntry(key4.get(), item1.get());
  }
}

}  // namespace test
}  // namespace peloton
