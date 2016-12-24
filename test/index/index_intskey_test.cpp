//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_test.cpp
//
// Identification: test/index/index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "common/logger.h"
#include "common/platform.h"
#include "index/index_factory.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index IntsKey Tests
//===--------------------------------------------------------------------===//

class IndexIntsKeyTests : public PelotonTest {};

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

std::shared_ptr<ItemPointer> item0(new ItemPointer(120, 5));
std::shared_ptr<ItemPointer> item1(new ItemPointer(120, 7));
std::shared_ptr<ItemPointer> item2(new ItemPointer(123, 19));

// Since we need index type to determine the result
// of the test, this needs to be made as a global static
static IndexType index_type = INDEX_TYPE_BWTREE;

/*
 * BuildIndex()
 */
index::Index *BuildIndex(const bool unique_keys, int num_cols) {
  // Build tuple and key schema
  std::vector<catalog::Column> column_list;
  std::vector<oid_t> key_attrs;

  char column_char = 'A';
  for (int i = 0; i < num_cols; i++) {
    std::ostringstream os;
    os << static_cast<char>((int)column_char + i);

    catalog::Column column(type::Type::INTEGER,
                           type::Type::GetTypeSize(type::Type::INTEGER),
                           os.str(), true);
    column_list.push_back(column);
    key_attrs.push_back(i);
  }  // FOR
  key_schema = new catalog::Schema(column_list);
  key_schema->SetIndexedColumns(key_attrs);
  tuple_schema = new catalog::Schema(column_list);

  // Build index metadata
  //
  // NOTE: Since here we use a relatively small key (size = 12)
  // so index_test is only testing with a certain kind of key
  // (most likely, GenericKey)
  //
  // For testing IntsKey and TupleKey we need more test cases
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "MAGIC_TEST_INDEX", 125,  // Index oid
      INVALID_OID, INVALID_OID, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, key_attrs, unique_keys);

  // Build index
  // The type of index key has been chosen inside this function, but we are
  // unable to know the exact type of key it has chosen
  index::Index *index = index::IndexFactory::GetIndex(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);

  return index;
}

TEST_F(IndexIntsKeyTests, BasicTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  // Scale up the number of integers that we have
  // in the index
  for (int num_cols = 1; num_cols <= 4; num_cols++) {
    // INDEX
    std::unique_ptr<index::Index> index(BuildIndex(false, num_cols));
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    for (int col_idx = 0; col_idx < num_cols; col_idx++) {
      int val = (10 * num_cols) + col_idx;
      key0->SetValue(0, type::ValueFactory::GetIntegerValue(val), pool);
    }

    // INSERT
    index->InsertEntry(key0.get(), item0.get());

    // SCAN
    std::vector<ItemPointer *> location_ptrs;
    index->ScanKey(key0.get(), location_ptrs);
    EXPECT_EQ(location_ptrs.size(), 1);
    EXPECT_EQ(location_ptrs[0]->block, item0->block);
    location_ptrs.clear();

    // DELETE
    index->DeleteEntry(key0.get(), item0.get());

    index->ScanKey(key0.get(), location_ptrs);
    EXPECT_EQ(location_ptrs.size(), 0);
    location_ptrs.clear();

    delete tuple_schema;
  }  // FOR
}

}  // End test namespace
}  // End peloton namespace
