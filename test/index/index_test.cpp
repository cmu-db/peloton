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

#include "gtest/gtest.h"
#include "common/harness.h"

#include "common/logger.h"
#include "common/platform.h"
#include "index/index_factory.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

class IndexTests : public PelotonTest {};

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

std::shared_ptr<ItemPointer> item0(new ItemPointer(120, 5));
std::shared_ptr<ItemPointer> item1(new ItemPointer(120, 7));
std::shared_ptr<ItemPointer> item2(new ItemPointer(123, 19));

// Since we need index type to determine the result
// of the test, this needs to be made as a global static
static IndexType index_type = INDEX_TYPE_BWTREE;

// Uncomment this to enable BwTree as index being tested
// static IndexType index_type = INDEX_TYPE_BWTREE;

/*
 * BuildIndex() - Builds an index with 4 columns, the first 2 being indexed
 */
index::Index *BuildIndex(const bool unique_keys) {
  // Identify the index type to simplify things
  if (index_type == INDEX_TYPE_BWTREE) {
    LOG_INFO("Build index type: peloton::index::BwTree");
  } else if (index_type == INDEX_TYPE_BTREE) {
    LOG_INFO("Build index type: stx::BTree");
  } else {
    LOG_INFO("Build index type: Other type");
  }

  // Build tuple and key schema
  std::vector<catalog::Column> column_list;

  // The following key are both in index key and tuple key and they are
  // indexed
  // The size of the key is:
  //   integer 4 + varchar 8 = total 12

  catalog::Column column1(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "A", true);

  catalog::Column column2(common::Type::VARCHAR, 1024, "B", false);

  // The following twoc constitutes tuple schema but does not appear in index

  catalog::Column column3(common::Type::DECIMAL, common::Type::GetTypeSize(common::Type::DECIMAL),
                          "C", true);

  catalog::Column column4(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "D", true);

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
      INVALID_OID, INVALID_OID, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
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
  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);

  return index;
}

TEST_F(IndexTests, BasicTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);

  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);

  // INSERT

  index->InsertEntry(key0.get(), item0.get());

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
}

// INSERT HELPER FUNCTION
void InsertTest(index::Index *index, common::VarlenPool *pool, size_t scale_factor,
                UNUSED_ATTRIBUTE uint64_t thread_itr) {
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

    key0->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
    key1->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
    key2->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);
    key3->SetValue(0, common::ValueFactory::GetIntegerValue(400 * scale_itr), pool);
    key3->SetValue(1, common::ValueFactory::GetVarcharValue("d"), pool);
    key4->SetValue(0, common::ValueFactory::GetIntegerValue(500 * scale_itr), pool);
    key4->SetValue(1, common::ValueFactory::GetVarcharValue(
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                   pool);
    keynonce->SetValue(0, common::ValueFactory::GetIntegerValue(1000 * scale_itr),
                       pool);
    keynonce->SetValue(1, common::ValueFactory::GetVarcharValue("f"), pool);

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

// DELETE HELPER FUNCTION
void DeleteTest(index::Index *index, common::VarlenPool *pool, size_t scale_factor,
                UNUSED_ATTRIBUTE uint64_t thread_itr) {
  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Delete a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

    key0->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
    key1->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
    key2->SetValue(0, common::ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);
    key3->SetValue(0, common::ValueFactory::GetIntegerValue(400 * scale_itr), pool);
    key3->SetValue(1, common::ValueFactory::GetVarcharValue("d"), pool);
    key4->SetValue(0, common::ValueFactory::GetIntegerValue(500 * scale_itr), pool);
    key4->SetValue(1, common::ValueFactory::GetVarcharValue(
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                   pool);

    // DELETE
    // key0 1 (100, a)   item0
    // key1 5  (100, b)  item 0 1 2 (0 1 1 1 2)
    // key2 1 (100, c) item 1
    // key3 1 (400, d) item 1
    // key4 1  (500, eeeeee...) item 1
    // no keyonce (1000, f)

    // item0 = 2
    // item1 = 6
    // item2 = 1
    index->DeleteEntry(key0.get(), item0.get());
    index->DeleteEntry(key1.get(), item1.get());
    index->DeleteEntry(key2.get(), item2.get());
    index->DeleteEntry(key3.get(), item1.get());
    index->DeleteEntry(key4.get(), item1.get());

    // should be no key0
    // key1 item 0 1 2
    // key2 item 1
    // no key3
    // no key4
  }
}

TEST_F(IndexTests, MultiMapInsertTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);

  // Checks
  index->ScanAllKeys(location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else {
    EXPECT_EQ(location_ptrs.size(), 9);
  }

  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));
  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  keynonce->SetValue(0, common::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, common::ValueFactory::GetVarcharValue("f"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, item0->block);
  location_ptrs.clear();

  delete tuple_schema;
}

#ifdef ALLOW_UNIQUE_KEY
TEST_F(IndexTests, UniqueKeyDeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(true));

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(1, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0].block, item1->block);
  location_ptrs.clear();

  delete tuple_schema;
}
#endif

TEST_F(IndexTests, NonUniqueKeyDeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(1, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 2);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, item1->block);
  location_ptrs.clear();

  delete tuple_schema;
}

TEST_F(IndexTests, MultiThreadedInsertTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);

  index->ScanAllKeys(location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else {
    EXPECT_EQ(location_ptrs.size(), 9 * num_threads);
  }

  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));

  keynonce->SetValue(0, common::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, common::ValueFactory::GetVarcharValue("f"), pool);

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), num_threads);
  }

  EXPECT_EQ(location_ptrs[0]->block, item0->block);
  location_ptrs.clear();

  delete tuple_schema;
}

#ifdef ALLOW_UNIQUE_KEY
TEST_F(IndexTests, UniqueKeyMultiThreadedTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(true));

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(num_threads, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);

  common::Value key0_val0 = (key0->GetValue(0));
  common::Value key0_val1 = (key0->GetValue(1));
  common::Value key1_val0 = (key1->GetValue(0));
  common::Value key1_val1 = (key1->GetValue(1));
  common::Value key2_val0 = (key2->GetValue(0));
  common::Value key2_val1 = (key2->GetValue(1));

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0].block, item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  location_ptrs.clear();

  // FORWARD SCAN
  index->ScanTest({key1_val0}, {0}, {EXPRESSION_TYPE_COMPARE_EQUAL},
                  SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_GREATERTHAN},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_GREATERTHAN, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  delete tuple_schema;
}
#endif

// key0 1 (100, a)   item0
// key1 5  (100, b)  item1 2 1 1 0
// key2 1 (100, c) item 1
// key3 1 (400, d) item 1
// key4 1  (500, eeeeee...) item 1
// no keyonce (1000, f)

// item0 = 2
// item1 = 6
// item2 = 1

// should be no key0
// key1 item 0 2
// key2 item 1
// no key3
// no key4

TEST_F(IndexTests, NonUniqueKeyMultiThreadedTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(num_threads, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);
  key4->SetValue(0, common::ValueFactory::GetIntegerValue(500), pool);
  key4->SetValue(1, common::ValueFactory::GetVarcharValue(
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                 pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
  }

  EXPECT_EQ(location_ptrs[0]->block, item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads);
  }

  location_ptrs.clear();

  // FORWARD SCAN
  common::Value key0_val0 = (key0->GetValue(0));
  common::Value key0_val1 = (key0->GetValue(1));
  common::Value key1_val0 = (key1->GetValue(0));
  common::Value key1_val1 = (key1->GetValue(1));
  common::Value key2_val0 = (key2->GetValue(0));
  common::Value key2_val1 = (key2->GetValue(1));
  common::Value key4_val0 = (key4->GetValue(0));
  common::Value key4_val1 = (key4->GetValue(1));
  index->ScanTest({key1_val0}, {0}, {EXPRESSION_TYPE_COMPARE_EQUAL},
                  SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_GREATERTHAN},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_GREATERTHAN, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key2_val0, key2_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_LESSTHAN},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key0_val0, key0_val1,
       key2_val0, key2_val1},
      {0, 1, 0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_GREATERTHAN,
       EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_LESSTHAN},
      SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1,
                   key4_val0, key4_val1},
                  {0, 1, 0, 1}, {EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
                                 EXPRESSION_TYPE_COMPARE_GREATERTHAN,
                                 EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
                                 EXPRESSION_TYPE_COMPARE_LESSTHAN},
                  SCAN_DIRECTION_TYPE_FORWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads);
  }

  location_ptrs.clear();

  // REVERSE SCAN
  index->ScanTest({key1_val0}, {0}, {EXPRESSION_TYPE_COMPARE_EQUAL},
                  SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_GREATERTHAN},
      SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_GREATERTHAN, EXPRESSION_TYPE_COMPARE_EQUAL},
      SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  EXPECT_EQ(location_ptrs.size(), 0);

  location_ptrs.clear();

  index->ScanTest(
      {key2_val0, key2_val1}, {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_LESSTHAN},
      SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest(
      {key0_val0, key0_val1,
       key2_val0, key2_val1},
      {0, 1, 0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_GREATERTHAN,
       EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_LESSTHAN},
      SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1,
                   key4_val0, key4_val1},
                  {0, 1, 0, 1}, {EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
                                 EXPRESSION_TYPE_COMPARE_GREATERTHAN,
                                 EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
                                 EXPRESSION_TYPE_COMPARE_LESSTHAN},
                  SCAN_DIRECTION_TYPE_BACKWARD, location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads);
  }

  location_ptrs.clear();

  delete tuple_schema;
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 3;

  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(num_threads, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, common::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
  }

  EXPECT_EQ(location_ptrs[0]->block, item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);

  if (index_type == INDEX_TYPE_BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads * scale_factor);
  }

  location_ptrs.clear();

  delete tuple_schema;
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest2) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false));

  // Parallel Test
  size_t num_threads = 15;
  size_t scale_factor = 3;
  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(num_threads, DeleteTest, index.get(), pool, scale_factor);

  index->ScanAllKeys(location_ptrs);
  if (index->HasUniqueKeys()) {
    if (index_type == INDEX_TYPE_BWTREE) {
      EXPECT_EQ(location_ptrs.size(), scale_factor);
    } else {
      EXPECT_EQ(location_ptrs.size(), scale_factor);
    }
  } else {
    if (index_type == INDEX_TYPE_BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
    } else {
      EXPECT_EQ(location_ptrs.size(), 3 * scale_factor * num_threads);
    }
  }

  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key1->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, common::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, common::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, common::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key1.get(), location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), 0);
  } else {
    if (index_type == INDEX_TYPE_BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 2);
    } else {
      EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
    }
  }
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), num_threads);
  } else {
    if (index_type == INDEX_TYPE_BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 1);
    } else {
      EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
    }
  }

  location_ptrs.clear();

  delete tuple_schema;
}

}  // End test namespace
}  // End peloton namespace
