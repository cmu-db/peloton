//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_performance_test.cpp
//
// Identification: test/performance/index_performance_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/platform.h"
#include "common/timer.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Performance Tests
//===--------------------------------------------------------------------===//

class IndexPerformanceTests : public PelotonTest {};

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

std::shared_ptr<ItemPointer> item(new ItemPointer(120, 5));

index::Index *BuildIndex(const bool unique_keys, const IndexType index_type) {
  // Build tuple and key schema
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "A",
                          true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "B",
                          true);
  catalog::Column column3(type::TypeId::DECIMAL,
                          type::Type::GetTypeSize(type::TypeId::DECIMAL), "C",
                          true);
  catalog::Column column4(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "D",
                          true);

  columns.push_back(column1);
  columns.push_back(column2);

  // INDEX KEY SCHEMA -- {column1, column2}
  std::vector<oid_t> key_attrs = {0, 1};
  key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns(key_attrs);

  columns.push_back(column3);
  columns.push_back(column4);

  // TABLE SCHEMA -- {column1, column2, column3, column4}
  tuple_schema = new catalog::Schema(columns);

  // Build index metadata
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "test_index", 125, INVALID_OID, INVALID_OID, index_type,
      IndexConstraintType::DEFAULT, tuple_schema, key_schema, key_attrs,
      unique_keys);

  // Build index
  index::Index *index = index::IndexFactory::GetIndex(index_metadata);
  EXPECT_TRUE(index != NULL);

  return index;
}

/*
 * InsertTest1() - Tests InsertEntry() performance for each index type
 *
 * This function tests threads inserting on its own consecutive interval
 * without any interleaving with each other.
 *
 * The insert pattern is depicted as follows:
 *
 * |<--- thread 0 --->|<--- thread 1 --->| ... |<--- thread (num_thread - 1)
 *--->|
 *  ^                ^
 * start key       end key
 */
static void InsertTest1(index::Index *index, size_t num_thread, size_t num_key,
                        uint64_t thread_id) {
  // To avoid compiler warning
  (void)num_thread;

  // Each thread is responsible for a consecutive range of keys
  // and here is the range: [start_key, end_key)
  size_t start_key = thread_id * num_key;
  size_t end_key = start_key + num_key;

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(key_schema, true));

  // Set the non-indexed column of the key to avoid undefined behaviour
  // key->SetValue(2, type::ValueFactory::GetDecimalValue(1.11), nullptr);
  // key->SetValue(3, type::ValueFactory::GetIntegerValue(12345), nullptr);

  for (size_t i = start_key; i < end_key; i++) {
    auto key_value = type::ValueFactory::GetIntegerValue(i);

    key->SetValue(0, key_value, nullptr);
    key->SetValue(1, key_value, nullptr);

    auto status = index->InsertEntry(key.get(), item.get());
    EXPECT_TRUE(status);
  }

  return;
}

/*
 * DeleteTest1() - Tests DeleteEntry() performance for each index type
 *
 * This function tests threads deleting on its own consecutive interval
 * without any interleaving with each other.
 *
 * The delete pattern is depicted as follows:
 *
 * |<--- thread 0 --->|<--- thread 1 --->| ... |<--- thread (num_thread - 1)
 *--->|
 *  ^                ^
 * start key       end key
 */
static void DeleteTest1(index::Index *index, size_t num_thread, size_t num_key,
                        uint64_t thread_id) {
  // To avoid compiler warning
  (void)num_thread;

  // Each thread is responsible for a consecutive range of keys
  // and here is the range: [start_key, end_key)
  size_t start_key = thread_id * num_key;
  size_t end_key = start_key + num_key;

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(key_schema, true));

  // Set the non-indexed column of the key to avoid undefined behaviour
  // key->SetValue(2, type::ValueFactory::GetDecimalValue(1.11), nullptr);
  // key->SetValue(3, type::ValueFactory::GetIntegerValue(12345), nullptr);

  for (size_t i = start_key; i < end_key; i++) {
    auto key_value = type::ValueFactory::GetIntegerValue(i);

    key->SetValue(0, key_value, nullptr);
    key->SetValue(1, key_value, nullptr);

    auto status = index->DeleteEntry(key.get(), item.get());
    EXPECT_TRUE(status);
  }

  return;
}

/*
 * InsertTest2() - Tests InsertEntry() performance for each index type
 *
 * This function tests threads inserting with an interleaved pattern
 *
 * The insert pattern is depicted as follows:
 *
 * |0 1 2 3 .. (num_thread - 1)|0 1 2 3 .. (num_thread - 1)| ... |0 1 2 3 ..
 *(num_thread - 1)|
 *  ^                           ^                                 ^
 * 1st key for thread 0       second key for thread 0            last key for
 *thread 0
 *
 * This test usually has higher contention and lower performance
 */
static void InsertTest2(index::Index *index, size_t num_thread, size_t num_key,
                        uint64_t thread_id) {
  // num_thread is the step for each iteration
  // thread_id is the offset of each iteration

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(key_schema, true));

  // Set the non-indexed column of the key to avoid undefined behaviour
  // key->SetValue(2, type::ValueFactory::GetDecimalValue(1.11), nullptr);
  // key->SetValue(3, type::ValueFactory::GetIntegerValue(12345), nullptr);

  size_t j = 0;
  for (size_t i = thread_id; j < num_key; (i += num_thread), j++) {
    auto key_value = type::ValueFactory::GetIntegerValue(i);

    key->SetValue(0, key_value, nullptr);
    key->SetValue(1, key_value, nullptr);

    auto status = index->InsertEntry(key.get(), item.get());
    EXPECT_TRUE(status);
  }

  return;
}

/*
 * DeleteTest2() - Tests DeleteEntry() performance for each index type
 *
 * This function tests threads deleting with an interleaved pattern
 *
 * The delete pattern is depicted as follows:
 *
 * |0 1 2 3 .. (num_thread - 1)|0 1 2 3 .. (num_thread - 1)| ... |0 1 2 3 ..
 *(num_thread - 1)|
 *  ^                           ^                                 ^
 * 1st key for thread 0       second key for thread 0            last key for
 *thread 0
 *
 * This test usually has higher contention and lower performance
 */
static void DeleteTest2(index::Index *index, size_t num_thread, size_t num_key,
                        uint64_t thread_id) {
  // num_thread is the step for each iteration
  // thread_id is the offset of each iteration

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(key_schema, true));

  // Set the non-indexed column of the key to avoid undefined behaviour
  // key->SetValue(2, type::ValueFactory::GetDecimalValue(1.11), nullptr);
  // key->SetValue(3, type::ValueFactory::GetIntegerValue(12345), nullptr);

  size_t j = 0;
  for (size_t i = thread_id; j < num_key; (i += num_thread), j++) {
    auto key_value = type::ValueFactory::GetIntegerValue(i);

    key->SetValue(0, key_value, nullptr);
    key->SetValue(1, key_value, nullptr);

    auto status = index->DeleteEntry(key.get(), item.get());
    EXPECT_TRUE(status);
  }

  return;
}

/*
 * TestIndexPerformance() - Test driver for indices of a given type
 *
 * This function tests Insert and Delete performance together with
 * key scan
 */
static void TestIndexPerformance(const IndexType &index_type) {
  // This is where we read all values in and verify them
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false, index_type));

  // Parallel Test by default 1 Million key

  // Number of threads doing insert or delete
  size_t num_thread = 4;

  // Number of keys inserted by each thread
  size_t num_key = 1024 * 256;

  Timer<> timer;

  ///////////////////////////////////////////////////////////////////
  // Start InsertTest1
  ///////////////////////////////////////////////////////////////////

  timer.Start();

  // First two arguments are used for launching tasks
  // All remaining arguments are passed to the thread body
  LaunchParallelTest(num_thread, InsertTest1, index.get(), num_thread, num_key);

  // Perform garbage collection
  if (index->NeedGC() == true) {
    index->PerformGC();
  }

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(num_thread * num_key, location_ptrs.size());
  location_ptrs.clear();

  timer.Stop();
  LOG_INFO("InsertTest1 :: Type=%s; Duration=%.2lf",
           IndexTypeToString(index_type).c_str(), timer.GetDuration());

  ///////////////////////////////////////////////////////////////////
  // Start DeleteTest1
  ///////////////////////////////////////////////////////////////////

  timer.Start();

  LaunchParallelTest(num_thread, DeleteTest1, index.get(), num_thread, num_key);

  // Perform garbage collection
  if (index->NeedGC() == true) {
    index->PerformGC();
  }

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  timer.Stop();
  LOG_INFO("DeleteTest1 :: Type=%s; Duration=%.2lf",
           IndexTypeToString(index_type).c_str(), timer.GetDuration());

  ///////////////////////////////////////////////////////////////////
  // Start InsertTest2
  ///////////////////////////////////////////////////////////////////

  timer.Start();

  LaunchParallelTest(num_thread, InsertTest2, index.get(), num_thread, num_key);

  // Perform garbage collection
  if (index->NeedGC() == true) {
    index->PerformGC();
  }

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(num_thread * num_key, location_ptrs.size());
  location_ptrs.clear();

  timer.Stop();
  LOG_INFO("InsertTest2 :: Type=%s; Duration=%.2lf",
           IndexTypeToString(index_type).c_str(), timer.GetDuration());

  ///////////////////////////////////////////////////////////////////
  // Start DeleteTest2
  ///////////////////////////////////////////////////////////////////

  timer.Start();

  LaunchParallelTest(num_thread, DeleteTest2, index.get(), num_thread, num_key);

  // Perform garbage collection
  if (index->NeedGC() == true) {
    index->PerformGC();
  }

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  timer.Stop();
  LOG_INFO("DeleteTest :: Type=%s; Duration=%.2lf",
           IndexTypeToString(index_type).c_str(), timer.GetDuration());

  ///////////////////////////////////////////////////////////////////
  // End of all tests
  ///////////////////////////////////////////////////////////////////

  delete tuple_schema;

  return;
}

TEST_F(IndexPerformanceTests, BwTreeMultiThreadedTest) {
  TestIndexPerformance(IndexType::BWTREE);
}

// TEST_F(IndexPerformanceTests, BTreeMultiThreadedTest) {
//  TestIndexPerformance(IndexType::BTREE);
//}

}  // namespace test
}  // namespace peloton
