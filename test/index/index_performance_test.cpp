//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_performance_test.cpp
//
// Identification: test/index/index_performance_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "common/harness.h"

#include <vector>
#include <thread>

#include "common/logger.h"
#include "common/platform.h"
#include "common/timer.h"
#include "index/index_factory.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Performance Tests
//===--------------------------------------------------------------------===//

class IndexPerformanceTests : public PelotonTest {};

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

ItemPointer item0(120, 5);
ItemPointer item1(120, 7);
ItemPointer item2(123, 19);

const std::size_t base_scale = 1000;
const std::size_t max_scale_factor = 10000;

index::Index *BuildIndex(const bool unique_keys,
                         const IndexType index_type) {
  // Build tuple and key schema
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  catalog::Column column3(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "D", true);

  columns.push_back(column1);
  columns.push_back(column2);

  // INDEX KEY SCHEMA -- {column1, column2}
  key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0, 1});

  columns.push_back(column3);
  columns.push_back(column4);

  // TABLE SCHEMA -- {column1, column2, column3, column4}
  tuple_schema = new catalog::Schema(columns);

  // Build index metadata
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "test_index", 125, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, unique_keys);

  // Build index
  index::Index *index = index::IndexFactory::GetInstance(index_metadata);
  EXPECT_TRUE(index != NULL);

  return index;
}

// INSERT HELPER FUNCTION
void InsertTest(index::Index *index, size_t scale_factor, uint64_t thread_itr) {

  size_t base = thread_itr * base_scale * max_scale_factor;
  size_t tuple_count = scale_factor * base_scale;

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(key_schema, true));

  for (size_t tuple_itr = 1; tuple_itr <= tuple_count; tuple_itr++) {
    oid_t tuple_offset = base + tuple_itr;
    auto key_value =  ValueFactory::GetIntegerValue(tuple_offset);

    key->SetValue(0, key_value, nullptr);
    key->SetValue(1, key_value, nullptr);

    auto status = index->InsertEntry(key.get(), item0);
    EXPECT_TRUE(status);
  }

}

static void TestIndexPerformance(const IndexType& index_type) {
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex(false, index_type));

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  Timer<> timer;

  std::vector<std::thread> thread_group;

  timer.Start();

  LaunchParallelTest(num_threads, InsertTest, index.get(), scale_factor);

  index->ScanAllKeys(locations);
  EXPECT_EQ(locations.size(), num_threads * scale_factor * base_scale);
  locations.clear();

  timer.Stop();
  LOG_INFO("Duration : %.2lf", timer.GetDuration());

  delete tuple_schema;
}

TEST_F(IndexPerformanceTests, MultiThreadedTest) {
  std::vector<IndexType> index_types = {INDEX_TYPE_BTREE, INDEX_TYPE_BWTREE};

  for(auto index_type : index_types) {
    TestIndexPerformance(index_type);
  }

}

}  // End test namespace
}  // End peloton namespace
