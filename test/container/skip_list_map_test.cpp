//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map_test.cpp
//
// Identification: test/common/cuckoo_map_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/skip_list_map.h"

#include "common/harness.h"
#include "common/logger.h"
#include "common/types.h"
#include "index/index_key.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SkipList Map Test
//===--------------------------------------------------------------------===//

class SkipListMapTest : public PelotonTest {};

ItemPointer foo(23, 47);
ItemPointer bar;

// Test basic functionality
TEST_F(SkipListMapTest, BasicTest) {

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
  columns.push_back(column1);
  catalog::Schema *schema(new catalog::Schema(columns));
  std::vector<storage::Tuple*> tuples;

  storage::Tuple *tuple1(new storage::Tuple(schema, true));
  tuple1->SetValue(0, ValueFactory::GetIntegerValue(1), nullptr);
  tuples.push_back(tuple1);
  storage::Tuple *tuple2(new storage::Tuple(schema, true));
  tuple2->SetValue(0, ValueFactory::GetIntegerValue(2), nullptr);
  tuples.push_back(tuple2);
  storage::Tuple *tuple3(new storage::Tuple(schema, true));
  tuple3->SetValue(0, ValueFactory::GetIntegerValue(3), nullptr);
  tuples.push_back(tuple3);

  LOG_INFO("%s", tuple1->GetInfo().c_str());

  typedef index::GenericKey<4>  key_type;
  typedef ItemPointer *  value_type;
  typedef index::GenericComparator<4> key_comparator;

  SkipListMap<key_type, value_type, key_comparator> map;

  EXPECT_TRUE(map.IsEmpty());

  size_t const element_count = tuples.size();

  for (size_t element = 0; element < element_count; ++element ) {
    key_type key;
    key.SetFromKey(tuples[element]);

    value_type val = &foo;
    auto status = map.Insert(key, val);
    EXPECT_TRUE(status);
    status = map.Insert(key, val);
    EXPECT_FALSE(status);
  }

  for (size_t element = 0; element < element_count; ++element ) {
    key_type key;
    key.SetFromKey(tuples[element]);

    value_type value = &bar;
    auto status = map.Find(key, value);
    EXPECT_TRUE(status);
    EXPECT_EQ(foo.block, value->block);
    LOG_INFO("bar : %d %d", value->block, value->offset);
  }

  for(auto tuple : tuples) {
    delete tuple;
  }
  delete schema;
}

class SkipListMap<uint32_t, uint32_t, std::less<uint32_t>> test_skip_list_map;

const std::size_t base_scale = 1000;
const std::size_t max_scale_factor = 10000;

// INSERT HELPER FUNCTION
void InsertTest(size_t scale_factor, uint64_t thread_itr) {

  uint32_t base = thread_itr * base_scale * max_scale_factor;
  uint32_t tuple_count = scale_factor * base_scale;

  for (uint32_t tuple_itr = 1; tuple_itr <= tuple_count; tuple_itr++) {
    uint32_t tuple_offset = base + tuple_itr;

    auto status = test_skip_list_map.Insert(tuple_offset, tuple_offset);
    EXPECT_TRUE(status);
  }

}

// Test multithreaded functionality
TEST_F(SkipListMapTest, MultithreadedTest) {

  // Parallel Test
  size_t num_threads = 1;
  size_t scale_factor = 10;

  std::vector<std::thread> thread_group;

  LaunchParallelTest(num_threads, InsertTest, scale_factor);
}

}  // End test namespace
}  // End peloton namespace
