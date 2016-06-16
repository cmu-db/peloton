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

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SkipList Map Test
//===--------------------------------------------------------------------===//

class SkipListMapTest : public PelotonTest {};

// Test basic functionality
TEST_F(SkipListMapTest, BasicTest) {

  typedef uint32_t  key_type;
  typedef uint32_t  value_type;

  {
    SkipListMap<key_type, value_type> map;

    EXPECT_TRUE(map.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      value_type val(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
      status = map.Insert(element, val);
      EXPECT_FALSE(status);
    }

    for (size_t element = 0; element < element_count; ++element ) {
      value_type value;
      auto status = map.Find(element, value);
      EXPECT_TRUE(status);
      EXPECT_EQ(value, element);
    }

  }

}

}  // End test namespace
}  // End peloton namespace
