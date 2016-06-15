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

#include "container/cuckoo_map.h"

#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Cuckoo Map Test
//===--------------------------------------------------------------------===//

class CuckooMapTest : public PelotonTest {};

// Test basic functionality
TEST_F(CuckooMapTest, BasicTest) {

  typedef uint32_t  key_type;
  typedef uint32_t  value_type;

  {
    CuckooMap<key_type, value_type> map;

    EXPECT_TRUE(map.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      value_type val(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
    }

    value_type value1;
    auto status = map.Find(1, value1);
    EXPECT_TRUE(status);
    EXPECT_EQ(value1, 1);

    auto map_size = map.GetSize();
    EXPECT_EQ(map_size, 3);
  }

}

}  // End test namespace
}  // End peloton namespace
