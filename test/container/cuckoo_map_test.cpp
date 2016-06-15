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
      status = map.Insert(element, val);
      EXPECT_FALSE(status);
    }

    for (size_t element = 0; element < element_count; ++element ) {
      value_type value;
      auto status = map.Find(element, value);
      EXPECT_TRUE(status);
      EXPECT_EQ(value, element);
    }

    auto map_size = map.GetSize();
    EXPECT_EQ(map_size, element_count);
  }

}

// Test shared pointers
TEST_F(CuckooMapTest, SharedPointerTest) {

  typedef oid_t  key_type;
  typedef std::shared_ptr<oid_t> value_type;

  {
    CuckooMap<key_type, value_type> map;

    EXPECT_TRUE(map.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      value_type val = std::make_shared<oid_t>(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
      status = map.Insert(element, val);
      EXPECT_FALSE(status);

      auto update_status = map.Update(element, val, true);
      EXPECT_TRUE(update_status.first);

      //auto delete_status = map.Erase(element);
      //EXPECT_TRUE(delete_status);

      //update_status = map.Update(element, val, false);
      //EXPECT_FALSE(update_status.first);

      //update_status = map.Update(element, val, true);
      //EXPECT_TRUE(update_status.first);
    }

    for (size_t element = 0; element < element_count; ++element ) {
      value_type value;
      auto status = map.Find(element, value);
      EXPECT_TRUE(status);
      EXPECT_NE(value.get(), nullptr);
    }

    auto map_size = map.GetSize();
    EXPECT_EQ(map_size, element_count);
  }

}

}  // End test namespace
}  // End peloton namespace
