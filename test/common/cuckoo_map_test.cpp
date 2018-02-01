//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map_test.cpp
//
// Identification: test/container/cuckoo_map_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/container/cuckoo_map.h"

#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Cuckoo Map Test
//===--------------------------------------------------------------------===//

class CuckooMapTests : public PelotonTest {};

// Test basic functionality
TEST_F(CuckooMapTests, BasicTest) {
  typedef uint32_t key_type;
  typedef uint32_t value_type;

  {
    CuckooMap<key_type, value_type> map;

    EXPECT_TRUE(map.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      value_type val(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
      status = map.Insert(element, val);
      EXPECT_FALSE(status);
    }

    for (size_t element = 0; element < element_count; ++element) {
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
TEST_F(CuckooMapTests, SharedPointerTest) {
  typedef oid_t key_type;
  typedef std::shared_ptr<oid_t> value_type;

  {
    CuckooMap<key_type, value_type> map;

    EXPECT_TRUE(map.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      value_type val = std::make_shared<oid_t>(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
      status = map.Insert(element, val);
      EXPECT_FALSE(status);

      auto update_status = map.Update(element, val);
      EXPECT_TRUE(update_status);
    }

    for (size_t element = 0; element < element_count; ++element) {
      value_type value;
      auto status = map.Find(element, value);
      EXPECT_TRUE(status);
      EXPECT_NE(value.get(), nullptr);
    }

    auto map_size = map.GetSize();
    EXPECT_EQ(map_size, element_count);
  }
}

// Test Iterator
TEST_F(CuckooMapTests, IteratorTest) {
  typedef oid_t key_type;
  typedef std::shared_ptr<oid_t> value_type;

  CuckooMap<key_type, value_type> map;
  
  {
    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      value_type val = std::make_shared<oid_t>(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
      status = map.Insert(element, val);
      EXPECT_FALSE(status);
    }

    auto iterator = map.GetIterator();
    for (auto &iter : iterator) {
      EXPECT_EQ(iter.first, *(iter.second));
    }
  }

  // It's out of the scope of iterator, the map is unlocked now.
  // Otherwise it would be blocked.
  EXPECT_TRUE(map.Contains(1));
}

// Test if we can use a shared pointer as the key
TEST_F(CuckooMapTests, SharedPointerKeyTest) {
  typedef std::shared_ptr<oid_t> key_type;
  {
    CuckooMap<key_type, key_type> map;
    size_t const element_count = 3;
    std::vector<key_type> ptr_vec;
    for (size_t element = 0; element < element_count; ++element) {
      key_type val = std::make_shared<oid_t>(element);
      auto status = map.Insert(val, val);
      EXPECT_TRUE(status);
      status = map.Insert(val, val);
      EXPECT_FALSE(status);
      ptr_vec.push_back(val);
    }

    for (size_t element = 0; element < element_count; ++element) {
      key_type key = std::make_shared<oid_t>(element);
      key_type val;
      auto status = map.Find(key, val);
      EXPECT_FALSE(status);
    }

    for (auto &key : ptr_vec) {
      key_type val;
      auto status = map.Find(key, val);
      EXPECT_TRUE(status);
    }
  }
}

}  // namespace test
}  // namespace peloton
