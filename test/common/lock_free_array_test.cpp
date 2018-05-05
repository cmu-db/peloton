//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_free_array_test.cpp
//
// Identification: test/container/lock_free_array_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container/lock_free_array.h"

#include "common/harness.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// LockFreeArray Test
//===--------------------------------------------------------------------===//

class LockFreeArrayTests : public PelotonTest {};

// Test basic functionality
TEST_F(LockFreeArrayTests, BasicTest) {
  typedef uint32_t value_type;

  {
    LockFreeArray<value_type> array;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      auto status = array.Append(element);
      EXPECT_TRUE(status);
    }

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count);
  }
}

// Test shared pointers
TEST_F(LockFreeArrayTests, SharedPointerTest1) {
  typedef std::shared_ptr<oid_t> value_type;

  {
    LockFreeArray<value_type> array;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      std::shared_ptr<oid_t> entry(new oid_t);
      auto status = array.Append(entry);
      EXPECT_TRUE(status);
    }

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count);
  }
}

TEST_F(LockFreeArrayTests, SharedPointerTest2) {
  typedef std::shared_ptr<oid_t> value_type;

  {
    LockFreeArray<value_type> array;

    std::thread t0([&] {
      size_t const element_count = 10000;
      for (size_t element = 0; element < element_count; ++element) {
        std::shared_ptr<oid_t> entry(new oid_t);
        auto status = array.Append(entry);
        EXPECT_TRUE(status);
      }
    });

    size_t const element_count = 10000;
    for (size_t element = 0; element < element_count; ++element) {
      std::shared_ptr<oid_t> entry(new oid_t);
      auto status = array.Append(entry);
      EXPECT_TRUE(status);
    }
    t0.join();

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count * 2);
  }
}

TEST_F(LockFreeArrayTests, IteratorTest) {
  LockFreeArray<oid_t> array;
  for (oid_t i = 0; i < 1000; i++) {
    array.Append(i);
  }

  EXPECT_EQ(1000, array.GetSize());
  oid_t count = 0;
  for (auto iter1 = array.Begin(), iter2 = array.Begin(); iter1 != array.End();
       ++iter1, ++iter2) {
    EXPECT_EQ(count, *iter1);
    EXPECT_EQ(count, *iter2);
    EXPECT_TRUE(iter1 == iter2);
    count++;
  }

  EXPECT_EQ(1000, count);
}
}  // namespace test
}  // namespace peloton
