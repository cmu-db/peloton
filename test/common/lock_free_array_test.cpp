//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_free_array_test.cpp
//
// Identification: test/container/lock_free_array_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

  typedef uint32_t  value_type;

  {
    LockFreeArray<value_type> array;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      array.Append(element);
    }

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count);
  }

}

//// Test shared pointers
TEST_F(LockFreeArrayTests, SharedPointerTest1) {

  typedef std::shared_ptr<oid_t> value_type;

  {
    LockFreeArray<value_type> array;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      std::shared_ptr<oid_t> entry(new oid_t);
      array.Append(entry);
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
      for (size_t element = 0; element < element_count; ++element ) {
        std::shared_ptr<oid_t> entry(new oid_t);
        array.Append(entry);
      }
    });


    size_t const element_count = 10000;
    for (size_t element = 0; element < element_count; ++element ) {
      std::shared_ptr<oid_t> entry(new oid_t);
      array.Append(entry);
    }
    t0.join();

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count*2);



  }
}

TEST_F(LockFreeArrayTests, FindValidAndEraseTest) {
  typedef uint32_t value_type;

  {
    LockFreeArray<value_type> array;

    value_type invalid_value = 6288;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      array.Append(element);
    }

    // in range, valid
    EXPECT_EQ(2, array.FindValid(2, invalid_value));

    // out of range
    EXPECT_EQ(invalid_value, array.FindValid(6, invalid_value));

    array.Erase(2, invalid_value);

    // in range, erased
    EXPECT_EQ(invalid_value, array.FindValid(2, invalid_value));
  }
}

TEST_F(LockFreeArrayTests, ClearAndIsEmptyTest) {
  typedef uint32_t value_type;

  {
    LockFreeArray<value_type> array;

    EXPECT_TRUE(array.IsEmpty());

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      array.Append(element);
    }

    EXPECT_TRUE(array.Contains(2));

    EXPECT_FALSE(array.IsEmpty());

    array.Clear();

    EXPECT_TRUE(array.IsEmpty());

    EXPECT_FALSE(array.Contains(2));
  }
}

TEST_F(LockFreeArrayTests, ContainsTest) {
  typedef uint32_t value_type;

  {
    LockFreeArray<value_type> array;

    EXPECT_FALSE(array.Contains(2));

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      array.Append(element);
    }

    EXPECT_TRUE(array.Contains(2));

    array.Clear();

    EXPECT_FALSE(array.Contains(2));
  }
}

TEST_F(LockFreeArrayTests, UpdateTest) {
  typedef uint32_t value_type;

  {
    LockFreeArray<value_type> array;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element) {
      array.Append(element);
    }

    EXPECT_EQ(2, array.Find(2));

    array.Update(2, 6288);

    EXPECT_EQ(6288, array.Find(2));
  }
}

}  // namespace test
}  // namespace peloton
