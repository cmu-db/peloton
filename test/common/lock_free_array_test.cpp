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
      auto status = array.Append(element);
      EXPECT_TRUE(status);
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
      for (size_t element = 0; element < element_count; ++element ) {
        std::shared_ptr<oid_t> entry(new oid_t);
        auto status = array.Append(entry);
        EXPECT_TRUE(status);
      }
    });


    size_t const element_count = 10000;
    for (size_t element = 0; element < element_count; ++element ) {
      std::shared_ptr<oid_t> entry(new oid_t);
      auto status = array.Append(entry);
      EXPECT_TRUE(status);
    }
    t0.join();

    auto array_size = array.GetSize();
    EXPECT_EQ(array_size, element_count*2);



  }
}

}  // namespace test
}  // namespace peloton
