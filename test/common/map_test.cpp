//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// map_test.cpp
//
// Identification: test/common/map_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "common/map.h"

#include "libcds/cds/init.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Map Test
//===--------------------------------------------------------------------===//

class MapTest : public PelotonTest {};

// Test basic functionality
TEST_F(MapTest, BasicTest) {

  typedef uint32_t  key_type;
  typedef uint32_t  value_type;
  typedef uint32_t*  value_ptr_type;

  // Initialize CDS library
  cds::Initialize();

  // Create URCU general_buffered singleton
  Map<key_type, value_type>::RCUImpl::Construct();

  cds::threading::Manager::attachThread();

  {
    Map<key_type, value_type> map;

    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      value_ptr_type val = new value_type(element);
      auto status = map.Insert(element, val);
      EXPECT_TRUE(status);
    }

    value_type value1;
    auto status = map.Find(1, value1);
    EXPECT_TRUE(status);
    EXPECT_EQ(value1, 1);
  }

  cds::threading::Manager::detachThread();

  // Destroy URCU general_buffered singleton
  Map<key_type, value_type>::RCUImpl::Destruct();

  // Terminate CDS library
  cds::Terminate();

}

}  // End test namespace
}  // End peloton namespace
