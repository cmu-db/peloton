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

  // Initialize CDS library
  cds::Initialize();

  typedef uint32_t  key_type;
  typedef uint32_t  value_type;

  {
    // Attach thread
    cds::threading::Manager::attachThread();

    Map<key_type, value_type> map;

    /*
    size_t const element_count = 3;
    for (size_t element = 0; element < element_count; ++element ) {
      value_ptr_type val = new value_type(element);
      LOG_INFO("Value: %p %d", val, *val);
      auto status = map.Insert(element, val);
      LOG_INFO("Status: %d", status);
    }

    EXPECT_EQ(map.GetSize(), element_count);
    */

    // Detach thread
    cds::threading::Manager::detachThread();
  }

  // Terminate CDS library
  cds::Terminate();

}

}  // End test namespace
}  // End peloton namespace
