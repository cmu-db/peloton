//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_pool_test.cpp
//
// Identification: test/logging/log_buffer_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/log_buffer_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Pool Tests
//===--------------------------------------------------------------------===//

class LogBufferPoolTests : public PelotonTest {};

TEST_F(LogBufferPoolTests, MyTest) {
  
  LogBufferPool log_buffer_pool(1);

  EXPECT_TRUE(true);
  
}

}
}
