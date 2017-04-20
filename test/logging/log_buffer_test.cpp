//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_test.cpp
//
// Identification: test/logging/log_buffer_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/log_buffer.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Tests
//===--------------------------------------------------------------------===//

class LogBufferTests : public PelotonTest {};

TEST_F(LogBufferTests, InitTest) {
  
  LogBuffer log_buffer(1, 1);

  log_buffer.Reset();

  EXPECT_TRUE(true);
  
}

}
}
