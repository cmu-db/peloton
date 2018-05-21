//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_test.cpp
//
// Identification: test/common/logger_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logger Tests
//===--------------------------------------------------------------------===//

class LoggerTests : public PelotonTests {};

TEST_F(LoggerTests, BasicTest) {
  LOG_TRACE("trace message");
  LOG_WARN("warning message");
  LOG_ERROR("error message");
}

}  // namespace test
}  // namespace peloton
