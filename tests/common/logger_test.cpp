//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logger_test.cpp
//
// Identification: tests/common/logger_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logger Tests
//===--------------------------------------------------------------------===//

TEST(LoggerTests, BasicTest) {
  LOG_TRACE("trace message");
  LOG_WARN("warning message");
  LOG_ERROR("error message");
}

}  // End test namespace
}  // End peloton namespace
