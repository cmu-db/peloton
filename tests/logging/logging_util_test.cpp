//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logging_util_test.cpp
//
// Identification: tests/logging/logging_util_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/logging/logging_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//
class LoggingUtilTests : public PelotonTest {};

TEST_F(LoggingUtilTests, BasicLoggingUtilTest) {
  auto status = logging::LoggingUtil::CreateDirectory("test_dir", 0700);
  EXPECT_EQ(status, true);

  status = logging::LoggingUtil::RemoveDirectory("test_dir", true);
  EXPECT_EQ(status, true);
}

}  // End test namespace
}  // End peloton namespace
