//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// new_logging_test.cpp
//
// Identification: test/logging/new_logging_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class NewLoggingTests : public PelotonTest {};

TEST_F(NewLoggingTests, MyTest) {
  // auto &log_manager = logging::LogManagerFactory::GetInstance();
  // log_manager.Reset();

  EXPECT_TRUE(true);
}
}
}
