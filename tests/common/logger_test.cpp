/*-------------------------------------------------------------------------
*
* logger_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/logger_test.cpp
*
*-------------------------------------------------------------------------
*/

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
