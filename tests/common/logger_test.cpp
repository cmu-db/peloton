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

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Logger Tests
//===--------------------------------------------------------------------===//

TEST(LoggerTests, BasicTest) {

  LOG4CXX_TRACE(logger, "trace message");
  LOG4CXX_WARN(logger, "warning message");
  LOG4CXX_ERROR(logger, "error message");

  log4cxx::LogManager::shutdown();
}

} // End test namespace
} // End nstore namespace
