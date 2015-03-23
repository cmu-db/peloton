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

#include "common/logger.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Logger Tests
//===--------------------------------------------------------------------===//

TEST(LoggerTests, BasicTest) {

  // Start logger
  nstore::Logger();


	//LOG4CXX_TRACE(logger, "trace message");
	//LOG4CXX_WARN(logger, "warning message");
	//LOG4CXX_ERROR(logger, "error message");

}

} // End test namespace
} // End nstore namespace
