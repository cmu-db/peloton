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

  LOG(TRACE) << "trace message";
	LOG(WARNING) << "warning message";
	LOG(ERROR) << "error message";

}

} // End test namespace
} // End nstore namespace
