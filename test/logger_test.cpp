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

	Logger();

	Logger::SetLevel(boost::log::trivial::trace);

	NLOG(trace, "trace message");
	NLOG(warning, "warning message");
	NLOG(error, "error message");

}

} // End test namespace
} // End nstore namespace
