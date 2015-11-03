#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

/**
 * @brief writing a simple log with multiple threads
 */
TEST(PelotonLoggingTest, writing_log_file) {
  // Test a simple log process
  EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_PELOTON));
}

/**
 * @brief recovery test
 */
TEST(PelotonLoggingTest, recovery) {
  LoggingTestsUtil::CheckRecovery(LOGGING_TYPE_PELOTON);
}

}  // End test namespace
}  // End peloton namespace
