#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

std::string aries_log_file_name = "aries.log";

/**
 * @brief writing a simple log with multiple threads
 */
TEST(AriesLoggingTest, writing_logfile) {

  // Prepare a simple log file
  EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_ARIES, aries_log_file_name));

  // Reset data
  LoggingTestsUtil::ResetSystem();
}

/**
 * @brief recovery test
 */
TEST(AriesLoggingTest, recovery) {

  // Do recovery
  LoggingTestsUtil::CheckRecovery(LOGGING_TYPE_ARIES, aries_log_file_name);
}

}  // End test namespace
}  // End peloton namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Setup testing configuration
  peloton::test::LoggingTestsUtil::ParseArguments(argc, argv);

  return RUN_ALL_TESTS();
}
