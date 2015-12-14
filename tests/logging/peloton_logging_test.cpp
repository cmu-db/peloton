#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

std::string peloton_log_file_name = "peloton.log";

/**
 * @brief writing a simple log with multiple threads and then do recovery
 */
TEST(PelotonLoggingTest, writing_log_file) {

  // Test a simple log process
  EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_PELOTON, peloton_log_file_name));

  // Do recovery
  LoggingTestsUtil::CheckRecovery(LOGGING_TYPE_PELOTON, peloton_log_file_name);
}

}  // End test namespace
}  // End peloton namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Setup testing configuration
  peloton::test::LoggingTestsUtil::ParseArguments(argc, argv);

  return RUN_ALL_TESTS();
}
