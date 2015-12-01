#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

std::string peloton_log_file_name = "/tmp/peloton.log";

/**
 * @brief writing a simple log with multiple threads
 */
TEST(PelotonLoggingTest, writing_log_file) {

  std::ifstream log_file(peloton_log_file_name.c_str());

  // Reset the log file if exists
  if( log_file.good() ){
    EXPECT_TRUE(std::remove(peloton_log_file_name.c_str()) == 0 );
  }
  log_file.close();

  // Test a simple log process
  EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_PELOTON, peloton_log_file_name));
}

/**
 * @brief recovery test
 */
TEST(PelotonLoggingTest, recovery) {
  std::ifstream log_file(peloton_log_file_name.c_str());

  // Reset the log file if exists
  EXPECT_TRUE(log_file.good());
  log_file.close();

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
