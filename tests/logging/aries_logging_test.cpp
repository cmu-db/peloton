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

  std::ifstream log_file(aries_log_file_name.c_str());

  // Reset the log file if exists
  if( log_file.good() ){
    EXPECT_TRUE(std::remove(aries_log_file_name.c_str()) == 0 );
  }

  // Prepare a simple log file
  if( LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_ARIES) == true){
  }else{
    LOG_ERROR("Could not prepare log file");
  }
}

/**
 * @brief recovery test
 */
TEST(AriesLoggingTest, recovery) {

  std::ifstream log_file(aries_log_file_name.c_str());

  // Do recovery if the log file exists
  if( log_file.good() ){
    LoggingTestsUtil::CheckAriesRecovery();
  }else{
    LOG_ERROR("Could not check recovery");
  }
}

}  // End test namespace
}  // End peloton namespace
