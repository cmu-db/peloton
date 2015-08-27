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
 * @brief writing a simple log with multiple threads
 */
TEST(PelotonLoggingTest, writing_log_file) {

  std::ifstream log_file(peloton_log_file_name.c_str());

  // Reset the log file if exists
  if( log_file.good() ){
    EXPECT_TRUE( std::remove(peloton_log_file_name.c_str()) == 0 );
  }

  // Prepare a simple log file
  if( LoggingTestsUtil::PrepareLogFile(LOGGING_TYPE_PELOTON) ){
    std::cout << "log file size : " <<
        LoggingTestsUtil::GetLogFileSize(peloton_log_file_name) << std::endl;
    //auto res = truncate( filename.c_str(), 1343);
    //if( res == -1 ){
      //LOG_ERROR("Failed to truncate the log file"); 
    //}
  }else{
    LOG_ERROR("Could not prepare log file");
  }
}

/**
 * @brief recovery test
 */
TEST(PelotonLoggingTest, recovery) {

  std::ifstream log_file(peloton_log_file_name.c_str());

  // Do recovery if the log file exists
  if( log_file.good() ){
    LoggingTestsUtil::CheckPelotonRecovery();
  }else{
    LOG_ERROR("Could not check recovery");
  }
}

}  // End test namespace
}  // End peloton namespace
