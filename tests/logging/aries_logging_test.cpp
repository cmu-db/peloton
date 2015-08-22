#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

std::string filename = "aries.log";

/**
 * @brief writing a simple log with multiple threads
 */
TEST(AriesLoggingTest, writing_logfile) {

  std::ifstream logfile(filename.c_str());
  // delete the log file if exists
  if( logfile.good() ){
    EXPECT_TRUE( std::remove(filename.c_str()) == 0 );
  }

  // writing simple log file
  if( LoggingTestsUtil::PrepareLogFile() ){
    // truncate the log file
    auto res = truncate( filename.c_str(), 1000);
    if( res == -1 ){
      LOG_ERROR("Failed to truncate the log file"); 
    }
  }else{
    //Something's wrong !!
    EXPECT_EQ(1,0);
  }
}

/**
 * @brief recovery test
 */
TEST(AriesLoggingTest, recovery) {

  std::ifstream logfile(filename.c_str());
  // recovery if the log file exists
  if( logfile.good() ){
   LoggingTestsUtil::CheckTupleAfterRecovery();
  }else{
    //Something's wrong !!
    EXPECT_EQ(1,0);
  }
}

}  // End test namespace
}  // End peloton namespace
