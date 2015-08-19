#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

TEST(AriesLoggingTest, logging_start_end) {

 //FIXME :: Hard coded file path
 std::string filename = "/home/parallels/git/peloton/build/aries.log";

 if( std::remove(filename.c_str()) < 0 ){
   LOG_ERROR("Failed to remove aries logfile"); 
 }

 // writing simple log file
 LoggingTestsUtil::PrepareLogFile();

 sleep(1);

 // recover database and check the tuples
 LoggingTestsUtil::CheckTupleAfterRecovery();

}

}  // End test namespace
}  // End peloton namespace
