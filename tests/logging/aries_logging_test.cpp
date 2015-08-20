#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

TEST(AriesLoggingTest, logging_start_end) {

 //FIXME :: Hard coded file path
  std::string filename = "/home/parallels/git/peloton/build/aries.log";
  std::ifstream logfile(filename.c_str());
  if( logfile.good() ){
    EXPECT_TRUE( std::remove(filename.c_str()) == 0 );
  }


 // writing simple log file
 if( LoggingTestsUtil::PrepareLogFile() ){
   // recover database and check the tuples
   LoggingTestsUtil::CheckTupleAfterRecovery();
 }else{
    //Something's wrong !!
    EXPECT_EQ(1,0);
 }

}

}  // End test namespace
}  // End peloton namespace
