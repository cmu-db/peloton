#include "gtest/gtest.h"

#include "backend/logging/logmanager.h"
#include "logging/logging_tests_util.h"

#include <thread>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

TEST(AriesLoggingTest, logging_start_end) {

 //FIXME
 std::string filename = "/home/parallels/git/peloton/build/aries.log";
 if( std::remove(filename.c_str()) < 0 ){
   LOG_ERROR("Failed to remove aries logfile"); 
 }
   
 //TODO :: Make it function?
 {
   // start a thread for logging
   auto& logManager = logging::LogManager::GetInstance();
   logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
   std::thread thread(&logging::LogManager::StandbyLogging, 
       &logManager, 
       logManager.GetMainLoggingType());

   // When the frontend logger gets ready to logging,
   // start logging
   while(1){
     if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
       logManager.StartLogging();
       break;
     }
   }

   LoggingTestsUtil::WritingSimpleLog(20000, 10000);

   sleep(1);

   if( logManager.EndLogging() ){
     thread.join();
   }else{
     LOG_ERROR("Failed to terminate logging thread"); 
   }
 }

  sleep(1);

  //TODO :: Make it function?
  {
    LoggingTestsUtil::CreateDatabaseAndTable(20000, 10000);

    // start a thread for logging
    auto& logManager = logging::LogManager::GetInstance();
    logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
    std::thread thread(&logging::LogManager::StandbyLogging, 
                       &logManager, 
                       logManager.GetMainLoggingType());

    // When the frontend logger gets ready to logging,
    // start logging
    while(1){
      if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
        // Create corresponding database and table
        logManager.StartLogging(); // Change status to recovery
        break;
      }
    }

    //wait recovery
    while(1){
      // escape when recovery is done
      if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_ONGOING){
        break;
      }
    }

    // Check the tuples
    LoggingTestsUtil::CheckTuples(20000,10000);

    // Check the next oid
    LoggingTestsUtil::CheckNextOid();

    if( logManager.EndLogging() ){
      thread.join();
    }else{
      LOG_ERROR("Failed to terminate logging thread"); 
    }
    LoggingTestsUtil::DropDatabaseAndTable(20000, 10000);
  }

}

}  // End test namespace
}  // End peloton namespace
