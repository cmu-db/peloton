#include "gtest/gtest.h"

#include "backend/logging/logmanager.h"

#include <thread>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

TEST(AriesLoggingTest, logging_start_end) {
  std::cout << "aries logging test start\n";

  auto& logManager = logging::LogManager::GetInstance();
  logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
  std::thread thread(&logging::LogManager::StandbyLogging, 
                     &logManager, 
                     logManager.GetMainLoggingType());
  // TODO
  // create database and tables 
  
  // insert tuples

  // delete tuples

  // update tuples

  // abort ??

  sleep(3);

  if( logManager.EndLogging() ){
    thread.join();
  }else{
    LOG_ERROR("Failed to terminate logging thread"); 
  }


  // TODO 

  // create another database

  // create table .. using bootstrap?

  // read log file and recovery 

}

}  // End test namespace
}  // End peloton namespace
