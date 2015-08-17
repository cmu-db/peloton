#include "gtest/gtest.h"

#include "backend/logging/logmanager.h"
#include "logging/logging_tests_util.h"
#include "backend/concurrency/transaction_manager.h"

#include <thread>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

TEST(AriesLoggingTest, logging_start_end) {
  std::cout << "aries logging test start\n";

  // start logging thread
  auto& logManager = logging::LogManager::GetInstance();
  logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
  std::thread thread(&logging::LogManager::StandbyLogging, 
                     &logManager, 
                     logManager.GetMainLoggingType());

  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  LoggingTestsUtil::CreateDatabase(20000);

  LoggingTestsUtil::CreateTable(20000, 10000, "test_table");
  
  // insert tuples

  // delete tuples

  // update tuples

  // abort ??

  LoggingTestsUtil::DropTable(20000, 10000);
  LoggingTestsUtil::DropDatabase(20000);
  txn_manager.CommitTransaction(txn);

  sleep(2);

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
