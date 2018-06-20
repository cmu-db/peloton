//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// recovery_test.cpp
//
// Identification: test/logging/recovery_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "logging/log_buffer.h"
#include "common/harness.h"
#include "gtest/gtest.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"
#include "network/connection_handle_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Recovery Tests
//===--------------------------------------------------------------------===//

class RecoveryTests : public PelotonTest {};

TEST_F(RecoveryTests, InsertRecoveryTest) {
  LOG_INFO("start InsertRecoveryTest");

  // Recover from the log file and test the results
  settings::SettingsManager::SetString(settings::SettingId::log_directory_name,
                                       "./logging");
  settings::SettingsManager::SetString(settings::SettingId::log_file_name,
                                       "wal.log");
  settings::SettingsManager::SetBool(settings::SettingId::enable_logging, true);
  settings::SettingsManager::SetBool(settings::SettingId::enable_recovery, true);

  LOG_INFO("before Initialize");

  PelotonInit::Initialize();

  LOG_INFO("after Initialize");

  // make sure the data in test_table is correct
  std::string sql1 = "SELECT * FROM test_table;";
  std::vector<std::string> expected1 = {"0|1.2|Aaron", "1|12.34|loves",
                                        "2|12345.7|databases"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql1, expected1, false);

  LOG_INFO("after ExecuteSQLQueryAndCheckResult");

  PelotonInit::Shutdown();
}

}
}