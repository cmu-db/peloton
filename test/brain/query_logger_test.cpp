//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_logger_test.cpp
//
// Identification: test/brain/query_logger_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"
#include "settings/settings_manager.h"
#include "parser/pg_query.h"
#include "threadpool/brain_thread_pool.h"

using std::vector;
using std::string;

namespace peloton {
namespace test {

class QueryLoggerTests : public PelotonTest {
 protected:
  virtual void SetUp() override {
    settings::SettingsManager::SetBool(settings::SettingId::brain, true);
    PelotonInit::Initialize();
  }

  virtual void TearDown() override {
    PelotonInit::Shutdown();
  }
};

TEST_F(QueryLoggerTests, SimpleInsertsTest) {
  std::vector<std::string> expected_result;
  std::string test_query;
  std::string test_query_fingerprint;
  std::vector<ResultValue> result;
  std::string select_query =
      "SELECT * FROM pg_catalog.pg_query_history;";
  std::string select_query_fingerprint =
      pg_query_fingerprint(select_query.c_str()).hexdigest;

  test_query = "CREATE TABLE test(a INT);";
  test_query_fingerprint = pg_query_fingerprint(test_query.c_str()).hexdigest;

  expected_result.push_back(test_query + "|" + test_query_fingerprint);

  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str());
  // bool check = settings::SettingsManager::GetBool(settings::SettingId::brain);

  test_query = "INSERT INTO test VALUES (11);";
  test_query_fingerprint = pg_query_fingerprint(test_query.c_str()).hexdigest;

  expected_result.push_back(test_query + "|" + test_query_fingerprint);

  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str());
  // check = settings::SettingsManager::GetBool(settings::SettingId::brain);

  test_query = "SELECT * FROM test;";
  test_query_fingerprint = pg_query_fingerprint(test_query.c_str()).hexdigest;

  expected_result.push_back(test_query + "|" + test_query_fingerprint);

  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  LOG_INFO("SELECT RESULT SIZE: %lu", result.size());
  LOG_INFO("SELECT RESULT VALUE: %s", TestingSQLUtil::GetResultValueAsString(result, 0).c_str());
  // check = settings::SettingsManager::GetBool(settings::SettingId::brain);


  test_query = "SELECT * FROM pg_catalog.pg_table WHERE table_name = 'pg_query_history';";
  test_query_fingerprint = pg_query_fingerprint(test_query.c_str()).hexdigest;

  expected_result.push_back(test_query + "|" + test_query_fingerprint);

  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  LOG_INFO("SELECT CATALOG RESULT SIZE: %lu", result.size());
  LOG_INFO("SELECT CATALOG RESULT VALUE: %s", TestingSQLUtil::GetResultValueAsString(result, 1).c_str());

  sleep(2);
  
  result.clear();

  TestingSQLUtil::ExecuteSQLQuery(select_query.c_str(), result);
  
  LOG_INFO("RESULT SIZE: %lu", result.size());
}

}  // namespace test
}  // namespace peloton
