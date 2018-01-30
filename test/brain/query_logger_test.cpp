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
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create test database
    CreateAndLoadTable();
  }

  virtual void TearDown() override {
    // Destroy test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }

  /*** Helper functions **/
  void CreateAndLoadTable() {
    // Create database
    // auto &txn_manager =
    // concurrency::TransactionManagerFactory::GetInstance();
    // auto txn = txn_manager.BeginTransaction();
    // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    // txn_manager.CommitTransaction(txn);

    PelotonInit::Initialize();
    // TODO (Priyatham and Siva): Figure out why this is not working
    // settings::SettingsManager::SetBool(settings::SettingId::brain, true);
    // threadpool::BrainThreadPool::GetInstance().Startup();

    //   auto catalog = catalog::Catalog::GetInstance();
    //   catalog->Bootstrap();
    // // set max thread number.
    // thread_pool.Initialize(0, std::thread::hardware_concurrency() + 3);

    // threadpool::BrainThreadPool::GetInstance().Startup();

    // // start epoch.
    // concurrency::EpochManagerFactory::GetInstance().StartEpoch();

    // // start GC.
    // gc::GCManagerFactory::GetInstance().StartGC();
  }
};

TEST_F(QueryLoggerTests, SimpleInsertsTest) {
  std::vector<std::string> expected_result;
  std::string test_query;
  std::string test_query_fingerprint;
  std::string select_query =
      "SELECT query_string, fingerprint FROM pg_catalog.pg_query_history;";
  std::string select_query_fingerprint =
      pg_query_fingerprint(select_query.c_str()).hexdigest;

  test_query = "CREATE TABLE test(a INT PRIMARY KEY, b INT);";
  test_query_fingerprint = pg_query_fingerprint(test_query.c_str()).hexdigest;

  expected_result.push_back(test_query + "|" + test_query_fingerprint);

  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str());
  bool check = settings::SettingsManager::GetBool(settings::SettingId::brain);
  LOG_INFO("Boolean: %d", check);
  sleep(5);
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(select_query.c_str(),
                                                expected_result, false);
}

}  // namespace test
}  // namespace peloton
