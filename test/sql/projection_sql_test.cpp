//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_sql_test.cpp
//
// Identification: test/sql/projection_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class ProjectionSQLTests : public PelotonTest {
 public:
  ProjectionSQLTests() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    SetupTestTable();
  }

  ~ProjectionSQLTests() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  void SetupTestTable() {
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a TINYINT, b SMALLINT, c INT, d BIGINT, e "
        "DECIMAL);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (1, 2, 3, 4, 5.0);");
  }
};

TEST_F(ProjectionSQLTests, SimpleProjectionSQLTest) {
  // Test TINYINT
  std::string sql = "SELECT a+a, a-a, a*a, a/a, a+b, a+c, a+d, a+e FROM test";
  std::vector<std::string> expected = {"2|0|1|1|3|4|5|6"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);

  // Test SMALLINT
  sql = "SELECT b+b, b-b, b*b, b/b, b+a, b+c, b+d, b+e FROM test";
  expected = {"4|0|4|1|3|5|6|7"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);

  // Test INT
  sql = "SELECT c+c, c-c, c*c, c/c, c+a, c+b, c+d, c+e FROM test";
  expected = {"6|0|9|1|4|5|7|8"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);

  // Test BIGINT
  sql = "SELECT d+d, d-d, d*d, d/d, d+a, d+b, d+c, d+e FROM test";
  expected = {"8|0|16|1|5|6|7|9"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);

  // Test DECIMAL
  sql = "SELECT e+e, e-e, e*e, e/e, e+a, e+b, e+c, e+d FROM test";
  expected = {"10|0|25|1|6|7|8|9"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);
}

TEST_F(ProjectionSQLTests, ProjectionSQLTest) {
  std::string sql = "SELECT a*5+b, -1+c, 6, a from test";
  std::vector<std::string> expected = {"7|2|6|1"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);

  sql = "SELECT d+e*2.0, e, e+(2*c) from test";
  expected = {"14|5|11"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql, expected, false);
}

}  // namespace test
}  // namespace peloton
