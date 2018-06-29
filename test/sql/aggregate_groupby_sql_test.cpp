//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_groupby_sql_test.cpp
//
// Identification: test/sql/aggregate_groupby_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/optimizer.h"
#include "sql/testing_sql_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

class AggregateGroupBySQLTests : public PelotonTest {
 protected:
  void CreateAndLoadTable() {
    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d INT);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 2, 3, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 2, 3, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 2, 6, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 2, 3, 2);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 3, 6, 2);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 4, 6, 2);");
  }
};

TEST_F(AggregateGroupBySQLTests, AggregateGroupByManyAVGsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT AVG(a), AVG(b), AVG(c), AVG(c), AVG(c) FROM test GROUP BY d;",
      {"5|3|5|5|5", "2|2|4|4|4"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

TEST_F(AggregateGroupBySQLTests, AggregateGroupByMixedAVGsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // This test especially tests several AVGs in a row
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT SUM(a), AVG(a), COUNT(b), AVG(b), MAX(c), AVG(c) FROM test GROUP "
      "BY d;",
      {"15|5|3|3|6|5", "6|2|3|2|6|4"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

TEST_F(AggregateGroupBySQLTests, AggregateGroupByAllAggregationsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT AVG(a), SUM(a), MAX(a), MIN(a), COUNT(a) FROM test GROUP BY d;",
      {"2|6|3|1|3", "5|15|6|4|3"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

TEST_F(AggregateGroupBySQLTests, AggregateGroupBySingleRowPerGroupSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // This test especially tests several AVGs in a row
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT COUNT(*), MIN(b), MAX(c), AVG(d) FROM test GROUP BY a;",
      {"1|4|6|2", "1|2|3|1", "1|3|6|2", "1|2|3|2", "1|2|3|1", "1|2|6|1"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
