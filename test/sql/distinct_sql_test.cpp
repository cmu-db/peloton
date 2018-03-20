//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// distinct_sql_test.cpp
//
// Identification: test/sql/distinct_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class DistinctSQLTests : public PelotonTest {
 protected:
  void CreateAndLoadTable() {
    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (2, 22, 333, 'abc');");
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (3, 11, 222, 'abcd');");
  }

  void ExecuteSQLQueryAndCheckUnorderedResult(std::string query, std::unordered_set<std::string> ref_result) {
    std::vector<ResultValue> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;

    // Execute query
    TestingSQLUtil::ExecuteSQLQuery(std::move(query), result, tuple_descriptor, rows_changed, error_message);
    unsigned int rows = result.size() / tuple_descriptor.size();


    // Build actual result as set of rows for comparison
    std::unordered_set<std::string> actual_result(rows);
    for (unsigned int i = 0; i < rows; i++) {
      std::string row_string;

      for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        row_string += TestingSQLUtil::GetResultValueAsString(result, i * tuple_descriptor.size() + j);
        if (j < tuple_descriptor.size() - 1) {
          row_string += "|";
        }
      }

      actual_result.insert(std::move(row_string));
    }


    // Compare actual result to expected result
    EXPECT_EQ(ref_result.size(), actual_result.size());

    for (auto &result_row : ref_result) {
      if (actual_result.erase(result_row) == 0) {

        EXPECT_EQ(ref_result, actual_result);
        break;
      }
    }
  }
};

TEST_F(DistinctSQLTests, DistinctIntTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT DISTINCT b FROM test;",
                                         {"22", "11"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctVarcharTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT DISTINCT d FROM test;",
                                         {"abcd", "abc"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctTupleTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT DISTINCT b, c FROM test;",
                                         {"22|333",
                                          "11|222"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctStarTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 222, 'abcd');");

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT DISTINCT * FROM test;",
                                         {"1|22|333|abcd",
                                          "1|22|222|abcd"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctDateTimeTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b TIMESTAMP);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, '2016-06-22 19:10:25-07');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, '2017-06-22 19:10:25-07');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, '2016-06-22 19:10:25-07');");

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT DISTINCT b FROM test;",
                                         {"2016-06-22 19:10:25.000000-07",
                                          "2017-06-22 19:10:25.000000-07"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
