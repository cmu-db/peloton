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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"
#include "sql/sql_tests_util.h"

namespace peloton {
namespace test {

class DistinctSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 22, 333, 'abc');");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 11, 222, 'abcd');");
}

TEST_F(DistinctSQLTests, DistinctIntTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT DISTINCT b FROM test;", result,
                                tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: 22, 11
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("22", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", SQLTestsUtil::GetResultValueAsString(result, 1));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctVarcharTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT DISTINCT d FROM test;", result,
                                tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: 'abcd', 'abc'
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(2, result.size() / tuple_descriptor.size());
  EXPECT_EQ("abcd", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("abc", SQLTestsUtil::GetResultValueAsString(result, 1));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctTupleTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT DISTINCT b, c FROM test;", result,
                                tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: [22,333]; [11,222]
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(2, result.size() / tuple_descriptor.size());
  EXPECT_EQ("22", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("333", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("11", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("222", SQLTestsUtil::GetResultValueAsString(result, 3));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctSQLTests, DistinctStarTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 222, 'abcd');");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT DISTINCT * FROM test;", result,
                                tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: [1,22,333,'abcd']; [1, 22, 222, 'abcd']
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(2, result.size() / tuple_descriptor.size());
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("333", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("abcd", SQLTestsUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("22", SQLTestsUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("222", SQLTestsUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("abcd", SQLTestsUtil::GetResultValueAsString(result, 7));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
