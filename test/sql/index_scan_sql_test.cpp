//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_sql_test.cpp
//
// Identification: test/sql/index_scan_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

class IndexScanSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 33, 111, 'bcda');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 11, 222, 'bcd');");
}

TEST_F(IndexScanSQLTests, CreateIndexAfterInsertTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX i1 ON test(a);", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  TestingSQLUtil::ExecuteSQLQuery("SELECT b FROM test WHERE a < 3;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  // Should be: 22, 33
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("33", TestingSQLUtil::GetResultValueAsString(result, 1));
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(IndexScanSQLTests, CreateIndexAfterInsertOnMultipleColumnsTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX i1 ON test(b, c);", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a FROM test WHERE b < 33 AND c > 100 ORDER BY a;", result,
      tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
TEST_F(IndexScanSQLTests, SQLTest) {
  LOG_INFO("Bootstrapping...");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, dept_name "
      "VARCHAR);");
  LOG_INFO("Table created!");

  std::vector<ResultValue> result;
  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2, 'hello_2');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Select a tuple...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT * FROM department_table WHERE dept_id = 1;", result);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("hello_1", TestingSQLUtil::GetResultValueAsString(result, 1));
  LOG_INFO("Tuple selected");

  LOG_INFO("Select a column...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT dept_name FROM department_table WHERE dept_id = 2;", result);
  EXPECT_EQ("hello_2", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Column selected");

  LOG_INFO("Select COUNT(*)...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id < 3;", result);
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Aggregation selected");

  LOG_INFO("Select COUNT(*)...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id > 1;", result);
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Aggregation selected");

  LOG_INFO("Select COUNT(*)...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id < 3 and dept_id > "
      "1;",
      result);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Aggregation selected");

  LOG_INFO("Select COUNT(*)...");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id < 3 and dept_id > "
      "2;",
      result);
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Aggregation selected");

  LOG_INFO("Select COUNT(*)... with removable predicate");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id = 2 and dept_name "
      "= 'hello_2';",
      result);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Removable preciate selected");

  LOG_INFO("Select COUNT(*)... with complex removable predicate");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id = 2 and dept_name = "
      "'hello_2' and dept_name = 'hello_2';",
      result);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Complex removable preciate selected");

  LOG_INFO("Select COUNT(*)... with complex removable predicate");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id = 1 and dept_name = "
      "'hello_2' and dept_name = 'hello_2';",
      result);
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Complex removable preciate selected");

  LOG_INFO("Select COUNT(*)... with complex removable predicate");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(*) FROM department_table WHERE dept_id = 2 and dept_name = "
      "'hello_1' and dept_name = 'hello_2';",
      result);
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));
  LOG_INFO("Complex removable preciate selected");

  // Those are checking update with removable predicates. Should move to other
  // place later.
  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE department_table set dept_name = 'hahaha' WHERE dept_id = 2 and "
      "dept_name = "
      "'hello_2' and dept_name = 'hello_2';",
      result);

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE department_table set dept_name = 'hahaha' WHERE dept_id = 2 and "
      " dept_name = 'hello_2';",
      result);

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE department_table set dept_name = 'hahaha' WHERE dept_id = 2;",
      result);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
