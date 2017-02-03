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
#include "executor/create_executor.h"
#include "planner/create_plan.h"


namespace peloton {
namespace test {

class IndexScanSQLTests : public PelotonTest {};

TEST_F(IndexScanSQLTests, SQLTest) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, dept_name "
      "VARCHAR);");
  LOG_INFO("Table created!");

  std::vector<StatementResult> result;
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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn =
      concurrency::TransactionManagerFactory::GetInstance().BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
