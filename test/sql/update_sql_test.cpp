//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_sql_test.cpp
//
// Identification: test/sql/update_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class UpdateSQLTests : public PelotonTest {};

TEST_F(UpdateSQLTests, SimpleUpdateSQLTest) {
  LOG_INFO("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  LOG_INFO("Query: CREATE TABLE test(a INT PRIMARY KEY, b double)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b double);");

  LOG_INFO("Table created!");

  // Insert a tuple into table
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO test VALUES (0, 1)");

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (0, 1);");

  LOG_INFO("Tuple inserted!");

  // Update a tuple into table
  LOG_INFO("Updating a tuple...");
  LOG_INFO("Query: UPDATE test SET b = 2.0 WHERE a = 0");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = 2.0 WHERE a = 0;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  LOG_INFO("Tuple Updated!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column b after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT b from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(result[0].second[0], '2');

  // Another update a tuple into table
  LOG_INFO("Another update a tuple...");
  LOG_INFO("Query: UPDATE test SET b = 2 WHERE a = 0");

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = 2 WHERE a = 0;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  LOG_INFO("Tuple Updated Again!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column b after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT b from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(result[0].second[0], '2');

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, ComplexUpdateSQLTest) {
  LOG_INFO("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  LOG_INFO(
      "Query: CREATE TABLE employees(e_id int primary key, salary double, "
      "bonus double)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE employees(e_id int primary key, salary double, bonus "
      "double);");

  LOG_INFO("Table created!");

  // Insert a tuple into table
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO employees VALUES (0, 1.1, 0.5)");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO employees VALUES (0, 1.1, 0.5);");

  LOG_INFO("Tuple inserted!");

  // Update a tuple into table
  LOG_INFO("Updating a tuple...");
  LOG_INFO(
      "Query: UPDATE employees SET "
      "salary = 2 + salary + bonus*salary + 3*(salary+1)+0.1*bonus*salary"
      " WHERE e_id = 0");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET "
      "salary = 2 + salary + bonus*salary + 3*(salary+1)+0.1*bonus*salary"
      " WHERE e_id = 0;",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_INFO("Tuple Updated!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column salary after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT salary from employees", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "10.005");

  // Another update a tuple into table
  LOG_INFO("Another update a tuple...");
  LOG_INFO(
      "Query: UPDATE employees SET "
      "salary = 10, bonus = bonus + 5 WHERE e_id = 0");

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET "
      "salary = 10, bonus = bonus + 5 WHERE e_id = 0;",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_INFO("Tuple Updated Again!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column salary and bonus after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT salary, bonus from employees", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "10");
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 1), "5.5");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
