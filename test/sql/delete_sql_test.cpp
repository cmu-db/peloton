//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_sql_test.cpp
//
// Identification: test/sql/delete_sql_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class DeleteSQLTests : public PelotonTest {};

TEST_F(DeleteSQLTests, SimpleDeleteSQLTest) {
  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG(
      "Query: CREATE TABLE department_table(dept_id int, dept_name "
      "varchar(32))");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE department_table(dept_id int, dept_name varchar(32));");

  LOG_DEBUG("Table created!");

  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");
  LOG_DEBUG(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(1,'hello_1')");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");

  LOG_DEBUG("Tuple inserted!");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Check value of column dept_name after inserting
  TestingSQLUtil::ExecuteSQLQuery("SELECT dept_name from department_table",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "hello_1");

  LOG_DEBUG("Inserting second tuple...");
  LOG_DEBUG(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(2,'hello_2')");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2,'hello_2');");

  LOG_DEBUG("Tuple inserted!");

  LOG_DEBUG("Inserting third tuple...");
  LOG_DEBUG(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(3,'hello_2')");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');");

  LOG_DEBUG("Tuple inserted!");

  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT COUNT(dept_name) from department_table", result, tuple_descriptor,
      rows_affected, error_message);
  // check the count of rows of the table
  EXPECT_EQ('3', result[0][0]);

  TestingSQLUtil::ExecuteSQLQuery("SELECT MAX(dept_id) FROM department_table",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ('3', result[0][0]);

  TestingSQLUtil::ExecuteSQLQuery(
      "DELETE FROM department_table WHERE dept_id < 3", result,
      tuple_descriptor, rows_affected, error_message);
  // check the count of rows deleted
  EXPECT_EQ(2, rows_affected);

  TestingSQLUtil::ExecuteSQLQuery("SELECT dept_name from department_table",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);
  // check the dept_name after deleting two rows
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "hello_2");

  TestingSQLUtil::ExecuteSQLQuery("DELETE FROM department_table;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // check the count of rows deleted
  EXPECT_EQ(1, rows_affected);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
