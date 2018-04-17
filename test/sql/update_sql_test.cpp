//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_sql_test.cpp
//
// Identification: test/sql/update_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class UpdateSQLTests : public PelotonTest {};

TEST_F(UpdateSQLTests, SimpleUpdateSQLTest) {
  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG("Query: CREATE TABLE test(a INT PRIMARY KEY, b double)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b double);");

  LOG_DEBUG("Table created!");

  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");
  LOG_DEBUG("Query: INSERT INTO test VALUES (0, 1)");

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (0, 1);");

  LOG_DEBUG("Tuple inserted!");

  // Update a tuple into table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET b = 2.0 WHERE a = 0");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = 2.0 WHERE a = 0;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  LOG_DEBUG("Tuple Updated!");

  // Check the return value
  EXPECT_EQ(1, rows_affected);

  // Check value of column b after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT b from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ('2', result[0][0]);

  // Another update a tuple into table
  LOG_DEBUG("Another update a tuple...");
  LOG_DEBUG("Query: UPDATE test SET b = 2 WHERE a = 0");

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = 2 WHERE a = 0;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  LOG_DEBUG("Tuple Updated Again!");

  // Check the return value
  EXPECT_EQ(1, rows_affected);

  // Check value of column b after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT b from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ('2', result[0][0]);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, ComplexUpdateSQLTest) {
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
      "Query: CREATE TABLE employees(e_id int primary key, salary double, "
      "bonus double)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE employees(e_id int primary key, salary double, bonus "
      "double);");

  LOG_DEBUG("Table created!");

  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");
  LOG_DEBUG("Query: INSERT INTO employees VALUES (0, 1.1, 0.5)");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO employees VALUES (0, 1.1, 0.5);");

  LOG_DEBUG("Tuple inserted!");

  // Update a tuple into table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG(
      "Query: UPDATE employees SET "
      "salary = 2 + salary + bonus*salary + 3*(salary+1)+0.1*bonus*salary"
      " WHERE e_id = 0");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET "
      "salary = 2 + salary + bonus*salary + 3*(salary+1)+0.1*bonus*salary"
      " WHERE e_id = 0;",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_DEBUG("Tuple Updated!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column salary after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT salary from employees", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "10.005");

  // Another update a tuple into table
  LOG_DEBUG("Another update a tuple...");
  LOG_DEBUG(
      "Query: UPDATE employees SET "
      "salary = 10, bonus = bonus + 5 WHERE e_id = 0");

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET "
      "salary = 10, bonus = bonus + 5 WHERE e_id = 0;",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_DEBUG("Tuple Updated Again!");

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

TEST_F(UpdateSQLTests, UpdateSQLCastTest) {
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
      "Query: CREATE TABLE employees(e_id int primary key, salary double, "
      "bonus double)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE employees(e_id int primary key, salary double, bonus "
      "double);");

  LOG_DEBUG("Table created!");

  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");
  LOG_DEBUG("Query: INSERT INTO employees VALUES (0, 1, 0.5)");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO employees VALUES (0, 1, 0.5);");

  LOG_DEBUG("Tuple inserted!");

  // Update a tuple into table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE employees SET salary = 2 WHERE e_id = 0");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET salary = 2.0 WHERE e_id = 0",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_DEBUG("Tuple Updated!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column salary after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT salary from employees", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "2");

  // Another update a tuple into table
  LOG_DEBUG("Another update a tuple...");
  LOG_DEBUG("Query: UPDATE employees SET salary = 3 WHERE e_id = 0");

  TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE employees SET salary = 3 WHERE e_id = 0",
      result, tuple_descriptor, rows_affected, error_message);

  LOG_DEBUG("Tuple Updated Again!");

  // Check the return value
  EXPECT_EQ(rows_affected, 1);

  // Check value of column salary and bonus after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT salary from employees", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "3");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, HalloweenProblemTest) {
  // This SQL Test verifies that executor does not cause the Halloween Problem
  // This test checks for tables without Primary Keys

  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  // Setting ActiveTileGroupCount to 3 in order to trigger the case where
  // the executor scans TileGroup 0, inserts an empty version in TileGroup 1.
  // It then inserts the updated version of the tuple in TileGroup 2.
  // When it scans TileGroup 2, without the statement level WriteSet,
  // it would have caused a second update on an already updated Tuple.

  size_t active_tilegroup_count = 3;
  storage::DataTable::SetActiveTileGroupCount(active_tilegroup_count);

  LOG_DEBUG("Active tile group count = %zu",
            storage::DataTable::GetActiveTileGroupCount());
  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG("Query: CREATE TABLE test(a INT, b INT)");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a INT, b INT);");

  LOG_DEBUG("Table created!");

  txn = txn_manager.BeginTransaction();
  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");

  LOG_DEBUG("Query: INSERT INTO test VALUES (10, 100)");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (10, 1000);");
  LOG_DEBUG("Tuple inserted!");

  // Update a tuple in the table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET a = a/2");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = a/2;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  // Check the return value
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Updated!");
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Selecting updated value.");
  txn = txn_manager.BeginTransaction();
  // Check value of column a after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT a from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "5");
  LOG_DEBUG("Successfully updated tuple.");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, HalloweenProblemTestWithPK) {
  // This SQL Test verifies that executor does not cause the Halloween Problem
  // This test checks for tables with Primary Keys
  // It checks for updates on both Non-Primary Key column & Primary Key column

  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  // active_tilegroup_count set to 3, [Reason: Refer to HalloweenProblemTest]
  size_t active_tilegroup_count = 3;
  storage::DataTable::SetActiveTileGroupCount(active_tilegroup_count);

  LOG_DEBUG("Active tile group count = %zu",
            storage::DataTable::GetActiveTileGroupCount());
  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG("Query: CREATE TABLE test(a INT PRIMARY KEY, b INT)");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a INT PRIMARY KEY, b INT);");

  LOG_DEBUG("Table created!");

  txn = txn_manager.BeginTransaction();
  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");

  LOG_DEBUG("Query: INSERT INTO test VALUES (10, 100)");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (10, 1000);");
  LOG_DEBUG("Tuple inserted!");

  // Update a tuple in table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET a = a/2");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = a/2;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  // Check the return value
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Primary Key column Updated!");
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Selecting updated value.");
  txn = txn_manager.BeginTransaction();
  // Check value of column a after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT a from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "5");
  LOG_DEBUG("Successfully updated tuple.");

  txn = txn_manager.BeginTransaction();

  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET b = b/2");

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = b/2;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Non-Primary Key column Updated!");
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Selecting updated value.");
  txn = txn_manager.BeginTransaction();
  // Check value of column b after updating
  TestingSQLUtil::ExecuteSQLQuery("SELECT b from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "500");
  LOG_DEBUG("Successfully updated tuple.");




  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, MultiTileGroupUpdateSQLTest) {
  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  // active_tilegroup_count set to 3, [Reason: Refer to HalloweenProblemTest]
  size_t active_tilegroup_count = 3;
  storage::DataTable::SetActiveTileGroupCount(active_tilegroup_count);

  LOG_DEBUG("Active tile group count = %zu",
            storage::DataTable::GetActiveTileGroupCount());
  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG("Query: CREATE TABLE test(a INT PRIMARY KEY, b INT)");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");

  LOG_DEBUG("Table created!");

  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");

  LOG_DEBUG("Query: INSERT INTO test VALUES (1, 100)");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 100);");

  LOG_DEBUG("Tuple inserted!");

  // Update a tuple in the table
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET a = 10 WHERE b = 100");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = 10 WHERE b = 100;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  LOG_DEBUG("Query: UPDATE test SET a = 1 WHERE b = 100");
  // Check the return value
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Update successful!");

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = 1 WHERE b = 100;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  // Check the return value
  // Updating the tuple the second time casued the assertion failure
  // on line 641 of timestamp_ordering_transaction_manager.cpp
  // This was because it tried to update an already updated version
  // of the tuple. This was fixed by the statement level WriteSet.
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Update successful, again!");


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UpdateSQLTests, AttributeOrderUpdateSQLTest) {
  // This test updates the attributes of the table in different orders.
  // It ensure that the updates in the  data_table are in the correct
  // order irrespective of the order of attributes in the update statement.

  LOG_DEBUG("Bootstrapping...");

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Bootstrapping completed!");

  txn = txn_manager.BeginTransaction();
  // Create a table first
  LOG_DEBUG("Creating a table...");
  LOG_DEBUG("Query: CREATE TABLE test(a INT, b INT)");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a INT, b INT);");
  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Table created!");

  txn = txn_manager.BeginTransaction();
  // Insert a tuple into table
  LOG_DEBUG("Inserting a tuple...");

  LOG_DEBUG("Query: INSERT INTO test VALUES (1, 100)");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 100);");

  txn_manager.CommitTransaction(txn);
  LOG_DEBUG("Tuple inserted!");

  txn = txn_manager.BeginTransaction();
  // Update a tuple in the table in the order (b, a)
  LOG_DEBUG("Updating a tuple...");
  LOG_DEBUG("Query: UPDATE test SET b = b * 2, a = a * 2");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET b = b * 2, a = a * 2;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  txn_manager.CommitTransaction(txn);
  // Check the return value
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Update successful!");

  // Check the updated values.
  txn = txn_manager.BeginTransaction();
  // Update a tuple in the table in the order (b, a)
  LOG_DEBUG("Selecting the updated tuple...");
  LOG_DEBUG("Query: SELECT a, b FROM test;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT a, b FROM test;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  txn_manager.CommitTransaction(txn);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "2");
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 1), "200");
  LOG_DEBUG("Attributes updated in the correct order!");

  txn = txn_manager.BeginTransaction();
  // Update a tuple in the table in the order (a, b)
  LOG_DEBUG("Updating a tuple again...");
  LOG_DEBUG("Query: UPDATE test SET a = a * 2, b = b * 2");

  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = a * 2, b = b * 2;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);

  txn_manager.CommitTransaction(txn);
  // Check the return value
  EXPECT_EQ(1, rows_affected);
  LOG_DEBUG("Tuple Update successful, agin!");

  // Check the updated values.
  txn = txn_manager.BeginTransaction();
  // Update a tuple in the table in the order (b, a)
  LOG_DEBUG("Selecting the updated tuple...");
  LOG_DEBUG("Query: SELECT a, b FROM test;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT a, b FROM test;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  txn_manager.CommitTransaction(txn);
  // Check the return value
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "4");
  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 1), "400");
  LOG_DEBUG("Attributes updated in the correct order, again!");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
