//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_sql_test.cpp
//
// Identification: test/sql/order_by_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"
#include "common/logger.h"

namespace peloton {
namespace test {

class UDFTests : public PelotonTest {};


// UDF Registering Tests

TEST_F(UDFTests, CUDFTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  //Insert UDF
  TestingSQLUtil::ExecuteSQLQuery("CREATE FUNCTION c_overpaid(integer, integer) RETURNS boolean AS 'DIRECTORY/funcs', 'c_overpaid' LANGUAGE C STRICT;");
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("SELECT function_name from pg_catalog.pg_proc", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "c_overpaid");

  // Tear Down
  TestingSQLUtil::ExecuteSQLQuery("DELETE from pg_catalog.pg_proc where function_name = 'c_overpaid' ");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(UDFTests, PLPGSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Insert UDF
  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$ BEGIN RETURN i + 1; END $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT function_name from pg_catalog.pg_proc", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "increment");

  // TestingSQLUtil::ShowTable("pg_catalog","pg_proc");
  // LOG_DEBUG("Statement executed. Result: %s", ResultTypeToString(status).c_str());

  // Tear Down
  TestingSQLUtil::ExecuteSQLQuery("DELETE from pg_catalog.pg_proc where function_name = 'increment' ");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
 
}

TEST_F(UDFTests, PLPGSQLInvocationTest){
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  //Insert the UDF
  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$ BEGIN RETURN i + 1 END $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT increment(5);", result, tuple_descriptor,rows_affected, error_message);

  EXPECT_EQ('6', result[0].second[0]);

  // Tear Down
  TestingSQLUtil::ExecuteSQLQuery("DELETE from pg_catalog.pg_proc where function_name = 'increment' ");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}


TEST_F(UDFTests, TableInvocationTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b double);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (0, 1);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 3);");

  //Insert the UDF
  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$ BEGIN RETURN i + 1 END $$ LANGUAGE plpgsql;");


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("SELECT increment(a) from test;", result, tuple_descriptor,rows_affected, error_message);
  
  EXPECT_EQ('1', result[0].second[0]);
  EXPECT_EQ('2', result[1].second[0]);
  EXPECT_EQ('3', result[2].second[0]);
  
  //Tear down
  TestingSQLUtil::ExecuteSQLQuery("DELETE from pg_catalog.pg_proc where function_name = 'increment' ");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}



TEST_F(UDFTests, AddTwoValues) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION add(a integer, b integer) RETURNS integer AS $$ BEGIN RETURN a + b END $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT add(5,6);", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  EXPECT_EQ('1', result[0].second[0]);
  EXPECT_EQ('1', result[0].second[1]);

  TestingSQLUtil::ExecuteSQLQuery("DELETE from pg_catalog.pg_proc where function_name = 'add' ");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(UDFTests, TableInvocationTest2) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (0, 1);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 3);");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION add(a integer, b integer) RETURNS integer AS $$ BEGIN RETURN a + b END $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("SELECT add(a, b) from test;", result, tuple_descriptor,rows_affected, error_message);

  EXPECT_EQ('1', result[0].second[0]);
  EXPECT_EQ('3', result[1].second[0]);
  EXPECT_EQ('5', result[2].second[0]);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UDFTests, TableInvocationTest3) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (0, 1);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 3);");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("CREATE OR REPLACE FUNCTION ifelse(a integer) RETURNS integer AS $$ BEGIN IF a%2=0 THEN RETURN a ELSE RETURN -a END IF END $$ LANGUAGE plpgsql;");
  

  TestingSQLUtil::ExecuteSQLQuery("SELECT ifelse(a) from test;", result, tuple_descriptor,rows_affected, error_message);

  EXPECT_EQ('0', result[0].second[0]);
  EXPECT_EQ('-', result[1].second[0]);
  EXPECT_EQ('1', result[1].second[1]);
  EXPECT_EQ('2', result[2].second[0]);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


}  // namespace test
}  // namespace peloton
