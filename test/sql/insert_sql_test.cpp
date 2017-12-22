//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_sql_test.cpp
//
// Identification: test/sql/insert_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class InsertSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
}

void CreateAndLoadTable2() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (5, 99, 888);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (6, 88, 777);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (7, 77, 666);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (8, 55, 999);");
}

void CreateAndLoadTable3() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test3(a INT, b CHAR(4), c VARCHAR(10));");
}

void CreateAndLoadTable4() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test4(a INT PRIMARY KEY, b TINYINT, c SMALLINT, "
      "d BIGINT, e DECIMAL, f DOUBLE, g TIMESTAMP, "
      "i CHAR, j VARCHAR, k VARBINARY, l BOOLEAN);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test4 VALUES "
      "(1, 2, 3, 4, 5.1, 6.1, '2017-10-10 00:00:00-00', "
      "'A', 'a', '1', 'true');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test4 VALUES "
      "(11, 12, 13, 14, 15.1, 16.1, '2017-10-11 00:00:00-00', "
      "'B', 'b', '2', 'false');");
}

void CreateAndLoadTable5() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test5(a INT PRIMARY KEY, b TINYINT, c SMALLINT, "
      "d BIGINT, e DECIMAL, f DOUBLE, g TIMESTAMP, "
      "i CHAR, j VARCHAR, k VARBINARY, l BOOLEAN);");
}

void CreateAndLoadTable6() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test6(a INT, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test6 VALUES (1, 22, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test6 VALUES (2, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test6 VALUES (3, 33, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test6 VALUES (4, 00, 555);");
}

void CreateAndLoadTable7() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test7(a INT, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test7 VALUES (99, 5, 888);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test7 VALUES (88, 6, 777);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test7 VALUES (77, 7, 666);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test7 VALUES (55, 8, 999);");
}


TEST_F(InsertSQLTests, InsertOneValue) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // INSERT a tuple
  std::string query("INSERT INTO test VALUES (5, 55, 555);");

  txn = txn_manager.BeginTransaction();
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  // SELECT to find out if the data is correctly inserted
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=5", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("5", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertMultipleValues) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // INSERT multiple tuples
  std::string query("INSERT INTO test VALUES (6, 11, 888), (7, 77, 999);");

  txn = txn_manager.BeginTransaction();
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(2, rows_changed);

  // SELECT to find out if the tuples are correctly inserted
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=6", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("6", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("888", TestingSQLUtil::GetResultValueAsString(result, 2));

  // SELECT to find out if the tuples are correctly inserted
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=7", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("7", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("77", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("999", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertSpecifyColumns) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // INSERT a tuple with columns specified
  std::string query("INSERT INTO test (b, a, c) VALUES (99, 8, 111);");

  txn = txn_manager.BeginTransaction();
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);

  // SELECT to find out if the tuple is correctly inserted
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=8", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("8", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("99", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("111", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertTooLargeVarchar) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable3();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("INSERT INTO test3 VALUES(1, 'abcd', 'abcdefghij');");
  //std::string query("INSERT INTO test3 VALUES(1, 'abcd', 'abcdefghijk');");

  txn = txn_manager.BeginTransaction();
  // This should be re-enabled when the check is properly done in catalog 
  // It used to be done at the insert query level
  //EXPECT_THROW(TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query,
  //             txn, peloton::Exception);
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);
  txn_manager.CommitTransaction(txn);

  rows_changed = 0;
  TestingSQLUtil::ExecuteSQLQuery(query, result, tuple_descriptor, rows_changed,
                                  error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test3;", result, tuple_descriptor, rows_changed,
      error_message);
  EXPECT_EQ(3, result.size());

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertIntoSelectSimple) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();
  CreateAndLoadTable2();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // TEST CASE 1
  std::string query_1("INSERT INTO test SELECT * FROM test2;");

  txn = txn_manager.BeginTransaction();
  auto plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query_1, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_1, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);

  EXPECT_EQ(4, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=8", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("8", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("999", TestingSQLUtil::GetResultValueAsString(result, 2));

  // TEST CASE 2
  std::string query_2("INSERT INTO test2 SELECT * FROM test WHERE a=1;");
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_2, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test2 WHERE a=1", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 2));

  // TEST CASE 3
  std::string query_3("INSERT INTO test2 SELECT b,a,c FROM test WHERE a=2;");
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_3, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test2 WHERE a=11", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertIntoSelectSimpleAllType) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable4();
  CreateAndLoadTable5();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // TEST CASE 1
  std::string query_1("INSERT INTO test5 SELECT * FROM test4;");

  txn = txn_manager.BeginTransaction();
  auto plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query_1, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_1, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);

  EXPECT_EQ(2, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test4 WHERE a=1", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(11, result.size());
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("5.1", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("6.1", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("2017-10-10 00:00:00.000000+00",
             TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("A", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("a", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ('1', TestingSQLUtil::GetResultValueAsString(result, 9).at(0));
  EXPECT_EQ("true", TestingSQLUtil::GetResultValueAsString(result, 10));

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test5 WHERE a=1", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(11, result.size());
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("5.1", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("6.1", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("2017-10-10 00:00:00.000000+00",
             TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("A", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("a", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ('1', TestingSQLUtil::GetResultValueAsString(result, 9).at(0));
  EXPECT_EQ("true", TestingSQLUtil::GetResultValueAsString(result, 10));

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test5 WHERE a=11", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(11, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("12", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("13", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("14", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("15.1", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("16.1", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("2017-10-11 00:00:00.000000+00",
             TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("B", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("b", TestingSQLUtil::GetResultValueAsString(result, 8));
  EXPECT_EQ('2', TestingSQLUtil::GetResultValueAsString(result, 9).at(0));
  EXPECT_EQ("false", TestingSQLUtil::GetResultValueAsString(result, 10));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertIntoSelectColumn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable6();
  CreateAndLoadTable7();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // TEST CASE 1
  std::string query_1("INSERT INTO test6 SELECT b,a,c FROM test7;");

  txn = txn_manager.BeginTransaction();
  auto plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query_1, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_1, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);

  EXPECT_EQ(4, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test6 WHERE a=8", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("8", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("999", TestingSQLUtil::GetResultValueAsString(result, 2));

  // TEST CASE 2
  std::string query_2("INSERT INTO test7 SELECT * FROM test6 WHERE a=1;");
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_2, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test7 WHERE a=1", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("333", TestingSQLUtil::GetResultValueAsString(result, 2));

  // TEST CASE 3
  std::string query_3("INSERT INTO test7 SELECT b,a,c FROM test6 WHERE a=2;");
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query_3, result,
                                               tuple_descriptor, rows_changed,
                                               error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test7 WHERE a=11", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("11", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 2));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
