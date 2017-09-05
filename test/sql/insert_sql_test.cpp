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

class InsertSQLTest : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
  LOG_INFO("Next");
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
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test3(a INT, b CHAR(4), c VARCHAR(10));");
}

TEST_F(InsertSQLTest, InsertOneValue) {
  LOG_INFO("START");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());
  LOG_INFO("finish creating table");
  // INSERT a tuple
  std::string query("INSERT INTO test VALUES (5, 55, 555);");

  txn = txn_manager.BeginTransaction();
  auto plan = TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::INSERT);
  LOG_INFO("GeneratePlanWithOptimizer");

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);

  EXPECT_EQ(1, rows_changed);
  LOG_INFO("GeneratePlanWithOptimizer1");

  // SELECT to find out if the data is correctly inserted
  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test WHERE a=5", result, tuple_descriptor,
      rows_changed, error_message);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("5", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("55", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("555", TestingSQLUtil::GetResultValueAsString(result, 2));
  LOG_INFO("GeneratePlanWithOptimizer2");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Commit Transaction");
}

TEST_F(InsertSQLTest, InsertMultipleValues) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  // INSERT multiple tuples
  std::string query("INSERT INTO test VALUES (6, 11, 888), (7, 77, 000);");

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

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTest, InsertSpecifyColumns) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<StatementResult> result;
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

TEST_F(InsertSQLTest, InsertTooLargeVarchar) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable3();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("INSERT INTO test3 VALUES(1, 'abcd', 'abcdefghijk');");

  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn),
               peloton::Exception);
  txn_manager.CommitTransaction(txn);

  rows_changed = 0;
  TestingSQLUtil::ExecuteSQLQuery(query, result, tuple_descriptor, rows_changed,
                                  error_message);
  EXPECT_EQ(0, rows_changed);

  TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT * FROM test3;", result, tuple_descriptor, rows_changed,
      error_message);
  EXPECT_EQ(0, result.size());

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTest, InsertIntoSelectSimple) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();
  CreateAndLoadTable2();

  std::vector<StatementResult> result;
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

}  // namespace test
}  // namespace peloton
