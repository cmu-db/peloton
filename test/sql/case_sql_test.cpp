//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_sql_test.cpp
//
// Identification: test/sql/optimizer_sql_test.cpp
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
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace test {

class CaseSQLTests : public PelotonTest {};

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

TEST_F(CaseSQLTests, Simple) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_DEBUG("Running SELECT a, case when a=1 then 2 else 0 end from test");

  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a, case when a=1 then 2 else 0 end from test",
      result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(CaseSQLTests, SimpleWithArg) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_DEBUG("Running SELECT a, case a when 1 then 2 when 2 then 3 "
                "else 100 end from test");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a, case a when 1 then 2 when 2 then 3 else 100 end from test",
      result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("100", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("100", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(CaseSQLTests, SimpleWithArgStringResult) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_DEBUG("Running SELECT a, case a when 1 then '2' when 2 then '3' "
                "else '100' end from test");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a, case a when 1 then '2' when 2 then '3' else '100' end "
          "from test", result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("100", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("100", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(CaseSQLTests, SimpleMultipleWhen) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_DEBUG("Running SELECT a, case when a=1 then 2 else 0 end from test");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a, case when a=1 then 2 when a=2 then 3 else 0 end from test",
      result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

TEST_F(CaseSQLTests, SimpleMultipleWhenWithoutElse) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_DEBUG("Running SELECT a, case when a=1 then 2 end from test");
  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT a, case when a=1 then 2 when a=2 then 3 end from test",
      result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

}  // namespace test
}  // namespace peloton
