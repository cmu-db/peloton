//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_concurrency_sql_test.cpp
//
// Identification: test/sql/index_concurrency_sql_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <thread>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class IndexConcurrencySQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  LOG_TRACE("create and load table");
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b INT);");

  // Insert 1m tuples into table
  for (int i = 0; i < 10000; i++) {
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (" + std::to_string(i) + ", " + std::to_string(i) + ");");
  }
  LOG_TRACE("create and load table complete");
}

void CreateIndex() {
  LOG_TRACE("create index");
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX i1 ON test(a);");
  LOG_TRACE("create index complete");
}

void InsertTuple() {
  LOG_TRACE("insert tuple");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (-1, -1);");
  LOG_TRACE("insert tuple complete");
}

void UpdateTuple() {
  LOG_TRACE("update tuple");
  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a = -1 WHERE a = 0;");
  LOG_TRACE("update tuple complete");
}

void DeleteTuple() {
  LOG_TRACE("delete tuple");
  TestingSQLUtil::ExecuteSQLQuery("DELETE FROM test WHERE a = 0;");
  LOG_TRACE("delete tuple complete");
}

// TODO: fix the test. Currently will corrupt memory.
/*
TEST_F(IndexConcurrencySQLTests, CreateIndexAndInsertTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  LOG_TRACE("select");
  TestingSQLUtil::ExecuteSQLQuery("SELECT b FROM test WHERE a < 1;",
  result, tuple_descriptor, rows_changed,
  error_message);
  LOG_TRACE("select complete");

  // Check the return value
  // Should be: 0
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("0", TestingSQLUtil::GetResultValueAsString(result, 0));

  std::thread thread1(CreateIndex);
  std::thread thread2(InsertTuple);

  thread1.join();
  thread2.join();

  LOG_TRACE("select");
  TestingSQLUtil::ExecuteSQLQuery("SELECT b FROM test WHERE a < 0;",
                                  result, tuple_descriptor, rows_changed,
                                  error_message);
  LOG_TRACE("select complete");

  // Check the return value
  // Should be: -1
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("-1", TestingSQLUtil::GetResultValueAsString(result, 0));
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(IndexConcurrencySQLTests, CreateIndexAndUpdateTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  std::thread thread1(CreateIndex);
  std::thread thread2(UpdateTuple);

  thread1.join();
  thread2.join();

  LOG_TRACE("select");
  TestingSQLUtil::ExecuteSQLQuery("SELECT b FROM test WHERE a < 0;",
                                  result, tuple_descriptor, rows_changed,
                                  error_message);
  LOG_TRACE("select complete");

  // Check the return value
  // Should be: -1
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("-1", TestingSQLUtil::GetResultValueAsString(result, 0));
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(IndexConcurrencySQLTests, CreateIndexAndDeleteTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  std::thread thread1(CreateIndex);
  std::thread thread2(DeleteTuple);

  thread1.join();
  thread2.join();

  LOG_TRACE("select");
  TestingSQLUtil::ExecuteSQLQuery("SELECT b FROM test WHERE a < 2;",
                                  result, tuple_descriptor, rows_changed,
                                  error_message);
LOG_TRACE("select complete");

  // Check the return value
  // Should be: 1
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
*/

}  // namespace test
}  // namespace peloton
