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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

#include "sql/sql_tests_util.h"

namespace peloton {
namespace test {

class OrderBySQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  SQLTestsUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  SQLTestsUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 33, 111, 'bcda');");
  SQLTestsUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 11, 222, 'bcd');");
}

TEST_F(OrderBySQLTests, PerformanceTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c "
      "INT, d INT, e TIMESTAMP);");

  SQLTestsUtil::ExecuteSQLQuery("CREATE INDEX idx_order ON test (b,c);");

  // Load table
  // You can increase the size of the table to test large result
  int table_size = 100;
  int min = 1;
  int max = 100;
  std::string insert_prefix = "INSERT INTO test VALUES (";
  for (int count = 0; count < table_size; count++) {
    int c_value = SQLTestsUtil::GetRandomInteger(min, max);
    std::string insert_statement = insert_prefix + std::to_string(count) + "," +
                                   "1" + "," + std::to_string(c_value) + "," +
                                   std::to_string(count) + "," +
                                   "'2016-12-06 00:00:02-04'" + ");";
    SQLTestsUtil::ExecuteSQLQuery(insert_statement);
  }

  SQLTestsUtil::ShowTable(DEFAULT_DB_NAME, "test");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  std::chrono::system_clock::time_point start_time =
      std::chrono::system_clock::now();

  // test OrderBy Limit
  SQLTestsUtil::ExecuteSQLQuery(
      "SELECT c from test WHERE b=1 ORDER BY c LIMIT 10", result,
      tuple_descriptor, rows_affected, error_message);

  std::chrono::system_clock::time_point end_time =
      std::chrono::system_clock::now();

  auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                     end_time - start_time)
                     .count();

  LOG_INFO(
      "OrderBy Query (table size:%d) with Limit 10 Execution Time is: %lu ms",
      table_size, latency);

  // test OrderBy without Limit
  start_time = std::chrono::system_clock::now();

  SQLTestsUtil::ExecuteSQLQuery("SELECT c from test WHERE b=1 ORDER BY c",
                                result, tuple_descriptor, rows_affected,
                                error_message);

  end_time = std::chrono::system_clock::now();

  latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                  start_time)
                .count();

  LOG_INFO("OrderBy Query (table size:%d) Execution Time is: %lu ms",
           table_size, latency);

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OrderBySQLTests, OrderByWithColumnsTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;


  SQLTestsUtil::ExecuteSQLQuery("SELECT a, b FROM test ORDER BY b;", result, tuple_descriptor, rows_changed, error_message);


  // Check the return value
  // Should be: 3, 1, 2
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 4));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithColumnsDescTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;


  SQLTestsUtil::ExecuteSQLQuery("SELECT a, b FROM test ORDER BY b DESC;", result, tuple_descriptor, rows_changed, error_message);


  // Check the return value
  // Should be: 2, 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 4));


  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithoutColumnsTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT a FROM test ORDER BY b;", result, tuple_descriptor, rows_changed, error_message);


  // Check the return value
  // Should be: 3, 1, 2
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 2));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OrderBySQLTests, OrderByWithoutColumnsDescTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT a FROM test ORDER BY b DESC;", result, tuple_descriptor, rows_changed, error_message);


  // Check the return value
  // Should be: 2, 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 2));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithColumnsAndLimitTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;


  SQLTestsUtil::ExecuteSQLQuery("SELECT a, b, d FROM test ORDER BY d LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);
  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 3));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithColumnsAndLimitDescTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;


  SQLTestsUtil::ExecuteSQLQuery("SELECT a, b, d FROM test ORDER BY d DESC LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);
  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 2, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 3));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithoutColumnsAndLimitTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT a FROM test ORDER BY d LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);
  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 1));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByWithoutColumnsAndLimitDescTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT a FROM test ORDER BY d DESC LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);
  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 2, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 1));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


TEST_F(OrderBySQLTests, OrderByStar) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test ORDER BY d;", result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: [1, 22, 333, 'abcd']; [3,...], [2,...];
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("22", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("333", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("abcd", SQLTestsUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 8));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByStarDesc) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test ORDER BY d DESC;", result, tuple_descriptor, rows_changed, error_message);

  // Check the return value
  // Should be: [2, 33, 111, 'bcda']; [3,...], [1,...];
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("33", SQLTestsUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("111", SQLTestsUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("bcda", SQLTestsUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 8));

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByStarWithLimit) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test ORDER BY d LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);


  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 4));


  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OrderBySQLTests, OrderByStarWithLimitDesc) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test ORDER BY d DESC LIMIT 2;", result, tuple_descriptor, rows_changed, error_message);


  //Check if the correct amount of results is here
  EXPECT_EQ(2, result.size()/tuple_descriptor.size());

  // Check the return value
  // Should be: 1, 3
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("2", SQLTestsUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", SQLTestsUtil::GetResultValueAsString(result, 4));


  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


}  // namespace test
}  // namespace peloton
