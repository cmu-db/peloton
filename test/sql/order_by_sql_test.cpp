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

TEST_F(OrderBySQLTests, PerformanceTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c "
      "INT, d INT, e TIMESTAMP);");

  SQLTestsUtil::ExecuteSQLQuery("CREATE INDEX idx_order ON test (b,c);");

  // Load table
  int table_size = 100000;
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

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
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
      end_time - start_time).count();

  LOG_INFO(
      "OrderBy Query (table size:%d) with Limit 10 Execution Time is: %lu ms",
      table_size, latency);

  // Check the return value
  EXPECT_EQ(result[0].second[0], '1');

  // test OrderBy without Limit
  start_time = std::chrono::system_clock::now();

  SQLTestsUtil::ExecuteSQLQuery("SELECT c from test WHERE b=1 ORDER BY c",
                                result, tuple_descriptor, rows_affected,
                                error_message);

  end_time = std::chrono::system_clock::now();

  latency = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time).count();

  LOG_INFO("OrderBy Query (table size:%d) Execution Time is: %lu ms",
           table_size, latency);

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
