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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

#include "sql/sql_tests_util.h"

namespace peloton {
namespace test {

class OptimizerSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  SQLTestsUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
}

TEST_F(OptimizerSQLTests, SimpleSelectTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("SELECT * from test");
  auto select_plan = SQLTestsUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PLAN_NODE_TYPE_SEQSCAN);

  // test small int
  SQLTestsUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 1, 22, 333
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ('1', result[0].second[0]);
  EXPECT_EQ('2', result[1].second[1]);

  // test small int
  SQLTestsUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, "SELECT b, a, c from test", result, tuple_descriptor,
      rows_changed, error_message);
  // Check the return value
  // Should be: 22, 1, 333
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ('2', result[0].second[1]);
  EXPECT_EQ('1', result[1].second[0]);

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerSQLTests, SelectProjectionTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  CreateAndLoadTable();

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());

  std::string query("SELECT a * 5 + b, -1 + c from test");

  // check for plan node type
  auto select_plan = SQLTestsUtil::GeneratePlanWithOptimizer(optimizer, query);
  EXPECT_EQ(select_plan->GetPlanNodeType(), PLAN_NODE_TYPE_PROJECTION);
  EXPECT_EQ(select_plan->GetChildren()[0]->GetPlanNodeType(),
            PLAN_NODE_TYPE_SEQSCAN);

  // test small int
  SQLTestsUtil::ExecuteSQLQueryWithOptimizer(
      optimizer, query, result, tuple_descriptor, rows_changed, error_message);
  // Check the return value
  // Should be: 27, 332
  EXPECT_EQ('2', result[0].second[0]);
  EXPECT_EQ('7', result[0].second[1]);
  EXPECT_EQ('2', result[1].second[2]);

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
