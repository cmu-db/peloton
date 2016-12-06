//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_sql_test.cpp
//
// Identification: test/sql/index_scan_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

#include "sql/sql_tests_util.h"

namespace peloton {
namespace test {

class IndexScanSQLTests : public PelotonTest {};

TEST_F(IndexScanSQLTests, SQLTest) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  // Set dept_id as the primary key (only that we can have a primary key index)
  catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema),
                           CreateType::CREATE_TYPE_TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);
  LOG_INFO("Table created!");

  std::vector<ResultType> result;
  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2, 'hello_2');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  SQLTestsUtil::ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');",
      result);
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Select a tuple...");
  SQLTestsUtil::ExecuteSQLQuery(
      "SELECT STAR", "SELECT * FROM department_table WHERE dept_id = 1;",
      result);
  LOG_INFO("Tuple selected");

  LOG_INFO("Select a column...");
  SQLTestsUtil::ExecuteSQLQuery(
      "SELECT COLUMN",
      "SELECT dept_name FROM department_table WHERE dept_id = 2;", result);
  LOG_INFO("Column selected");

  LOG_INFO("Select COUNT(*)...");
  SQLTestsUtil::ExecuteSQLQuery(
      "SELECT AGGREGATE",
      "SELECT COUNT(*) FROM department_table WHERE dept_id < 3;", result);
  LOG_INFO("Aggregation selected");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
