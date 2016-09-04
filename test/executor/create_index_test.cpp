//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_test.cpp
//
// Identification: test/executor/create_index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "planner/create_plan.h"
#include "planner/insert_plan.h"
#include "planner/delete_plan.h"
#include "planner/update_plan.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/delete_executor.h"
#include "executor/update_executor.h"
#include "executor/plan_executor.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CreateIndexTests : public PelotonTest {};

TEST_F(CreateIndexTests, CreatingIndex) {

  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE department_table(dept_id INT PRIMARY KEY,student_id "
      "INT, dept_name TEXT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE department_table(dept_id INT "
                                "PRIMARY KEY, student_id INT, dept_name "
                                "TEXT);"));

  auto& peloton_parser = parser::Parser::GetInstance();

  LOG_INFO("Building parse tree...");
  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, student_id INT, "
      "dept_name TEXT);");
  LOG_INFO("Building parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(create_stmt));
  LOG_INFO("Building plan tree completed!");

  std::vector<common::Value *> params;
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Table Created");
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  // Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,student_id ,dept_name) "
      "VALUES (1,52,'hello_1');");
  statement.reset(new Statement("INSERT",
                                "INSERT INTO department_table(dept_id, "
                                "student_id, dept_name) VALUES "
                                "(1,52,'hello_1');"));

  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,student_id,dept_name) VALUES "
      "(1,52,'hello_1');");
  LOG_INFO("Building parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");

  LOG_INFO("Executing plan...");
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);

  // Now Updating end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Creating and Index");
  LOG_INFO("Query: CREATE INDEX saif ON department_table (student_id);");
  statement.reset(new Statement(
      "CREATE", "CREATE INDEX saif ON department_table (student_id);"));

  LOG_INFO("Building parse tree...");
  auto update_stmt = peloton_parser.BuildParseTree(
      "CREATE INDEX saif ON department_table (student_id);");
  LOG_INFO("Building parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");

  LOG_INFO("Executing plan...");
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("INDEX CREATED!");
  txn_manager.CommitTransaction(txn);

  auto target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table");
  // Expected 2 , Primary key index + created index
  EXPECT_EQ(target_table_->GetIndexCount(), 2);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
