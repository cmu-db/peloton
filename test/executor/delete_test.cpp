//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_test.cpp
//
// Identification: test/executor/delete_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstdio>

#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "catalog/bootstrapper.h"
#include "catalog/catalog.h"
#include "planner/create_plan.h"
#include "planner/insert_plan.h"
#include "planner/delete_plan.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/delete_executor.h"
#include "executor/plan_executor.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"


#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DeleteTests : public PelotonTest {};

TEST_F(DeleteTests, Deleting) {

  LOG_INFO("Bootstrapping...");
  catalog::Bootstrapper::bootstrap();
  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
  LOG_INFO("Bootstrapping completed!");


  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "dept_id", true);
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, 32,
                      "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", std::move(table_schema));
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);
  LOG_INFO("Table created!");

  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT", "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');"));
  auto& peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree("INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<Value> params;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");

  // Now deleting end-to-end
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_id < 2 AND dept_name = 'hello_1'");
  statement.reset(new Statement("DELETE", "DELETE FROM department_table WHERE dept_id < 2 AND dept_name = 'hello_1'"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt = peloton_parser.BuildParseTree("DELETE FROM department_table WHERE dept_id < 2 AND dept_name = 'hello_1'");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");

  // Test Another delete. Should not find any tuple to be deleted
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_id < 2");
  statement.reset(new Statement("DELETE", "DELETE FROM department_table WHERE dept_id < 2"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt_2 = peloton_parser.BuildParseTree("DELETE FROM department_table WHERE dept_id < 2");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt_2));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");
}

}  // End test namespace
}  // End peloton namespace

