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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class UpdateTests : public PelotonTest {};

TEST_F(UpdateTests, Updating) {
  LOG_INFO("Bootstrapping...");
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto manager_id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "manager_id", true);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, manager_id_column, name_column}));
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
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
  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);

  LOG_INFO("Table created!");

  // Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,manager_id,dept_name) "
      "VALUES (1,12,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT",
                                "INSERT INTO "
                                "department_table(dept_id,manager_id,dept_name)"
                                " VALUES (1,12,'hello_1');"));
  auto& peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,manager_id,dept_name) VALUES "
      "(1,12,'hello_1');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<common::Value> params;
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  txn_manager.CommitTransaction(txn);

  // Now Updating end-to-end
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Updating a tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  statement.reset(new Statement(
      "UPDATE",
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  auto update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple Updated!");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  LOG_INFO("Updating another tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET manager_id = manager_id + 1 WHERE "
      "dept_id = 1");
  statement.reset(new Statement("UPDATE",
                                "UPDATE department_table SET manager_id = "
                                "manager_id + 1 WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET manager_id = manager_id + 1 WHERE dept_id = "
      "1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple Updated!");
  txn_manager.CommitTransaction(txn);

  // Deleting now
  txn = txn_manager.BeginTransaction();
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_name = 'CS'");
  statement.reset(new Statement(
      "DELETE", "DELETE FROM department_table WHERE dept_name = 'CS'"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_name = 'CS'");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
