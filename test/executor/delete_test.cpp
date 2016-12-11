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
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "tcop/tcop.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DeleteTests : public PelotonTest {};

void ShowTable(std::string database_name, std::string table_name) {
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                                 table_name);
  std::unique_ptr<Statement> statement;
  auto& peloton_parser = parser::Parser::GetInstance();
  bridge::peloton_status status;
  std::vector<common::Value> params;
  std::vector<ResultType> result;
  statement.reset(new Statement("SELECT", "SELECT * FROM " + table->GetName()));
  auto select_stmt =
      peloton_parser.BuildParseTree("SELECT * FROM " + table->GetName());
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format;
  auto tuple_descriptor =
      tcop::TrafficCop::GetInstance().GenerateTupleDescriptor(
          (parser::SelectStatement*)select_stmt->GetStatement(0));
  result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
}

TEST_F(DeleteTests, VariousOperations) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
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
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);
  LOG_INFO("Table created!");

  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(1,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');"));
  auto& peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");
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
  result_format = std::move(std::vector<int>(0, 0));
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");

  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(2,'hello_2');");
  statement.reset(new Statement(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2,'hello_2');"));
  LOG_INFO("Building parse tree...");
  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2,'hello_2');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format = std::move(std::vector<int>(0, 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");

  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(3,'hello_2');");
  statement.reset(new Statement(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');"));
  LOG_INFO("Building parse tree...");
  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format = std::move(std::vector<int>(0, 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");

  // Just Counting number of tuples in table
  LOG_INFO("Selecting MAX(dept_id)");
  LOG_INFO("Query: SELECT MAX(dept_id) FROM department_table;");
  statement.reset(
      new Statement("MAX", "SELECT MAX(dept_id) FROM department_table;"));
  LOG_INFO("Building parse tree...");
  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT MAX(dept_id) FROM department_table;");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  auto tuple_descriptor =
      tcop::TrafficCop::GetInstance().GenerateTupleDescriptor(
          select_stmt->GetStatement(0));
  result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Counted Tuples!");

  // Now deleting end-to-end
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table");
  statement.reset(new Statement("DELETE", "DELETE FROM department_table"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt =
      peloton_parser.BuildParseTree("DELETE FROM department_table");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format = std::move(std::vector<int>(0, 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");

  // Test Another delete. Should not find any tuple to be deleted
  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_id < 2");
  statement.reset(new Statement(
      "DELETE", "DELETE FROM department_table WHERE dept_id < 2"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt_2 = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_id < 2");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(delete_stmt_2));
  LOG_INFO("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  result_format = std::move(std::vector<int>(0, 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
