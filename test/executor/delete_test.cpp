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
#include "sql/testing_sql_util.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/plan_util.h"
#include "traffic_cop/traffic_cop.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DeleteTests : public PelotonTest {};

void ShowTable(std::string database_name, std::string table_name) {
  // auto table =
  // catalog::Catalog::GetInstance()->GetTableWithName(database_name,
  //                                                                table_name);
  (void)database_name;
  std::unique_ptr<Statement> statement;
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  executor::ExecutionResult status;
  std::vector<type::Value> params;
  std::vector<ResultValue> result;

  optimizer::Optimizer optimizer;

  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  statement.reset(new Statement("SELECT", "SELECT * FROM " + table_name));
  auto select_stmt =
      peloton_parser.BuildParseTree("SELECT * FROM " + table_name);
  statement->SetPlanTree(
      optimizer.BuildPelotonPlanTree(select_stmt, DEFAULT_DB_NAME, txn));
  LOG_TRACE("Query Plan\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  std::vector<int> result_format;
  auto tuple_descriptor = traffic_cop.GenerateTupleDescriptor(
      (parser::SelectStatement *)select_stmt->GetStatement(0));
  result_format = std::vector<int>(tuple_descriptor.size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  traffic_cop.CommitQueryHelper();
}

TEST_F(DeleteTests, VariousOperations) {
  LOG_INFO("Bootstrapping...");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  LOG_INFO("Bootstrapping completed!");

  //  optimizer::SimpleOptimizer optimizer;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer;
  optimizer.reset(new optimizer::Optimizer);
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);
  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  EXPECT_EQ(1, (int)catalog::Catalog::GetInstance()
                   ->GetDatabaseObject(DEFAULT_DB_NAME, txn)
                   ->GetTableObjects()
                   .size());
  LOG_INFO("Table created!");

  storage::DataTable *table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "department_table", txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(1,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');"));
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(insert_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  std::vector<type::Value> params;
  std::vector<ResultValue> result;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  std::vector<int> result_format;
  result_format = std::vector<int>(0, 0);
  TestingSQLUtil::counter_.store(1);
  executor::ExecutionResult status = traffic_cop.ExecuteHelper(
      statement->GetPlanTree(), params, result, result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");
  traffic_cop.CommitQueryHelper();
  ShowTable(DEFAULT_DB_NAME, "department_table");

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
      optimizer->BuildPelotonPlanTree(insert_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_INFO("Executing plan...");
  result_format = std::vector<int>(0, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");
  traffic_cop.CommitQueryHelper();
  ShowTable(DEFAULT_DB_NAME, "department_table");

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
      optimizer->BuildPelotonPlanTree(insert_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_INFO("Executing plan...");
  result_format = std::vector<int>(0, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");
  traffic_cop.CommitQueryHelper();
  ShowTable(DEFAULT_DB_NAME, "department_table");

  LOG_INFO("%s", table->GetInfo().c_str());

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
      optimizer->BuildPelotonPlanTree(select_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_INFO("Executing plan...");
  auto tuple_descriptor =
      traffic_cop.GenerateTupleDescriptor(select_stmt->GetStatement(0));
  result_format = std::vector<int>(tuple_descriptor.size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Counted Tuples!");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
      optimizer->BuildPelotonPlanTree(delete_stmt_2, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_INFO("Executing plan...");
  result_format = std::vector<int>(0, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple deleted!");
  traffic_cop.CommitQueryHelper();
  ShowTable(DEFAULT_DB_NAME, "department_table");

  LOG_INFO("%s", table->GetInfo().c_str());

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
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
      optimizer->BuildPelotonPlanTree(delete_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_INFO("Executing plan...");
  result_format = std::vector<int>(0, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple deleted!");
  traffic_cop.CommitQueryHelper();
  ShowTable(DEFAULT_DB_NAME, "department_table");

  LOG_INFO("%s", table->GetInfo().c_str());

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
