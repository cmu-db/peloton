//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_index_test.cpp
//
// Identification: test/executor/create_index_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include "sql/testing_sql_util.h"

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/plan_util.h"
#include "planner/update_plan.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CreateIndexTests : public PelotonTest {};

TEST_F(CreateIndexTests, CreatingIndex) {
  LOG_INFO("Bootstrapping...");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Bootstrapping completed!");

  std::unique_ptr<optimizer::AbstractOptimizer> optimizer;
  optimizer.reset(new optimizer::Optimizer);

  auto &traffic_cop = tcop::Tcop::GetInstance();
  auto callback = [] {
    TestingSQLUtil::UtilTestTaskCallback(&TestingSQLUtil::counter_);
  };

  // Create a table first
  txn = txn_manager.BeginTransaction();
  tcop::ClientProcessState state;
  state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  LOG_INFO("Creating table");
  LOG_INFO(
      "Query: CREATE TABLE department_table(dept_id INT PRIMARY KEY,student_id "
      "INT, dept_name TEXT);");
  std::shared_ptr<Statement> statement;
  statement.reset(new Statement("CREATE",
                                "CREATE TABLE department_table(dept_id INT "
                                "PRIMARY KEY, student_id INT, dept_name "
                                "TEXT);"));

  auto &peloton_parser = parser::PostgresParser::GetInstance();

  LOG_INFO("Building parse tree...");
  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE department_table(dept_id INT PRIMARY KEY, student_id INT, "
      "dept_name TEXT);");
  LOG_INFO("Building parse tree completed!");

  LOG_INFO("Binding parse tree...");
  auto parse_tree = create_stmt->GetStatement(0);
  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parse_tree);
  LOG_INFO("Binding parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer->BuildPelotonPlanTree(create_stmt, txn));
  LOG_INFO("Building plan tree completed!");

  std::vector<type::Value> params;
  std::vector<ResultValue> result;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  std::vector<PostgresDataFormat> result_format(statement->GetTupleDescriptor().size(),
                                                PostgresDataFormat::TEXT);

  TestingSQLUtil::counter_.store(1);
  state.statement_ = statement;
  state.param_values_ = params;
  state.result_format_ = result_format;
  executor::ExecutionResult status = traffic_cop.ExecuteHelper(state, callback);

  if (state.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = state.p_status_;
    state.is_queuing_ = false;
  }

  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  traffic_cop.CommitQueryHelper(state);

  txn = txn_manager.BeginTransaction();
  // Inserting a tuple end-to-end
  state.Reset();
  state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
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

  LOG_INFO("Binding parse tree...");
  parse_tree = insert_stmt->GetStatement(0);
  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parse_tree);
  LOG_INFO("Binding parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer->BuildPelotonPlanTree(insert_stmt, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  LOG_INFO("Executing plan...");
  result_format = std::vector<PostgresDataFormat>(statement->GetTupleDescriptor().size(),
                                                  PostgresDataFormat::TEXT);
  TestingSQLUtil::counter_.store(1);
  state.statement_ = statement;
  state.param_values_ = params;
  state.result_format_ = result_format;
  status = traffic_cop.ExecuteHelper(state, callback);
  if (state.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = state.p_status_;
    state.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");
  traffic_cop.CommitQueryHelper(state);

  // Now Updating end-to-end
  txn = txn_manager.BeginTransaction();
  state.Reset();
  state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  LOG_INFO("Creating and Index");
  LOG_INFO("Query: CREATE INDEX saif ON department_table (student_id);");
  statement.reset(new Statement(
      "CREATE", "CREATE INDEX saif ON department_table (student_id);"));

  LOG_INFO("Building parse tree...");
  auto update_stmt = peloton_parser.BuildParseTree(
      "CREATE INDEX saif ON department_table (student_id);");
  LOG_INFO("Building parse tree completed!");

  LOG_INFO("Binding parse tree...");
  parse_tree = update_stmt->GetStatement(0);
  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parse_tree);
  LOG_INFO("Binding parse tree completed!");

  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(optimizer->BuildPelotonPlanTree(update_stmt, txn));
  LOG_INFO("Building plan tree completed!\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  LOG_INFO("Executing plan...");
  result_format = std::vector<PostgresDataFormat>(statement->GetTupleDescriptor().size(),
                                                  PostgresDataFormat::TEXT);
  TestingSQLUtil::counter_.store(1);
  state.statement_ = statement;
  state.param_values_ = params;
  state.result_format_ = result_format;
  status = traffic_cop.ExecuteHelper(state, callback);
  if (state.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = state.p_status_;
    state.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("INDEX CREATED!");
  traffic_cop.CommitQueryHelper(state);

  txn = txn_manager.BeginTransaction();
  auto target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                                         DEFAULT_DB_NAME,
                                                                         DEFAULT_SCHEMA_NAME,
                                                                         "department_table");
  // Expected 2 , Primary key index + created index
  EXPECT_EQ(target_table_->GetIndexCount(), 2);

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
