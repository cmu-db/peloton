//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.cpp
//
// Identification: test/sql/sql_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "executor/plan_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "tcop/tcop.h"

#include "sql/sql_tests_util.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

// Show the content in the specific table in the specific database
void SQLTestsUtil::ShowTable(std::string database_name,
                             std::string table_name) {
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                                 table_name);
  std::unique_ptr<Statement> statement;
  auto &peloton_parser = parser::Parser::GetInstance();
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
          select_stmt->GetStatement(0));
  result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
}

// Execute a SQL query end-to-end
void SQLTestsUtil::ExecuteSQLQuery(const std::string statement_name,
                                   const std::string query_string,
                                   std::vector<ResultType> &result) {
  LOG_INFO("Query: %s", query_string.c_str());
  static std::unique_ptr<Statement> statement;
  statement.reset(new Statement(statement_name, query_string));
  auto &peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(query_string);
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<common::Value> params;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  std::vector<int> result_format;
  auto tuple_descriptor =
      tcop::TrafficCop::GetInstance().GenerateTupleDescriptor(
          insert_stmt->GetStatement(0));
  result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  UNUSED_ATTRIBUTE bridge::peloton_status status =
      bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params,
                                        result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
}
}  // namespace test
}  // namespace peloton
