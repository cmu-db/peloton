//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.cpp
//
// Identification: tests/include/statistics/stats_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/stats_tests_util.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/executor_tests_util.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile_factory.h"
#include "executor/mock_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"

namespace peloton {
namespace test {

void StatsTestsUtil::ShowTable(std::string database_name,
                               std::string table_name) {
  catalog::Catalog::GetInstance()->GetTableWithName(database_name, table_name);
  std::unique_ptr<Statement> statement;
  auto &peloton_parser = parser::Parser::GetInstance();
  std::vector<common::Value> params;
  std::vector<ResultType> result;
  std::string sql = "SELECT * FROM " + database_name + "." + table_name;
  statement.reset(new Statement("SELECT", sql));
  auto select_stmt = peloton_parser.BuildParseTree(sql);
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params,
                                    result, result_format);
}

storage::Tuple StatsTestsUtil::PopulateTuple(const catalog::Schema *schema,
                                             int first_col_val,
                                             int second_col_val,
                                             int third_col_val,
                                             int fourth_col_val) {
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  storage::Tuple tuple(schema, true);
  tuple.SetValue(0, common::ValueFactory::GetIntegerValue(first_col_val),
                 testing_pool);

  tuple.SetValue(1, common::ValueFactory::GetIntegerValue(second_col_val),
                 testing_pool);

  tuple.SetValue(2, common::ValueFactory::GetDoubleValue(third_col_val),
                 testing_pool);

  common::Value string_value =
      common::ValueFactory::GetVarcharValue(std::to_string(fourth_col_val));
  tuple.SetValue(3, string_value, testing_pool);
  return tuple;
}

std::shared_ptr<stats::QueryMetric::QueryParams>
StatsTestsUtil::GetQueryParams() {
  //   Construct a query param object
  std::shared_ptr<std::vector<uchar>> type_buf(new std::vector<uchar>(1));
  type_buf->push_back('x');
  std::vector<uchar> format_buf;
  format_buf.push_back('y');
  std::shared_ptr<std::vector<uchar>> value_buf(new std::vector<uchar>(1));
  value_buf->push_back('z');

  std::shared_ptr<stats::QueryMetric::QueryParams> query_params(
      new stats::QueryMetric::QueryParams(format_buf, type_buf, value_buf, 1));
  return query_params;
}

void StatsTestsUtil::CreateTable() {
  LOG_INFO("Creating a table...");

  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
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
  planner::CreatePlan node("department_table", "emp_db",
                           std::move(table_schema),
                           CreateType::CREATE_TYPE_TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction(txn);
}

std::shared_ptr<Statement> StatsTestsUtil::GetInsertStmt(int id,
                                                         std::string val) {
  std::shared_ptr<Statement> statement;
  std::string sql =
      "INSERT INTO EMP_DB.department_table(dept_id,dept_name) VALUES "
      "(" +
      std::to_string(id) + ",'" + val + "');";
  LOG_INFO("Query: %s", sql.c_str());
  statement.reset(new Statement("INSERT", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

std::shared_ptr<Statement> StatsTestsUtil::GetDeleteStmt() {
  std::shared_ptr<Statement> statement;
  std::string sql = "DELETE FROM EMP_DB.department_table";
  LOG_INFO("Query: %s", sql.c_str());
  statement.reset(new Statement("DELETE", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

std::shared_ptr<Statement> StatsTestsUtil::GetUpdateStmt() {
  std::shared_ptr<Statement> statement;
  std::string sql =
      "UPDATE EMP_DB.department_table SET dept_name = 'CS' WHERE dept_id = 1";
  LOG_INFO("Query: %s", sql.c_str());
  statement.reset(new Statement("UPDATE", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

void StatsTestsUtil::ParseAndPlan(Statement *statement, std::string sql) {
  auto &peloton_parser = parser::Parser::GetInstance();
  auto update_stmt = peloton_parser.BuildParseTree(sql);
  LOG_DEBUG("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(update_stmt));
  LOG_DEBUG("Building plan tree completed!");
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
}

}  // namespace test
}  // namespace peloton
