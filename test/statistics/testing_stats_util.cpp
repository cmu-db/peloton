//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.cpp
//
// Identification: tests/include/statistics/stats_tests_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/test_metric.h"
#include "statistics/testing_stats_util.h"

#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile_factory.h"
#include "executor/mock_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/testing_executor_util.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/plan_util.h"
#include "storage/tile.h"
#include "traffic_cop/traffic_cop.h"
#include "statistics/stats_aggregator.h"

namespace peloton {
namespace test {

void TestingStatsUtil::ShowTable(std::string database_name,
                                 std::string table_name) {
  std::unique_ptr<Statement> statement;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto &traffic_cop = tcop::TrafficCop::GetInstance();

  std::vector<type::Value> params;
  std::vector<ResultValue> result;
  std::string sql = "SELECT * FROM " + database_name + "." + table_name;
  statement.reset(new Statement("SELECT", sql));
  // using transaction to optimize
  auto txn = txn_manager.BeginTransaction();
  auto select_stmt = peloton_parser.BuildParseTree(sql);
  statement->SetPlanTree(
      optimizer::Optimizer().BuildPelotonPlanTree(select_stmt, txn));
  LOG_DEBUG("%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                            result_format);
  txn_manager.CommitTransaction(txn);
}

storage::Tuple TestingStatsUtil::PopulateTuple(const catalog::Schema *schema,
                                               int first_col_val,
                                               int second_col_val,
                                               int third_col_val,
                                               int fourth_col_val) {
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  storage::Tuple tuple(schema, true);
  tuple.SetValue(0, type::ValueFactory::GetIntegerValue(first_col_val),
                 testing_pool);

  tuple.SetValue(1, type::ValueFactory::GetIntegerValue(second_col_val),
                 testing_pool);

  tuple.SetValue(2, type::ValueFactory::GetDecimalValue(third_col_val),
                 testing_pool);

  type::Value string_value =
      type::ValueFactory::GetVarcharValue(std::to_string(fourth_col_val));
  tuple.SetValue(3, string_value, testing_pool);
  return tuple;
}

void TestingStatsUtil::CreateTable(bool has_primary_key) {
  LOG_INFO("Creating a table...");

  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  if (has_primary_key) {
    catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
    id_column.AddConstraint(constraint);
  }
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 256, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFUALT_SCHEMA_NAME, "emp_db",
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction(txn);
}

std::shared_ptr<Statement> TestingStatsUtil::GetInsertStmt(int id,
                                                           std::string val) {
  std::shared_ptr<Statement> statement;
  std::string sql =
      "INSERT INTO emp_db.public.department_table(dept_id,dept_name) VALUES "
      "(" +
      std::to_string(id) + ",'" + val + "');";
  LOG_TRACE("Query: %s", sql.c_str());
  statement.reset(new Statement("INSERT", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

std::shared_ptr<Statement> TestingStatsUtil::GetDeleteStmt() {
  std::shared_ptr<Statement> statement;
  std::string sql = "DELETE FROM emp_db.public.department_table";
  LOG_INFO("Query: %s", sql.c_str());
  statement.reset(new Statement("DELETE", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

std::shared_ptr<Statement> TestingStatsUtil::GetUpdateStmt() {
  std::shared_ptr<Statement> statement;
  std::string sql =
      "UPDATE emp_db.public.department_table SET dept_name = 'CS' WHERE "
      "dept_id = 1";
  LOG_INFO("Query: %s", sql.c_str());
  statement.reset(new Statement("UPDATE", sql));
  ParseAndPlan(statement.get(), sql);
  return statement;
}

void TestingStatsUtil::ParseAndPlan(Statement *statement, std::string sql) {
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // using transaction to optimize
  auto txn = txn_manager.BeginTransaction();
  auto update_stmt = peloton_parser.BuildParseTree(sql);
  LOG_TRACE("Building plan tree...");
  statement->SetPlanTree(
      optimizer::Optimizer().BuildPelotonPlanTree(update_stmt, txn));
  LOG_TRACE("Building plan tree completed!");
  LOG_TRACE("%s", statement->GetPlanTree().get()->GetInfo().c_str());
  txn_manager.CommitTransaction(txn);
}

int TestingStatsUtil::AggregateCounts() {
  stats::StatsAggregator aggregator(1);
  auto result = aggregator.AggregateRawData();

  // Only TestRawData should be collected
  EXPECT_LE(result.size(), 1);

  if (result.empty()) return 0;

  return dynamic_cast<stats::TestMetricRawData &>(*result[0]).count_;
}

}  // namespace test
}  // namespace peloton
