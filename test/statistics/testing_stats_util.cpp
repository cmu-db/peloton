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
#include "storage/storage_manager.h"
#include "traffic_cop/traffic_cop.h"
#include "statistics/stats_aggregator.h"

namespace peloton {
namespace test {

// TODO(Tianyi) remove this thing when we fix COPY and change copy test
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

// TODO(Tianyi) remove this thing when we fix COPY and change copy test
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

// TODO(Tianyi) remove this thing when we fix COPY and change copy test
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

void TestingStatsUtil::Initialize() {
  // Setup Metric
  settings::SettingsManager::SetInt(settings::SettingId::stats_mode,
                                    static_cast<int>(StatsModeType::ENABLE));
  // Initialize catalog
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  settings::SettingsManager::GetInstance().InitializeCatalog();
  EXPECT_EQ(1, storage::StorageManager::GetInstance()->GetDatabaseCount());
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // begin a transaction
  // initialize the catalog and add the default database, so we don't do this on
  // the first query
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  storage::Database *database =
      catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME, txn);
  EXPECT_EQ(ResultType::SUCCESS, txn_manager.CommitTransaction(txn));
  EXPECT_NE(nullptr, database);
}

std::pair<oid_t, oid_t> TestingStatsUtil::GetDbTableID(
    const std::string &table_name) {
  auto txn =
      concurrency::TransactionManagerFactory::GetInstance().BeginTransaction();
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, table_name, txn);
  auto table_id = table->GetOid();
  auto database_id = table->GetDatabaseOid();
  concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(txn);
  return {database_id, table_id};
}

}  // namespace test
}  // namespace peloton
