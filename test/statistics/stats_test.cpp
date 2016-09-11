//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_test.cpp
//
// Identification: test/statistics/stats_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "common/harness.h"
#include "common/value.h"
#include "common/config.h"

#include "common/statement.h"
#include "common/value_factory.h"

#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_tests_util.h"

#include "statistics/stats_aggregator.h"
#include "statistics/backend_stats_context.h"
#include "statistics/stats_tests_util.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "executor/executor_tests_util.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "executor/create_executor.h"
#include "executor/plan_executor.h"
#include "planner/insert_plan.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"

#define NUM_ITERATION 50
#define NUM_TABLE_INSERT 1
#define NUM_TABLE_DELETE 2
#define NUM_TABLE_UPDATE 3
#define NUM_TABLE_READ 4
#define NUM_INDEX_INSERT 1
#define NUM_INDEX_DELETE 2
#define NUM_INDEX_READ 4
#define NUM_DB_COMMIT 1
#define NUM_DB_ABORT 2

namespace peloton {
namespace test {

class StatsTest : public PelotonTest {};

void ShowTable(std::string database_name, std::string table_name) {
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                                 table_name);
  std::unique_ptr<Statement> statement;
  auto &peloton_parser = parser::Parser::GetInstance();
  std::vector<common::Value *> params;
  std::vector<ResultType> result;
  statement.reset(new Statement("SELECT", "SELECT * FROM " + table->GetName()));
  auto select_stmt =
      peloton_parser.BuildParseTree("SELECT * FROM " + table->GetName());
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params,
                                    result);
}

void TransactionTest(storage::Database *database, storage::DataTable *table,
                     UNUSED_ATTRIBUTE uint64_t thread_itr) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();
  auto tile_group_id = table->GetTileGroup(0)->GetTileGroupId();
  auto index_metadata = table->GetIndex(0)->GetMetadata();
  auto db_oid = database->GetOid();
  auto context = stats::BackendStatsContext::GetInstance();

  for (oid_t txn_itr = 1; txn_itr <= NUM_ITERATION; txn_itr++) {

    context->InitQueryMetric("query_string", db_oid);
    context->GetOnGoingQueryMetric()->GetQueryLatency().StartTimer();

    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    // Record table stat
    for (int i = 0; i < NUM_TABLE_READ; i++) {
      context->IncrementTableReads(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_UPDATE; i++) {
      context->IncrementTableUpdates(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_INSERT; i++) {
      context->IncrementTableInserts(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_DELETE; i++) {
      context->IncrementTableDeletes(tile_group_id);
    }

    // Record index stat
    context->IncrementIndexReads(NUM_INDEX_READ, index_metadata);
    context->IncrementIndexDeletes(NUM_INDEX_DELETE, index_metadata);
    for (int i = 0; i < NUM_INDEX_INSERT; i++) {
      context->IncrementIndexInserts(index_metadata);
    }

    // Record database stat
    for (int i = 0; i < NUM_DB_COMMIT; i++) {
      context->GetOnGoingQueryMetric()->GetQueryLatency().RecordLatency();
      context->IncrementTxnCommitted(db_oid);
    }
    for (int i = 0; i < NUM_DB_ABORT; i++) {
      context->IncrementTxnAborted(db_oid);
    }
  }
}

TEST_F(StatsTest, MultiThreadStatsTest) {

  // Register to StatsAggregator
  FLAGS_stats_mode = STATS_TYPE_ENABLE;
  int64_t stat_interval = 100;
  auto &aggregator =
      peloton::stats::StatsAggregator::GetInstance(stat_interval);
  aggregator.GetAggregatedStats().ResetQueryCount();

  // Create database, table and index
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "id", true);
  auto name_column = catalog::Column(common::Type::VARCHAR, 32, "name", true);
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog->CreateDatabase("EMP_DB", txn);
  catalog::Catalog::GetInstance()->CreateTable("EMP_DB", "emp_table",
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // Create multiple stat worker threads
  int num_threads = 8;
  storage::Database *database = catalog->GetDatabaseWithName("EMP_DB");
  storage::DataTable *table = database->GetTableWithName("emp_table");
  LaunchParallelTest(num_threads, TransactionTest, database, table);

  // Wait for aggregation to finish
  std::chrono::microseconds sleep_time(stat_interval * 3 * 1000);
  std::this_thread::sleep_for(sleep_time);

  // Force a final aggregation
  int64_t interval_cnt = 0;
  double alpha = 0;
  double weighted_avg_throughput = 0;
  aggregator.Aggregate(interval_cnt, alpha, weighted_avg_throughput);

  // Check query metrics
  auto &aggregated_stats =
      peloton::stats::StatsAggregator::GetInstance().GetAggregatedStats();
  ASSERT_EQ(aggregated_stats.GetQueryCount(), num_threads * NUM_ITERATION);

  // Check database metrics
  auto db_oid = database->GetOid();
  auto db_metric = aggregated_stats.GetDatabaseMetric(db_oid);
  ASSERT_EQ(db_metric->GetTxnCommitted().GetCounter(),
            num_threads * NUM_ITERATION * NUM_DB_COMMIT);
  ASSERT_EQ(db_metric->GetTxnAborted().GetCounter(),
            num_threads * NUM_ITERATION * NUM_DB_ABORT);

  // Check table metrics
  auto table_oid = table->GetOid();
  auto table_metric = aggregated_stats.GetTableMetric(db_oid, table_oid);
  auto table_access = table_metric->GetTableAccess();
  ASSERT_EQ(table_access.GetReads(),
            num_threads * NUM_ITERATION * NUM_TABLE_READ);
  ASSERT_EQ(table_access.GetUpdates(),
            num_threads * NUM_ITERATION * NUM_TABLE_UPDATE);
  ASSERT_EQ(table_access.GetDeletes(),
            num_threads * NUM_ITERATION * NUM_TABLE_DELETE);
  ASSERT_EQ(table_access.GetInserts(),
            num_threads * NUM_ITERATION * NUM_TABLE_INSERT);

  // Check index metrics
  auto index_oid = table->GetIndex(0)->GetOid();
  auto index_metric =
      aggregated_stats.GetIndexMetric(db_oid, table_oid, index_oid);
  auto index_access = index_metric->GetIndexAccess();
  ASSERT_EQ(index_access.GetReads(),
            num_threads * NUM_ITERATION * NUM_INDEX_READ);
  ASSERT_EQ(index_access.GetDeletes(),
            num_threads * NUM_ITERATION * NUM_INDEX_DELETE);
  ASSERT_EQ(index_access.GetInserts(),
            num_threads * NUM_ITERATION * NUM_INDEX_INSERT);

  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName("EMP_DB", txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(StatsTest, PerThreadStatsTest) {
  FLAGS_stats_mode = STATS_TYPE_ENABLE;

  // Register to StatsAggregator
  auto &aggregator = peloton::stats::StatsAggregator::GetInstance(1000000);
  aggregator.GetAggregatedStats().ResetQueryCount();

  // int tuple_count = 10;
  int tups_per_tile_group = 100;
  int num_rows = 10;

  // Create a table and wrap it in logical tiles
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tups_per_tile_group, true));

  // Ensure that the tile group is as expected.
  const catalog::Schema *schema = data_table->GetSchema();
  PL_ASSERT(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  std::vector<ItemPointer> tuple_slot_ids;

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple = StatsTestsUtil::PopulateTuple(
        schema, ExecutorTestsUtil::PopulatedValue(populate_value, 0),
        ExecutorTestsUtil::PopulatedValue(populate_value, 1),
        ExecutorTestsUtil::PopulatedValue(populate_value, 2),
        ExecutorTestsUtil::PopulatedValue(populate_value, 3));

    std::unique_ptr<const planner::ProjectInfo> project_info{
        TransactionTestsUtil::MakeProjectInfoFromTuple(&tuple)};

    // Insert
    planner::InsertPlan node(data_table.get(), std::move(project_info));
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }
  txn_manager.CommitTransaction(txn);
  oid_t database_id = data_table->GetDatabaseOid();
  oid_t table_id = data_table->GetOid();

  // Check: # transactions committed = 1, # table inserts = 10
  int64_t txn_commited = stats::BackendStatsContext::GetInstance()
                             ->GetDatabaseMetric(database_id)
                             ->GetTxnCommitted()
                             .GetCounter();
  int64_t inserts = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetInserts();
  EXPECT_EQ(1, txn_commited);
  EXPECT_EQ(num_rows, inserts);

  // Read every other tuple
  txn = txn_manager.BeginTransaction();
  for (int i = 0; i < num_rows; i += 2) {
    int result;
    TransactionTestsUtil::ExecuteRead(
        txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(i, 0), result);
  }
  txn_manager.CommitTransaction(txn);

  // Check: # transactions committed = 2, # inserts = 10, # reads = 5
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  inserts = stats::BackendStatsContext::GetInstance()
                ->GetTableMetric(database_id, table_id)
                ->GetTableAccess()
                .GetInserts();
  int64_t reads = stats::BackendStatsContext::GetInstance()
                      ->GetTableMetric(database_id, table_id)
                      ->GetTableAccess()
                      .GetReads();
  EXPECT_EQ(2, txn_commited);
  EXPECT_EQ(num_rows, inserts);
  EXPECT_EQ(5, reads);

  // Do a single read and abort
  txn = txn_manager.BeginTransaction();
  int result;
  TransactionTestsUtil::ExecuteRead(
      txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(0, 0), result);
  txn_manager.AbortTransaction(txn);

  // Check: # txns committed = 2, # txns aborted = 1, # reads = 6
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  int64_t txn_aborted = stats::BackendStatsContext::GetInstance()
                            ->GetDatabaseMetric(database_id)
                            ->GetTxnAborted()
                            .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  EXPECT_EQ(2, txn_commited);
  EXPECT_EQ(1, txn_aborted);
  EXPECT_EQ(6, reads);

  // Read and update the first tuple
  txn = txn_manager.BeginTransaction();
  TransactionTestsUtil::ExecuteUpdate(txn, data_table.get(), 0, 2);
  txn_manager.CommitTransaction(txn);

  // Check: # txns committed = 3, # updates = 1, # reads = 7
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t updates = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetUpdates();
  EXPECT_EQ(3, txn_commited);
  EXPECT_EQ(7, reads);
  EXPECT_EQ(1, updates);

  // Delete the 6th tuple and read the 1st tuple
  txn = txn_manager.BeginTransaction();
  TransactionTestsUtil::ExecuteDelete(txn, data_table.get(),
                                      ExecutorTestsUtil::PopulatedValue(5, 0));
  LOG_INFO("before read");
  TransactionTestsUtil::ExecuteRead(
      txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(1, 0), result);
  txn_manager.CommitTransaction(txn);

  // Check: # txns committed = 4, # deletes = 1, # reads = 8
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t deletes = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetDeletes();
  EXPECT_EQ(4, txn_commited);
  EXPECT_EQ(9, reads);
  EXPECT_EQ(1, deletes);

  aggregator.ShutdownAggregator();
}

TEST_F(StatsTest, PerQueryStatsTest) {

  FLAGS_stats_mode = STATS_TYPE_ENABLE;
  int64_t stat_interval = 1000;
  auto &aggregator =
      peloton::stats::StatsAggregator::GetInstance(stat_interval);
  aggregator.GetAggregatedStats().ResetQueryCount();

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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", std::move(table_schema),
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
  std::string query_string =
      "INSERT INTO department_table(dept_id,dept_name) VALUES "
      "(1,'hello_1');";
  statement.reset(new Statement("INSERT", query_string));
  auto &peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(query_string);
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<common::Value *> params;
  std::vector<ResultType> result;

  auto backend_context = stats::BackendStatsContext::GetInstance();
  backend_context->InitQueryMetric(statement->GetQueryString(), DEFAULT_DB_ID);

  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");

  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");
  ShowTable(DEFAULT_DB_NAME, "department_table");
  txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Wait for aggregation to finish
  std::chrono::microseconds sleep_time(stat_interval * 3 * 1000);
  std::this_thread::sleep_for(sleep_time);
  aggregator.ShutdownAggregator();

  // Force a final aggregation
  int64_t interval_cnt = 0;
  double alpha = 0;
  double weighted_avg_throughput = 0;
  aggregator.Aggregate(interval_cnt, alpha, weighted_avg_throughput);
  EXPECT_EQ(aggregator.GetAggregatedStats().GetQueryCount(), 1);
}
}  // namespace stats
}  // namespace peloton
