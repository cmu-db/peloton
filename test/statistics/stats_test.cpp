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
#include "common/value_factory.h"

#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_tests_util.h"
#include "statistics/stats_aggregator.h"
#include "statistics/backend_stats_context.h"
#include "statistics/stats_tests_util.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "executor/executor_tests_util.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace test {

class StatsTest : public PelotonTest {};

TEST_F(StatsTest, PerThreadStatsTest) {
  FLAGS_stats_mode = STATS_TYPE_ENABLE;

  /*
   * Register to StatsAggregator
   */
  peloton::stats::StatsAggregator::GetInstance(1000000);

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
                             .GetDatabaseMetric(database_id)
                             ->GetTxnCommitted()
                             .GetCounter();
  int64_t inserts = stats::BackendStatsContext::GetInstance()
                        .GetTableMetric(database_id, table_id)
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
                     .GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  inserts = stats::BackendStatsContext::GetInstance()
                .GetTableMetric(database_id, table_id)
                ->GetTableAccess()
                .GetInserts();
  int64_t reads = stats::BackendStatsContext::GetInstance()
                      .GetTableMetric(database_id, table_id)
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
                     .GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  int64_t txn_aborted = stats::BackendStatsContext::GetInstance()
                            .GetDatabaseMetric(database_id)
                            ->GetTxnAborted()
                            .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              .GetTableMetric(database_id, table_id)
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
                     .GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              .GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t updates = stats::BackendStatsContext::GetInstance()
                        .GetTableMetric(database_id, table_id)
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
                     .GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              .GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t deletes = stats::BackendStatsContext::GetInstance()
                        .GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetDeletes();
  EXPECT_EQ(4, txn_commited);
  EXPECT_EQ(9, reads);
  EXPECT_EQ(1, deletes);
}
}  // namespace stats
}  // namespace peloton
