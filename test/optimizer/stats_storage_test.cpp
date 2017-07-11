//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_storage_test.cpp
//
// Identification: test/optimizer/stats_storage_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#define private public
#define protected public

#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/table_stats.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/tile.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"
#include "catalog/column_stats_catalog.h"
#include "executor/testing_executor_util.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class StatsStorageTests : public PelotonTest {};

const int tuple_count = 100;
const int tuple_per_tilegroup = 100;

std::unique_ptr<storage::DataTable> InitializeTestTable() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);
  return data_table;
}

storage::DataTable *CreateTestDBAndTable() {
  const std::string test_db_name = "test_db";
  auto database = TestingExecutorUtil::InitializeDatabase(test_db_name);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  storage::DataTable *data_table =
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false);
  TestingExecutorUtil::PopulateTable(data_table, tuple_count, false, false,
                                     true, txn);
  database->AddTable(data_table);
  txn_manager.CommitTransaction(txn);
  return data_table;
}

/**
 * VerifyAndPrintColumnStats - Verify whether the stats of the test table are
 * correctly stored in catalog and print them out.
 *
 * The column stats are retrieved by calling the
 * StatsStorage::GetColumnStatsByID function.
 * TODO:
 * 1. Verify the number of tuples in column_stats_catalog.
 * 2. Compare the column stats values with the ground truth.
 */
void VerifyAndPrintColumnStats(storage::DataTable *data_table,
                               int expect_tuple_count) {
  // Check the tuple count in the 'pg_column_stats' catalog.
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  // Print out all four column stats.
  for (int column_id = 0; column_id < expect_tuple_count; ++column_id) {
    auto column_stats = stats_storage->GetColumnStatsByID(
        data_table->GetDatabaseOid(), data_table->GetOid(), column_id);
    LOG_TRACE("num_rows: %lu", column_stats->num_rows);
    LOG_TRACE("cardinality: %lf", column_stats->cardinality);
    LOG_TRACE("frac_null: %lf", column_stats->frac_null);
    auto most_common_vals = column_stats->most_common_vals;
    auto most_common_freqs = column_stats->most_common_freqs;
    auto hist_bounds = column_stats->histogram_bounds;
  }
}

TEST_F(StatsStorageTests, InsertAndGetTableStatsTest) {
  auto data_table = InitializeTestTable();

  // Collect stats.
  std::unique_ptr<optimizer::TableStatsCollector> table_stats_collector(
      new TableStatsCollector(data_table.get()));
  table_stats_collector->CollectColumnStats();

  // Insert stats.
  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  stats_storage->InsertOrUpdateTableStats(data_table.get(),
                                          table_stats_collector.get());

  VerifyAndPrintColumnStats(data_table.get(), 4);
}

TEST_F(StatsStorageTests, InsertAndGetColumnStatsTest) {
  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  oid_t database_id = 1;
  oid_t table_id = 2;
  oid_t column_id = 3;
  int num_rows = 10;
  double cardinality = 8;
  double frac_null = 0.56;
  std::string most_common_vals = "12";
  std::string most_common_freqs = "3";
  std::string histogram_bounds = "1,5,7";
  std::string column_name = "random";

  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_rows, cardinality, frac_null,
      most_common_vals, most_common_freqs, histogram_bounds, column_name);

  auto column_stats_ptr =
      stats_storage->GetColumnStatsByID(database_id, table_id, column_id);

  // Check the result
  EXPECT_NE(column_stats_ptr, nullptr);

  EXPECT_EQ(column_stats_ptr->num_rows, num_rows);
  EXPECT_EQ(column_stats_ptr->cardinality, cardinality);
  EXPECT_EQ(column_stats_ptr->frac_null, frac_null);

  EXPECT_EQ(column_stats_ptr->column_name, column_name);

  // Should return nullptr
  auto column_stats_ptr2 =
      stats_storage->GetColumnStatsByID(database_id, table_id, column_id + 1);
  EXPECT_EQ(column_stats_ptr2, nullptr);
}

TEST_F(StatsStorageTests, UpdateColumnStatsTest) {
  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  oid_t database_id = 1;
  oid_t table_id = 2;
  oid_t column_id = 3;

  int num_row_0 = 10;
  double cardinality_0 = 8;
  double frac_null_0 = 0.56;
  std::string most_common_vals_0 = "12";
  std::string most_common_freqs_0 = "3";
  std::string histogram_bounds_0 = "1,5,7";
  std::string column_name_0 = "random0";

  int num_row_1 = 20;
  double cardinality_1 = 16;
  double frac_null_1 = 1.56;
  std::string most_common_vals_1 = "24";
  std::string most_common_freqs_1 = "6";
  std::string histogram_bounds_1 = "2,10,14";
  std::string column_name_1 = "random1";

  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_row_0, cardinality_0, frac_null_0,
      most_common_vals_0, most_common_freqs_0, histogram_bounds_0,
      column_name_0);
  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_row_1, cardinality_1, frac_null_1,
      most_common_vals_1, most_common_freqs_1, histogram_bounds_1,
      column_name_1);

  auto column_stats_ptr =
      stats_storage->GetColumnStatsByID(database_id, table_id, column_id);

  // Check the result
  EXPECT_NE(column_stats_ptr, nullptr);

  EXPECT_EQ(column_stats_ptr->num_rows, num_row_1);
  EXPECT_EQ(column_stats_ptr->cardinality, cardinality_1);
  EXPECT_EQ(column_stats_ptr->frac_null, frac_null_1);

  EXPECT_EQ(column_stats_ptr->column_name, column_name_1);
}

TEST_F(StatsStorageTests, AnalyzeStatsForTableTest) {
  auto data_table = InitializeTestTable();

  // Analyze table.
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  // Must pass in the transaction.
  ResultType result = stats_storage->AnalyzeStatsForTable(data_table.get());
  EXPECT_EQ(result, ResultType::FAILURE);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  result = stats_storage->AnalyzeStatsForTable(data_table.get(), txn);
  EXPECT_EQ(result, ResultType::SUCCESS);
  txn_manager.CommitTransaction(txn);

  // Check the correctness of the stats.
  VerifyAndPrintColumnStats(data_table.get(), 4);
}

// TODO: Add more tables.
TEST_F(StatsStorageTests, AnalyzeStatsForAllTablesTest) {
  auto data_table = CreateTestDBAndTable();

  StatsStorage *stats_storage = StatsStorage::GetInstance();

  // Must pass in the transaction.
  ResultType result = stats_storage->AnalyzeStatsForAllTables();
  EXPECT_EQ(result, ResultType::FAILURE);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  result = stats_storage->AnalyzeStatsForAllTables(txn);
  EXPECT_EQ(result, ResultType::SUCCESS);
  txn_manager.CommitTransaction(txn);

  // Check the correctness of the stats.
  VerifyAndPrintColumnStats(data_table, 4);
}

TEST_F(StatsStorageTests, GetTableStatsTest) {
  auto data_table = InitializeTestTable();

  StatsStorage *stats_storage = StatsStorage::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  stats_storage->AnalyzeStatsForAllTables(txn);
  txn_manager.CommitTransaction(txn);

  std::shared_ptr<TableStats> table_stats = stats_storage->GetTableStats(
      data_table->GetDatabaseOid(), data_table->GetOid());
  EXPECT_EQ(table_stats->num_rows, tuple_count);
}

} /* namespace test */
} /* namespace peloton */
