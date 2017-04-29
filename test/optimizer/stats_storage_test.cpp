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

std::unique_ptr<storage::DataTable> InitializeTestTable() {
  const int tuple_count = 100;
  const int tuple_per_tilegroup = 100;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);
  return std::move(data_table);
}

storage::DataTable *CreateTestDBAndTable() {
  const std::string test_db_name = "test_db";
  auto database = TestingExecutorUtil::InitializeDatabase(test_db_name);

  const int tuple_count = 100;
  const int tuple_per_tilegroup = 100;

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
 *correctly stored in catalog
 * and print them out.
 *
 * The column stats are retrieved by calling the
 *StatsStorage::GetColumnStatsByID function.
 * TODO:
 * 1. Verify the number of tuples in column_stats_catalog.
 * 2. Compare the column stats values with the ground truth.
 */
void VerifyAndPrintColumnStats(storage::DataTable *data_table,
                               int expect_tuple_count) {
  // Check the tuple count in the 'pg_column_stats' catalog.
  // auto column_stats_catalog =
  // catalog::ColumnStatsCatalog::GetInstance(nullptr);
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  // Verify both the internal data and the stats got by interface.
  // storage::DataTable *stats_table = column_stats_catalog->catalog_table_;
  // EXPECT_EQ(stats_table->GetTupleCount(), expect_tuple_count);

  // auto tile_group = stats_table->GetTileGroup(0);
  // auto tile = tile_group->GetTile(0);
  // auto tile_schemas = tile_group->GetTileSchemas();
  // const catalog::Schema &schema = tile_schemas[0];

  // Print out all four column stats.
  for (int column_id = 0; column_id < expect_tuple_count; ++column_id) {
    // storage::Tuple tile_tuple(&schema, tile->GetTupleLocation(column_id));
    // LOG_DEBUG("Tuple Info: %s", tile_tuple.GetInfo().c_str());

    // Get column stats using the GetColumnStatsByID function and compare the
    // results.
    auto column_stats = stats_storage->GetColumnStatsByID(
        data_table->GetDatabaseOid(), data_table->GetOid(), column_id);
    for (auto value : *(column_stats.get())) {
      LOG_DEBUG("Value: %s", value.GetInfo().c_str());
    }
  }
}

TEST_F(StatsStorageTests, InsertAndGetTableStatsTest) {
  auto data_table = InitializeTestTable();

  // Collect stats.
  std::unique_ptr<optimizer::TableStats> table_stats(
      new TableStats(data_table.get()));
  table_stats->CollectColumnStats();

  // Insert stats.
  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  stats_storage->InsertOrUpdateTableStats(data_table.get(), table_stats.get());

  VerifyAndPrintColumnStats(data_table.get(), 4);
}

TEST_F(StatsStorageTests, InsertAndGetColumnStatsTest) {
  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();

  oid_t database_id = 1;
  oid_t table_id = 2;
  oid_t column_id = 3;
  int num_row = 10;
  double cardinality = 8;
  double frac_null = 0.56;
  std::string most_common_vals = "12";
  double most_common_freqs = 3;
  std::string histogram_bounds = "1,5,7";

  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_row, cardinality, frac_null,
      most_common_vals, most_common_freqs, histogram_bounds);

  auto column_stats_ptr =
      stats_storage->GetColumnStatsByID(database_id, table_id, column_id);

  // Check the result
  EXPECT_NE(column_stats_ptr, nullptr);
  std::vector<type::Value> column_stats = *(column_stats_ptr.get());
  for (auto value : *(column_stats_ptr.get())) {
    LOG_DEBUG("Value: %s", value.GetInfo().c_str());
  }
  EXPECT_TRUE(column_stats[0].CompareEquals(
      type::ValueFactory::GetIntegerValue(num_row)));
  EXPECT_TRUE(column_stats[1].CompareEquals(
      type::ValueFactory::GetDecimalValue(cardinality)));
  EXPECT_TRUE(column_stats[2].CompareEquals(
      type::ValueFactory::GetDecimalValue(frac_null)));
  EXPECT_TRUE(column_stats[3].CompareEquals(
      type::ValueFactory::GetVarcharValue(most_common_vals)));
  EXPECT_TRUE(column_stats[4].CompareEquals(
      type::ValueFactory::GetDecimalValue(most_common_freqs)));
  EXPECT_TRUE(column_stats[5].CompareEquals(
      type::ValueFactory::GetVarcharValue(histogram_bounds)));

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
  double most_common_freqs_0 = 3;
  std::string histogram_bounds_0 = "1,5,7";

  int num_row_1 = 20;
  double cardinality_1 = 16;
  double frac_null_1 = 1.56;
  std::string most_common_vals_1 = "24";
  double most_common_freqs_1 = 6;
  std::string histogram_bounds_1 = "2,10,14";

  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_row_0, cardinality_0, frac_null_0,
      most_common_vals_0, most_common_freqs_0, histogram_bounds_0);
  stats_storage->InsertOrUpdateColumnStats(
      database_id, table_id, column_id, num_row_1, cardinality_1, frac_null_1,
      most_common_vals_1, most_common_freqs_1, histogram_bounds_1);

  auto column_stats_ptr =
      stats_storage->GetColumnStatsByID(database_id, table_id, column_id);

  // Check the result
  EXPECT_NE(column_stats_ptr, nullptr);
  std::vector<type::Value> column_stats = *(column_stats_ptr.get());
  for (auto value : *(column_stats_ptr.get())) {
    LOG_DEBUG("Value: %s", value.GetInfo().c_str());
  }
  EXPECT_TRUE(column_stats[0].CompareEquals(
      type::ValueFactory::GetIntegerValue(num_row_1)));
  EXPECT_TRUE(column_stats[1].CompareEquals(
      type::ValueFactory::GetDecimalValue(cardinality_1)));
  EXPECT_TRUE(column_stats[2].CompareEquals(
      type::ValueFactory::GetDecimalValue(frac_null_1)));
  EXPECT_TRUE(column_stats[3].CompareEquals(
      type::ValueFactory::GetVarcharValue(most_common_vals_1)));
  EXPECT_TRUE(column_stats[4].CompareEquals(
      type::ValueFactory::GetDecimalValue(most_common_freqs_1)));
  EXPECT_TRUE(column_stats[5].CompareEquals(
      type::ValueFactory::GetVarcharValue(histogram_bounds_1)));
}

// TEST_F(StatsStorageTests, AnalyzeStatsForAllTablesTest) {

// }

TEST_F(StatsStorageTests, AnalyzeStatsForTable) {
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
TEST_F(StatsStorageTests, AnalyzeStatsForAllTables) {
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

} /* namespace test */
} /* namespace peloton */
