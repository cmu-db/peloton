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
#include "optimizer/stats/tuple_sampler.h"

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

TEST_F(StatsStorageTests, InsertAndGetStatsTest) {
  const int tuple_count = 100;
  const int tuple_per_tilegroup = 100;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<optimizer::TableStats> table_stats(
      new TableStats(data_table.get()));
  table_stats->CollectColumnStats();
  LOG_DEBUG("Finish collect column stats");

  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  stats_storage->AddOrUpdateTableStats(data_table.get(), table_stats.get());

  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  storage::DataTable *stats_table = column_stats_catalog->catalog_table_;
  EXPECT_EQ(stats_table->GetTupleCount(), 4);

  auto tile_group = stats_table->GetTileGroup(0);

  auto tile = tile_group->GetTile(0);
  auto tile_schemas = tile_group->GetTileSchemas();
  const catalog::Schema &schema = tile_schemas[0];

  for (int column_id = 0; column_id < 4; ++column_id) {
    // Print out all four column stats.
    storage::Tuple tile_tuple(&schema, tile->GetTupleLocation(column_id));
    LOG_DEBUG("Tuple Info: %s", tile_tuple.GetInfo().c_str());

    // Get column stats using the GetColumnStatsByID function and compare the
    // results.
    auto column_stats = stats_storage->GetColumnStatsByID(
        data_table->GetDatabaseOid(), data_table->GetOid(), column_id);
    for (auto value : *(column_stats.get())) {
      LOG_DEBUG("Value: %s", value.GetInfo().c_str());
    }
  }
}
/*
TEST_F(StatsStorageTests, GetColumnStatsTest) {
  const int tuple_count = 100;
  const int tuple_per_tilegroup = 100;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<optimizer::TableStats> table_stats(
      new TableStats(data_table.get()));
  table_stats->CollectColumnStats();
  LOG_DEBUG("Finish collect column stats");

  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  stats_storage->AddOrUpdateTableStats(data_table.get(), table_stats.get());

  auto column_stats =
stats_storage->GetColumnStatsByID(data_table->GetDatabaseOid(),
                                                        data_table->GetOid(),
                                                        0);
  for(auto value : *(column_stats.get())) {
    LOG_DEBUG("Value: %s", value.GetInfo().c_str());
  }
}
*/
TEST_F(StatsStorageTests, SamplesDBTest) {
  catalog::Catalog *catalog = catalog::Catalog::GetInstance();
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  (void)stats_storage;
  storage::Database *samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  EXPECT_TRUE(samples_db != nullptr);
  EXPECT_EQ(samples_db->GetDBName(), SAMPLES_DB_NAME);
  EXPECT_EQ(samples_db->GetTableCount(), 0);
}

TEST_F(StatsStorageTests, AddSamplesTableTest) {
  const int tuple_count = 100;

  // Create a table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  // Acquire samples
  TupleSampler sampler(data_table.get());
  size_t sampled_count = sampler.AcquireSampleTuples(10);
  std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples =
      sampler.GetSampledTuples();

  // Add samples into database
  catalog::Catalog *catalog = catalog::Catalog::GetInstance();
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  stats_storage->AddSamplesTable(data_table.get(), sampled_tuples);

  // Check the sampled tuples
  std::string samples_table_name = stats_storage->GenerateSamplesTableName(
      data_table->GetDatabaseOid(), data_table->GetOid());
  storage::Database *samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  storage::DataTable *samples_table =
      samples_db->GetTableWithName(samples_table_name);
  EXPECT_TRUE(samples_table != nullptr);
  EXPECT_EQ(samples_table->GetTupleCount(), sampled_count);
}

} /* namespace test */
} /* namespace peloton */
