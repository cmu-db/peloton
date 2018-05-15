//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric_test.cpp
//
// Identification: tests/include/statistics/table_metric_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/statistics/stats_aggregator.h>
#include "common/harness.h"
#include "statistics/table_metric.h"
#include "statistics/testing_stats_util.h"
#include "sql/testing_sql_util.h"
#include "catalog/table_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/system_catalogs.h"
#include "storage/tile_group.h"

namespace peloton {
namespace test {

class TableMetricTests : public PelotonTest {
 public:
  static std::shared_ptr<catalog::TableMetricsCatalogObject>
  GetTableMetricObject(oid_t database_id, oid_t table_id) {
    auto table_metrics_catalog = catalog::Catalog::GetInstance()
                                     ->GetSystemCatalogs(database_id)
                                     ->GetTableMetricsCatalog();
    auto txn = concurrency::TransactionManagerFactory::GetInstance()
                   .BeginTransaction();
    auto table_metric_object =
        table_metrics_catalog->GetTableMetricsObject(table_id, txn);
    concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(
        txn);
    return table_metric_object;
  }
};

TEST_F(TableMetricTests, InitSetup) { TestingStatsUtil::Initialize(); };

TEST_F(TableMetricTests, AccessTest) {
  stats::StatsAggregator aggregator(1);

  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery(
                                     "CREATE TABLE foo (ID INT, YEAR INT);"));
  auto db_table_ids = TestingStatsUtil::GetDbTableID("foo");

  aggregator.Aggregate();
  auto initial_table_metric = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  size_t inserts = 0;
  size_t reads = 0;
  size_t updates = 0;
  size_t deletes = 0;

  // no metrics logged initially
  EXPECT_EQ(inserts, initial_table_metric->GetInserts());
  EXPECT_EQ(updates, initial_table_metric->GetUpdates());
  EXPECT_EQ(reads, initial_table_metric->GetReads());
  EXPECT_EQ(deletes, initial_table_metric->GetDeletes());

  StatsWorkload workload;

  // no primary key, so all range scans
  workload.AddQuery("INSERT INTO foo VALUES (1, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (2, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (5, 2);");
  inserts += 3;

  workload.AddQuery("SELECT * FROM foo;");  // 3 rows in table
  reads += 3;

  workload.AddQuery("SELECT * FROM foo;");  // 3 rows in table
  reads += 3;

  workload.AddQuery(
      "UPDATE foo SET year = 2018 WHERE id = 2");  // 3 rows in table
  reads += 3;
  updates += 1;

  workload.AddQuery(
      "UPDATE foo SET year = 2016 WHERE id = 1");  // 3 rows in table
  reads += 3;
  updates += 1;

  workload.AddQuery("DELETE FROM foo WHERE id = 1");  // 3 rows in table
  reads += 3;
  deletes += 1;

  workload.AddQuery("DELETE FROM foo WHERE year = 2018");  // 2 rows in table
  reads += 2;
  deletes += 1;

  // execute workload
  workload.DoQueries();

  aggregator.Aggregate();
  auto final_table_metric = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  EXPECT_EQ(inserts, final_table_metric->GetInserts());
  EXPECT_EQ(updates, final_table_metric->GetUpdates());
  EXPECT_EQ(reads, final_table_metric->GetReads());
  EXPECT_EQ(deletes, final_table_metric->GetDeletes());

  // Clean up
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("DROP TABLE foo;"));
};

TEST_F(TableMetricTests, MemoryMetricTest) {
  stats::StatsAggregator aggregator(1);

  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "CREATE TABLE foo (ID INT, CONTENT TEXT);"));
  auto db_table_ids = TestingStatsUtil::GetDbTableID("foo");

  aggregator.Aggregate();
  auto origin_metric = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  StatsWorkload workload;

  size_t varlen_alloc_inc = 30;
  size_t varlen_usage_inc = 30;
  size_t inline_tuple_size =
      storage::TileGroupHeader::header_entry_size + 4 + 8;
  size_t tuple_inc = 3;
  size_t inline_alloc_inc = 0;
  size_t inline_usage_inc = tuple_inc * inline_tuple_size;

  workload.AddQuery("INSERT INTO foo VALUES (1, \'test1\');");
  workload.AddQuery("INSERT INTO foo VALUES (2, \'test2\');");
  workload.AddQuery("INSERT INTO foo VALUES (5, \'test3\');");
  workload.DoQueries();

  aggregator.Aggregate();
  auto table_metric_object = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  EXPECT_EQ(origin_metric->GetInlineMemoryAlloc() + inline_alloc_inc,
            table_metric_object->GetInlineMemoryAlloc());
  EXPECT_EQ(origin_metric->GetInlineMemoryUsage() + inline_usage_inc,
            table_metric_object->GetInlineMemoryUsage());
  EXPECT_EQ(origin_metric->GetVarlenMemoryUsage() + varlen_usage_inc,
            table_metric_object->GetVarlenMemoryUsage());
  EXPECT_EQ(origin_metric->GetVarlenMemoryAlloc() + varlen_alloc_inc,
            table_metric_object->GetVarlenMemoryAlloc());

  LOG_DEBUG("%lu", table_metric_object->GetInlineMemoryAlloc());
  LOG_DEBUG("%lu", table_metric_object->GetInlineMemoryUsage());
  LOG_DEBUG("%lu", table_metric_object->GetVarlenMemoryAlloc());
  LOG_DEBUG("%lu", table_metric_object->GetVarlenMemoryUsage());
};
}
}
