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
  auto origin_metric = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  StatsWorkload workload;

  size_t insert_inc = 3;
  // FIXME: This is wrong now
  size_t read_inc = 2;
  size_t update_inc = 2;
  size_t delete_inc = 2;

  workload.AddQuery("INSERT INTO foo VALUES (1, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (2, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (5, 2);");
  workload.AddQuery("SELECT * FROM foo;");
  workload.AddQuery("SELECT * FROM foo;");
  workload.AddQuery("UPDATE foo SET year = 2018 WHERE id = 2");
  workload.AddQuery("UPDATE foo SET year = 2016 WHERE id = 1");
  workload.AddQuery("DELETE FROM foo WHERE id = 1");
  workload.AddQuery("DELETE FROM foo WHERE year = 2018");

  workload.DoQueries();

  aggregator.Aggregate();
  auto table_metric_object = TableMetricTests::GetTableMetricObject(
      db_table_ids.first, db_table_ids.second);

  EXPECT_EQ(origin_metric->GetInserts() + insert_inc,
            table_metric_object->GetInserts());
  EXPECT_EQ(origin_metric->GetUpdates() + update_inc,
            table_metric_object->GetUpdates());
  // FIXME this is wrong now
  EXPECT_EQ(origin_metric->GetReads() + read_inc,
            table_metric_object->GetReads());
  EXPECT_EQ(origin_metric->GetDeletes() + delete_inc,
            table_metric_object->GetDeletes());

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