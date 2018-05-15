//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.cpp
//
// Identification: tests/include/statistics/index_metric_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/statistics/stats_aggregator.h>
#include "common/harness.h"
#include "statistics/index_metric.h"
#include "statistics/testing_stats_util.h"
#include "sql/testing_sql_util.h"
#include "catalog/index_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/system_catalogs.h"
#include "storage/tile_group.h"
namespace peloton {
namespace test {

class IndexMetricTests : public PelotonTest {
 public:
  static std::shared_ptr<catalog::IndexMetricsCatalogObject>
  GetIndexMetricObject(oid_t database_id, oid_t index_id) {
    auto index_metrics_catalog = catalog::Catalog::GetInstance()
                                     ->GetSystemCatalogs(database_id)
                                     ->GetIndexMetricsCatalog();
    auto txn = concurrency::TransactionManagerFactory::GetInstance()
                   .BeginTransaction();
    auto index_metric_object =
        index_metrics_catalog->GetIndexMetricsObject(index_id, txn);
    concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(
        txn);
    return index_metric_object;
  }
};

TEST_F(IndexMetricTests, AccessTest) {
  TestingStatsUtil::Initialize();

  stats::StatsAggregator aggregator(1);

  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "CREATE TABLE foo (ID INT PRIMARY KEY, YEAR INT);"));
  auto db_index_ids = TestingStatsUtil::GetDbIndexID("foo");

  aggregator.Aggregate();
  auto initial_index_metric = IndexMetricTests::GetIndexMetricObject(
      db_index_ids.first, db_index_ids.second);

  // no catalog entry for this index metric until index first gets used
  EXPECT_EQ(nullptr, initial_index_metric);

  size_t inserts = 0;
  size_t reads = 0;

  StatsWorkload workload;

  // each insert also goes to index
  workload.AddQuery("INSERT INTO foo VALUES (1, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (2, 2);");
  workload.AddQuery("INSERT INTO foo VALUES (5, 2);");
  inserts += 3;

  // range scan -> no index use
  workload.AddQuery("SELECT * FROM foo;");
  workload.AddQuery("SELECT * FROM foo;");

  // do 3 index scans
  workload.AddQuery("UPDATE foo SET year = 2018 WHERE id = 2");
  workload.AddQuery("UPDATE foo SET year = 2016 WHERE id = 1");
  workload.AddQuery("DELETE FROM foo WHERE id = 1");
  reads += 3;

  // not an index scan (year is not primary key)
  workload.AddQuery("DELETE FROM foo WHERE year = 2018");

  workload.DoQueries();
  aggregator.Aggregate();

  auto final_index_metric = IndexMetricTests::GetIndexMetricObject(
      db_index_ids.first, db_index_ids.second);

  EXPECT_EQ(inserts, final_index_metric->GetInserts());
  EXPECT_EQ(reads, final_index_metric->GetReads());
}
}  // namespace test
}  // namespace peloton
