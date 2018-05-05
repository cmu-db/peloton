//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats_collector_test.cpp
//
// Identification: test/optimizer/table_stats_collector_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/harness.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/testing_executor_util.h"
#include "optimizer/stats/column_stats_collector.h"
#include "optimizer/stats/table_stats_collector.h"
#include "sql/testing_sql_util.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace peloton {
namespace test {

using namespace optimizer;

class TableStatsCollectorTests : public PelotonTest {};

TEST_F(TableStatsCollectorTests, BasicTests) {
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable());
  TableStatsCollector table_stats_collector{data_table.get()};
  table_stats_collector.CollectColumnStats();
  EXPECT_EQ(table_stats_collector.GetActiveTupleCount(), 0);
  EXPECT_EQ(table_stats_collector.GetColumnCount(), 4);
}

TEST_F(TableStatsCollectorTests, SingleColumnTableTest) {
  // Boostrap database
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(id integer);");
  int nrow = 100;
  for (int i = 0; i < nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  txn = txn_manager.BeginTransaction();
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME,
                                         "test", txn);
  txn_manager.CommitTransaction(txn);
  TableStatsCollector stats{table};
  stats.CollectColumnStats();

  EXPECT_EQ(stats.GetColumnCount(), 1);
  EXPECT_EQ(stats.GetActiveTupleCount(), nrow);

  auto column_stats_collector = stats.GetColumnStats(0);
  EXPECT_EQ(column_stats_collector->GetFracNull(), 0);
  uint64_t cardinality = column_stats_collector->GetCardinality();
  double cardinality_error = column_stats_collector->GetCardinalityError();
  EXPECT_GE(cardinality, nrow * (1 - cardinality_error));
  EXPECT_LE(cardinality, nrow * (1 + cardinality_error));
  std::vector<double> bounds = column_stats_collector->GetHistogramBound();
  EXPECT_GE(bounds.size(), 1);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// Table with four columns with types Integer, Varchar, Decimal and Timestamp
// BOOLEAN insertion seems not supported.
TEST_F(TableStatsCollectorTests, MultiColumnTableTest) {
  // Boostrap database
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b VARCHAR, c DOUBLE, d TIMESTAMP);");

  int nrow = 10;
  for (int i = 0; i < nrow; i++) {
    if (i % 2 == 0) {
      TestingSQLUtil::ExecuteSQLQuery(
          "INSERT INTO test VALUES (0, 'even', 1.234, '2017-04-01 "
          "00:00:02-04');");
    } else {
      TestingSQLUtil::ExecuteSQLQuery(
          "INSERT INTO test VALUES (1, 'odd', 1.234, '2017-01-01 "
          "11:11:12-04');");
    }
  }

  txn = txn_manager.BeginTransaction();
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME,
                                         "test", txn);
  txn_manager.CommitTransaction(txn);
  TableStatsCollector stats{table};
  stats.CollectColumnStats();

  EXPECT_EQ(stats.GetColumnCount(), 4);
  EXPECT_EQ(stats.GetActiveTupleCount(), nrow);

  // Varchar stats
  ColumnStatsCollector *b_stats = stats.GetColumnStats(1);
  EXPECT_EQ(b_stats->GetFracNull(), 0);
  EXPECT_EQ(b_stats->GetCardinality(), 2);
  EXPECT_EQ((b_stats->GetHistogramBound()).size(),
            0);  // varchar has no histogram dist

  // Double stats
  ColumnStatsCollector *c_stats = stats.GetColumnStats(2);
  EXPECT_EQ(c_stats->GetFracNull(), 0);
  EXPECT_EQ(c_stats->GetCardinality(), 1);
  EXPECT_EQ(c_stats->GetHistogramBound().size() + 1, 1);

  // Timestamp stats
  ColumnStatsCollector *d_stats = stats.GetColumnStats(3);
  EXPECT_EQ(d_stats->GetFracNull(), 0);
  EXPECT_EQ(d_stats->GetCardinality(), 2);
  EXPECT_EQ(d_stats->GetHistogramBound().size() + 1, 2);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
