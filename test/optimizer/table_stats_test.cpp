//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats_test.cpp
//
// Identification: test/optimizer/table_stats_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <memory>

#include "common/logger.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/column_stats.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "catalog/catalog.h"
#include "type/type.h"
#include "type/value.h"
#include "executor/testing_executor_util.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "sql/testing_sql_util.h"

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class TableStatsTests : public PelotonTest {};

TEST_F(TableStatsTests, BasicTests) {
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable());
  TableStats table_stats{data_table.get()};
  table_stats.CollectColumnStats();
  EXPECT_EQ(table_stats.GetActiveTupleCount(), 0);
  EXPECT_EQ(table_stats.GetColumnCount(), 4);
}

TEST_F(TableStatsTests, SingleColumnTableTest) {
  // Boostrap database
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(id integer);");
  int nrow = 10000;
  for (int i = 0; i < nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, "test");
  TableStats stats{table};
  stats.CollectColumnStats();

  EXPECT_EQ(stats.GetColumnCount(), 1);
  EXPECT_EQ(stats.GetActiveTupleCount(), nrow);

  auto column_stats = stats.GetColumnStats(0);
  EXPECT_EQ(column_stats->GetFracNull(), 0);
  uint64_t cardinality = column_stats->GetCardinality();
  double cardinality_error = column_stats->GetCardinalityError();
  EXPECT_GE(cardinality, nrow * (1 - cardinality_error));
  EXPECT_LE(cardinality, nrow * (1 + cardinality_error));
  std::vector<double> bounds = column_stats->GetHistogramBound();
  EXPECT_GE(bounds.size(), 1);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// Table with four columns with types Integer, Varchar, Decimal and Timestamp
// BOOLEAN insertion seems not supported.
TEST_F(TableStatsTests, MultiColumnTableTest) {
  // Boostrap database
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
    "CREATE TABLE test(a INT, b VARCHAR, c DOUBLE, d TIMESTAMP);");

  int nrow = 10000;
  for (int i = 0; i < nrow; i++) {
    if (i % 2  == 0) {
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (0, 'even', 1.234, '2017-04-01 00:00:02-04');");
    } else {
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 'odd', 1.234, '2017-01-01 11:11:12-04');");
    }
  }

  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, "test");
  TableStats stats{table};
  stats.CollectColumnStats();

  EXPECT_EQ(stats.GetColumnCount(), 4);
  EXPECT_EQ(stats.GetActiveTupleCount(), nrow);

  // Varchar stats
  ColumnStats* b_stats = stats.GetColumnStats(1);
  EXPECT_EQ(b_stats->GetFracNull(), 0);
  EXPECT_EQ(b_stats->GetCardinality(), 2);
  EXPECT_EQ((b_stats->GetHistogramBound()).size(), 0); // varchar has no histogram dist

  // Double stats
  ColumnStats* c_stats = stats.GetColumnStats(2);
  EXPECT_EQ(c_stats->GetFracNull(), 0);
  EXPECT_EQ(c_stats->GetCardinality(), 1);
  EXPECT_EQ(c_stats->GetHistogramBound().size() + 1, 1);

  // Timestamp stats
  ColumnStats* d_stats = stats.GetColumnStats(3);
  EXPECT_EQ(d_stats->GetFracNull(), 0);
  EXPECT_EQ(d_stats->GetCardinality(), 2);
  EXPECT_EQ(d_stats->GetHistogramBound().size() + 1, 2);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
