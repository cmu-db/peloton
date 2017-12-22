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

#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/column_stats.h"

#define private public
#define protected public

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class TableStatsTests : public PelotonTest {};

std::shared_ptr<ColumnStats> CreateTestColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id, std::string column_name,
    bool has_index, double cardinality = 10) {
  return std::shared_ptr<ColumnStats>(
      new ColumnStats(database_id, table_id, column_id, column_name, has_index,
                      0, cardinality, 0, {}, {}, {}));
}

// Test the constructors of TableStats
// Test the Get function of TableStats
TEST_F(TableStatsTests, BaiscTests) {
  TableStats table_stats0;
  EXPECT_EQ(table_stats0.num_rows, 0);

  TableStats table_stats1(10);
  EXPECT_EQ(table_stats1.num_rows, 10);

  auto col_stats0 = CreateTestColumnStats(0, 0, 0, "col0", true, 10);
  auto col_stats1 = CreateTestColumnStats(1, 1, 1, "col1", false, 20);

  TableStats table_stats2(20, {col_stats0, col_stats1});
  EXPECT_EQ(table_stats2.num_rows, 20);

  auto col_stats_result0 = table_stats2.GetColumnStats("col0");
  EXPECT_EQ(col_stats_result0->database_id, 0);
  EXPECT_TRUE(col_stats_result0->has_index);
  auto col_stats_result1 = table_stats2.GetColumnStats("col1");
  EXPECT_EQ(col_stats_result1->database_id, 1);
  EXPECT_FALSE(col_stats_result1->has_index);

  EXPECT_TRUE(table_stats2.HasIndex("col0"));
  EXPECT_FALSE(table_stats2.HasIndex("col1"));
  EXPECT_FALSE(table_stats2.HasIndex("col3"));

  EXPECT_EQ(table_stats2.GetCardinality("col0"), 10);
  EXPECT_EQ(table_stats2.GetCardinality("col1"), 20);

  EXPECT_EQ(table_stats2.GetColumnCount(), 2);

  LOG_INFO("%s", table_stats2.ToCSV().c_str());
}

// Test all update functions of TableStats, including UpdateNumRows,
// AddColumnStats, RemoveColumnStats and ClearColumnStats.
TEST_F(TableStatsTests, UpdateTests) {
  auto col_stats0 = CreateTestColumnStats(0, 0, 0, "col0", true, 10);
  auto col_stats1 = CreateTestColumnStats(1, 1, 1, "col1", false, 20);
  auto col_stats2 = CreateTestColumnStats(2, 2, 2, "col2", true, 30);

  TableStats table_stats(20, {col_stats0, col_stats1});

  table_stats.UpdateNumRows(30);
  EXPECT_EQ(table_stats.num_rows, 30);

  EXPECT_EQ(table_stats.GetColumnStats("col2"), nullptr);
  table_stats.AddColumnStats(col_stats2);
  EXPECT_NE(table_stats.GetColumnStats("col2"), nullptr);
  EXPECT_TRUE(table_stats.HasIndex("col2"));
  EXPECT_EQ(table_stats.GetCardinality("col2"), 30);

  table_stats.RemoveColumnStats("col0");
  EXPECT_EQ(table_stats.GetColumnStats("col0"), nullptr);

  table_stats.ClearColumnStats();
  EXPECT_EQ(table_stats.GetColumnStats("col1"), nullptr);
  EXPECT_EQ(table_stats.GetColumnStats("col2"), nullptr);
}

}  // namespace test
}  // namespace peloton
