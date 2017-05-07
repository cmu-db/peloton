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

std::shared_ptr<ColumnStats> CreateTestColumnStats(oid_t database_id,
                                                   oid_t table_id,
                                                   oid_t column_id,
                                                   std::string column_name) {
  return std::shared_ptr<ColumnStats>(new ColumnStats(
      database_id, table_id, column_id, column_name, 0, 0, 0, {}, {}, {}));
}

// Test the constructors of TableStats
// Test the Get/Add/Remove functions of TableStats
TEST_F(TableStatsTests, ConstructorTests) {
  TableStats table_stats0;
  EXPECT_EQ(table_stats0.num_rows, 0);

  TableStats table_stats1(10);
  EXPECT_EQ(table_stats1.num_rows, 10);

  auto col_stats0 = CreateTestColumnStats(0, 0, 0, "col0");
  auto col_stats1 = CreateTestColumnStats(1, 1, 1, "col1");
  auto col_stats2 = CreateTestColumnStats(2, 2, 2, "col2");

  TableStats table_stats2(20, {col_stats0, col_stats1});
  EXPECT_EQ(table_stats2.num_rows, 20);

  auto col_stats_result0 = table_stats2.GetColumnStats("col0");
  EXPECT_EQ(col_stats_result0->database_id, 0);
  auto col_stats_result1 = table_stats2.GetColumnStats("col1");
  EXPECT_EQ(col_stats_result1->database_id, 1);
}

} /* namespace test */
} /* namespace peloton */
