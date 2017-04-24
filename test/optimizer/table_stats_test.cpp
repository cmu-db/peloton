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

#include "common/logger.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/column_stats.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "type/type.h"
#include "type/value.h"
#include "executor/testing_executor_util.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

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

} /* namespace test */
} /* namespace peloton */
