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

#include "optimizer/stats/stats_storage.h"

#include "storage/data_table.h"
#include "storage/database.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class StatsStorageTests : public PelotonTest {};


TEST_F(StatsStorageTests, StatsTableTest) {
  StatsStorage *stats_storage = StatsStorage::GetInstance();
  storage::DataTable *stats_table = stats_storage->GetStatsTable();
  EXPECT_EQ(stats_table->GetName(), STATS_TABLE_NAME);
  EXPECT_EQ(stats_table->GetTupleCount(), 0);

  catalog::Schema *schema = stats_table->GetSchema();
  EXPECT_TRUE(schema != nullptr);
  EXPECT_EQ(schema->GetColumnCount(), 9);
}

TEST_F(StatsStorageTests, StatsInsertTest) {
}

TEST_F(StatsStorageTests, SamplesDBTest) {
  catalog::Catalog *catalog = catalog::Catalog::GetInstance();
  storage::Database *samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  EXPECT_TRUE(samples_db != nullptr);
  EXPECT_EQ(samples_db->GetDBName(), SAMPLES_DB_NAME);
  EXPECT_EQ(samples_db->GetTableCount(), 0);
}

} /* namespace test */
} /* namespace peloton */
