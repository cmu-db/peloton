//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_test.cpp
//
// Identification: test/storage/zone_map_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <stdio.h>
#include <stdlib.h>
#include "common/harness.h"

#include "storage/data_table.h"

#include "executor/testing_executor_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "storage/zone_map_manager.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"
#include "catalog/zone_map_catalog.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class ZoneMapTests : public PelotonTest {};

TEST_F(ZoneMapTests, ZoneMapContentsTest) {

  std::unique_ptr<storage::DataTable> data_table(TestingExecutorUtil::CreateTable(5, false, 1));
  // storage::DataTable *data_table = TestingExecutorUtil::CreateTable(5, false, 1);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  TestingExecutorUtil::PopulateTable(data_table.get(), 20, false, false, false, txn);
  txn_manager.CommitTransaction(txn);


  oid_t num_tile_groups = (data_table.get())->GetTileGroupCount();
  for (oid_t i = 0; i < num_tile_groups - 1; i++) {
    auto tile_group = (data_table.get())->GetTileGroup(i);
    auto tile_group_ptr = tile_group.get();
    auto tile_group_header = tile_group_ptr->GetHeader();
    tile_group_header->SetImmutability();
  }

  auto catalog = catalog::Catalog::GetInstance();
  (void)catalog;
  storage::ZoneMapManager *zone_map_manager = storage::ZoneMapManager::GetInstance();
  zone_map_manager->CreateZoneMapTableInCatalog();
  txn = txn_manager.BeginTransaction();
  zone_map_manager->CreateZoneMapsForTable((data_table.get()), txn);
  txn_manager.CommitTransaction(txn);

  for (oid_t i = 0; i < num_tile_groups - 1; i++) {
    oid_t database_id = (data_table.get())->GetDatabaseOid();
    oid_t table_id = (data_table.get())->GetOid();
    for (int j = 0; j < 4; j++) {
      std::shared_ptr<storage::ZoneMapManager::ColumnStatistics> stats = zone_map_manager->GetZoneMapFromCatalog(database_id, table_id, i, j);
      type::Value min_val = (stats.get())->min;
      type::Value max_val = (stats.get())->max;
      int max = ((TESTS_TUPLES_PER_TILEGROUP * (i+1)) - 1)*10;
      int min = (TESTS_TUPLES_PER_TILEGROUP * (i))*10;
      // Integer Columns
      if (j == 0 || j == 1) {
        int min_zone_map = min_val.GetAs<int>();
        int max_zone_map = max_val.GetAs<int>();
        EXPECT_EQ(min + j, min_zone_map);
        EXPECT_EQ(max + j, max_zone_map);
      } else if (j == 2) {
        // Decimal Column
        double min_zone_map = min_val.GetAs<double>();
        double max_zone_map = max_val.GetAs<double>();
        EXPECT_EQ((double)(min + j), min_zone_map);
        EXPECT_EQ((double)(max + j), max_zone_map);
      } else {
        // VARCHAR Column
        const char *min_zone_map_str;
        const char *max_zone_map_str;

        min_zone_map_str = min_val.GetData();
        max_zone_map_str = max_val.GetData();

        std::stringstream min_ss;
        std::stringstream max_ss;
        if (i == 0) {
          min_ss << (min + j + 10);
        } else {
          min_ss << (min + j);
        }
        max_ss << (max +j);
        std::string min_str = min_ss.str();
        std::string max_str = max_ss.str();
        EXPECT_EQ(min_str, min_zone_map_str);
        EXPECT_EQ(max_str, max_zone_map_str);
      }
    }
  }
}
}  // End test namespace
}  // End peloton namespace