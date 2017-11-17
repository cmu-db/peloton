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
#include "storage/zone_map.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class ZoneMapTests : public PelotonTest {};

TEST_F(ZoneMapTests, ZoneMapContentsTest) {
  storage::DataTable *data_table = TestingExecutorUtil::CreateTable(5, false, 1);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  TestingExecutorUtil::PopulateTable(data_table, 15, false, false, true, txn);
  txn_manager.CommitTransaction(txn);
  data_table->CreateZoneMaps();
  oid_t num_tile_groups = data_table->GetTileGroupCount();
  for (oid_t i = 1; i < num_tile_groups; i++) {
    storage::TileGroup *tile_group = (data_table->GetTileGroupById(i)).get();
    storage::ZoneMap *zone_map = tile_group->GetZoneMap();
    int max = ((TESTS_TUPLES_PER_TILEGROUP * i) - 1)*10;
    int min = (TESTS_TUPLES_PER_TILEGROUP * (i - 1))*10;
    for (int j = 0; j < 4; j++) {
      // Integer Columns
      if (j == 0 || j == 1) {
        type::Value min_val = zone_map->GetMinValue(j);
        type::Value max_val = zone_map->GetMaxValue(j);

        int min_zone_map = min_val.GetAs<int>();
        int max_zone_map = max_val.GetAs<int>();
        EXPECT_EQ(min + j, min_zone_map);
        EXPECT_EQ(max + j, max_zone_map);
      } else if (j == 2) {
        // Decimal Column
        type::Value min_val = zone_map->GetMinValue(j);
        type::Value max_val = zone_map->GetMaxValue(j);

        double min_zone_map = min_val.GetAs<double>();
        double max_zone_map = max_val.GetAs<double>();
        EXPECT_EQ((double)(min + j), min_zone_map);
        EXPECT_EQ((double)(max + j), max_zone_map);
      } else {
        // VARCHAR Column
        type::Value min_val = zone_map->GetMinValue(j);
        type::Value max_val = zone_map->GetMaxValue(j);

        const char *min_zone_map_str;
        const char *max_zone_map_str;

        min_zone_map_str = min_val.GetData();
        max_zone_map_str = max_val.GetData();

        std::stringstream min_ss;
        std::stringstream max_ss;
        if (i == 1) {
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