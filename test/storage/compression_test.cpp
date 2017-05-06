//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compression_test.cpp
//
// Identification: test/storage/compression_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "storage/data_table.h"

#include "executor/testing_executor_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class CompressionTests : public PelotonTest {};

/*
The following test inserts 5500 tuples in the datatable. Since a 1000 tuples
are inserted in each tile_group, there will be 5 compressed tiles and 1
uncompressed tile. After insertion of all the tuples, we call the Compress Table
function.
We then calculate the new size of the table. This should be less than the
original size of the table.
*/

TEST_F(CompressionTests, SizeTest) {
  std::unique_ptr<storage::DataTable> data_table_test_table;
  const int tuples_per_tile_group = 1000;
  const int total_tuples = 5500;
  UNUSED_ATTRIBUTE oid_t tuple_size = 24;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  data_table_test_table.reset(
      TestingExecutorUtil::CreateTable(tuples_per_tile_group, false));

  for (int i = 0; i < total_tuples; i++) {
    TestingExecutorUtil::PopulateTable(data_table_test_table.get(), 1, false,
                                       false, false, txn);
  }

  txn_manager.CommitTransaction(txn);
  auto num_tile_groups = data_table_test_table->GetTileGroupCount();
  auto uncompressed_size = 0;
  auto compressed_size = 0;

  for (oid_t i = 1; i <= num_tile_groups; i++) {
    storage::TileGroup *tile_group =
        (data_table_test_table->GetTileGroupById(i)).get();

    for (oid_t k = 0; k < tile_group->GetTileCount(); k++) {
      uncompressed_size += tile_group->GetTile(k)->GetSize();
    }
  }

  data_table_test_table->CompressTable();

  for (oid_t i = 1; i <= num_tile_groups; i++) {
    storage::TileGroup *tile_group =
        (data_table_test_table->GetTileGroupById(i)).get();

    for (oid_t k = 0; k < tile_group->GetTileCount(); k++) {
      compressed_size += tile_group->GetTile(k)->GetSize();
    }
  }

  LOG_INFO("Tuples Per Tile Group: %d", (int)tuples_per_tile_group);
  LOG_INFO("Number of Tile Groups: %d", (int)num_tile_groups);
  LOG_INFO("Tuples size in bytes: %d", (int)tuple_size);
  LOG_INFO("Uncompressed_Size in bytes: %d", (int)uncompressed_size);
  LOG_INFO("Compressed_Size in bytes: %d", (int)compressed_size);

  EXPECT_LE(compressed_size, uncompressed_size);
}

}  // End test namespace
}  // End peloton namespace
