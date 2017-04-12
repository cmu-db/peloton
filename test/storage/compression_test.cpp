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

//===--------------------------------------------------------------------===//
// Compression Tests
//===--------------------------------------------------------------------===//

std::unique_ptr<storage::DataTable> data_table_test_table;

class CompressionTests : public PelotonTest {};

TEST_F(CompressionTests, SizeTest) {
  const int tuple_count = 100;
  UNUSED_ATTRIBUTE oid_t tuple_size = 24;
  UNUSED_ATTRIBUTE oid_t tuples_per_tile_group = 10;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  data_table_test_table.reset(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table_test_table.get(), tuple_count,
                                   false, false, true, txn);

  txn_manager.CommitTransaction(txn);
  auto num_tile_groups = data_table_test_table->GetTileGroupCount();
  auto uncompressed_size = tuple_count * tuple_size;
  auto compressed_size = 0;
  for(oid_t i = 1; i < num_tile_groups; i++) {
    storage::TileGroup *tile_group = &*(data_table_test_table->GetTileGroupById(i));

    for (oid_t k = 0; k < tile_group->GetTileCount(); k++) {
      compressed_size+= tile_group->GetTile(k)->GetSize();
    }
  }
  std::cout<< "Tuples Per Tile Group: " << tuples_per_tile_group << "\n";
  std::cout<< "Number of Tile Groups: " << num_tile_groups << "\n";
  std::cout<< "Tuples size in bytes: " << tuple_size << "\n";
  std::cout<< "Uncompressed_Size in bytes: " << uncompressed_size << "\n";
  std::cout<< "Compressed_Size in bytes: " << compressed_size << "\n";
  
  auto data_table_pointer = data_table_test_table.release();
  delete data_table_pointer;

  EXPECT_LE(compressed_size,uncompressed_size);
}

}  // End test namespace
}  // End peloton namespace
