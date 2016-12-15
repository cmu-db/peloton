//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_test.cpp
//
// Identification: test/executor/delete_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/for_update_executor.h"
#include "optimizer/simple_optimizer.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// For Update Tests
//===--------------------------------------------------------------------===//

class ForUpdateTests : public PelotonTest {};

TEST_F(ForUpdateTests, Updating) {

  // Create a tile group
  const int tuple_count = 1;
  std::shared_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateTileGroup(tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Create a tuple and insert it
  storage::Tuple tuple(schema.get(), true);

  tuple1.SetValue(0, common::ValueFactory::GetIntegerValue(1));
  tuple1.SetValue(1, common::ValueFactory::GetIntegerValue(2));
  tuple1.SetValue(2, common::ValueFactory::GetTinyIntValue(3));
  tuple1.SetValue(3, common::ValueFactory::GetVarcharValue("tuple 1"));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto tuple_id1 = tile_group->InsertTuple(&tuple1);

  ItemPointer *index_entry_ptr = nullptr;
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_id1), index_entry_ptr);

  txn_manager.CommitTransaction(txn);

  // Selecting tuple for update
  txn = txn_manager.BeginTransaction();
  executor::LogicalTile *logical_tile = LogicalTileFactory::WrapTileGroup(&tile_group)
  bool locked = for_update_executor.DExecute(logical_tile);
  txn_manager.CommitTransaction(txn);
  EXPECT_TRUE(locked);

  // Try grabbing the same tuple again
  txn = txn_manager.BeginTransaction();
  bool locked = for_update_executor.DExecute(logical_tile);
  txn_manager.CommitTransaction(txn);
  EXPECT_FALSE(locked);

}

}  // End test namespace
}  // End peloton namespace
