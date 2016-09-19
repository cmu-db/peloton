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

//  LOG_INFO("Bootstrapping...");
//  auto catalog = catalog::Catalog::GetInstance();
//  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);
//  LOG_INFO("Bootstrapping completed!");
//
//  // Create a table first
//  LOG_INFO("Creating a table...");
//
//  // Define columns and schema
//  auto id_column = catalog::Column(
//      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER), "dept_id", true);
//  auto manager_id_column = catalog::Column(
//      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER), "manager_id", true);
//  auto name_column =
//      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);
//  std::unique_ptr<catalog::Schema> table_schema(
//      new catalog::Schema({id_column, manager_id_column, name_column}));
//
//  // Initialize transaction manager, plan node, and executors
//  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  std::unique_ptr<executor::ExecutorContext> context(
//      new executor::ExecutorContext(txn));
//  planner::CreatePlan node("department_table", std::move(table_schema),
//                           CreateType::CREATE_TYPE_TABLE);
//  executor::ForUpdateExecutor for_update_executor(&node, &context);
//  executor::CreateExecutor create_executor(&node, context.get());
//
//  // Initialize executors
//  for_update_executor.DInit();
//  create_executor.Init();
//  create_executor.Execute();
//  txn_manager.CommitTransaction(txn);
//  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);
//
//  LOG_INFO("Table created!");

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
  bool locked = for_update_executor.DRetrieveLock(logical_tile);
  txn_manager.CommitTransaction(txn);
  EXPECT_TRUE(locked);

  // Try grabbing the same tuple again
  txn = txn_manager.BeginTransaction();
  bool locked = for_update_executor.DRetrieveLock(logical_tile);
  txn_manager.CommitTransaction(txn);
  EXPECT_FALSE(locked);

//  // Free the database just created
//  txn = txn_manager.BeginTransaction();
//  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
//  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
