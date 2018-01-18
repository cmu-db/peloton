//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager_test.cpp
//
// Identification: test/gc/transaction_level_gc_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"
#include "executor/testing_executor_util.h"
#include "common/harness.h"
#include "gc/transaction_level_gc_manager.h"
#include "concurrency/epoch_manager.h"

#include "catalog/catalog.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/storage_manager.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// TransactionContext-Level GC Manager Tests
//===--------------------------------------------------------------------===//

class TransactionLevelGCManagerTests : public PelotonTest {};

ResultType UpdateTuple(storage::DataTable *table, const int key) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Update(key, rand() % 15721);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType InsertTuple(storage::DataTable *table, const int key) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Insert(key, rand() % 15721);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType DeleteTuple(storage::DataTable *table, const int key) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Delete(key);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType SelectTuple(storage::DataTable *table, const int key,
                       std::vector<int> &results) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Read(key);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  results = scheduler.schedules[0].results;

  return scheduler.schedules[0].txn_result;
}

// update -> delete
TEST_F(TransactionLevelGCManagerTests, UpdateDeleteTest) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);
  std::vector<std::unique_ptr<std::thread>> gc_threads;

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();
  // create database
  auto database = TestingExecutorUtil::InitializeDatabase("DATABASE0");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 1;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "TABLE0", db_id, INVALID_OID, 1234, true));

  EXPECT_TRUE(gc_manager.GetTableCount() == 1);

  //===========================
  // update a version here.
  //===========================
  auto ret = UpdateTuple(table.get(), 0);
  EXPECT_TRUE(ret == ResultType::SUCCESS);

  epoch_manager.SetCurrentEpochId(2);

  // get expired epoch id.
  // as the current epoch id is set to 2,
  // the expected expired epoch id should be 1.
  auto expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, expired_eid);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(2, current_eid);

  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  auto unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(3);

  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(2, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(3, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(1, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  //===========================
  // delete a version here.
  //===========================
  ret = DeleteTuple(table.get(), 0);
  EXPECT_TRUE(ret == ResultType::SUCCESS);

  epoch_manager.SetCurrentEpochId(4);

  // get expired epoch id.
  // as the current epoch id is set to 4,
  // the expected expired epoch id should be 3.
  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(3, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(4, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(5);

  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(4, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(5, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(1, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase("DATABASE0");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("DATABASE0", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
  // EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

// insert -> delete -> insert
TEST_F(TransactionLevelGCManagerTests, ReInsertTest) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  std::vector<std::unique_ptr<std::thread>> gc_threads;

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();

  auto storage_manager = storage::StorageManager::GetInstance();
  // create database
  auto database = TestingExecutorUtil::InitializeDatabase("DATABASE1");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 1;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "TABLE1", db_id, INVALID_OID, 1234, true));

  EXPECT_TRUE(gc_manager.GetTableCount() == 1);

  //===========================
  // insert a tuple here.
  //===========================
  auto ret = InsertTuple(table.get(), 100);
  EXPECT_TRUE(ret == ResultType::SUCCESS);

  epoch_manager.SetCurrentEpochId(2);

  // get expired epoch id.
  // as the current epoch id is set to 2,
  // the expected expired epoch id should be 1.
  auto expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, expired_eid);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(2, current_eid);

  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  auto unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  epoch_manager.SetCurrentEpochId(3);

  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(2, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(3, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  //===========================
  // select the tuple.
  //===========================
  std::vector<int> results;

  results.clear();
  ret = SelectTuple(table.get(), 100, results);
  EXPECT_TRUE(ret == ResultType::SUCCESS);
  EXPECT_TRUE(results[0] != -1);

  //===========================
  // delete the tuple.
  //===========================
  ret = DeleteTuple(table.get(), 100);
  EXPECT_TRUE(ret == ResultType::SUCCESS);

  epoch_manager.SetCurrentEpochId(4);

  // get expired epoch id.
  // as the current epoch id is set to 4,
  // the expected expired epoch id should be 3.
  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(3, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(4, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(5);

  expired_eid = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(4, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(5, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(1, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  //===========================
  // select the tuple.
  //===========================
  results.clear();
  ret = SelectTuple(table.get(), 100, results);
  EXPECT_TRUE(ret == ResultType::SUCCESS);
  EXPECT_TRUE(results[0] == -1);

  //===========================
  // insert the tuple again.
  //===========================
  ret = InsertTuple(table.get(), 100);
  EXPECT_TRUE(ret == ResultType::SUCCESS);

  //===========================
  // select the tuple.
  //===========================
  results.clear();
  ret = SelectTuple(table.get(), 100, results);
  EXPECT_TRUE(ret == ResultType::SUCCESS);
  EXPECT_TRUE(results[0] != -1);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase("DATABASE1");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("DATABASE0", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
  // EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

/*
Brief Summary : This tests tries to check immutability of a tile group.
Once a tile group is set immutable, gc should not recycle slots from the
tile group. We will first insert into a tile group and then delete tuples
from the tile group. After setting immutability further inserts or updates
should not use slots from the tile group where delete happened.
*/
TEST_F(TransactionLevelGCManagerTests, ImmutabilityTest) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  std::vector<std::unique_ptr<std::thread>> gc_threads;

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();

  auto storage_manager = storage::StorageManager::GetInstance();
  // create database
  auto database = TestingExecutorUtil::InitializeDatabase("ImmutabilityDB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 25;
  const size_t tuples_per_tilegroup = 5;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "TABLE1", db_id, INVALID_OID, 1234, true, tuples_per_tilegroup));

  EXPECT_TRUE(gc_manager.GetTableCount() == 1);

  oid_t num_tile_groups = (table.get())->GetTileGroupCount();
  EXPECT_EQ(num_tile_groups, (num_key / tuples_per_tilegroup) + 1);

  // Making the 1st tile group immutable
  auto tile_group = (table.get())->GetTileGroup(0);
  auto tile_group_ptr = tile_group.get();
  auto tile_group_header = tile_group_ptr->GetHeader();
  tile_group_header->SetImmutability();

  // Deleting a tuple from the 1st tilegroup
  auto ret = DeleteTuple(table.get(), 2);
  EXPECT_TRUE(ret == ResultType::SUCCESS);
  epoch_manager.SetCurrentEpochId(2);
  auto expired_eid = epoch_manager.GetExpiredEpochId();
  EXPECT_EQ(1, expired_eid);
  auto current_eid = epoch_manager.GetCurrentEpochId();
  EXPECT_EQ(2, current_eid);
  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);
  auto unlinked_count = gc_manager.Unlink(0, expired_eid);
  EXPECT_EQ(0, reclaimed_count);
  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(3);
  expired_eid = epoch_manager.GetExpiredEpochId();
  EXPECT_EQ(2, expired_eid);
  current_eid = epoch_manager.GetCurrentEpochId();
  EXPECT_EQ(3, current_eid);
  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
  unlinked_count = gc_manager.Unlink(0, expired_eid);
  EXPECT_EQ(1, reclaimed_count);
  EXPECT_EQ(0, unlinked_count);

  // ReturnFreeSlot() should return null because deleted tuple was from
  // immutable tilegroup.
  auto location = gc_manager.ReturnFreeSlot((table.get())->GetOid());
  EXPECT_EQ(location.IsNull(), true);

  // Deleting a tuple from the 2nd tilegroup which is mutable.
  ret = DeleteTuple(table.get(), 6);

  EXPECT_TRUE(ret == ResultType::SUCCESS);
  epoch_manager.SetCurrentEpochId(4);
  expired_eid = epoch_manager.GetExpiredEpochId();
  EXPECT_EQ(3, expired_eid);
  current_eid = epoch_manager.GetCurrentEpochId();
  EXPECT_EQ(4, current_eid);
  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
  unlinked_count = gc_manager.Unlink(0, expired_eid);
  EXPECT_EQ(0, reclaimed_count);
  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(5);
  expired_eid = epoch_manager.GetExpiredEpochId();
  EXPECT_EQ(4, expired_eid);
  current_eid = epoch_manager.GetCurrentEpochId();
  EXPECT_EQ(5, current_eid);
  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
  unlinked_count = gc_manager.Unlink(0, expired_eid);
  EXPECT_EQ(1, reclaimed_count);
  EXPECT_EQ(0, unlinked_count);

  // ReturnFreeSlot() should not return null because deleted tuple was from
  // mutable tilegroup.
  location = gc_manager.ReturnFreeSlot((table.get())->GetOid());
  EXPECT_EQ(location.IsNull(), false);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();
  // DROP!
  TestingExecutorUtil::DeleteDatabase("ImmutabilityDB");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("ImmutabilityDB", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
