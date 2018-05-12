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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/epoch_manager.h"
#include "concurrency/testing_transaction_util.h"
#include "executor/testing_executor_util.h"
#include "gc/transaction_level_gc_manager.h"
#include "gc/tile_group_compactor.h"
#include "sql/testing_sql_util.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// TransactionContext-Level GC Manager Tests
//===--------------------------------------------------------------------===//

class TileGroupCompactorTests : public PelotonTest {};

//Basic functionality
//
//Test that compaction is triggered and successful for sparse tile groups
//    Fill up tile group with 10 tuples
//    Delete 9 of the tuples
//    Check that tile group is compacted
//    Ensure that tuples have the same values
TEST_F(TileGroupCompactorTests, BasicTest) {
  // start worker pool
  threadpool::MonoQueuePool::GetInstance().Startup();

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  std::vector<std::unique_ptr<std::thread>> gc_threads;

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();

  // create database
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase("basiccompactdb");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 0;
  size_t tuples_per_tilegroup = 10;

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "table0", db_id, INVALID_OID, 1234, true, tuples_per_tilegroup));

  auto &manager = catalog::Manager::GetInstance();
  size_t tile_group_count_after_init = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_init: %zu\n", tile_group_count_after_init);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  epoch_manager.SetCurrentEpochId(++current_eid);
  //===========================
  // insert tuples here, this will allocate another tile group
  //===========================
  size_t num_inserts = tuples_per_tilegroup;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // capture num tile groups occupied
  size_t tile_group_count_after_insert = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_insert: %zu", tile_group_count_after_insert);
  EXPECT_GT(tile_group_count_after_insert, tile_group_count_after_init);

  epoch_manager.SetCurrentEpochId(++current_eid);
  //===========================
  // delete the tuples all but 1 tuple, this will not allocate another tile group
  //===========================
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), num_inserts - 1);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  size_t tile_group_count_after_delete = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_delete: %zu", tile_group_count_after_delete);
  EXPECT_EQ(tile_group_count_after_delete, tile_group_count_after_insert);

  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);

  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);

  //===========================
  // run GC then sleep for 1 second to allow for tile compaction to work
  //===========================
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  size_t tile_group_count_after_compact = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_compact: %zu", tile_group_count_after_compact);

  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);

  size_t tile_group_count_after_gc = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_gc: %zu", tile_group_count_after_gc);
  EXPECT_EQ(tile_group_count_after_gc, tile_group_count_after_init);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase("basiccompactdb");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("freetilegroupsdb", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}


// TODO
//Test that compaction is NOT triggered for non-sparse tile groups
//    Fill up tile group with 10 tuples
//    Delete 5 of the tuples
//    Check that tile group is NOT compacted
//


////////// Concurrency test ///////////////////////////////////
//Test updates during compaction
//Create tile group
//    Insert tuples to fill tile group completely
//Delete 80% of tuples
//Start txn that updates the last of these tuples, dont commit
//Start MoveTuplesOutOfTileGroup
//Confirm that returns false
//Verify that tuples values are correct
//Commit update txn
//    Start MoveTuplesOutOfTileGroup
//    Confirm that returns true
//Verify that tuples values are correct
//
TEST_F(TileGroupCompactorTests, ConcurrentUpdateTest) {
  std::string test_name = "abortinsert";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "table", db_id, INVALID_OID, 1234, true, 10));
  TestingTransactionUtil::AddSecondaryIndex(table.get());

  epoch_manager.SetCurrentEpochId(++current_epoch);

  auto &catalog_manager = catalog::Manager::GetInstance();
  size_t starting_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  size_t num_inserts = 10;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // Delete compaction_threshold tuples from tile_group
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  size_t num_delete_tuples = 8;
  for (size_t i=1; i <= num_delete_tuples; i++) {
    scheduler.Txn(0).Delete(i, false);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);

  auto txn = txn_manager.BeginTransaction();
  auto update_result = TestingTransactionUtil::ExecuteUpdate(txn, table.get(), 9, 100, true);
  EXPECT_TRUE(update_result);

  auto tile_group = table->GetTileGroup(0);
  bool compact_result = gc::TileGroupCompactor::MoveTuplesOutOfTileGroup(table.get(), tile_group);
  EXPECT_FALSE(compact_result);

  txn_manager.CommitTransaction(txn);

  // clear garbage
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  compact_result = gc::TileGroupCompactor::MoveTuplesOutOfTileGroup(table.get(), tile_group);
  EXPECT_TRUE(compact_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // assert num live tile groups is what it was before started
  auto current_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(starting_num_live_tile_groups, current_num_live_tile_groups);

  // assert that tuples 8 and 9 exist with expected values
  std::vector<int> results;
  auto ret = TestingTransactionUtil::SelectTuple(table.get(), 10, results);
  EXPECT_EQ(ResultType::SUCCESS, ret);
  EXPECT_EQ(10, results[0]);

  results.clear();
  ret = TestingTransactionUtil::SelectTuple(table.get(), 9, results);
  EXPECT_EQ(ResultType::SUCCESS, ret);
  EXPECT_EQ(100, results[0]);

//  LOG_DEBUG("%s", table->GetInfo().c_str());

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "db");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
  EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

//Edge cases
//
//Test that Compaction ignores all tuples for dropped tile group
//    Create tile group
//Save tg_id
//Insert tuples to fill tile group completely
//    Delete all tuples in tile group
//    Run compaction on freed tile group
//    Shouldn't crash
//Ensure that no tuples moved (by checking that num tile groups didnt change)
//Ensure that tuples have the same values
//
//    Test that compaction ignores tile group if table dropped
//Create tile group
//    Save tg_id
//    Drop table
//    Run compaction on freed tile group
//    Shouldn't crash
//Ensure that no tuples moved (by checking that num tile groups didnt change)
//
//Test that compaction ignores all tuples for tile group full of all garbage
//Create tile group
//    Insert tuples to fill tile group completely
//Update all tuples in tile group
//Run compaction on first tile group
//Ensure that no tuples moved (by checking that num tile groups didnt change)
//

//Test retry mechanism
//    Create tile group
//Delete 80%
//Start txn that updates 1 of these tuples but does not commit
//    Run CompactTileGroups in separate thread
//Sleep .1 second
//    Commit txn
//    Sleep .1 second
//    Test that tile group was compacted


}  // namespace test
}  // namespace peloton
