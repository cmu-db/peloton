//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager_test.cpp
//
// Identification: test/gc/tile_group_compactor_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
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

oid_t test_index_oid = 1234;
double recycling_threshold = 0.8;

// Test that GCManager triggers compaction for sparse tile groups
// And test it doesn't trigger compaction for dense tile groups
// Runs MonoQueuePool to do compaction in separate threads
TEST_F(TileGroupCompactorTests, GCIntegrationTestSparse) {
  std::string test_name = "gc_integration_test_sparse";

  // start worker pool
  threadpool::MonoQueuePool::GetInstance().Startup();

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  gc_manager.SetTileGroupRecyclingThreshold(recycling_threshold);
  gc_manager.SetTileGroupFreeing(true);
  gc_manager.SetTileGroupCompaction(true);

  // create database
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create table
  const int num_key = 0;
  size_t tuples_per_tilegroup = 10;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "table0", db_id, INVALID_OID, test_index_oid++, true, tuples_per_tilegroup));

  auto &manager = catalog::Manager::GetInstance();
  size_t tile_group_count_after_init = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_init: %zu\n", tile_group_count_after_init);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  epoch_manager.SetCurrentEpochId(++current_eid);

  // insert tuples here, this will allocate another tile group
  size_t num_inserts = tuples_per_tilegroup;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // capture num tile groups occupied
  size_t tile_group_count_after_insert = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_insert: %zu", tile_group_count_after_insert);
  EXPECT_GT(tile_group_count_after_insert, tile_group_count_after_init);

  epoch_manager.SetCurrentEpochId(++current_eid);

  // delete all but 1 of the tuples
  // this will create 9 tombstones, so won't fill another tile group
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), num_inserts - 1);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  size_t tile_group_count_after_delete = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_delete: %zu", tile_group_count_after_delete);
  EXPECT_EQ(tile_group_count_after_delete, tile_group_count_after_insert);

  // first clear garbage from outdated versions and tombstones
  // should also add tile groups to compaction queue
  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);
  // submit compaction tasks to worker pool
  gc_manager.ProcessCompactionQueue();

  // sleep to allow tile group compaction to happen
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  size_t tile_group_count_after_compact = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_compact: %zu", tile_group_count_after_compact);

  // Run GC to free compacted tile groups
  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);

  size_t tile_group_count_after_gc = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_gc: %zu", tile_group_count_after_gc);
  EXPECT_EQ(tile_group_count_after_gc, tile_group_count_after_init);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "db");
}

// Test that GCManager doesn't trigger compaction for dense tile groups
// Runs MonoQueuePool to do compaction in separate threads
TEST_F(TileGroupCompactorTests, GCIntegrationTestDense) {
  std::string test_name = "gc_integration_test_dense";
  // start worker pool
  threadpool::MonoQueuePool::GetInstance().Startup();

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  gc_manager.SetTileGroupRecyclingThreshold(recycling_threshold);
  gc_manager.SetTileGroupFreeing(true);
  gc_manager.SetTileGroupCompaction(true);

  // create database
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create table
  const int num_key = 0;
  size_t tuples_per_tilegroup = 10;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "table0", db_id, INVALID_OID, test_index_oid++, true, tuples_per_tilegroup));

  auto &manager = catalog::Manager::GetInstance();
  size_t tile_group_count_after_init = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_init: %zu\n", tile_group_count_after_init);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  epoch_manager.SetCurrentEpochId(++current_eid);

  // insert tuples here, this will allocate another tile group
  size_t num_inserts = tuples_per_tilegroup;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // capture num tile groups occupied
  size_t tile_group_count_after_insert = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_insert: %zu", tile_group_count_after_insert);
  EXPECT_GT(tile_group_count_after_insert, tile_group_count_after_init);

  epoch_manager.SetCurrentEpochId(++current_eid);

  // delete 3/10 of the tuples
  // this will create 3 tombstones, so won't fill another tile group
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), 3);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  size_t tile_group_count_after_delete = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_delete: %zu", tile_group_count_after_delete);
  EXPECT_EQ(tile_group_count_after_init + 1, tile_group_count_after_delete);

  // first clear garbage from outdated versions and tombstones
  // should also add tile groups to compaction queue
  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);
  // submit compaction tasks to worker pool
  gc_manager.ProcessCompactionQueue();

  // sleep to allow tile group compaction to happen
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  size_t tile_group_count_after_compact = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_compact: %zu", tile_group_count_after_compact);

  // Run GC to free compacted tile groups
  epoch_manager.SetCurrentEpochId(++current_eid);
  gc_manager.ClearGarbage(0);

  size_t tile_group_count_after_gc = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_gc: %zu", tile_group_count_after_gc);
  EXPECT_EQ(tile_group_count_after_delete, tile_group_count_after_gc);

  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "db");
}

// Test compaction during a concurrent update txn
TEST_F(TileGroupCompactorTests, ConcurrentUpdateTest) {
  std::string test_name = "concurrentupdatetest";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  gc_manager.SetTileGroupRecyclingThreshold(recycling_threshold);
  gc_manager.SetTileGroupFreeing(true);
  gc_manager.SetTileGroupCompaction(true);

  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "table", db_id, INVALID_OID, test_index_oid++, true, 10));
  TestingTransactionUtil::AddSecondaryIndex(table.get());

  epoch_manager.SetCurrentEpochId(++current_epoch);

  auto &catalog_manager = catalog::Manager::GetInstance();
  size_t starting_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Fill a tile group with tuples
  size_t num_inserts = 10;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // Delete compaction_threshold tuples from tile_group
  size_t num_deletes = 8;
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), num_deletes);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  // Start txn that updates one of the remaining tuples
  // Don't commit yet
  auto txn = txn_manager.BeginTransaction();
  auto update_result = TestingTransactionUtil::ExecuteUpdate(txn, table.get(), 9, 100, true);
  EXPECT_TRUE(update_result);

  // Then try to compact the table's first tile_group while update txn is in progress
  auto starting_tg = table->GetTileGroup(0);
  bool compact_result = gc::TileGroupCompactor::MoveTuplesOutOfTileGroup(table.get(), starting_tg);
  EXPECT_FALSE(compact_result);

  // Commit the update txn so that the compaction is able to proceed
  txn_manager.CommitTransaction(txn);

  // clear garbage
  // Marks first & second tilegroups for compaction
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  auto num_tg_before_compaction = catalog_manager.GetNumLiveTileGroups();

  // Try to compact the tile again. This time it should succeed
  compact_result = gc::TileGroupCompactor::MoveTuplesOutOfTileGroup(table.get(), starting_tg);
  EXPECT_TRUE(compact_result);

  // Clear garbage, trigger freeing of compacted tile group
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // assert num live tile groups decreased
  auto num_tg_now = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(num_tg_before_compaction - 1, num_tg_now);

  // Compact all tile groups
  for (size_t i=0; i < table->GetTileGroupCount(); i++) {
    auto tg = table->GetTileGroup(i);
    if (tg != nullptr) {
      gc::TileGroupCompactor::MoveTuplesOutOfTileGroup(table.get(), tg);
    }
  }
  // Clear garbage from compaction
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // Assert that num live tile groups is back to starting value
  num_tg_now = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(starting_num_live_tile_groups, num_tg_now);

  // assert that we have the moved tuple and updated tuple with expected values
  // 8 was moved, 9 was updated
  std::vector<int> results;
  auto ret = TestingTransactionUtil::SelectTuple(table.get(), 8, results);
  EXPECT_EQ(ResultType::SUCCESS, ret);
  EXPECT_EQ(8, results[0]);

  results.clear();
  ret = TestingTransactionUtil::SelectTuple(table.get(), 9, results);
  EXPECT_EQ(ResultType::SUCCESS, ret);
  EXPECT_EQ(100, results[0]);

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "db");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
  EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

// Test that TileGroupCompactor can handle:
//  - tile groups that are entirely filled with garbage
//  - tile groups that no longer exist (already freed)
//  - tile groups that belong to dropped tables
TEST_F(TileGroupCompactorTests, EdgeCasesTest) {
  std::string test_name = "edgecasestest";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  gc_manager.SetTileGroupRecyclingThreshold(recycling_threshold);
  gc_manager.SetTileGroupFreeing(true);
  gc_manager.SetTileGroupCompaction(true);
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "table", db_id, INVALID_OID, test_index_oid++, true, 10));
  TestingTransactionUtil::AddSecondaryIndex(table.get());

  epoch_manager.SetCurrentEpochId(++current_epoch);

  auto &catalog_manager = catalog::Manager::GetInstance();
  size_t starting_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();

  oid_t starting_tgid = table->GetTileGroup(0)->GetTileGroupId();

  // First insert 1 tile group full of tuples
  size_t num_inserts = 10;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  size_t num_deletes = num_inserts;
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), num_deletes);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  auto post_delete_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(starting_num_live_tile_groups + 2, post_delete_num_live_tile_groups);

  // Compact tile group that is all garbage. It should ignore all slots
  gc::TileGroupCompactor::CompactTileGroup(starting_tgid);

  // assert num live tile groups did not change
  auto current_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(post_delete_num_live_tile_groups, current_num_live_tile_groups);

  // clear garbage, triggers freeing of starting tile group
  // also clears tombstones from second tile group
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // assert num live tile groups decreased by 1
  current_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(starting_num_live_tile_groups, current_num_live_tile_groups);

  // Compact tile group that no longer exists
  // it should ignore the tile group (& not crash)
  EXPECT_EQ(nullptr, table->GetTileGroupById(starting_tgid));
  gc::TileGroupCompactor::CompactTileGroup(starting_tgid);

  // assert num live tile groups is what it was before started
  current_num_live_tile_groups = catalog_manager.GetNumLiveTileGroups();
  EXPECT_EQ(starting_num_live_tile_groups, current_num_live_tile_groups);

  table.release();

  // Compact tile group on a table that was dropped. Shouldn't crash
  gc::TileGroupCompactor::CompactTileGroup(starting_tgid);

  TestingExecutorUtil::DeleteDatabase(test_name + "db");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
  EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

// Test retry mechanism
TEST_F(TileGroupCompactorTests, RetryTest) {
  std::string test_name = "retrytest";

  // start worker pool
  threadpool::MonoQueuePool::GetInstance().Startup();

  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  gc_manager.SetTileGroupRecyclingThreshold(recycling_threshold);
  gc_manager.SetTileGroupFreeing(true);
  gc_manager.SetTileGroupCompaction(true);
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "db");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "table", db_id, INVALID_OID, test_index_oid++, true, 10));
  TestingTransactionUtil::AddSecondaryIndex(table.get());

  epoch_manager.SetCurrentEpochId(++current_epoch);

  auto &catalog_manager = catalog::Manager::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Fill a tile group with tuples
  size_t num_inserts = 10;
  auto insert_result = TestingTransactionUtil::BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // Delete compaction_threshold tuples from tile_group
  size_t num_deletes = 8;
  auto delete_result = TestingTransactionUtil::BulkDeleteTuples(table.get(), num_deletes);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  // Start txn that updates one of the remaining tuples
  // Don't commit yet
  auto txn = txn_manager.BeginTransaction();
  auto update_result = TestingTransactionUtil::ExecuteUpdate(txn, table.get(), 9, 100, true);
  EXPECT_TRUE(update_result);

  auto num_tg_before_compaction = catalog_manager.GetNumLiveTileGroups();

  // Now trigger GC, which should add this TG to compaction queue
  // Marks first tilegroup for compaction
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // try to compact the table's first tile_group while update txn is in progress
  gc_manager.ProcessCompactionQueue();

  // sleep to give it time to try and fail compaction
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // assert num live tile groups stays the same since compaction is blocked
  auto num_tg_now = catalog_manager.GetNumLiveTileGroups();
  EXPECT_LE(num_tg_before_compaction, num_tg_now);

  // Commit the update txn so that the compaction is able to proceed
  txn_manager.CommitTransaction(txn);

  // assert num live tile groups decreased
  num_tg_now = catalog_manager.GetNumLiveTileGroups();
  EXPECT_LE(num_tg_before_compaction, num_tg_now);

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "db");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
  EXPECT_FALSE(storage_manager->HasDatabase(db_id));
}

}  // namespace test
}  // namespace peloton
