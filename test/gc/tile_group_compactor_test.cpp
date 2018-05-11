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
#include "sql/testing_sql_util.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// TransactionContext-Level GC Manager Tests
//===--------------------------------------------------------------------===//

class TileGroupCompactorTests : public PelotonTest {};

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

ResultType BulkInsertTuples(storage::DataTable *table, const size_t num_tuples) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (size_t i=1; i <= num_tuples; i++) {
    scheduler.Txn(0).Insert(i, i);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType BulkDeleteTuples(storage::DataTable *table, const size_t num_tuples) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (size_t i=1; i <= num_tuples; i++) {
    scheduler.Txn(0).Delete(i, false);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType DeleteTuple(storage::DataTable *table, const int key) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Delete(key);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  return scheduler.schedules[0].txn_result;
}

ResultType SelectTuple(storage::DataTable *table, const int key,
                       std::vector<int> &results) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  scheduler.Txn(0).Read(key);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  results = scheduler.schedules[0].results;

  return scheduler.schedules[0].txn_result;
}

int GetNumRecycledTuples(storage::DataTable *table) {
  int count = 0;
//  auto table_id = table->GetOid();
  while (!gc::GCManagerFactory::GetInstance()
      .GetRecycledTupleSlot(table)
      .IsNull())
    count++;

  LOG_INFO("recycled version num = %d", count);
  return count;
}

size_t CountOccurrencesInAllIndexes(storage::DataTable *table, int first_val,
                                    int second_val) {
  size_t num_occurrences = 0;
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto primary_key = type::ValueFactory::GetIntegerValue(first_val);
  auto value = type::ValueFactory::GetIntegerValue(second_val);

  tuple->SetValue(0, primary_key, nullptr);
  tuple->SetValue(1, value, nullptr);

  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> current_key(
        new storage::Tuple(index_schema, true));
    current_key->SetFromTuple(tuple.get(), indexed_columns, index->GetPool());

    std::vector<ItemPointer *> index_entries;
    index->ScanKey(current_key.get(), index_entries);
    num_occurrences += index_entries.size();
  }
  return num_occurrences;
}

size_t CountOccurrencesInIndex(storage::DataTable *table, int idx,
                               int first_val, int second_val) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto primary_key = type::ValueFactory::GetIntegerValue(first_val);
  auto value = type::ValueFactory::GetIntegerValue(second_val);

  tuple->SetValue(0, primary_key, nullptr);
  tuple->SetValue(1, value, nullptr);

  auto index = table->GetIndex(idx);
  if (index == nullptr) return 0;
  auto index_schema = index->GetKeySchema();
  auto indexed_columns = index_schema->GetIndexedColumns();

  // build key.
  std::unique_ptr<storage::Tuple> current_key(
      new storage::Tuple(index_schema, true));
  current_key->SetFromTuple(tuple.get(), indexed_columns, index->GetPool());

  std::vector<ItemPointer *> index_entries;
  index->ScanKey(current_key.get(), index_entries);

  return index_entries.size();
}

//Test that compaction is triggered and successful for sparse tile groups
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
  auto insert_result = BulkInsertTuples(table.get(), num_inserts);
  EXPECT_EQ(ResultType::SUCCESS, insert_result);

  // capture num tile groups occupied
  size_t tile_group_count_after_insert = manager.GetNumLiveTileGroups();
  LOG_DEBUG("tile_group_count_after_insert: %zu", tile_group_count_after_insert);
  EXPECT_GT(tile_group_count_after_insert, tile_group_count_after_init);

  epoch_manager.SetCurrentEpochId(++current_eid);
  //===========================
  // delete the tuples all but 1 tuple, this will not allocate another tile group
  //===========================
  auto delete_result = BulkDeleteTuples(table.get(), num_inserts - 1);
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

//Basic functionality
//
//Test that compaction is triggered and successful for sparse tile groups
//    Fill up tile group with 10 tuples
//    Delete 9 of the tuples
//    Check that tile group is compacted
//    Ensure that tuples have the same values
//
//Test that compaction is NOT triggered for non-sparse tile groups
//    Fill up tile group with 10 tuples
//    Delete 5 of the tuples
//    Check that tile group is NOT compacted
//
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
//Concurrency tests
//
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
