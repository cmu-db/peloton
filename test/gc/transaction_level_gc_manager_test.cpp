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
  auto table_id = table->GetOid();
  while (!gc::GCManagerFactory::GetInstance().ReturnFreeSlot(table_id).IsNull())
    count++;

  LOG_INFO("recycled version num = %d", count);
  return count;
}

size_t CountNumIndexOccurrences(storage::DataTable *table, int first_val, int second_val) {

  size_t num_occurrences = 0;
  std::unique_ptr<storage::Tuple> aborted_tuple(new storage::Tuple(table->GetSchema(), true));
  auto primary_key = type::ValueFactory::GetIntegerValue(first_val);
  auto value = type::ValueFactory::GetIntegerValue(second_val);

  aborted_tuple->SetValue(0, primary_key, nullptr);
  aborted_tuple->SetValue(1, value, nullptr);

  // check that tuple was removed from indexes
  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> current_key(new storage::Tuple(index_schema, true));
    current_key->SetFromTuple(aborted_tuple.get(), indexed_columns,
                              index->GetPool());

    std::vector<ItemPointer *> index_entries;
    index->ScanKey(current_key.get(), index_entries);
    num_occurrences += index_entries.size();
  }
  return num_occurrences;
}

///////////////////////////////////////////////////////////////////////
// Scenarios
///////////////////////////////////////////////////////////////////////

// Scenario:  Abort Insert (due to other operation)
// Insert tuple
// Some other operation fails
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, AbortInsertTest) {
  // set up
  std::string test_name= "AbortInsert";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, then abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  auto delete_result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::ABORTED, delete_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(0, CountNumIndexOccurrences(table.get(), 2, 1));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}



// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert or FK constraints) violated)
// Fail to insert a tuple
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, FailedInsertPrimaryKeyTest) {
  // set up
  std::string test_name= "FailedInsertPrimaryKey";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert duplicate key (failure), try to commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1); // key already exists in table
  scheduler.Txn(0).Commit();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::ABORTED, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(1, CountNumIndexOccurrences(table.get(), 0, 1));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert or FK constraints) violated)
// Fail to insert a tuple
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, FailedInsertSecondaryKeyTest) {
  // set up
  std::string test_name= "FailedInsertSecondaryKey";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "Table", db_id, INVALID_OID, 1234, true));

  TestingTransactionUtil::AddSecondaryIndex(table.get());

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert duplicate value (secondary index requires uniqueness, so fails)
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1); // succeeds
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Insert(1, 1); // fails, dup value
  scheduler.Txn(1).Commit();
  scheduler.Run();
  auto result0 = scheduler.schedules[0].txn_result;
  auto result1 = scheduler.schedules[1].txn_result;
  EXPECT_EQ(ResultType::SUCCESS, result0);
  EXPECT_EQ(ResultType::ABORTED, result1);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(2, CountNumIndexOccurrences(table.get(), 0, 1));
  EXPECT_EQ(0, CountNumIndexOccurrences(table.get(), 1, 1));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  COMMIT_UPDATE
// Insert tuple
// Commit
// Update tuple
// Commit
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, CommitUpdateSecondaryKeyTest) {
  // set up
  std::string test_name= "CommitUpdateSecondaryKey";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "Table", db_id, INVALID_OID, 1234, true));

  TestingTransactionUtil::AddSecondaryIndex(table.get());

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, commit. update, commit.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(5, 1);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Update(5, 2);
  scheduler.Txn(1).Commit();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;
  EXPECT_EQ(ResultType::SUCCESS, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // old version should be gone from secondary index
  EXPECT_EQ(1, CountNumIndexOccurrences(table.get(), 5, 1));

  // new version should be present in 2 indexes
  EXPECT_EQ(2, CountNumIndexOccurrences(table.get(), 5, 2));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_UPDATE
// Insert tuple
// Commit
// Update tuple
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, AbortUpdateSecondaryKeyTest) {
  // set up
  std::string test_name= "AbortUpdateSecondaryKey";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      0, test_name + "Table", db_id, INVALID_OID, 1234, true));

  TestingTransactionUtil::AddSecondaryIndex(table.get());

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1); // succeeds
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Update(0, 2); // fails, dup value
  scheduler.Txn(1).Abort();
  scheduler.Run();
  auto result0 = scheduler.schedules[0].txn_result;
  auto result1 = scheduler.schedules[1].txn_result;
  EXPECT_EQ(ResultType::SUCCESS, result0);
  EXPECT_EQ(ResultType::ABORTED, result1);

  auto result = scheduler.schedules[0].txn_result;
  EXPECT_EQ(ResultType::ABORTED, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));


  // old version should be present in 2 indexes
  EXPECT_EQ(2, CountNumIndexOccurrences(table.get(), 0, 1));

  // new version should be present in primary index
  EXPECT_EQ(1, CountNumIndexOccurrences(table.get(), 0, 2));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

TEST_F(TransactionLevelGCManagerTests, CommitUpdatePrimaryKeyTest) {
  // set up
  std::string test_name= "CommitUpdatePrimaryKey";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, commit
  auto update_result = UpdateTuple(table.get(), 1);
  EXPECT_EQ(ResultType::SUCCESS, update_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_INS_UPDATE (not a GC type)
// Insert tuple
// Update tuple
// Commit
// Assert RQ.size = 0
TEST_F(TransactionLevelGCManagerTests, CommitInsertUpdateTest) {
  // set up
  std::string test_name= "CommitInsertUpdate";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, update, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(3, 1);
  scheduler.Txn(0).Update(3, 2);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  auto result = scheduler.schedules[0].txn_result;
  EXPECT_EQ(ResultType::SUCCESS, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: ABORT_INS_UPDATE
// Insert tuple
// Update tuple
// Abort
// Assert RQ.size = 1 or 2?
TEST_F(TransactionLevelGCManagerTests, AbortInsertUpdateTest) {
  // set up
  std::string test_name= "AbortInsertUpdate";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, update, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(3, 1);
  scheduler.Txn(0).Update(3, 2);
  scheduler.Txn(0).Abort();
  scheduler.Run();

  auto result = scheduler.schedules[0].txn_result;
  EXPECT_EQ(ResultType::ABORTED, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  COMMIT_DELETE
// Insert tuple
// Commit
// Delete tuple
// Commit
// Assert RQ size = 2
TEST_F(TransactionLevelGCManagerTests, CommitDeleteTest) {
  // set up
  std::string test_name= "CommitDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // delete, commit
  auto delete_result = DeleteTuple(table.get(), 1);
  EXPECT_EQ(ResultType::SUCCESS, delete_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // expect 2 slots reclaimed
  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));

  // create tuple (2, 1);
  std::unique_ptr<storage::Tuple> aborted_tuple(new storage::Tuple(table->GetSchema(), true));
  auto primary_key = type::ValueFactory::GetIntegerValue(1);
  auto value = type::ValueFactory::GetIntegerValue(1);

  aborted_tuple->SetValue(0, primary_key, nullptr);
  aborted_tuple->SetValue(1, value, nullptr);

  // check that tuple was removed from indexes
  for (size_t idx = 0; idx < table.get()->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> current_key(new storage::Tuple(index_schema, true));
    current_key->SetFromTuple(aborted_tuple.get(), indexed_columns,
                              index->GetPool());

    std::vector<ItemPointer *> result;
    index->ScanKey(current_key.get(), result);
    EXPECT_EQ(0, result.size());
  }

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_DELETE
// Insert tuple
// Commit
// Delete tuple
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, AbortDeleteTest) {
  // set up
  std::string test_name= "AbortDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // delete, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Delete(1);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  auto delete_result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::ABORTED, delete_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_INS_DEL
// Insert tuple
// Delete tuple
// Commit
// Assert RQ.size = 1
TEST_F(TransactionLevelGCManagerTests, CommitInsertDeleteTest) {
  // set up
  std::string test_name= "CommitInsertDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, delete, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(3, 1);
  scheduler.Txn(0).Delete(3);
  scheduler.Txn(0).Commit();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::SUCCESS, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_INS_DEL
// Insert tuple
// Delete tuple
// Abort
// Assert RQ size = 1
TEST_F(TransactionLevelGCManagerTests, AbortInsertDeleteTest) {
  // set up
  std::string test_name= "AbortInsertDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, delete, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(3, 1);
  scheduler.Txn(0).Delete(3);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::ABORTED, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

//Scenario: COMMIT_UPDATE_DEL
// Insert tuple
// Commit
// Update tuple
// Delete tuple
// Commit
// Assert RQ.size = 2
TEST_F(TransactionLevelGCManagerTests, CommitUpdateDeleteTest) {
  // set up
  std::string test_name= "CommitUpdateDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, delete, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Update(1, 3);
  scheduler.Txn(0).Delete(1);
  scheduler.Txn(0).Commit();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::SUCCESS, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));

  // delete database,
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: ABORT_UPDATE_DEL
// Insert tuple
// Commit
// Update tuple
// Delete tuple
// Abort
// Assert RQ size = 2
TEST_F(TransactionLevelGCManagerTests, AbortUpdateDeleteTest) {
  // set up
  std::string test_name= "AbortUpdateDelete";
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      2, test_name + "Table", db_id, INVALID_OID, 1234, true));

  // expect no garbage initially
  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, delete, then abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Update(1, 3);
  scheduler.Txn(0).Delete(1);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  auto result = scheduler.schedules[0].txn_result;

  EXPECT_EQ(ResultType::ABORTED, result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));

  // delete database
  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);

  // clean up garbage after database deleted
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
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
  auto database = TestingExecutorUtil::InitializeDatabase("database0");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 1;
  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
      num_key, "TABLE0", db_id, INVALID_OID, 1234, true));

  EXPECT_EQ(1, gc_manager.GetTableCount());

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
  TestingExecutorUtil::DeleteDatabase("database0");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("database0", txn),
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
  auto database = TestingExecutorUtil::InitializeDatabase("database1");
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
  TestingExecutorUtil::DeleteDatabase("database1");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("database1", txn),
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
  auto database = TestingExecutorUtil::InitializeDatabase("immutabilitydb");
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
  TestingExecutorUtil::DeleteDatabase("immutabilitydb");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseObject("immutabilitydb", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
