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
  while (!gc::GCManagerFactory::GetInstance()
              .GetRecycledTupleSlot(table_id)
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

////////////////////////////////////////////
// NEW TESTS
////////////////////////////////////////////

// Scenario:  Abort Insert (due to other operation)
// Insert tuple
// Some other operation fails
// Abort
// Assert RQ size = 1
// Assert not present in indexes
TEST_F(TransactionLevelGCManagerTests, AbortInsertTest) {
  std::string test_name = "AbortInsert";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, then abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Abort();
  scheduler.Run();

  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 2, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Fail to insert a tuple
// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert or
// FK constraints) violated)
// Abort
// Assert RQ size = 1
// Assert old copy in 2 indexes
// Assert new copy in 0 indexes
TEST_F(TransactionLevelGCManagerTests, FailedInsertPrimaryKeyTest) {
  std::string test_name = "FailedInsertPrimaryKey";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert duplicate key (failure), try to commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 0);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Insert(0, 1);  // primary key already exists in table
  scheduler.Txn(1).Commit();
  scheduler.Run();

  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 0));
  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 0));

  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

//// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert
///or FK constraints) violated)
//// Fail to insert a tuple
//// Abort
//// Assert RQ size = 1
//// Assert old tuple in 2 indexes
//// Assert new tuple in 0 indexes
TEST_F(TransactionLevelGCManagerTests, FailedInsertSecondaryKeyTest) {
  std::string test_name = "FailedInsertSecondaryKey";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert duplicate value (secondary index requires uniqueness, so fails)
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);  // succeeds
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Insert(1, 1);  // fails, dup value
  scheduler.Txn(1).Commit();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 1));
  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 1));

  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 0, 1, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  COMMIT_UPDATE
// Insert tuple
// Commit
// Update tuple
// Commit
// Assert RQ size = 1
// Assert old version in 1 index (primary key)
// Assert new version in 2 indexes
TEST_F(TransactionLevelGCManagerTests, CommitUpdateSecondaryKeyTest) {
  std::string test_name = "CommitUpdateSecondaryKey";
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
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 5, 1));

  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 5, 2));
  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 5, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_UPDATE
// Insert tuple
// Commit
// Update tuple
// Abort
// Assert RQ size = 1
// Assert old version is in 2 indexes
// Assert new version is in 1 index (primary key)
TEST_F(TransactionLevelGCManagerTests, AbortUpdateSecondaryKeyTest) {
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);  // succeeds
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Update(0, 2);
  scheduler.Txn(1).Abort();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 1));
  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 1));

  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_INS_UPDATE (not a GC type)
// Insert tuple
// Update tuple
// Commit
// Assert RQ.size = 0
// Assert old tuple in 1 index (primary key)
// Assert new tuple in 2 indexes
TEST_F(TransactionLevelGCManagerTests, CommitInsertUpdateTest) {
  std::string test_name = "CommitInsertUpdate";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, update, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Update(0, 2);
  scheduler.Txn(0).Commit();
  scheduler.Run();

  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 1));

  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 2));
  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: ABORT_INS_UPDATE
// Insert tuple
// Update tuple
// Abort
// Assert RQ.size = 1 or 2?
// Assert inserted tuple in 0 indexes
// Assert updated tuple in 0 indexes
TEST_F(TransactionLevelGCManagerTests, AbortInsertUpdateTest) {
  std::string test_name = "AbortInsertUpdate";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, update, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Update(0, 2);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_DELETE
// Insert tuple
// Commit
// Delete tuple
// Commit
// Assert RQ size = 2
// Assert deleted tuple appears in 0 indexes
TEST_F(TransactionLevelGCManagerTests, CommitDeleteTest) {
  std::string test_name = "CommitDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, commit, delete, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Delete(0);
  scheduler.Txn(1).Commit();
  scheduler.Run();

  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_DELETE
// Insert tuple
// Commit
// Delete tuple
// Abort
// Assert RQ size = 1
// Assert tuple found in 2 indexes
TEST_F(TransactionLevelGCManagerTests, AbortDeleteTest) {
  std::string test_name = "AbortDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // delete, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Delete(0);
  scheduler.Txn(1).Abort();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table.get(), 0, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_INS_DEL
// Insert tuple
// Delete tuple
// Commit
// Assert RQ.size = 1
// Assert tuple found in 0 indexes
TEST_F(TransactionLevelGCManagerTests, CommitInsertDeleteTest) {
  std::string test_name = "CommitInsertDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, delete, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Delete(0);
  scheduler.Txn(0).Commit();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario:  ABORT_INS_DEL
// Insert tuple
// Delete tuple
// Abort
// Assert RQ size = 1
// Assert tuple found in 0 indexes
TEST_F(TransactionLevelGCManagerTests, AbortInsertDeleteTest) {
  std::string test_name = "AbortInsertDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // insert, delete, abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Delete(0);
  scheduler.Txn(0).Abort();
  scheduler.Run();
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: COMMIT_UPDATE_DEL
// Insert tuple
// Commit
// Update tuple
// Delete tuple
// Commit
// Assert RQ.size = 2
// Assert old tuple in 0 indexes
// Assert new tuple in 0 indexes
TEST_F(TransactionLevelGCManagerTests, CommitUpdateDeleteTest) {
  std::string test_name = "CommitUpdateDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, delete, commit
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Update(0, 2);
  scheduler.Txn(1).Delete(0);
  scheduler.Txn(1).Commit();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
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
// Assert old tuple in 2 indexes
// Assert new tuple in 1 index (primary key)
TEST_F(TransactionLevelGCManagerTests, AbortUpdateDeleteTest) {
  std::string test_name = "AbortUpdateDelete";
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

  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  // update, delete, then abort
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(2, table.get(), &txn_manager);
  scheduler.Txn(0).Insert(0, 1);
  scheduler.Txn(0).Commit();
  scheduler.Txn(1).Update(0, 2);
  scheduler.Txn(1).Delete(0);
  scheduler.Txn(1).Abort();
  scheduler.Run();
  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));

  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table.get(), 0, 1));
  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 2));

  table.release();
  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

// Scenario: Update Primary Key Test
// Insert tuple
// Commit
// Update primary key and value
// Commit
// Assert RQ.size = 2 (primary key update causes delete and insert)
// Assert old tuple in 0 indexes
// Assert new tuple in 2 indexes
TEST_F(TransactionLevelGCManagerTests, CommitUpdatePrimaryKeyTest) {
  uint64_t current_epoch = 0;
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(++current_epoch);
  std::vector<std::unique_ptr<std::thread>> gc_threads;
  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
  gc_manager.Reset();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");
  auto table = database->GetTableWithName("test");
  TestingTransactionUtil::AddSecondaryIndex(table);

  EXPECT_EQ(0, GetNumRecycledTuples(table));

  epoch_manager.SetCurrentEpochId(++current_epoch);

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 30);");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // confirm setup
  TestingSQLUtil::ExecuteSQLQuery("SELECT * from test WHERE b=30", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ('3', result[0][0]);
  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table, 3, 30));

  // Perform primary key and value update
  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a=5, b=40", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);

  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.ClearGarbage(0);

  // confirm update
  TestingSQLUtil::ExecuteSQLQuery("SELECT * from test WHERE b=40", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ('5', result[0][0]);

  EXPECT_EQ(2, GetNumRecycledTuples(table));
  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table, 3, 30));
  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table, 5, 40));

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  epoch_manager.SetCurrentEpochId(++current_epoch);
  gc_manager.StopGC();
  gc::GCManagerFactory::Configure(0);
}

//////////////////////////////////////////////////////
// OLD TESTS
/////////////////////////////////////////////////////

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
