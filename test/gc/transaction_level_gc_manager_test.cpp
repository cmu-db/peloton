////===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// transaction_level_gc_manager_test.cpp
////
//// Identification: test/gc/transaction_level_gc_manager_test.cpp
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//#include <sql/testing_sql_util.h>
//#include <com_err.h>
//#include "concurrency/testing_transaction_util.h"
//#include "executor/testing_executor_util.h"
//#include "common/harness.h"
//#include "gc/transaction_level_gc_manager.h"
//#include "concurrency/epoch_manager.h"
//
//#include "catalog/catalog.h"
//#include "storage/data_table.h"
//#include "storage/tile_group.h"
//#include "storage/database.h"
//#include "storage/storage_manager.h"
//
//namespace peloton {
//
//namespace test {
//
////===--------------------------------------------------------------------===//
//// TransactionContext-Level GC Manager Tests
////===--------------------------------------------------------------------===//
//
//class TransactionLevelGCManagerTests : public PelotonTest {};
//
//ResultType UpdateTuple(storage::DataTable *table, const int key) {
//  srand(15721);
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  scheduler.Txn(0).Update(key, rand() % 15721);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  return scheduler.schedules[0].txn_result;
//}
//
//ResultType InsertTuple(storage::DataTable *table, const int key) {
//  srand(15721);
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  scheduler.Txn(0).Insert(key, rand() % 15721);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  return scheduler.schedules[0].txn_result;
//}
//
//ResultType BulkInsertTuples(storage::DataTable *table, const size_t num_tuples) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  for (size_t i=1; i <= num_tuples; i++) {
//    scheduler.Txn(0).Insert(i, i);
//  }
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  return scheduler.schedules[0].txn_result;
//
//
//  // Insert tuple
//  //  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  //  auto txn = txn_manager.BeginTransaction();
//  //  for (size_t i = 0; i < num_tuples; i++) {
//  //    TestingTransactionUtil::ExecuteInsert(txn, table, i, 0);
//  //  }
//  //  return txn_manager.CommitTransaction(txn);
//}
//
//ResultType BulkDeleteTuples(storage::DataTable *table, const size_t num_tuples) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  for (size_t i=1; i <= num_tuples; i++) {
//    scheduler.Txn(0).Delete(i, false);
//  }
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  return scheduler.schedules[0].txn_result;
//}
//
//ResultType DeleteTuple(storage::DataTable *table, const int key) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  scheduler.Txn(0).Delete(key);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  return scheduler.schedules[0].txn_result;
//}
//
//ResultType SelectTuple(storage::DataTable *table, const int key,
//                       std::vector<int> &results) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table, &txn_manager);
//  scheduler.Txn(0).Read(key);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  results = scheduler.schedules[0].results;
//
//  return scheduler.schedules[0].txn_result;
//}
//
//int GetNumRecycledTuples(storage::DataTable *table) {
//  int count = 0;
//  auto table_id = table->GetOid();
//  while (!gc::GCManagerFactory::GetInstance()
//              .GetRecycledTupleSlot(table_id)
//              .IsNull())
//    count++;
//
//  LOG_INFO("recycled version num = %d", count);
//  return count;
//}
//
//size_t CountOccurrencesInAllIndexes(storage::DataTable *table, int first_val,
//                                    int second_val) {
//  size_t num_occurrences = 0;
//  std::unique_ptr<storage::Tuple> tuple(
//      new storage::Tuple(table->GetSchema(), true));
//  auto primary_key = type::ValueFactory::GetIntegerValue(first_val);
//  auto value = type::ValueFactory::GetIntegerValue(second_val);
//
//  tuple->SetValue(0, primary_key, nullptr);
//  tuple->SetValue(1, value, nullptr);
//
//  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
//    auto index = table->GetIndex(idx);
//    if (index == nullptr) continue;
//    auto index_schema = index->GetKeySchema();
//    auto indexed_columns = index_schema->GetIndexedColumns();
//
//    // build key.
//    std::unique_ptr<storage::Tuple> current_key(
//        new storage::Tuple(index_schema, true));
//    current_key->SetFromTuple(tuple.get(), indexed_columns, index->GetPool());
//
//    std::vector<ItemPointer *> index_entries;
//    index->ScanKey(current_key.get(), index_entries);
//    num_occurrences += index_entries.size();
//  }
//  return num_occurrences;
//}
//
//size_t CountOccurrencesInIndex(storage::DataTable *table, int idx,
//                               int first_val, int second_val) {
//  std::unique_ptr<storage::Tuple> tuple(
//      new storage::Tuple(table->GetSchema(), true));
//  auto primary_key = type::ValueFactory::GetIntegerValue(first_val);
//  auto value = type::ValueFactory::GetIntegerValue(second_val);
//
//  tuple->SetValue(0, primary_key, nullptr);
//  tuple->SetValue(1, value, nullptr);
//
//  auto index = table->GetIndex(idx);
//  if (index == nullptr) return 0;
//  auto index_schema = index->GetKeySchema();
//  auto indexed_columns = index_schema->GetIndexedColumns();
//
//  // build key.
//  std::unique_ptr<storage::Tuple> current_key(
//      new storage::Tuple(index_schema, true));
//  current_key->SetFromTuple(tuple.get(), indexed_columns, index->GetPool());
//
//  std::vector<ItemPointer *> index_entries;
//  index->ScanKey(current_key.get(), index_entries);
//
//  return index_entries.size();
//}
//
//////////////////////////////////////////////
//// NEW TESTS
//////////////////////////////////////////////
//
//// Scenario:  Abort Insert (due to other operation)
//// Insert tuple
//// Some other operation fails
//// Abort
//// Assert RQ size = 1
//// Assert not present in indexes
//TEST_F(TransactionLevelGCManagerTests, AbortInsertTest) {
//  std::string test_name = "AbortInsert";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, then abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Abort();
//  scheduler.Run();
//
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 2, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Fail to insert a tuple
//// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert or
//// FK constraints) violated)
//// Abort
//// Assert RQ size = 1
//// Assert old copy in 2 indexes
//// Assert new copy in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, FailedInsertPrimaryKeyTest) {
//  std::string test_name = "FailedInsertPrimaryKey";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert duplicate key (failure), try to commit
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 0);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Insert(0, 1);  // primary key already exists in table
//  scheduler.Txn(1).Commit();
//  scheduler.Run();
//
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
////  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 0));
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 0));
//
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
////// Scenario:  Failed Insert (due to insert failure (e.g. index rejects insert
///// or FK constraints) violated)
////// Fail to insert a tuple
////// Abort
////// Assert RQ size = 1
////// Assert old tuple in 2 indexes
////// Assert new tuple in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, FailedInsertSecondaryKeyTest) {
//  std::string test_name = "FailedInsertSecondaryKey";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert duplicate value (secondary index requires uniqueness, so fails)
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);  // succeeds
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Insert(1, 1);  // fails, dup value
//  scheduler.Txn(1).Commit();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 1));
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 1));
//
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 0, 1, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
////// Scenario:  COMMIT_UPDATE
////// Insert tuple
////// Commit
////// Update tuple
////// Commit
////// Assert RQ size = 1
////// Assert old version in 1 index (primary key)
////// Assert new version in 2 indexes
//TEST_F(TransactionLevelGCManagerTests, CommitUpdateSecondaryKeyTest) {
//  std::string test_name = "CommitUpdateSecondaryKey";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, commit. update, commit.
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(5, 1);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Update(5, 2);
//  scheduler.Txn(1).Commit();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 5, 1));
//
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 5, 2));
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 5, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario:  ABORT_UPDATE
//// Insert tuple
//// Commit
//// Update tuple
//// Abort
//// Assert RQ size = 1
//// Assert old version is in 2 indexes
//// Assert new version is in 1 index (primary key)
//TEST_F(TransactionLevelGCManagerTests, AbortUpAdateSecondaryKeyTest) {
//  std::string test_name = "AbortUpdateSecondaryKey";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // update, abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);  // succeeds
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Update(0, 2);
//  scheduler.Txn(1).Abort();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 1));
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 1));
//
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: COMMIT_INS_UPDATE (not a GC type)
//// Insert tuple
//// Update tuple
//// Commit
//// Assert RQ.size = 0
//// Assert old tuple in 1 index (primary key)
//// Assert new tuple in 2 indexes
//TEST_F(TransactionLevelGCManagerTests, DISABLED_CommitInsertUpdateTest) {
//  std::string test_name = "CommitInsertUpdate";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, update, commit
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Update(0, 2);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 1));
//
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 0, 0, 2));
//  EXPECT_EQ(1, CountOccurrencesInIndex(table.get(), 1, 0, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: ABORT_INS_UPDATE
//// Insert tuple
//// Update tuple
//// Abort
//// Assert RQ.size = 1 or 2?
//// Assert inserted tuple in 0 indexes
//// Assert updated tuple in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, DISABLED_AbortInsertUpdateTest) {
//  std::string test_name = "AbortInsertUpdate";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, update, abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Update(0, 2);
//  scheduler.Txn(0).Abort();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: COMMIT_DELETE
//// Insert tuple
//// Commit
//// Delete tuple
//// Commit
//// Assert RQ size = 2
//// Assert deleted tuple appears in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, CommitDeleteTest) {
//  std::string test_name = "CommitDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, commit, delete, commit
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Delete(0);
//  scheduler.Txn(1).Commit();
//  scheduler.Run();
//
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario:  ABORT_DELETE
//// Insert tuple
//// Commit
//// Delete tuple
//// Abort
//// Assert RQ size = 1
//// Assert tuple found in 2 indexes
//TEST_F(TransactionLevelGCManagerTests, AbortDeleteTest) {
//  std::string test_name = "AbortDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // delete, abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Delete(0);
//  scheduler.Txn(1).Abort();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: COMMIT_INS_DEL
//// Insert tuple
//// Delete tuple
//// Commit
//// Assert RQ.size = 1
//// Assert tuple found in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, CommitInsertDeleteTest) {
//  std::string test_name = "CommitInsertDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, delete, commit
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Delete(0);
//  scheduler.Txn(0).Commit();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario:  ABORT_INS_DEL
//// Insert tuple
//// Delete tuple
//// Abort
//// Assert RQ size = 1
//// Assert tuple found in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, AbortInsertDeleteTest) {
//  std::string test_name = "AbortInsertDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // insert, delete, abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(1, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Delete(0);
//  scheduler.Txn(0).Abort();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: COMMIT_UPDATE_DEL
//// Insert tuple
//// Commit
//// Update tuple
//// Delete tuple
//// Commit
//// Assert RQ.size = 2
//// Assert old tuple in 0 indexes
//// Assert new tuple in 0 indexes
//TEST_F(TransactionLevelGCManagerTests, DISABLED_CommitUpdateDeleteTest) {
//  std::string test_name = "CommitUpdateDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // update, delete, commit
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Update(0, 2);
//  scheduler.Txn(1).Delete(0);
//  scheduler.Txn(1).Commit();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(2, GetNumRecycledTuples(table.get()));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table.get(), 0, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: ABORT_UPDATE_DEL
//// Insert tuple
//// Commit
//// Update tuple
//// Delete tuple
//// Abort
//// Assert RQ size = 2
//// Assert old tuple in 2 indexes
//// Assert new tuple in 1 index (primary key)
//TEST_F(TransactionLevelGCManagerTests, DISABLED_AbortUpdateDeleteTest) {
//  std::string test_name = "AbortUpdateDelete";
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase(test_name + "DB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      0, test_name + "Table", db_id, INVALID_OID, 1234, true));
//  TestingTransactionUtil::AddSecondaryIndex(table.get());
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table.get()));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  // update, delete, then abort
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  TransactionScheduler scheduler(2, table.get(), &txn_manager);
//  scheduler.Txn(0).Insert(0, 1);
//  scheduler.Txn(0).Commit();
//  scheduler.Txn(1).Update(0, 2);
//  scheduler.Txn(1).Delete(0);
//  scheduler.Txn(1).Abort();
//  scheduler.Run();
//  EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
//  EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  EXPECT_EQ(1, GetNumRecycledTuples(table.get()));
//
//  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table.get(), 0, 1));
//  EXPECT_EQ(0, CountOccurrencesInIndex(table.get(), 1, 0, 2));
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase(test_name + "DB");
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//// Scenario: Update Primary Key Test
//// Insert tuple
//// Commit
//// Update primary key and value
//// Commit
//// Assert RQ.size = 2 (primary key update causes delete and insert)
//// Assert old tuple in 0 indexes
//// Assert new tuple in 2 indexes
//TEST_F(TransactionLevelGCManagerTests, CommitUpdatePrimaryKeyTest) {
//  uint64_t current_epoch = 0;
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(++current_epoch);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  auto catalog = catalog::Catalog::GetInstance();
//  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
//  txn_manager.CommitTransaction(txn);
//  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);
//
//  TestingSQLUtil::ExecuteSQLQuery(
//      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");
//  auto table = database->GetTable(0);
//  TestingTransactionUtil::AddSecondaryIndex(table);
//
//  EXPECT_EQ(0, GetNumRecycledTuples(table));
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 30);");
//
//  std::vector<ResultValue> result;
//  std::vector<FieldInfo> tuple_descriptor;
//  std::string error_message;
//  int rows_affected;
//
//  // confirm setup
//  TestingSQLUtil::ExecuteSQLQuery("SELECT * from test WHERE b=30", result,
//                                  tuple_descriptor, rows_affected,
//                                  error_message);
//  EXPECT_EQ('3', result[0][0]);
//  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table, 3, 30));
//
//  // Perform primary key and value update
//  TestingSQLUtil::ExecuteSQLQuery("UPDATE test SET a=5, b=40", result,
//                                  tuple_descriptor, rows_affected,
//                                  error_message);
//
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.ClearGarbage(0);
//
//  // confirm update
//  TestingSQLUtil::ExecuteSQLQuery("SELECT * from test WHERE b=40", result,
//                                  tuple_descriptor, rows_affected,
//                                  error_message);
//  EXPECT_EQ('5', result[0][0]);
//
//  EXPECT_EQ(2, GetNumRecycledTuples(table));
//  EXPECT_EQ(0, CountOccurrencesInAllIndexes(table, 3, 30));
//  EXPECT_EQ(2, CountOccurrencesInAllIndexes(table, 5, 40));
//
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
//  txn_manager.CommitTransaction(txn);
//  epoch_manager.SetCurrentEpochId(++current_epoch);
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//}
//
//////////////////////////////////////////////////////////
////// OLD TESTS
/////////////////////////////////////////////////////////
//
//// update -> delete
//TEST_F(TransactionLevelGCManagerTests, UpdateDeleteTest) {
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(1);
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  auto storage_manager = storage::StorageManager::GetInstance();
//  // create database
//  auto database = TestingExecutorUtil::InitializeDatabase("DATABASE0");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  // create a table with only one key
//  const int num_key = 1;
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      num_key, "TABLE0", db_id, INVALID_OID, 1234, true));
//
//  EXPECT_EQ(1, gc_manager.GetTableCount());
//
//  //===========================
//  // update a version here.
//  //===========================
//  auto ret = UpdateTuple(table.get(), 0);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//
//  epoch_manager.SetCurrentEpochId(2);
//
//  // get expired epoch id.
//  // as the current epoch id is set to 2,
//  // the expected expired epoch id should be 1.
//  auto expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(1, expired_eid);
//
//  auto current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(2, current_eid);
//
//  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  auto unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(1, unlinked_count);
//
//  epoch_manager.SetCurrentEpochId(3);
//
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(2, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(3, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(1, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  //===========================
//  // delete a version here.
//  //===========================
//  ret = DeleteTuple(table.get(), 0);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//
//  epoch_manager.SetCurrentEpochId(4);
//
//  // get expired epoch id.
//  // as the current epoch id is set to 4,
//  // the expected expired epoch id should be 3.
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(3, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(4, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(1, unlinked_count);
//
//  epoch_manager.SetCurrentEpochId(5);
//
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(4, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(5, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(1, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//
//  table.release();
//
//  // DROP!
//  TestingExecutorUtil::DeleteDatabase("DATABASE0");
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  EXPECT_THROW(
//      catalog::Catalog::GetInstance()->GetDatabaseObject("DATABASE0", txn),
//      CatalogException);
//  txn_manager.CommitTransaction(txn);
//  // EXPECT_FALSE(storage_manager->HasDatabase(db_id));
//}
//
//// insert -> delete -> insert
//TEST_F(TransactionLevelGCManagerTests, ReInsertTest) {
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(1);
//
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//
//  auto storage_manager = storage::StorageManager::GetInstance();
//  // create database
//  auto database = TestingExecutorUtil::InitializeDatabase("DATABASE1");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  // create a table with only one key
//  const int num_key = 1;
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      num_key, "TABLE1", db_id, INVALID_OID, 1234, true));
//
//  EXPECT_TRUE(gc_manager.GetTableCount() == 1);
//
//  //===========================
//  // insert a tuple here.
//  //===========================
//  auto ret = InsertTuple(table.get(), 100);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//
//  epoch_manager.SetCurrentEpochId(2);
//
//  // get expired epoch id.
//  // as the current epoch id is set to 2,
//  // the expected expired epoch id should be 1.
//  auto expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(1, expired_eid);
//
//  auto current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(2, current_eid);
//
//  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  auto unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  epoch_manager.SetCurrentEpochId(3);
//
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(2, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(3, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  //===========================
//  // select the tuple.
//  //===========================
//  std::vector<int> results;
//
//  results.clear();
//  ret = SelectTuple(table.get(), 100, results);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//  EXPECT_TRUE(results[0] != -1);
//
//  //===========================
//  // delete the tuple.
//  //===========================
//  ret = DeleteTuple(table.get(), 100);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//
//  epoch_manager.SetCurrentEpochId(4);
//
//  // get expired epoch id.
//  // as the current epoch id is set to 4,
//  // the expected expired epoch id should be 3.
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(3, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(4, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(1, unlinked_count);
//
//  epoch_manager.SetCurrentEpochId(5);
//
//  expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(4, expired_eid);
//
//  current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(5, current_eid);
//
//  reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(1, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  //===========================
//  // select the tuple.
//  //===========================
//  results.clear();
//  ret = SelectTuple(table.get(), 100, results);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//  EXPECT_TRUE(results[0] == -1);
//
//  //===========================
//  // insert the tuple again.
//  //===========================
//  ret = InsertTuple(table.get(), 100);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//
//  //===========================
//  // select the tuple.
//  //===========================
//  results.clear();
//  ret = SelectTuple(table.get(), 100, results);
//  EXPECT_TRUE(ret == ResultType::SUCCESS);
//  EXPECT_TRUE(results[0] != -1);
//
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//
//  table.release();
//
//  // DROP!
//  TestingExecutorUtil::DeleteDatabase("DATABASE1");
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  EXPECT_THROW(
//      catalog::Catalog::GetInstance()->GetDatabaseObject("DATABASE1", txn),
//      CatalogException);
//  txn_manager.CommitTransaction(txn);
//  // EXPECT_FALSE(storage_manager->HasDatabase(db_id));
//}
//
//// TODO: add an immutability test back in, old one was not valid because it
//// modified
//// a TileGroup that was supposed to be immutable.
//
//// check mem -> insert 100k -> check mem -> delete all -> check mem
//TEST_F(TransactionLevelGCManagerTests, FreeTileGroupsTest) {
//
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(1);
//
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//
//  auto storage_manager = storage::StorageManager::GetInstance();
//  // create database
//  auto database = TestingExecutorUtil::InitializeDatabase("FreeTileGroupsDB");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//  // create a table with only one key
//  const int num_key = 0;
//  size_t tuples_per_tilegroup = 2;
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable(
//      num_key, "TABLE1", db_id, INVALID_OID, 1234, true, tuples_per_tilegroup));
//
//  auto &manager = catalog::Manager::GetInstance();
//  size_t tile_group_count_after_init = manager.GetNumLiveTileGroups();
//  LOG_DEBUG("tile_group_count_after_init: %zu\n", tile_group_count_after_init);
//
//  auto current_eid = epoch_manager.GetCurrentEpochId();
//
//  //  int round = 1;
//  for(int round = 1; round <= 3; round++) {
//
//    LOG_DEBUG("Round: %d\n", round);
//
//    epoch_manager.SetCurrentEpochId(++current_eid);
//    //===========================
//    // insert tuples here.
//    //===========================
//    size_t num_inserts = 100;
//    auto insert_result = BulkInsertTuples(table.get(), num_inserts);
//    EXPECT_EQ(ResultType::SUCCESS, insert_result);
//
//    // capture memory usage
//    size_t tile_group_count_after_insert = manager.GetNumLiveTileGroups();
//    LOG_DEBUG("Round %d: tile_group_count_after_insert: %zu", round, tile_group_count_after_insert);
//
//    epoch_manager.SetCurrentEpochId(++current_eid);
//    //===========================
//    // delete the tuples.
//    //===========================
//    auto delete_result = BulkDeleteTuples(table.get(), num_inserts);
//    EXPECT_EQ(ResultType::SUCCESS, delete_result);
//
//    size_t tile_group_count_after_delete = manager.GetNumLiveTileGroups();
//    LOG_DEBUG("Round %d: tile_group_count_after_delete: %zu", round, tile_group_count_after_delete);
//
//    epoch_manager.SetCurrentEpochId(++current_eid);
//
//    gc_manager.ClearGarbage(0);
//
//    size_t tile_group_count_after_gc = manager.GetNumLiveTileGroups();
//    LOG_DEBUG("Round %d: tile_group_count_after_gc: %zu", round, tile_group_count_after_gc);
//    EXPECT_LT(tile_group_count_after_gc, tile_group_count_after_init + 1);
//  }
//
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//
//  table.release();
//
//  // DROP!
//  TestingExecutorUtil::DeleteDatabase("FreeTileGroupsDB");
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  EXPECT_THROW(
//      catalog::Catalog::GetInstance()->GetDatabaseObject("FreeTileGroupsDB", txn),
//      CatalogException);
//  txn_manager.CommitTransaction(txn);
//}
//
////// Insert a tuple, delete that tuple. Insert 2 tuples. Recycling should make it such that
////// the next_free_slot in the tile_group_header did not increase
//TEST_F(TransactionLevelGCManagerTests, InsertDeleteInsertX2) {
//
//  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
//  epoch_manager.Reset(1);
//
//  std::vector<std::unique_ptr<std::thread>> gc_threads;
//
//  gc::GCManagerFactory::Configure(1);
//  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();
//  gc_manager.Reset();
//
//  auto storage_manager = storage::StorageManager::GetInstance();
//  auto database = TestingExecutorUtil::InitializeDatabase("InsertDeleteInsertX2");
//  oid_t db_id = database->GetOid();
//  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
//
//
//  std::unique_ptr<storage::DataTable> table(TestingTransactionUtil::CreateTable());
//
////  auto &manager = catalog::Manager::GetInstance();
//
//  auto tile_group = table->GetTileGroup(0);
//  auto tile_group_header = tile_group->GetHeader();
//
//  size_t current_next_tuple_slot_after_init = tile_group_header->GetCurrentNextTupleSlot();
//  LOG_DEBUG("current_next_tuple_slot_after_init: %zu\n", current_next_tuple_slot_after_init);
//
//
//  epoch_manager.SetCurrentEpochId(2);
//
//  // get expired epoch id.
//  // as the current epoch id is set to 2,
//  // the expected expired epoch id should be 1.
//  auto expired_eid = epoch_manager.GetExpiredEpochId();
//
//  EXPECT_EQ(1, expired_eid);
//
//  auto current_eid = epoch_manager.GetCurrentEpochId();
//
//  EXPECT_EQ(2, current_eid);
//
//  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//  auto unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  EXPECT_EQ(0, reclaimed_count);
//
//  EXPECT_EQ(0, unlinked_count);
//
//  //===========================
//  // delete the tuples.
//  //===========================
//  auto delete_result = DeleteTuple(table.get(), 1);
//  EXPECT_EQ(ResultType::SUCCESS, delete_result);
//
//  size_t current_next_tuple_slot_after_delete = tile_group_header->GetCurrentNextTupleSlot();
//  LOG_DEBUG("current_next_tuple_slot_after_delete: %zu\n", current_next_tuple_slot_after_delete);
//  EXPECT_EQ(current_next_tuple_slot_after_init + 1, current_next_tuple_slot_after_delete);
//
//  do {
//    epoch_manager.SetCurrentEpochId(++current_eid);
//
//    expired_eid = epoch_manager.GetExpiredEpochId();
//    current_eid = epoch_manager.GetCurrentEpochId();
//
//    EXPECT_EQ(expired_eid, current_eid - 1);
//
//    reclaimed_count = gc_manager.Reclaim(0, expired_eid);
//
//    unlinked_count = gc_manager.Unlink(0, expired_eid);
//
//  } while (reclaimed_count || unlinked_count);
//
//  size_t current_next_tuple_slot_after_gc = tile_group_header->GetCurrentNextTupleSlot();
//  LOG_DEBUG("current_next_tuple_slot_after_gc: %zu\n", current_next_tuple_slot_after_gc);
//  EXPECT_EQ(current_next_tuple_slot_after_delete, current_next_tuple_slot_after_gc);
//
//
//  auto insert_result = InsertTuple(table.get(), 15721);
//  EXPECT_EQ(ResultType::SUCCESS, insert_result);
//
//  insert_result = InsertTuple(table.get(), 6288);
//  EXPECT_EQ(ResultType::SUCCESS, insert_result);
//
//  size_t current_next_tuple_slot_after_insert = tile_group_header->GetCurrentNextTupleSlot();
//  LOG_DEBUG("current_next_tuple_slot_after_insert: %zu\n", current_next_tuple_slot_after_insert);
//  EXPECT_EQ(current_next_tuple_slot_after_delete, current_next_tuple_slot_after_insert);
//
//  gc_manager.StopGC();
//  gc::GCManagerFactory::Configure(0);
//
//  table.release();
//  TestingExecutorUtil::DeleteDatabase("InsertDeleteInsertX2");
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  EXPECT_THROW(
//      catalog::Catalog::GetInstance()->GetDatabaseObject("InsertDeleteInsertX2", txn),
//      CatalogException);
//  txn_manager.CommitTransaction(txn);
//}
//
//}  // namespace test
//}  // namespace peloton
