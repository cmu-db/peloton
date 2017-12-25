//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_util.h
//
// Identification: test/index/testing_art_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/testing_art_util.h"

#include "gtest/gtest.h"

#include "common/harness.h"
#include "catalog/catalog.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "index/index.h"
#include "index/index_util.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "index/scan_optimizer.h"
#include "storage/table_factory.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/testing_executor_util.h"
#include "common/timer.h"
#include <chrono>
#include <thread>

namespace peloton {
namespace test {

bool TestingArtUtil::map_populated = false;
std::map<index::TID, index::ARTKey *> TestingArtUtil::value_to_key;
std::array<TestingArtUtil::KeyAndValues, 10000> TestingArtUtil::key_to_values;

void loadKeyForTest(index::TID tid, index::ARTKey &key,
                    UNUSED_ATTRIBUTE index::IndexMetadata *metadata) {
  index::MultiValues *value_list = reinterpret_cast<index::MultiValues *>(tid);
  if (TestingArtUtil::value_to_key.find(value_list->tid) !=
      TestingArtUtil::value_to_key.end()) {
    index::ARTKey *key_p = TestingArtUtil::value_to_key.at(value_list->tid);
    key.setKeyLen(key_p->getKeyLen());
    key.set((const char *)(key_p->data), key.getKeyLen());
    return;
  }
  key = 0;
}

void TestingArtUtil::BasicTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  catalog::Schema *tuple_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::vector<oid_t> key_attrs = {0};
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);

  bool unique = false;
  index::IndexMetadata *index_metadata =
      new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                               IndexType::ART, IndexConstraintType::DEFAULT,
                               tuple_schema, key_schema, key_attrs, unique);

  index::ArtIndex art_index(index_metadata, loadKeyForTest);

  if (!map_populated) {
    PopulateMap(art_index);
  }

  art_index.InsertEntry(key_to_values[0].tuple,
                        (ItemPointer *)key_to_values[0].values[0]);
  art_index.InsertEntry(key_to_values[1].tuple,
                        (ItemPointer *)key_to_values[1].values[0]);

  std::vector<ItemPointer *> result;
  art_index.ScanAllKeys(result);
  EXPECT_EQ(2, result.size());

  result.clear();
  art_index.ScanKey(key_to_values[0].tuple, result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t)key_to_values[0].values[0], (uint64_t)result[0]);

  result.clear();
  art_index.ScanKey(key_to_values[1].tuple, result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t)key_to_values[1].values[0], (uint64_t)result[0]);

  art_index.DeleteEntry(key_to_values[0].tuple,
                        (ItemPointer *)key_to_values[0].values[0]);

  result.clear();
  art_index.ScanAllKeys(result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t)key_to_values[1].values[0], (uint64_t)result[0]);

  delete tuple_schema;
}

void TestingArtUtil::NonUniqueKeyDeleteTest(UNUSED_ATTRIBUTE const IndexType
                                                index_type) {
  catalog::Schema *tuple_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::vector<oid_t> key_attrs = {0};
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);

  bool unique = false;
  index::IndexMetadata *index_metadata =
      new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                               IndexType::ART, IndexConstraintType::DEFAULT,
                               tuple_schema, key_schema, key_attrs, unique);

  index::ArtIndex art_index(index_metadata, loadKeyForTest);

  if (!map_populated) {
    PopulateMap(art_index);
  }

  int num_rows = 10;
  LaunchParallelTest(2, TestingArtUtil::InsertHelperMicroBench, &art_index,
                     num_rows);

  std::vector<ItemPointer *> result;
  for (int i = 0; i < num_rows; i++) {
    result.clear();
    art_index.ScanKey(key_to_values[i].tuple, result);
    EXPECT_EQ(2, result.size());
  }

  LaunchParallelTest(1, TestingArtUtil::DeleteHelperMicroBench, &art_index,
                     num_rows);

  for (int i = 0; i < num_rows; i++) {
    result.clear();
    art_index.ScanKey(key_to_values[i].tuple, result);
    EXPECT_EQ(1, result.size());
  }

  delete tuple_schema;
}

void TestingArtUtil::MultiThreadedInsertTest(UNUSED_ATTRIBUTE const IndexType
                                                 index_type) {
  catalog::Schema *tuple_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::vector<oid_t> key_attrs = {0};
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);

  bool unique = false;
  index::IndexMetadata *index_metadata =
      new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                               IndexType::ART, IndexConstraintType::DEFAULT,
                               tuple_schema, key_schema, key_attrs, unique);

  index::ArtIndex art_index(index_metadata, loadKeyForTest);

  if (!map_populated) {
    PopulateMap(art_index);
  }

  int num_rows = 100;
  size_t num_threads = 16;

  Timer<> timer;
  timer.Start();
  LaunchParallelTest(num_threads, TestingArtUtil::InsertHelperMicroBench,
                     &art_index, num_rows);
  timer.Stop();
  LOG_INFO("1,600 tuples inserted in %.5lfs", timer.GetDuration());

  std::vector<ItemPointer *> result;
  art_index.ScanAllKeys(result);
  EXPECT_EQ(num_rows * num_threads, result.size());

  int test_key_index = std::rand() % num_rows;
  result.clear();
  art_index.ScanKey(key_to_values[test_key_index].tuple, result);
  EXPECT_EQ(16, result.size());

  for (int i = 0; i < 16; i++) {
    art_index.DeleteEntry(key_to_values[test_key_index].tuple, result[i]);
  }

  result.clear();
  art_index.ScanKey(key_to_values[test_key_index].tuple, result);
  EXPECT_EQ(0, result.size());

  delete tuple_schema;
}

void TestingArtUtil::NonUniqueKeyMultiThreadedScanTest(
    UNUSED_ATTRIBUTE const IndexType index_type) {
  catalog::Schema *tuple_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::vector<oid_t> key_attrs = {0};
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);

  bool unique = false;
  index::IndexMetadata *index_metadata =
      new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                               IndexType::ART, IndexConstraintType::DEFAULT,
                               tuple_schema, key_schema, key_attrs, unique);

  index::ArtIndex art_index(index_metadata, loadKeyForTest);

  if (!map_populated) {
    PopulateMap(art_index);
  }

  int num_rows = 10000;
  size_t num_threads = 16;

  Timer<> timer;
  timer.Start();
  LaunchParallelTest(num_threads, TestingArtUtil::InsertHelperMicroBench,
                     &art_index, num_rows);
  timer.Stop();
  LOG_INFO("160,000 tuples inserted in %.5lfs", timer.GetDuration());

  std::vector<ItemPointer *> result;
  art_index.ScanAllKeys(result);
  EXPECT_EQ(num_rows * num_threads, result.size());

  int scan_scale_factor = 100;
  size_t scan_workers_threads = 16;
  timer.Reset();
  timer.Start();
  LaunchParallelTest(scan_workers_threads, TestingArtUtil::ScanHelperMicroBench,
                     &art_index, scan_scale_factor, num_rows, num_threads);
  timer.Stop();
  LOG_INFO("1,600 scans in %.5lfs", timer.GetDuration());

  delete tuple_schema;
}

void TestingArtUtil::NonUniqueKeyMultiThreadedStressTest(
    UNUSED_ATTRIBUTE const IndexType index_type) {
  catalog::Schema *tuple_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::vector<oid_t> key_attrs = {0};
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);

  bool unique = false;
  index::IndexMetadata *index_metadata =
      new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                               IndexType::ART, IndexConstraintType::DEFAULT,
                               tuple_schema, key_schema, key_attrs, unique);

  index::ArtIndex art_index(index_metadata, loadKeyForTest);

  if (!map_populated) {
    PopulateMap(art_index);
  }

  int num_rows = 10000;
  size_t num_threads = 16;

  Timer<> timer;
  timer.Start();
  LaunchParallelTest(num_threads, TestingArtUtil::InsertHelperMicroBench,
                     &art_index, num_rows);
  timer.Stop();
  LOG_INFO("160,000 tuples inserted in %.5lfs", timer.GetDuration());

  std::vector<ItemPointer *> result;
  art_index.ScanAllKeys(result);
  EXPECT_EQ(num_rows * num_threads, result.size());

  int delete_rows = 10000;
  size_t delete_workers_threads = 15;
  timer.Reset();
  timer.Start();
  LaunchParallelTest(delete_workers_threads,
                     TestingArtUtil::DeleteHelperMicroBench, &art_index,
                     delete_rows);
  timer.Stop();
  LOG_INFO("150,000 tuples deleted in %.5lfs", timer.GetDuration());
  result.clear();
  art_index.ScanAllKeys(result);
  EXPECT_EQ(num_rows * num_threads - delete_rows * delete_workers_threads,
            result.size());

  for (int i = 0; i < num_rows; i++) {
    result.clear();
    art_index.ScanKey(key_to_values[i].tuple, result);
    EXPECT_EQ(1, result.size());
    EXPECT_EQ((uint64_t)key_to_values[i].values[15], (uint64_t)result[0]);
  }

  delete tuple_schema;
}

storage::DataTable *TestingArtUtil::CreateTable(int tuples_per_tilegroup_count,
                                                bool indexes, oid_t table_oid) {
  catalog::Schema *table_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  std::string table_name("test_table");

  // Create table.
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, table_oid, table_schema, table_name,
      tuples_per_tilegroup_count, own_schema, adapt_table);

  if (indexes == true) {
    // This holds column ID in the underlying table that are being indexed
    std::vector<oid_t> key_attrs;

    // This holds schema of the underlying table, which stays all the same
    // for all indices on the same underlying table
    auto tuple_schema = table->GetSchema();

    // This points to the schmea of only columns indiced by the index
    // This is basically selecting tuple_schema() with key_attrs as index
    // but the order inside tuple schema is preserved - the order of schema
    // inside key_schema is not the order of real key
    catalog::Schema *key_schema;

    // This will be created for each index on the table
    // and the metadata is passed as part of the index construction paratemter
    // list
    index::IndexMetadata *index_metadata;

    // Whether keys should be unique. For primary key this must be true;
    // for secondary keys this might be true as an extra constraint
    bool unique;

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

    // This is not redundant
    // since the key schema always follows the ordering of the base table
    // schema, we need real ordering of the key columns
    key_schema->SetIndexedColumns(key_attrs);

    unique = true;

    index_metadata =
        new index::IndexMetadata("art_index", 123, INVALID_OID, INVALID_OID,
                                 IndexType::ART, IndexConstraintType::DEFAULT,
                                 tuple_schema, key_schema, key_attrs, unique);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(pkey_index);

    /////////////////////////////////////////////////////////////////
    // Add index on table column 0 and 1
    /////////////////////////////////////////////////////////////////

    key_attrs = {0, 1};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "secondary_btree_index", 124, INVALID_OID, INVALID_OID, IndexType::ART,
        IndexConstraintType::DEFAULT, tuple_schema, key_schema, key_attrs,
        unique);
    std::shared_ptr<index::Index> sec_index(
        index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(sec_index);
  }

  return table;
}

void TestingArtUtil::PopulateMap(UNUSED_ATTRIBUTE index::Index &index) {
  catalog::Schema *table_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1)});

  // Random values
  std::srand(std::time(nullptr));
  std::unordered_set<uint64_t> values_set;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int i = 0; i < 10000; i++) {
    // create the key
    int populate_value = i;

    auto v0 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(populate_value, 0));

    auto v1 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(std::rand() % (10000 / 3), 1));

    char *c = new char[8];
    index::ArtIndex::WriteValueInBytes(v0, c, 0, 4);
    index::ArtIndex::WriteValueInBytes(v1, c, 4, 4);

    storage::Tuple *tuple = new storage::Tuple(table_schema, true);
    tuple->SetValue(0, v0, testing_pool);
    tuple->SetValue(1, v1, testing_pool);

    key_to_values[i].tuple = tuple;

    index::ARTKey index_key;
    index_key.setKeyLen(8);
    index_key.set(c, 8);

    key_to_values[i].key.setKeyLen(index_key.getKeyLen());
    key_to_values[i].key.set((const char *)index_key.data,
                             index_key.getKeyLen());

    // generate 16 random values
    for (int j = 0; j < 16; j++) {
      uint64_t new_value = ((uint64_t)(std::rand()) << 30) +
                           ((uint64_t)(std::rand()) << 15) +
                           (uint64_t)(std::rand());
      while (values_set.find(new_value) != values_set.end()) {
        new_value = ((uint64_t)(std::rand()) << 30) +
                    ((uint64_t)(std::rand()) << 15) + (uint64_t)(std::rand());
      }
      values_set.insert(new_value);

      key_to_values[i].values[j] = new_value;
      value_to_key[(index::TID)new_value] = &(key_to_values[i].key);
    }

    delete[] c;
  }

  map_populated = true;
}

void TestingArtUtil::InsertHelper(storage::DataTable *table,
                                  type::AbstractPool *testing_pool,
                                  size_t scale_factor, int num_rows,
                                  bool random,
                                  std::vector<storage::Tuple *> *keys,
                                  std::vector<ItemPointer *> *expected_values,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  const catalog::Schema *schema = table->GetSchema();

  // get secondary index which is built on the first and second columns
  auto index = table->GetIndex(1);
  auto key_schema = index->GetKeySchema();

  const bool allocate = true;
  // Random values
  if (random) std::srand(std::time(nullptr));

  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    for (int rowid = 0; rowid < num_rows; rowid++) {
      int populate_value = rowid;

      storage::Tuple tuple(schema, allocate);

      auto v0 = type::ValueFactory::GetIntegerValue(
          TestingExecutorUtil::PopulatedValue(populate_value, 0) * scale_itr);
      tuple.SetValue(0, v0, testing_pool);

      auto v1 = type::ValueFactory::GetIntegerValue(
          TestingExecutorUtil::PopulatedValue(
              random ? std::rand() % (num_rows / 3) : populate_value, 1) *
          scale_itr);
      tuple.SetValue(1, v1, testing_pool);

      tuple.SetValue(2, type::ValueFactory::GetDecimalValue(
                            TestingExecutorUtil::PopulatedValue(
                                random ? std::rand() : populate_value, 2) *
                            scale_itr),
                     testing_pool);

      // In case of random, make sure this column has duplicated values
      auto string_value = type::ValueFactory::GetVarcharValue(std::to_string(
          TestingExecutorUtil::PopulatedValue(
              random ? std::rand() % (num_rows / 3) : populate_value, 3) *
          scale_itr));
      tuple.SetValue(3, string_value, testing_pool);

      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer tuple_slot_id =
          table->InsertTuple(&tuple, txn, &index_entry_ptr);
      PL_ASSERT(tuple_slot_id.block != INVALID_OID);
      PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

      storage::Tuple *key = new storage::Tuple(key_schema, true);
      key->SetValue(0, v0, testing_pool);
      key->SetValue(1, v1, testing_pool);
      keys->push_back(key);
      expected_values->push_back(index_entry_ptr);

      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    }
  }
  txn_manager.CommitTransaction(txn);
}

void TestingArtUtil::InsertHelperMicroBench(index::ArtIndex *index,
                                            int num_rows,
                                            UNUSED_ATTRIBUTE uint64_t
                                                thread_itr) {
  for (int rowid = 0; rowid < num_rows; rowid++) {
    index->InsertEntry(key_to_values[rowid].tuple,
                       (ItemPointer *)key_to_values[rowid].values[thread_itr]);
  }
}

void TestingArtUtil::DeleteHelper(storage::DataTable *table,
                                  UNUSED_ATTRIBUTE int num_rows,
                                  std::vector<storage::Tuple *> keys,
                                  std::vector<ItemPointer *> expected_values,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  // get secondary index which is built on the first and second columns
  auto index = table->GetIndex(1);
  int start_row = thread_itr * num_rows;
  if (keys.size() < (uint64_t)start_row) {
    start_row = keys.size();
  }
  int end_row = start_row + num_rows;
  if ((keys.size() < (uint64_t)end_row)) {
    end_row = keys.size();
  }
  for (int rowid = start_row; rowid < end_row; rowid++) {
    index->DeleteEntry(keys[rowid], expected_values[rowid]);
  }
}

void TestingArtUtil::DeleteHelperMicroBench(index::ArtIndex *index,
                                            int num_rows, uint64_t thread_itr) {
  for (int rowid = 0; rowid < num_rows; rowid++) {
    index->DeleteEntry(key_to_values[rowid].tuple,
                       (ItemPointer *)key_to_values[rowid].values[thread_itr]);
  }
}

void TestingArtUtil::ScanHelperMicroBench(index::ArtIndex *index,
                                          size_t scale_factor, int total_rows,
                                          int insert_workers,
                                          UNUSED_ATTRIBUTE uint64_t
                                              thread_itr) {
  for (size_t scale_itr = 0; scale_itr < scale_factor; scale_itr++) {
    int begin_index = std::rand() % (total_rows - 1);
    int end_index = std::rand() % (total_rows - begin_index) + begin_index + 1;
    if (end_index >= total_rows) {
      end_index = total_rows - 1;
    }
    index::ARTKey continue_key;
    std::vector<ItemPointer *> result;
    size_t result_found = 0;
    auto &t = index->art_.GetThreadInfo();
    index->art_.LookupRange(key_to_values[begin_index].key,
                            key_to_values[end_index].key, continue_key, result,
                            0, result_found, t);
    EXPECT_EQ(insert_workers * (end_index - begin_index + 1), result_found);
  }
}
}
}