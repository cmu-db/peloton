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

namespace peloton {
namespace test {

void TestingArtUtil::BasicTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  // the index created in this table is ART index
  std::unique_ptr<storage::DataTable> table(CreateTable(5));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  const catalog::Schema *schema = table->GetSchema();

  // get secondary index which is built on the first and second columns
  auto index = table->GetIndex(1);
  auto key_schema = index->GetKeySchema();
  std::vector<storage::Tuple *> keys;
  std::vector<ItemPointer *> expected_values;

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  int num_rows = 2;
  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(schema, allocate);

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(10 * populate_value + 0), testing_pool);

    tuple.SetValue(1, type::ValueFactory::GetIntegerValue(10 * populate_value + 1), testing_pool);

    tuple.SetValue(2, type::ValueFactory::GetDecimalValue(10 * populate_value + 2), testing_pool);

    auto string_value =
      type::ValueFactory::GetVarcharValue(std::to_string(10 * populate_value + 3));
    tuple.SetValue(3, string_value, testing_pool);

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
      table->InsertTuple(&tuple, txn, &index_entry_ptr);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

    storage::Tuple *key = new storage::Tuple(key_schema, true);
    key->SetValue(0, type::ValueFactory::GetIntegerValue(10 * populate_value + 0), testing_pool);
    key->SetValue(1, type::ValueFactory::GetIntegerValue(10 * populate_value + 1), testing_pool);
    keys.push_back(key);
    expected_values.push_back(index_entry_ptr);

    txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
  }

  txn_manager.CommitTransaction(txn);

  std::vector<ItemPointer *> result;
  index->ScanAllKeys(result);
  EXPECT_EQ(2, result.size());

  result.clear();
  index->ScanKey(keys[0], result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t) expected_values[0], (uint64_t) result[0]);

  result.clear();
  index->ScanKey(keys[1], result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t) expected_values[1], (uint64_t) result[0]);

  result.clear();
  index->DeleteEntry(keys[0], expected_values[0]);
  index->ScanAllKeys(result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t) expected_values[1], (uint64_t) result[0]);
}

void TestingArtUtil::NonUniqueKeyDeleteTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  // the index created in this table is ART index
  // std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable(5));
  storage::DataTable *table = CreateTable(5);
  std::vector<storage::Tuple *> keys;
  std::vector<ItemPointer *> expected_values;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  bool random = false;
  int num_rows = 7;

  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingArtUtil::InsertHelper, table, testing_pool,
                     scale_factor, num_rows, random, &keys, &expected_values);
  LaunchParallelTest(1, TestingArtUtil::InsertHelper, table, testing_pool,
                     scale_factor, num_rows, random, &keys, &expected_values);
  auto index = table->GetIndex(1);
  std::vector<ItemPointer *> result;
  index->ScanAllKeys(result);
  // 7 different keys, each has two values
  EXPECT_EQ(14, result.size());


  int delete_rows = 4;
  LaunchParallelTest(1, TestingArtUtil::DeleteHelper, table, delete_rows, keys, expected_values);

  result.clear();
  index->ScanAllKeys(result);
  EXPECT_EQ(10, result.size());

  // the first 4 keys should have one value left
  // the last 3 keys should have two values left
  result.clear();
  index->ScanKey(keys[0], result);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ((uint64_t) expected_values[7], (uint64_t) result[0]);

  result.clear();
  index->ScanKey(keys[13], result);
  EXPECT_EQ(2, result.size());
  EXPECT_EQ((uint64_t) expected_values[13], (uint64_t) result[1]);
}

void TestingArtUtil::MultiThreadedInsertTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  // the index created in this table is ART index
  storage::DataTable *table = CreateTable(5);
  std::vector<storage::Tuple *> keys;
  std::vector<ItemPointer *> expected_values;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  bool random = false;
  int num_rows = 7;

  size_t scale_factor = 5;
  LaunchParallelTest(4, TestingArtUtil::InsertHelper, table, testing_pool,
                     scale_factor, num_rows, random, &keys, &expected_values);
  auto index = table->GetIndex(1);
  std::vector<ItemPointer *> result;
  index->ScanAllKeys(result);
  EXPECT_EQ(140, result.size());
  for (uint32_t i = 0; i < keys.size(); i++) {
    result.clear();
    index->ScanKey(keys[i], result);
    EXPECT_EQ(4, result.size());
  }
}

void TestingArtUtil::NonUniqueKeyMultiThreadedStressTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  // the index created in this table is ART index
  // std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable(5));
  storage::DataTable *table = CreateTable(5);
  std::vector<storage::Tuple *> keys;
  std::vector<ItemPointer *> expected_values;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  bool random = false;
  int num_rows = 20;

  // first, remember some key value pairs to delete
  size_t scale_factor = 1;
  size_t num_threads = 1;
  // add 200 key-values in sequential order first, so the data
  // in vector keys and vector expected_values are in correct order
  for (uint32_t i = 0; i < 10; i++) {
    LaunchParallelTest(num_threads, TestingArtUtil::InsertHelper, table, testing_pool,
                       scale_factor, num_rows, random, &keys, &expected_values);
  }

  // insert huge amount of tuples (random data) at the same time
  random = true;
  scale_factor = 5;
  num_threads = 15;
  std::vector<storage::Tuple *> useless_keys;
  std::vector<ItemPointer *> useless_values;
  LaunchParallelTest(num_threads, TestingArtUtil::InsertHelper, table, testing_pool,
                     scale_factor, num_rows, random, &useless_keys, &useless_values);

  auto index = table->GetIndex(1);
  std::vector<ItemPointer *> result;
  index->ScanAllKeys(result);
  // 200 + 20 * 5 * 15 = 1540 in total
  EXPECT_EQ(1700, result.size());


  int delete_rows = 19;
  num_threads = 10;
  LaunchParallelTest(num_threads, TestingArtUtil::DeleteHelper, table, delete_rows, keys, expected_values);

  result.clear();
  index->ScanAllKeys(result);
  // 1700 - 190 = 1510
  EXPECT_EQ(1510, result.size());

  // the first 190 key-values in vector keys should be gone
  // INCORRECT!! it's possible that these key is generated randomly!
//  for (uint32_t i = 0; i < 190; i++) {
//    result.clear();
//    index->ScanKey(keys[i], result);
//    EXPECT_EQ(0, result.size());
//  }

  // the last 10 key-values in vector keys should remain
  for (uint32_t i = 190; i < 200; i++) {
    result.clear();
    index->ScanKey(keys[i], result);
    EXPECT_EQ((uint64_t) expected_values[i], (uint64_t) result[0]);
  }

}


storage::DataTable *TestingArtUtil::CreateTable(
  int tuples_per_tilegroup_count, bool indexes, oid_t table_oid) {
  catalog::Schema *table_schema = new catalog::Schema(
    {TestingExecutorUtil::GetColumnInfo(0), TestingExecutorUtil::GetColumnInfo(1),
     TestingExecutorUtil::GetColumnInfo(2), TestingExecutorUtil::GetColumnInfo(3)});
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

    index_metadata = new index::IndexMetadata(
      "primary_btree_index", 123, INVALID_OID, INVALID_OID, IndexType::ART,
      IndexConstraintType::DEFAULT, tuple_schema, key_schema, key_attrs,
      unique);

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
      "secondary_btree_index", 124, INVALID_OID, INVALID_OID,
      IndexType::ART, IndexConstraintType::DEFAULT, tuple_schema,
      key_schema, key_attrs, unique);
    std::shared_ptr<index::Index> sec_index(
      index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(sec_index);
  }

  return table;
}



void TestingArtUtil::InsertHelper(storage::DataTable *table,
                                  type::AbstractPool *testing_pool, size_t scale_factor, int num_rows,
                                  bool random, std::vector<storage::Tuple *> *keys,
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
                random ? std::rand() % (num_rows / 3) : populate_value, 1) * scale_itr);
      tuple.SetValue(1, v1, testing_pool);

      tuple.SetValue(2, type::ValueFactory::GetDecimalValue(
                     TestingExecutorUtil::PopulatedValue(
                     random ? std::rand() : populate_value, 2) * scale_itr),
                     testing_pool);

      // In case of random, make sure this column has duplicated values
      auto string_value =
        type::ValueFactory::GetVarcharValue(std::to_string(TestingExecutorUtil::PopulatedValue(
          random ? std::rand() % (num_rows / 3) : populate_value, 3) * scale_itr));
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

void TestingArtUtil::DeleteHelper(storage::DataTable *table, UNUSED_ATTRIBUTE int num_rows,
                                  std::vector<storage::Tuple *> keys,
                                  std::vector<ItemPointer *> expected_values,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  // get secondary index which is built on the first and second columns
  auto index = table->GetIndex(1);
  printf("thread_iter = %llu\n", thread_itr);
  int start_row = thread_itr * num_rows;
  if (keys.size() < (uint64_t) start_row) {
    start_row = keys.size();
  }
  int end_row = start_row + num_rows;
  if ((keys.size() < (uint64_t) end_row)) {
    end_row = keys.size();
  }
  for (int rowid = start_row; rowid < end_row; rowid++) {
    index->DeleteEntry(keys[rowid], expected_values[rowid]);
  }

}

}
}