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

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

void TestingArtUtil::BasicTest(UNUSED_ATTRIBUTE const IndexType index_type) {
  // the index created in this table is ART index
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable(5));
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
}
}