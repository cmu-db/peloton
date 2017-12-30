//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tests_util.cpp
//
// Identification: test/index/index_tests_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/testing_index_util.h"

#include "gtest/gtest.h"

#include "common/harness.h"
#include "catalog/catalog.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "index/index.h"
#include "index/index_util.h"
#include "storage/tuple.h"
#include "common/internal_types.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

std::shared_ptr<ItemPointer> TestingIndexUtil::item0(new ItemPointer(120, 5));
std::shared_ptr<ItemPointer> TestingIndexUtil::item1(new ItemPointer(120, 7));
std::shared_ptr<ItemPointer> TestingIndexUtil::item2(new ItemPointer(123, 19));

void TestingIndexUtil::BasicTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  // INSERT
  index->InsertEntry(key0.get(), TestingIndexUtil::item0.get());

  // SCAN
  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item0->block);
  location_ptrs.clear();

  // DELETE
  index->DeleteEntry(key0.get(), TestingIndexUtil::item0.get());

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();
}

void TestingIndexUtil::MultiMapInsertTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);

  // Checks
  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 7);
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));
  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item0->block);
  location_ptrs.clear();
}

void TestingIndexUtil::UniqueKeyInsertTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(1, location_ptrs.size());
  location_ptrs.clear();
}

void TestingIndexUtil::UniqueKeyDeleteTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);
  LaunchParallelTest(1, TestingIndexUtil::DeleteHelper, index.get(), pool,
                     scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(1, location_ptrs.size());
  EXPECT_EQ(TestingIndexUtil::item1->block, location_ptrs[0]->block);
  location_ptrs.clear();

  LOG_INFO("INDEX:\n%s", index->GetInfo().c_str());
}

void TestingIndexUtil::NonUniqueKeyDeleteTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);
  LaunchParallelTest(1, TestingIndexUtil::DeleteHelper, index.get(), pool,
                     scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 2);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item1->block);
  location_ptrs.clear();
}

void TestingIndexUtil::MultiThreadedInsertTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 7);
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));

  keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item0->block);
  location_ptrs.clear();
}

void TestingIndexUtil::UniqueKeyMultiThreadedTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);
  LaunchParallelTest(num_threads, TestingIndexUtil::DeleteHelper, index.get(),
                     pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);

  type::Value key0_val0 = (key0->GetValue(0));
  type::Value key0_val1 = (key0->GetValue(1));
  type::Value key1_val0 = (key1->GetValue(0));
  type::Value key1_val1 = (key1->GetValue(1));
  type::Value key2_val0 = (key2->GetValue(0));
  type::Value key2_val1 = (key2->GetValue(1));

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  location_ptrs.clear();

  // FORWARD SCAN
  // DVANAKEN: 2017-03-06
  // This test used to expect '0' result, but since we removed Index::Compare()
  // it now returns '1' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest({key1_val0}, {0}, {ExpressionType::COMPARE_EQUAL},
                  ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  // DVANAKEN: 2017-03-06
  // This test used to expect '0' result, but since we removed Index::Compare()
  // it now returns '1' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  location_ptrs.clear();

  // DVANAKEN: 2017-03-06
  // This test used to expect '0' result, but since we removed Index::Compare()
  // it now returns '1' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  location_ptrs.clear();
}

void TestingIndexUtil::NonUniqueKeyMultiThreadedTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);
  LaunchParallelTest(num_threads, TestingIndexUtil::DeleteHelper, index.get(),
                     pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);
  key4->SetValue(0, type::ValueFactory::GetIntegerValue(500), pool);
  key4->SetValue(
      1, type::ValueFactory::GetVarcharValue(StringUtil::Repeat("e", 1000)),
      pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(2, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(1, location_ptrs.size());
  EXPECT_EQ(TestingIndexUtil::item1->block, location_ptrs[0]->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // FORWARD SCAN
  type::Value key0_val0 = (key0->GetValue(0));
  type::Value key0_val1 = (key0->GetValue(1));
  type::Value key1_val0 = (key1->GetValue(0));
  type::Value key1_val1 = (key1->GetValue(1));
  type::Value key2_val0 = (key2->GetValue(0));
  type::Value key2_val1 = (key2->GetValue(1));
  type::Value key4_val0 = (key4->GetValue(0));
  type::Value key4_val1 = (key4->GetValue(1));
  index->ScanTest({key1_val0}, {0}, {ExpressionType::COMPARE_EQUAL},
                  ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(2, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '1' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '0' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '2' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key2_val0, key2_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '2' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key0_val0, key0_val1, key2_val0, key2_val1}, {0, 1, 0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN,
       ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1, key4_val0, key4_val1}, {0, 1, 0, 1},
                  {ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                   ExpressionType::COMPARE_GREATERTHAN,
                   ExpressionType::COMPARE_LESSTHANOREQUALTO,
                   ExpressionType::COMPARE_LESSTHAN},
                  ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // REVERSE SCAN
  index->ScanTest({key1_val0}, {0}, {ExpressionType::COMPARE_EQUAL},
                  ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(2, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '1' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '0' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '2' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key2_val0, key2_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHAN},
      ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '2' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key0_val0, key0_val1, key2_val0, key2_val1}, {0, 1, 0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN,
       ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHAN},
      ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1, key4_val0, key4_val1}, {0, 1, 0, 1},
                  {ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                   ExpressionType::COMPARE_GREATERTHAN,
                   ExpressionType::COMPARE_LESSTHANOREQUALTO,
                   ExpressionType::COMPARE_LESSTHAN},
                  ScanDirectionType::BACKWARD, location_ptrs);
  EXPECT_EQ(3, location_ptrs.size());
  location_ptrs.clear();
}

void TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 3;

  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);
  LaunchParallelTest(num_threads, TestingIndexUtil::DeleteHelper, index.get(),
                     pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key1.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 2);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
  location_ptrs.clear();
}

void TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(const IndexType index_type) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void(*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false), DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 15;
  size_t scale_factor = 3;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);
  LaunchParallelTest(num_threads, TestingIndexUtil::DeleteHelper, index.get(),
                     pool, scale_factor);

  index->ScanAllKeys(location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), scale_factor);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
  }
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key1->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
  key2->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);

  index->ScanKey(key1.get(), location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), 0);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2);
  }
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), num_threads);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1);
  }

  location_ptrs.clear();
}


index::Index *TestingIndexUtil::BuildIndex(const IndexType index_type,
                                           const bool unique_keys) {
  LOG_DEBUG("Build index type: %s", IndexTypeToString(index_type).c_str());

  catalog::Schema *key_schema = nullptr;
  catalog::Schema *tuple_schema = nullptr;

  // Build tuple and key schema
  std::vector<catalog::Column> column_list;

  // The following key are both in index key and tuple key and they are
  // indexed
  // The size of the key is:
  //   integer 4 + varchar 8 = total 12

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "A",
                          true);

  catalog::Column column2(type::TypeId::VARCHAR, 1024, "B", false);

  // The following two constitutes tuple schema but does not appear in index

  catalog::Column column3(type::TypeId::DECIMAL,
                          type::Type::GetTypeSize(type::TypeId::DECIMAL), "C",
                          true);

  catalog::Column column4(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "D",
                          true);

  // Use the first two columns to build key schema
  column_list.push_back(column1);
  column_list.push_back(column2);

  // This will be copied into the key schema as well as into the IndexMetadata
  // object to identify indexed columns
  std::vector<oid_t> key_attrs = {0, 1};
  key_schema = new catalog::Schema(column_list);
  key_schema->SetIndexedColumns(key_attrs);

  // Use all four columns to build tuple schema
  column_list.push_back(column3);
  column_list.push_back(column4);
  tuple_schema = new catalog::Schema(column_list);

  // Build index metadata
  //
  // NOTE: Since here we use a relatively small key (size = 12)
  // so index_test is only testing with a certain kind of key
  // (most likely, GenericKey)
  //
  // For testing IntsKey and TupleKey we need more test cases
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "test_index", 125,  // Index oid
      INVALID_OID, INVALID_OID, index_type, IndexConstraintType::DEFAULT,
      tuple_schema, key_schema, key_attrs, unique_keys);

  // Build index
  //
  // The type of index key has been chosen inside this function, but we are
  // unable to know the exact type of key it has chosen
  //
  // The index interface always accept tuple key from the external world
  // and transforms into the correct index key format, so the caller
  // do not need to worry about the actual implementation of the index
  // key, and only passing tuple key suffices
  index::Index *index = index::IndexFactory::GetIndex(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);
  EXPECT_EQ(unique_keys, index->HasUniqueKeys());

  return index;
}

void TestingIndexUtil::DestroyIndex(index::Index *index) {
  delete index->GetMetadata()->GetTupleSchema();
  delete index;
}

void TestingIndexUtil::InsertHelper(index::Index *index, type::AbstractPool *pool,
                                  size_t scale_factor,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Insert a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> keynonce(
        new storage::Tuple(key_schema, true));

    key0->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
    key1->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
    key2->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);
    key3->SetValue(0, type::ValueFactory::GetIntegerValue(400 * scale_itr),
                   pool);
    key3->SetValue(1, type::ValueFactory::GetVarcharValue("d"), pool);
    key4->SetValue(0, type::ValueFactory::GetIntegerValue(500 * scale_itr),
                   pool);
    key4->SetValue(
        1, type::ValueFactory::GetVarcharValue(StringUtil::Repeat("e", 1000)),
        pool);
    keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000 * scale_itr),
                       pool);
    keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

    // INSERT
    // key0 1x (100, a)      -> item0
    // key1 5x (100, b)      -> item1 2 1 1 0
    // key2 1x (100, c)      -> item 1
    // key3 1x (400, d)      -> item 1
    // key4 1x (500, eee...) -> item 1
    // no keyonce (1000, f)

    // item0 = 2
    // item1 = 6
    // item2 = 1
    index->InsertEntry(key0.get(), item0.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item2.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item1.get());
    index->InsertEntry(key1.get(), item0.get());

    index->InsertEntry(key2.get(), item1.get());
    index->InsertEntry(key3.get(), item1.get());
    index->InsertEntry(key4.get(), item1.get());
  }
}

// DELETE HELPER FUNCTION
void TestingIndexUtil::DeleteHelper(index::Index *index, type::AbstractPool *pool,
                                  size_t scale_factor,
                                  UNUSED_ATTRIBUTE uint64_t thread_itr) {
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Delete a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

    key0->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
    key1->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key1->SetValue(1, type::ValueFactory::GetVarcharValue("b"), pool);
    key2->SetValue(0, type::ValueFactory::GetIntegerValue(100 * scale_itr),
                   pool);
    key2->SetValue(1, type::ValueFactory::GetVarcharValue("c"), pool);
    key3->SetValue(0, type::ValueFactory::GetIntegerValue(400 * scale_itr),
                   pool);
    key3->SetValue(1, type::ValueFactory::GetVarcharValue("d"), pool);
    key4->SetValue(0, type::ValueFactory::GetIntegerValue(500 * scale_itr),
                   pool);
    key4->SetValue(
        1, type::ValueFactory::GetVarcharValue(StringUtil::Repeat("e", 1000)),
        pool);

    // DELETE
    // key0 1 (100, a)   TestingIndexUtil::item0
    // key1 5  (100, b)  item 0 1 2 (0 1 1 1 2)
    // key2 1 (100, c) item 1
    // key3 1 (400, d) item 1
    // key4 1  (500, eeeeee...) item 1
    // no keyonce (1000, f)

    // TestingIndexUtil::item0 = 2
    // TestingIndexUtil::item1 = 6
    // TestingIndexUtil::item2 = 1
    index->DeleteEntry(key0.get(), TestingIndexUtil::item0.get());
    index->DeleteEntry(key1.get(), TestingIndexUtil::item1.get());
    index->DeleteEntry(key2.get(), TestingIndexUtil::item2.get());
    index->DeleteEntry(key3.get(), TestingIndexUtil::item1.get());
    index->DeleteEntry(key4.get(), TestingIndexUtil::item1.get());

    // should be no key0
    // key1 item 0 1 2
    // key2 item 1
    // no key3
    // no key4
  }
}

}  // namespace test
}  // namespace peloton
