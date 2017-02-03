//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_test.cpp
//
// Identification: test/index/index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "common/logger.h"
#include "common/platform.h"
#include "index/index_factory.h"
#include "index/testing_index_util.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

class IndexTests : public PelotonTest {};

// Since we need index type to determine the result
// of the test, this needs to be made as a global static
static IndexType index_type = IndexType::BWTREE;
// static IndexType index_type = IndexType::SKIPLIST;

TEST_F(IndexTests, BasicTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
  const catalog::Schema *key_schema = index->GetKeySchema();

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);

  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  // INSERT

  index->InsertEntry(key0.get(), TestingIndexUtil::item0.get());

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item0->block);
  location_ptrs.clear();

  // DELETE
  index->DeleteEntry(key0.get(), TestingIndexUtil::item0.get());

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}

TEST_F(IndexTests, MultiMapInsertTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);

  // Checks
  index->ScanAllKeys(location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else {
    EXPECT_EQ(location_ptrs.size(), 9);
  }

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

  delete index->GetMetadata()->GetTupleSchema();
}

#ifdef ALLOW_UNIQUE_KEY
TEST_F(IndexTests, UniqueKeyDeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, true));
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
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 1);
  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item1->block);
  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}
#endif

TEST_F(IndexTests, NonUniqueKeyDeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
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

  delete index->GetMetadata()->GetTupleSchema();
}

TEST_F(IndexTests, MultiThreadedInsertTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);

  index->ScanAllKeys(location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 7);
  } else {
    EXPECT_EQ(location_ptrs.size(), 9 * num_threads);
  }

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

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), num_threads);
  }

  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item0->block);
  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}

#ifdef ALLOW_UNIQUE_KEY
TEST_F(IndexTests, UniqueKeyMultiThreadedTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, true));
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
  index->ScanTest({key1_val0}, {0}, {ExpressionType::COMPARE_EQUAL},
                  ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  EXPECT_EQ(location_ptrs.size(), 0);
  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}
#endif

// key0 1 (100, a)   TestingIndexUtil::item0
// key1 5  (100, b)  TestingIndexUtil::item1 2 1 1 0
// key2 1 (100, c) item 1
// key3 1 (400, d) item 1
// key4 1  (500, eeeeee...) item 1
// no keyonce (1000, f)

// TestingIndexUtil::item0 = 2
// TestingIndexUtil::item1 = 6
// TestingIndexUtil::item2 = 1

// should be no key0
// key1 item 0 2
// key2 item 1
// no key3
// no key4

TEST_F(IndexTests, NonUniqueKeyMultiThreadedTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
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

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(2, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(2, location_ptrs.size());
  } else {
    EXPECT_EQ(2 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(1, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(1, location_ptrs.size());
  } else {
    EXPECT_EQ(1 * num_threads, location_ptrs.size());
  }

  EXPECT_EQ(TestingIndexUtil::item1->block, location_ptrs[0]->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
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

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }

  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::FORWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(2, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(2, location_ptrs.size());
  } else {
    EXPECT_EQ(2 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '1' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::FORWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
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
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
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
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1, key4_val0, key4_val1}, {0, 1, 0, 1},
                  {ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                   ExpressionType::COMPARE_GREATERTHAN,
                   ExpressionType::COMPARE_LESSTHANOREQUALTO,
                   ExpressionType::COMPARE_LESSTHAN},
                  ScanDirectionType::FORWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  // REVERSE SCAN
  index->ScanTest({key1_val0}, {0}, {ExpressionType::COMPARE_EQUAL},
                  ScanDirectionType::BACKWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      ScanDirectionType::BACKWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(2, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(2, location_ptrs.size());
  } else {
    EXPECT_EQ(2 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  // PAVLO: 2016-12-29
  // This test used to expect '1' result, but since we removed Index::Compare()
  // it now returns '3' results. We have to rely on the IndexScanExecutor
  // to do the final filtering.
  index->ScanTest(
      {key1_val0, key1_val1}, {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_GREATERTHAN},
      ScanDirectionType::BACKWARD, location_ptrs);
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
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
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
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
  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  index->ScanTest({key0_val0, key0_val1, key4_val0, key4_val1}, {0, 1, 0, 1},
                  {ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                   ExpressionType::COMPARE_GREATERTHAN,
                   ExpressionType::COMPARE_LESSTHANOREQUALTO,
                   ExpressionType::COMPARE_LESSTHAN},
                  ScanDirectionType::BACKWARD, location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(3, location_ptrs.size());
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(3, location_ptrs.size());
  } else {
    EXPECT_EQ(3 * num_threads, location_ptrs.size());
  }
  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
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

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 2);
  } else {
    EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
  }

  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 1);
  } else {
    EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
  }

  EXPECT_EQ(location_ptrs[0]->block, TestingIndexUtil::item1->block);
  location_ptrs.clear();

  index->ScanAllKeys(location_ptrs);

  if (index_type == IndexType::BWTREE) {
    EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
  } else if (index_type == IndexType::SKIPLIST) {
    EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
  } else {
    EXPECT_EQ(location_ptrs.size(), 3 * num_threads * scale_factor);
  }

  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest2) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index> index(
      TestingIndexUtil::BuildIndex(index_type, false));
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
    if (index_type == IndexType::BWTREE) {
      EXPECT_EQ(location_ptrs.size(), scale_factor);
    } else if (index_type == IndexType::SKIPLIST) {
      EXPECT_EQ(location_ptrs.size(), scale_factor);
    } else {
      EXPECT_EQ(location_ptrs.size(), scale_factor);
    }
  } else {
    if (index_type == IndexType::BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
    } else if (index_type == IndexType::SKIPLIST) {
      EXPECT_EQ(location_ptrs.size(), 3 * scale_factor);
    } else {
      EXPECT_EQ(location_ptrs.size(), 3 * scale_factor * num_threads);
    }
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
    if (index_type == IndexType::BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 2);
    } else if (index_type == IndexType::SKIPLIST) {
      EXPECT_EQ(location_ptrs.size(), 2);
    } else {
      EXPECT_EQ(location_ptrs.size(), 2 * num_threads);
    }
  }
  location_ptrs.clear();

  index->ScanKey(key2.get(), location_ptrs);
  if (index->HasUniqueKeys()) {
    EXPECT_EQ(location_ptrs.size(), num_threads);
  } else {
    if (index_type == IndexType::BWTREE) {
      EXPECT_EQ(location_ptrs.size(), 1);
    } else if (index_type == IndexType::SKIPLIST) {
      EXPECT_EQ(location_ptrs.size(), 1);
    } else {
      EXPECT_EQ(location_ptrs.size(), 1 * num_threads);
    }
  }

  location_ptrs.clear();

  delete index->GetMetadata()->GetTupleSchema();
}

}  // End test namespace
}  // End peloton namespace
