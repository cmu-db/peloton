//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index_test.cpp
//
// Identification: test/index/hash_index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_index_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Hash Index Tests
//===--------------------------------------------------------------------===//

class HashIndexTests : public PelotonTest {};

TEST_F(HashIndexTests, BasicTest) {
	TestingIndexUtil::BasicTest(IndexType::HASH);
}

TEST_F(HashIndexTests, MultiMapInsertTest) {
  IndexType index_type = IndexType::HASH;
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void (*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false),
      TestingIndexUtil::DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, TestingIndexUtil::InsertHelper, index.get(), pool,
                     scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));
  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);
  keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(1, location_ptrs.size());
  EXPECT_EQ(TestingIndexUtil::item0->block, location_ptrs[0]->block);
  location_ptrs.clear();
}

TEST_F(HashIndexTests, UniqueKeyInsertTest) {
  TestingIndexUtil::UniqueKeyInsertTest(IndexType::HASH);
}

// TEST_F(BwTreeIndexTests, UniqueKeyDeleteTest) {
//  TestingIndexUtil::UniqueKeyDeleteTest(IndexType::BWTREE);
// }

// TEST_F(HashIndexTests, NonUniqueKeyDeleteTest) {
//   TestingIndexUtil::NonUniqueKeyDeleteTest(IndexType::HASH);
// }

TEST_F(HashIndexTests, MultiThreadedInsertTest) {
  // TestingIndexUtil::MultiThreadedInsertTest(IndexType::HASH);
  IndexType index_type = IndexType::HASH;
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  std::unique_ptr<index::Index, void (*)(index::Index *)> index(
      TestingIndexUtil::BuildIndex(index_type, false),
      TestingIndexUtil::DestroyIndex);
  const catalog::Schema *key_schema = index->GetKeySchema();

  // Parallel Test
  size_t num_threads = 4;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, TestingIndexUtil::InsertHelper, index.get(),
                     pool, scale_factor);

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));

  keynonce->SetValue(0, type::ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, type::ValueFactory::GetVarcharValue("f"), pool);

  key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  index->ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index->ScanKey(key0.get(), location_ptrs);
  EXPECT_EQ(1, location_ptrs.size());
  EXPECT_EQ(TestingIndexUtil::item0->block, location_ptrs[0]->block);
  location_ptrs.clear();
}

// TEST_F(HashIndexTests, UniqueKeyMultiThreadedTest) {
//  TestingIndexUtil::UniqueKeyMultiThreadedTest(IndexType::HASH);
// }

// TEST_F(HashIndexTests, NonUniqueKeyMultiThreadedTest) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedTest(IndexType::HASH);
// }

// TEST_F(HashIndexTests, NonUniqueKeyMultiThreadedStressTest) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::HASH);
// }

// TEST_F(HashIndexTests, NonUniqueKeyMultiThreadedStressTest2) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(IndexType::HASH);
// }

}  // namespace test
}  // namespace peloton
