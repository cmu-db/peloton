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
  // IndexType index_type = IndexType::HASH;
  // auto pool = TestingHarness::GetInstance().GetTestingPool();
  // std::vector<ItemPointer *> location_ptrs;

  // // INDEX
  // std::unique_ptr<index::Index, void(*)(index::Index *)> index(
  //     TestingIndexUtil::BuildIndex(index_type, false), 
  //     TestingIndexUtil::DestroyIndex);
  // const catalog::Schema *key_schema = index->GetKeySchema();

  // std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  // key0->SetValue(0, type::ValueFactory::GetIntegerValue(100), pool);
  // key0->SetValue(1, type::ValueFactory::GetVarcharValue("a"), pool);

  // // INSERT
  // index->InsertEntry(key0.get(), TestingIndexUtil::item0.get());

  // // SCAN
  // index->ScanKey(key0.get(), location_ptrs);
  // EXPECT_EQ(1, location_ptrs.size());
  // bool ret = false;
  // size_t const element_count = 3;  
}

// TEST_F(BwTreeIndexTests, MultiMapInsertTest) {
//   TestingIndexUtil::MultiMapInsertTest(IndexType::BWTREE);
// }

// TEST_F(BwTreeIndexTests, UniqueKeyInsertTest) {
//   TestingIndexUtil::UniqueKeyInsertTest(IndexType::BWTREE);
// }

// //TEST_F(BwTreeIndexTests, UniqueKeyDeleteTest) {
// //  TestingIndexUtil::UniqueKeyDeleteTest(IndexType::BWTREE);
// //}

// TEST_F(BwTreeIndexTests, NonUniqueKeyDeleteTest) {
//   TestingIndexUtil::NonUniqueKeyDeleteTest(IndexType::BWTREE);
// }

// TEST_F(BwTreeIndexTests, MultiThreadedInsertTest) {
//   TestingIndexUtil::MultiThreadedInsertTest(IndexType::BWTREE);
// }

// //TEST_F(BwTreeIndexTests, UniqueKeyMultiThreadedTest) {
// //  TestingIndexUtil::UniqueKeyMultiThreadedTest(IndexType::BWTREE);
// //}

// TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedTest) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedTest(IndexType::BWTREE);
// }

// TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedStressTest) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::BWTREE);
// }

// TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedStressTest2) {
//   TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(IndexType::BWTREE);
// }

}  // namespace test
}  // namespace peloton
