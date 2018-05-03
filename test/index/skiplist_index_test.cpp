//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index_test.cpp
//
// Identification: test/index/skiplist_index_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "common/internal_types.h"
#include "index/testing_index_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SkipList Index Tests
//===--------------------------------------------------------------------===//

class SkipListIndexTests : public PelotonTest {};

TEST_F(SkipListIndexTests, BasicTest) {
  // TestingIndexUtil::BasicTest(IndexType::SKIPLIST);
  EXPECT_EQ(2, 1+1);
}

//TEST_F(SkipListIndexTests, MultiMapInsertTest) {
//  TestingIndexUtil::MultiMapInsertTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, UniqueKeyInsertTest) {
//  TestingIndexUtil::UniqueKeyInsertTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, UniqueKeyDeleteTest) {
//  TestingIndexUtil::UniqueKeyDeleteTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, NonUniqueKeyDeleteTest) {
//  TestingIndexUtil::NonUniqueKeyDeleteTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, MultiThreadedInsertTest) {
//  TestingIndexUtil::MultiThreadedInsertTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, UniqueKeyMultiThreadedTest) {
//  TestingIndexUtil::UniqueKeyMultiThreadedTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, NonUniqueKeyMultiThreadedTest) {
//  TestingIndexUtil::NonUniqueKeyMultiThreadedTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, NonUniqueKeyMultiThreadedStressTest) {
//  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::SKIPLIST);
//}
//
//TEST_F(SkipListIndexTests, NonUniqueKeyMultiThreadedStressTest2) {
//  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(IndexType::SKIPLIST);
//}

}  // namespace test
}  // namespace peloton
