//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index_test.cpp
//
// Identification: test/index/bwtree_index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_index_util.h"
#include "index/testing_index_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// BwTree Index Tests
//===--------------------------------------------------------------------===//

class BwTreeIndexTests : public PelotonTest {};

TEST_F(BwTreeIndexTests, BasicTest) {
  TestingIndexUtil::BasicTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, MultiMapInsertTest) {
  TestingIndexUtil::MultiMapInsertTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, UniqueKeyInsertTest) {
  TestingIndexUtil::UniqueKeyInsertTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, UniqueKeyDeleteTest) {
  TestingIndexUtil::UniqueKeyDeleteTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, NonUniqueKeyDeleteTest) {
  TestingIndexUtil::NonUniqueKeyDeleteTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, MultiThreadedInsertTest) {
  TestingIndexUtil::MultiThreadedInsertTest(IndexType::BWTREE);
}

//TEST_F(BwTreeIndexTests, UniqueKeyMultiThreadedTest) {
//  TestingIndexUtil::UniqueKeyMultiThreadedTest(IndexType::BWTREE);
//}

TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedTest) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedStressTest) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::BWTREE);
}

TEST_F(BwTreeIndexTests, NonUniqueKeyMultiThreadedStressTest2) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(IndexType::BWTREE);
}

}  // namespace test
}  // namespace peloton
