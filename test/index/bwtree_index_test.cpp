//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index_test.cpp
//
// Identification: test/index/index_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_index_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

class IndexTests : public PelotonTest {};

TEST_F(IndexTests, BasicTest) {
  TestingIndexUtil::BasicTest(IndexType::BWTREE);
}

TEST_F(IndexTests, MultiMapInsertTest) {
  TestingIndexUtil::MultiMapInsertTest(IndexType::BWTREE);
}

//TEST_F(IndexTests, UniqueKeyDeleteTest) {
//  TestingIndexUtil::UniqueKeyDeleteTest(IndexType::BWTREE);
//}

TEST_F(IndexTests, NonUniqueKeyDeleteTest) {
  TestingIndexUtil::NonUniqueKeyDeleteTest(IndexType::BWTREE);
}

TEST_F(IndexTests, MultiThreadedInsertTest) {
  TestingIndexUtil::MultiThreadedInsertTest(IndexType::BWTREE);
}

//TEST_F(IndexTests, UniqueKeyMultiThreadedTest) {
//  TestingIndexUtil::UniqueKeyMultiThreadedTest(IndexType::BWTREE);
//}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedTest) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedTest(IndexType::BWTREE);
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::BWTREE);
}

TEST_F(IndexTests, NonUniqueKeyMultiThreadedStressTest2) {
  TestingIndexUtil::NonUniqueKeyMultiThreadedStressTest2(IndexType::BWTREE);
}

}  // End test namespace
}  // End peloton namespace
