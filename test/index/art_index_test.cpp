//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index_test.cpp
//
// Identification: test/index/art_index_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_art_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// ART Index Tests
//===--------------------------------------------------------------------===//

class ARTIndexTests : public PelotonTest {};

TEST_F(ARTIndexTests, BasicTest) { TestingArtUtil::BasicTest(IndexType::ART); }

TEST_F(ARTIndexTests, NonUniqueKeyDeleteTest) {
  TestingArtUtil::NonUniqueKeyDeleteTest(IndexType::ART);
}

TEST_F(ARTIndexTests, MultiThreadedInsertTest) {
  TestingArtUtil::MultiThreadedInsertTest(IndexType::ART);
}

TEST_F(ARTIndexTests, NonUniqueKeyMultiThreadedScanTest) {
  TestingArtUtil::NonUniqueKeyMultiThreadedScanTest(IndexType::ART);
}

TEST_F(ARTIndexTests, NonUniqueKeyMultiThreadedStressTest) {
  TestingArtUtil::NonUniqueKeyMultiThreadedStressTest(IndexType::ART);
}
}
}