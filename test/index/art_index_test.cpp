//
// Created by Min Huang on 9/20/17.
//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_art_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// ART Index Tests
//===--------------------------------------------------------------------===//

class ARTIndexTests : public PelotonTest {};

TEST_F(ARTIndexTests, BasicTest) {
  TestingArtUtil::BasicTest(IndexType::ART);
}


TEST_F(ARTIndexTests, NonUniqueKeyDeleteTest) {
  TestingArtUtil::NonUniqueKeyDeleteTest(IndexType::ART);
}

//TEST_F(ARTIndexTests, MultiThreadedInsertTest) {
//  TestingArtUtil::MultiThreadedInsertTest(IndexType::ART);
//}
//
//TEST_F(ARTIndexTests, NonUniqueKeyMultiThreadedTest) {
//  TestingArtUtil::NonUniqueKeyMultiThreadedTest(IndexType::ART);
//}

}
}