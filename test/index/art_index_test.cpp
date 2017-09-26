//
// Created by Min Huang on 9/20/17.
//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "index/testing_index_util.h"
#include "index/testing_index_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// ART Index Tests
//===--------------------------------------------------------------------===//

class ARTIndexTests : public PelotonTest {};

TEST_F(ARTIndexTests, BasicTest) {
TestingIndexUtil::BasicTest(IndexType::ART);
}

//TEST_F(ARTIndexTests, UniqueKeyInsertTest) {
//  TestingIndexUtil::UniqueKeyInsertTest(IndexType::ART);
//}

}
}