//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample_test.cpp
//
// Identification: test/common/sample_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Sample Test
//===--------------------------------------------------------------------===//

class SampleTests : public PelotonTest {};

TEST_F(SampleTests, Test1) { EXPECT_EQ(3, 1 + 2); }

TEST_F(SampleTests, Test2) { EXPECT_NE(1, 1 + 2); }

}  // End test namespace
}  // End peloton namespace
