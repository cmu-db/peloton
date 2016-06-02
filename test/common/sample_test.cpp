//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample_test.cpp
//
// Identification: tests/common/sample_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Sample Test
//===--------------------------------------------------------------------===//

class SampleTest : public PelotonTest {};

TEST_F(SampleTest, Test1) { EXPECT_EQ(3, 1 + 2); }

TEST_F(SampleTest, Test2) { EXPECT_NE(1, 1 + 2); }

}  // End test namespace
}  // End peloton namespace
