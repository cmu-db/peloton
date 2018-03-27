//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tensorflow_util_test.cpp
//
// Identification: test/brain/tensorflow_util_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include "brain/tf_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class TensorflowTests : public PelotonTest {};

TEST_F(TensorflowTests, BasicTest) {
  // Check that the tensorflow library imports and prints version info correctly
  EXPECT_TRUE(brain::TFUtil::GetTFVersion());
}

}  // namespace test
}  // namespace peloton
