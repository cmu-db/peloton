//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tensorflow_test.cpp
//
// Identification: test/brain/tensorflow_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_util.h"
#include "brain/workload/lstm.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class TensorflowUtilTests : public PelotonTest {};

TEST_F(TensorflowUtilTests, DISABLED_BasicTFTest) {
  // Check that the tensorflow library imports and prints version info correctly
  EXPECT_TRUE(brain::TFUtil::GetTFVersion());
}

}  // namespace test
}  // namespace peloton
