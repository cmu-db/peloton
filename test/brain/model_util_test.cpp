//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// model_util_test.cpp
//
// Identification: test/brain/model_util_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/model_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {
class ModelUtilTests : public PelotonTest {};

TEST_F(ModelUtilTests, MeanSquareErrorTest) {
  matrix_eig m1 = brain::EigenUtil::ToEigenMat({{1, 1, 0}, {0, 0, 1}});
  matrix_eig m2 = brain::EigenUtil::ToEigenMat({{0, 1, 0}, {1, 0, 1}});
  EXPECT_FLOAT_EQ(brain::ModelUtil::MeanSqError(m1, m1), 0.0);
  EXPECT_FLOAT_EQ(brain::ModelUtil::MeanSqError(m1, m2), 0.3333333);
}

// TODO: Add other Util tests after cleanup

}  // namespace test
}  // namespace peloton