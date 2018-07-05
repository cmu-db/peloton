//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eigen_util_test.cpp
//
// Identification: test/brain/eigen_util_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/eigen_util.h"
#include <algorithm>
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Eigen Util Tests
//===--------------------------------------------------------------------===//

class EigenUtilTests : public PelotonTest {};

TEST_F(EigenUtilTests, BasicEigenTest) {
  /**
   * Notes on Eigen:
   * 1. Don't use 'auto'!!
   */
  // Eigen Matrix
  matrix_eig m = matrix_eig::Random(2, 2);
  EXPECT_EQ(m.rows(), 2);
  EXPECT_EQ(m.cols(), 2);
  EXPECT_TRUE(m.IsRowMajor);
  // Eigen Vector
  vector_eig v = vector_eig::Random(2);
  EXPECT_EQ(v.rows(), 2);
  EXPECT_EQ(v.cols(), 1);
  // Transpose(if you try to store as `vec_eig` it will be 2x1)
  matrix_eig vT = v.transpose();
  EXPECT_EQ(vT.rows(), 1);
  EXPECT_EQ(vT.cols(), 2);
  // Matrix multiplication(1)
  vector_eig vTv = vT * v;
  EXPECT_EQ(vTv.rows(), 1);
  EXPECT_EQ(vTv.cols(), 1);
  // Matrix multiplication(2)
  matrix_eig vvT = v * vT;
  EXPECT_EQ(vvT.rows(), 2);
  EXPECT_EQ(vvT.cols(), 2);
  // Element-wise multiplication
  matrix_eig mvvT = m.array() * vvT.array();
  EXPECT_EQ(mvvT.rows(), 2);
  EXPECT_EQ(mvvT.cols(), 2);
  EXPECT_EQ(m(0, 0) * vvT(0, 0), mvvT(0, 0));
  EXPECT_EQ(m(0, 1) * vvT(0, 1), mvvT(0, 1));
  EXPECT_EQ(m(1, 0) * vvT(1, 0), mvvT(1, 0));
  EXPECT_EQ(m(1, 1) * vvT(1, 1), mvvT(1, 1));
}

TEST_F(EigenUtilTests, EigenMatToFromMatrixTConversionTest) {
  matrix_t matrix_simple = {
      {1, 2, 3},
      {4, 5, 6},
  };
  matrix_t matrix_simple_recon =
      brain::EigenUtil::ToMatrixT(brain::EigenUtil::ToEigenMat(matrix_simple));
  EXPECT_EQ(matrix_simple, matrix_simple_recon);
}

TEST_F(EigenUtilTests, EigenVecToFromVectorTConversionTest) {
  vector_t v = {1, 2, 3, 4};
  vector_t v_recon =
      brain::EigenUtil::ToVectorT(brain::EigenUtil::ToEigenVec(v));
  EXPECT_EQ(v, v_recon);
}

TEST_F(EigenUtilTests, FlattenTest) {
  matrix_t matrix_simple = {
      {1, 2, 3},
      {4, 5, 6},
  };
  matrix_eig matrix_simple_eig = brain::EigenUtil::ToEigenMat(matrix_simple);
  vector_t matrix_simple_flattened = {1, 2, 3, 4, 5, 6};
  // Flatten matrix_t -> vector_t
  EXPECT_EQ(brain::EigenUtil::Flatten(matrix_simple), matrix_simple_flattened);
  // Flatten matrix_eig -> vector_t
  EXPECT_EQ(brain::EigenUtil::Flatten(matrix_simple_eig),
            matrix_simple_flattened);

  // Flatten std::vector<matrix_eig> -> vector_t
  std::vector<matrix_eig> matrix_vecs = {
      brain::EigenUtil::ToEigenMat({matrix_simple[0]}),
      brain::EigenUtil::ToEigenMat({matrix_simple[1]}),
  };
  EXPECT_EQ(brain::EigenUtil::Flatten(matrix_vecs), matrix_simple_flattened);
}

TEST_F(EigenUtilTests, VStackTest) {
  matrix_eig m = brain::EigenUtil::ToEigenMat({
      {1, 2, 3},
      {4, 5, 6},
      {7, 8, 9},
  });
  matrix_eig m1 = brain::EigenUtil::ToEigenMat({
      {1, 2, 3},
      {4, 5, 6},
  });
  matrix_eig m2 = brain::EigenUtil::ToEigenMat({
      {7, 8, 9},
  });
  EXPECT_EQ(m, brain::EigenUtil::VStack({m1, m2}));
}

TEST_F(EigenUtilTests, PairwiseEuclideanDistTest) {
  matrix_eig m1 = brain::EigenUtil::ToEigenMat({
      {0, 1, 0},
      {1, 1, 1},
  });
  matrix_eig m2 = brain::EigenUtil::ToEigenMat({
      {1, 1, 0},
      {1, 1, 1},
      {1, 2, 0},
  });
  matrix_eig m_dist = brain::EigenUtil::ToEigenMat({
      {1.0, 1.41421, 1.41421},
      {1, 0, 1.41421},
  });
  EXPECT_TRUE(m_dist.isApprox(brain::EigenUtil::PairwiseEuclideanDist(m1, m2)));
}

TEST_F(EigenUtilTests, StandardDeviationTest1) {
  matrix_eig m = brain::EigenUtil::ToEigenMat({
      {0, 1, 0},
      {1, 1, 1},
      {2, 1, 3},
  });
  float expected_stdev = 0.8748897;
  EXPECT_FLOAT_EQ(expected_stdev, brain::EigenUtil::StandardDeviation(m));
}

TEST_F(EigenUtilTests, StandardDeviationTest2) {
  matrix_eig m = brain::EigenUtil::ToEigenMat({
      {0, 1, 0},
      {1, 1, 1},
      {2, 1, 3},
  });
  vector_eig expected_stdev =
      brain::EigenUtil::ToEigenVec({0.816496, 0.0, 1.247219});
  vector_eig stdev = brain::EigenUtil::StandardDeviation(m, 0);
  EXPECT_TRUE(expected_stdev.isApprox(stdev));
}

TEST_F(EigenUtilTests, PadTopTest) {
  matrix_eig m = brain::EigenUtil::ToEigenMat({
      {0, 1, 0},
      {1, 1, 1},
  });
  matrix_eig m_padded = brain::EigenUtil::ToEigenMat({
      {0, 0, 0},
      {0, 1, 0},
      {1, 1, 1},
  });
  EXPECT_EQ(m_padded, brain::EigenUtil::PadTop(m, 0, 1));
}

}  // namespace test
}  // namespace peloton
