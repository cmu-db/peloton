//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eigen_util.h
//
// Identification: src/include/brain/util/eigen_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>
#include "common/internal_types.h"



namespace peloton{
namespace brain{

class EigenUtil {
 public:
  /**
   * Fn to convert matrix_t type to matrix_r
   * @param mat: matrix_t matrix
   * @return the converted matrix_r matrix
   */
  static matrix_eig ToEigenMat(const matrix_t &mat);
  /**
   * Fn to convert matrix_r type to matrix_t.
   * @param mat: mratrix_r matrix
   * @return the converted matrix_t matrix
   */
  static matrix_t ToMatrixT(const matrix_eig &mat);
  static matrix_eig VStack(const std::vector<matrix_eig> &mat_vec);
  static matrix_eig PairwiseEuclideanDist(matrix_eig m1, matrix_eig m2);
  static vector_t Flatten(const matrix_eig &mat);
  static vector_t Flatten(const std::vector<matrix_eig> &mat);
  static vector_t Flatten(const matrix_t &mat);
};
}
}
