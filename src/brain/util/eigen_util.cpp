//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eigen_util.cpp
//
// Identification: src/brain/util/eigen_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/eigen_util.h"

namespace peloton{
namespace brain{

/**
 * Fn to convert matrix_t type to matrix_r
 * @param mat: matrix_t matrix
 * @return the converted matrix_r matrix
 */
matrix_eig EigenUtil::MatrixTToEigenMat(matrix_t &mat) {
  std::vector<float> mat_flat;

  uint rows = 0;
  for (auto &vec : mat) {
    for (auto el : vec) {
      mat_flat.push_back(el);
    }
    ++rows;
  }

  return Eigen::Map<matrix_eig>(mat_flat.data(), rows, mat_flat.size() / rows);
}

/**
 * Fn to convert matrix_r type to matrix_t.
 * @param mat: mratrix_r matrix
 * @return the converted matrix_t matrix
 */
matrix_t EigenUtil::EigenMatToMatrixT(matrix_eig &mat) {
  matrix_t out_mat;
  out_mat.resize(mat.rows());
  auto data = mat.data();

  for (int i = 0; i < mat.rows(); i++) {
    out_mat[i] = std::vector<float>(data, data + mat.cols());
    data += mat.cols();
  }

  return out_mat;
}

std::vector<float> EigenUtil::FlattenMatrix(const matrix_eig &mat) {
  return std::vector<float>(mat.data(), mat.data() + mat.size());
}

}
}