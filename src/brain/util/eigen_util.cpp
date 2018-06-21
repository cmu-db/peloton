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

matrix_eig EigenUtil::ToEigenMat(const matrix_t &mat) {
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

matrix_t EigenUtil::ToMatrixT(const matrix_eig &mat) {
  matrix_t out_mat;
  out_mat.resize(mat.rows());
  auto data = mat.data();

  for (int i = 0; i < mat.rows(); i++) {
    out_mat[i] = std::vector<float>(data, data + mat.cols());
    data += mat.cols();
  }

  return out_mat;
}

matrix_eig EigenUtil::VStack(const std::vector<matrix_eig> &mat_vec) {
  PELOTON_ASSERT(!mat_vec.empty());
  long num_cols = mat_vec[0].cols();
  size_t num_rows = 0;
  for (size_t mat_idx = 0; mat_idx < mat_vec.size(); ++mat_idx) {
    PELOTON_ASSERT(mat_vec[mat_idx].cols() == num_cols);
    num_rows += mat_vec[mat_idx].rows();
  }
  matrix_eig vstacked_mat(num_rows, num_cols);
  size_t row_offset = 0;
  for (size_t mat_idx = 0; mat_idx < mat_vec.size(); ++mat_idx) {
    long cur_rows = mat_vec[mat_idx].rows();
    vstacked_mat.middleRows(row_offset, cur_rows) = mat_vec[mat_idx];
    row_offset += cur_rows;
  }
  return vstacked_mat;
}

matrix_eig EigenUtil::PairwiseEuclideanDist(matrix_eig m1, matrix_eig m2) {
  matrix_eig m_dist(m1.rows(), m2.rows());
  for (int i = 0; i < m1.rows(); i++) {
    for (int j = 0; j < m2.rows(); j++) {
      m_dist(i, j) = (m1.row(i) - m2.row(j)).norm();
    }
  }
  return m_dist;
}

vector_t EigenUtil::Flatten(const matrix_eig &mat) {
  return std::vector<float>(mat.data(), mat.data() + mat.size());
}

vector_t EigenUtil::Flatten(const std::vector<matrix_eig> &mat_vect) {
  std::vector<float> flattened_mat;
  for (auto &mat : mat_vect) {
    flattened_mat.insert(flattened_mat.end(), mat.data(),
                         mat.data() + mat.size());
  }
  return flattened_mat;
}

vector_t EigenUtil::Flatten(const matrix_t &mat) {
  vector_t flattened_mat;
  for (auto &mat_row : mat) {
    flattened_mat.insert(flattened_mat.end(), mat_row.begin(), mat_row.end());
  }
  return flattened_mat;
}

}  // namespace brain
}