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
#include <random>

namespace peloton {
namespace brain {

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

vector_eig EigenUtil::ToEigenVec(const vector_t &mat) {
  return vector_eig::Map(mat.data(), mat.size());
}

vector_t EigenUtil::ToVectorT(const vector_eig &mat) {
  return vector_t(mat.data(), mat.data() + mat.size());
}

matrix_eig EigenUtil::VStack(const std::vector<matrix_eig> &mat_vec) {
  PELOTON_ASSERT(!mat_vec.empty());
  if (mat_vec.size() == 1) {
    return mat_vec[0];
  }
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

matrix_eig EigenUtil::PairwiseEuclideanDist(const matrix_eig &m1,
                                            const matrix_eig &m2) {
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

matrix_eig EigenUtil::GaussianNoise(size_t rows, size_t cols, float mean,
                                    float stdev) {
  std::default_random_engine generator;
  std::normal_distribution<> distribution{mean, stdev};
  auto gaussian_sampler = [&](UNUSED_ATTRIBUTE float dummy) {
    return distribution(generator);
  };
  return matrix_eig::NullaryExpr(rows, cols, gaussian_sampler);
}

vector_eig EigenUtil::StandardDeviation(const matrix_eig &mat, uint8_t axis) {
  if (axis == 0) {
    matrix_eig sqdiff_mat =
        (mat.rowwise() - mat.colwise().mean()).array().square();
    vector_eig var_mat = sqdiff_mat.colwise().mean();
    return var_mat.cwiseSqrt();
  } else {
    throw "Not Implemented";
  }
}

float EigenUtil::StandardDeviation(const matrix_eig &mat) {
  matrix_eig sqdiff_mat = (mat.array() - mat.mean()).array().square();
  float var = sqdiff_mat.mean();
  return std::sqrt(var);
}

matrix_eig EigenUtil::PadTop(const matrix_eig &mat, float pad_value,
                             int num_rows) {
  int num_cols = mat.cols();
  matrix_eig pad_mat = matrix_eig::Ones(num_rows, num_cols) * pad_value;
  return EigenUtil::VStack({pad_mat, mat});
}

}  // namespace brain
}  // namespace peloton