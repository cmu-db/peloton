//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rlse.cpp
//
// Identification: src/brain/indextune/lspi/rlse.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/lspi/rlse.h"
#include <iostream>

namespace peloton {
namespace brain {
RLSEModel::RLSEModel(size_t feat_len, double variance_init, double reg_coeff, bool random_weights)
    : feat_len_(feat_len),
      reg_coeff_(reg_coeff) {
  model_variance_ = matrix_eig::Zero(feat_len, feat_len);
  model_variance_.diagonal().array() += variance_init;
  if (random_weights) {
    weights_ = vector_eig::Random(feat_len);
    float min_weight = weights_.minCoeff();
    float max_weight = weights_.maxCoeff();
    weights_ = 2*(weights_.array() - min_weight)/(max_weight - min_weight) - 1;
  } else {
    weights_ = vector_eig::Zero(feat_len);
  }

}

void RLSEModel::Update(const vector_eig &feat_vector, double true_val) {
  double err = Predict(feat_vector) - true_val;
  double gamma =
      reg_coeff_ + (feat_vector.transpose() * model_variance_).dot(feat_vector);
  matrix_eig H = model_variance_ * (1 / gamma);
  model_variance_ -= model_variance_ * feat_vector * (feat_vector.transpose()) *
                     model_variance_;
  weights_ -= (H * feat_vector) * err;
}

double RLSEModel::Predict(const vector_eig &feat_vector) const {
  return weights_.dot(feat_vector);
}
}  // namespace brain
}  // namespace peloton
