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

namespace peloton {
namespace brain {
RLSEModel::RLSEModel(size_t feat_len, double variance_init)
    : feat_len_(feat_len) {
  model_variance_ = matrix_eig::Zero(feat_len, feat_len);
  model_variance_.diagonal().array() += variance_init;
  weights_ = vector_eig::Zero(feat_len);
}

void RLSEModel::Update(const vector_eig &feat_vector, double true_val) {
  double err = Predict(feat_vector) - true_val;
  double gamma =
      1 + (feat_vector.transpose() * model_variance_).dot(feat_vector);
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