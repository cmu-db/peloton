#pragma once

#include "brain/util/eigen_util.h"

namespace peloton{
namespace brain{
class RLSEModel{
 public:
  explicit RLSEModel(int feat_len, double variance_init=1e-3);
  void Update(vector_eig feat_vector, double true_val);
  double Predict(vector_eig feat_vector);
 private:
  int feat_len_;
  matrix_eig model_variance_;
  vector_eig weights_;
};
}
}
