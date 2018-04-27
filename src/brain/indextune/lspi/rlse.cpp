#include "brain/indextune/lspi/rlse.h"

namespace peloton{
namespace brain{
RLSEModel::RLSEModel(int feat_len, double variance_init): feat_len_(feat_len) {
  model_variance_ = matrix_eig::Zero(feat_len, feat_len);
  model_variance_.diagonal().array() += variance_init;
  weights_ = vector_eig::Zero(feat_len);
}

void RLSEModel::Update(vector_eig feat_vector, double true_val) {
  double err = Predict(feat_vector) - true_val;
  double gamma = 1 + (feat_vector.transpose()*model_variance_).dot(feat_vector);
  matrix_eig H = model_variance_*(1/gamma);
  model_variance_ -= model_variance_*feat_vector*(feat_vector.transpose())*model_variance_;
  weights_ -= (H*feat_vector)*err;
}

double RLSEModel::Predict(vector_eig feat_vector) {
  return weights_.dot(feat_vector);
}
}
}
