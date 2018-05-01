#include "brain/indextune/lspi/lstd.h"

namespace peloton{
namespace brain{
LSTDModel::LSTDModel(size_t feat_len, double variance_init, double gamma): feat_len_(feat_len),
                                                                        gamma_(gamma) {
  model_variance_ = matrix_eig::Zero(feat_len, feat_len);
  model_variance_.diagonal().array() += variance_init;
  weights_ = vector_eig::Zero(feat_len);
}

// TODO(saatvik): Recheck and better variable naming
void LSTDModel::Update(vector_eig state_feat_curr, vector_eig state_feat_next, double true_cost) {
  vector_eig var1 = state_feat_curr - state_feat_next*gamma_;
  double var2 = 1 + (var1.transpose()*model_variance_).dot(state_feat_curr);
  matrix_eig var3 = model_variance_*(state_feat_curr)*var1.transpose()*model_variance_;
  double epsilon = true_cost -  var1.dot(weights_);
  vector_eig error = model_variance_*state_feat_curr*(epsilon/var2);
  model_variance_ -= var3/var2;
  // TODO(saatvik): Log error here?
}

double LSTDModel::Predict(vector_eig state_feat) {
  return weights_.dot(state_feat);
}
}
}
