#pragma once

#include "brain/util/eigen_util.h"

/**
 * Least Squares Temporal-Differencing Estimator(LSTD(0))
 * References:
 *  [1] Cost Model Oblivious DB Tuning by Basu et. al.
 *  [2] Linear Least-Squares Algorithms for Temporal Difference Learning by Barto et. al.(Page 13)
 *  The Least Squares TD Estimator(based on the Recursive least squares formulation)
 *  provides an efficient way to evaluate the value function of a parameterized state.
 *  TODO(saatvik): The formula used below is a reproduction from the code of [1]. Some parts of
 *  the formulation don't match whats present in the literature. Might be worth revisiting.
 *  TODO(saatvik): Figure out a good way to test this.
**/

namespace peloton{
namespace brain{
class LSTDModel{
 public:
  explicit LSTDModel(size_t feat_len, double variance_init=1e-3, double gamma=0.9999);
  void Update(vector_eig state_feat_curr, vector_eig state_feat_next, double true_cost);
  double Predict(vector_eig state_feat);
 private:
  // feature length
  size_t feat_len_;
  // discounting-factor
  double gamma_;
  // model variance
  matrix_eig model_variance_;
  // parameters of model
  vector_eig weights_;
};
}
}