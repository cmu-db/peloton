#pragma once

#include "brain/util/eigen_util.h"

/**
 * Recursive Least Squares Estimator:
 * References:
 *  [1] https://www.otexts.org/1582
 *  [2] Cost Model Oblivious DB Tuning by Basu et. al.
 * Used for efficiently estimating the immediate cost of executing
 * a query on a given configuration.
 * TODO(saatvik): The formula used below is a reproduction from the code of [2]. Some parts of
 * the formulation don't match whats present in the literature. Might be worth revisiting.
 */

namespace peloton{
namespace brain{
class RLSEModel{
 public:
  /**
   * Constructor for RLSE model: Initializes the
   * (1) Variance matrix
   * (2) Zero weight model params
   * Note that feature length must stay constant
   * Any changes to feature length will need model reinitialization
   * explicitly by the user
   */
  explicit RLSEModel(size_t feat_len, double variance_init=1e-3);
  /**
   * Update model weights
   * @param feat_vector: Feature vector(X) - Independent variables
   * For example in Index tuning this should represent the workload
   * and current Index config
   * @param true_val: Labels(y) - Dependent variable
   * For example in Index tuning this should represent the cost of
   * running the workload with the current Index config
   */
  void Update(const vector_eig& feat_vector, double true_val);
  /**
   * Predicts the dependent variable(y) given the independent variable(X)
   * @param feat_vector: X
   * @return: y
   */
  double Predict(const vector_eig& feat_vector) const;
 private:
  // feature length
  size_t feat_len_;
  // model variance
  matrix_eig model_variance_;
  // parameters of model
  vector_eig weights_;
};
}
}
