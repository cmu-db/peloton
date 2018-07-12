//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lstd.h
//
// Identification: src/include/brain/indextune/lspi/lstd.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/util/eigen_util.h"

/**
 * LSTDQ Estimator
 * References:
 *  [1] Cost Model Oblivious DB Tuning by Basu et. al.
 *  [2] Least Squares Policy Iteration by M. Lagoudakis et. al.(Pg. 18) of JMLR article
 *  Good Resources: https://www2.cs.duke.edu/research/AI/LSPI/jmlr03.pdf,
 *  https://www.cs.utexas.edu/~pstone/Courses/394Rspring11/resources/week14a-lspi.pdf
 *  provides the LSTDQ-Opt formula which the authors in [1] seem to have used.
 *  LSTDQ provides a way of determining the Q value for a parametrized state-action pair given
 *  such a state-action for the current and previous timesteps along with associated "reward"(or cost as
 *  we see it here).
 *  TODO(saatvik): The formula used below is a reproduction from the code of
 *[1]. Some parts of the formulation don't match whats present in the
 *literature. Might be worth revisiting.
 *  TODO(saatvik): Figure out a good way to test this.
 **/

namespace peloton {
namespace brain {
class LSTDQModel {
 public:
  explicit LSTDQModel(size_t feat_len, double variance_init = 1e-3,
                     double gamma = 0.9999);
  void Update(const vector_eig &state_feat_curr,
              const vector_eig &state_feat_next, double true_cost);
  double Predict(const vector_eig &state_feat) const;

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
}  // namespace brain
}  // namespace peloton