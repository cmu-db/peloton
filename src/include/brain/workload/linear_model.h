//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// linear_model.h
//
// Identification: src/include/brain/workload/linear_model.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {
class TimeSeriesLinearReg : public virtual BaseForecastModel {
 public:
  TimeSeriesLinearReg(int bptt, int horizon, int interval);
  void Fit(const matrix_eig &X, const matrix_eig &y,
           UNUSED_ATTRIBUTE int bsz = 1) override;
  matrix_eig Predict(const matrix_eig &X, int bsz = 1) const override;
  float TrainEpoch(const matrix_eig &data) override;
  float ValidateEpoch(const matrix_eig &data) override;
  /**
   * @return std::string representing model object
   */
  std::string ToString() const override;

 private:
  std::vector<vector_eig> weights_;
  float epsilon_ = 1e-3;
};
}  // namespace brain
}  // namespace peloton
