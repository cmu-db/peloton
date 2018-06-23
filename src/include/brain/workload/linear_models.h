//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// linear_models.h
//
// Identification: src/include/brain/workload/linear_models.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {
class TimeSeriesLinearReg : public BaseForecastModel {
 public:
  TimeSeriesLinearReg(int bptt, int horizon, int interval);
  void Fit(const matrix_eig &X, const matrix_eig &y,
           UNUSED_ATTRIBUTE int bsz = 1) override;
  matrix_eig Predict(const matrix_eig &X, int bsz = 1) const override;
  float TrainEpoch(const matrix_eig &data) override;
  float ValidateEpoch(const matrix_eig &data,
                      matrix_eig &test_true,
                      matrix_eig &test_pred,
                      bool return_preds) override;
  /**
   * @return std::string representing model object
   */
  std::string ToString() const override;

 private:
  std::vector<vector_eig> weights_;
};

class TimeSeriesKernelReg : public BaseForecastModel {
 public:
  TimeSeriesKernelReg(int bptt, int horizon, int interval);
  void Fit(const matrix_eig &X, const matrix_eig &y,
           int bsz = 1) override;
  matrix_eig Predict(const matrix_eig &X, int bsz = 1) const override;
  float TrainEpoch(const matrix_eig &data) override;
  float ValidateEpoch(const matrix_eig &data,
                      matrix_eig &test_true,
                      matrix_eig &test_pred,
                      bool return_preds) override;
  /**
   * @return std::string representing model object
   */
  std::string ToString() const override;

 private:
  matrix_eig kernel_x_;
  matrix_eig kernel_y_;
};
}  // namespace brain
}  // namespace peloton
