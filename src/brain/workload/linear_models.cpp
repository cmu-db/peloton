//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// linear_models.cpp
//
// Identification: src/brain/workload/linear_models.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/linear_models.h"
#include "brain/util/model_util.h"

namespace peloton {
namespace brain {

/**
 * Linear Regression model
 **/

TimeSeriesLinearReg::TimeSeriesLinearReg(int bptt, int horizon, int interval)
    : BaseForecastModel(bptt, horizon, interval) {}

std::string TimeSeriesLinearReg::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesLinearReg(";
  model_str_builder << "bptt = " << bptt_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", interval = " << interval_;
  model_str_builder << ")";
  return model_str_builder.str();
}

void TimeSeriesLinearReg::Fit(const matrix_eig &X,
                              const matrix_eig &y,
                              UNUSED_ATTRIBUTE int bsz) {
  matrix_eig XTX = X.transpose() * X;
  XTX += matrix_eig::Identity(XTX.rows(), XTX.rows());
  XTX = (XTX.inverse() * (X.transpose()));
  matrix_eig y_hat(y.rows(), y.cols());
  for (long label_idx = 0; label_idx < y.cols(); label_idx++) {
    weights_.emplace_back(XTX * y.col(label_idx));
  }
}

matrix_eig TimeSeriesLinearReg::Predict(const matrix_eig &X,
                                        UNUSED_ATTRIBUTE int bsz) const {
  matrix_eig y_hat(X.rows(), weights_.size());
  for (long label_idx = 0; label_idx < y_hat.cols(); label_idx++) {
    y_hat.col(label_idx) = (X * weights_[label_idx]).transpose();
  }
  return y_hat;
}

float TimeSeriesLinearReg::TrainEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, X, y);
  Fit(X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesLinearReg::ValidateEpoch(const matrix_eig &data,
                                         matrix_eig &test_true,
                                         matrix_eig &test_pred,
                                         bool return_preds) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, X, y);
  matrix_eig y_hat = Predict(X);
  if (return_preds) {
    test_true = y;
    test_pred = y_hat;
  }
  return ModelUtil::MeanSqError(y, y_hat);
}

/**
 * Kernel Regression model
 **/
TimeSeriesKernelReg::TimeSeriesKernelReg(int bptt, int horizon, int interval)
    : BaseForecastModel(bptt, horizon, interval) {}

std::string TimeSeriesKernelReg::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesKernelReg(";
  model_str_builder << "bptt = " << bptt_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", interval = " << interval_;
  model_str_builder << ")";
  return model_str_builder.str();
}

void TimeSeriesKernelReg::Fit(const matrix_eig &X,
                              const matrix_eig &y,
                              UNUSED_ATTRIBUTE int bsz) {
  kernel_x_ = X;
  kernel_y_ = y;
}

matrix_eig TimeSeriesKernelReg::Predict(const matrix_eig &X,
                                        UNUSED_ATTRIBUTE int bsz) const {
  matrix_eig kernel =
      (-EigenUtil::PairwiseEuclideanDist(X, kernel_x_)).array().exp();
  // Eigen doesnt auto-broadcast on division
  matrix_eig norm = kernel.rowwise().sum().replicate(1, kernel_y_.cols());
  matrix_eig y_hat = (kernel * kernel_y_).array() / (norm.array());
  return y_hat;
}

float TimeSeriesKernelReg::TrainEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, X, y);
  Fit(X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesKernelReg::ValidateEpoch(const matrix_eig &data,
                                         matrix_eig &test_true,
                                         matrix_eig &test_pred,
                                         bool return_preds) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, X, y);
  matrix_eig y_hat = Predict(X);
  if(return_preds) {
    test_true = y;
    test_pred = y_hat;
  }
  return ModelUtil::MeanSqError(y, y_hat);
}

}  // namespace brain
}  // namespace peloton