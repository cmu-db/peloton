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

TimeSeriesLinearReg::TimeSeriesLinearReg(int regress_dim, int horizon,
                                         int segment)
    : BaseForecastModel(horizon, segment), regress_dim_(regress_dim) {}

std::string TimeSeriesLinearReg::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesLinearReg(";
  model_str_builder << "regress_dim = " << regress_dim_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", segment = " << segment_;
  model_str_builder << ")";
  return model_str_builder.str();
}

float TimeSeriesLinearReg::TrainEpoch(matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, regress_dim_, X, y);
  matrix_eig XTX = X.transpose() * X;
  XTX += matrix_eig::Identity(XTX.rows(), XTX.rows());
  XTX = (XTX.inverse() * (X.transpose()));
  matrix_eig y_hat(y.rows(), y.cols());
  for (long label_idx = 0; label_idx < y.cols(); label_idx++) {
    weights_.emplace_back(XTX * y.col(label_idx));
    y_hat.col(label_idx) = (X * weights_[label_idx]).transpose();
  }
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesLinearReg::ValidateEpoch(matrix_eig &data,
                                         matrix_eig &test_true,
                                         matrix_eig &test_pred,
                                         bool return_preds) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, regress_dim_, X, y);
  matrix_eig y_hat(y.rows(), y.cols());
  for (long label_idx = 0; label_idx < y.cols(); label_idx++) {
    y_hat.col(label_idx) = (X * weights_[label_idx]).transpose();
  }
  if (return_preds) {
    test_true = y;
    test_pred = y_hat;
  }
  return ModelUtil::MeanSqError(y, y_hat);
}

/**
 * Kernel Regression model
 **/
TimeSeriesKernelReg::TimeSeriesKernelReg(int regress_dim, int horizon,
                                         int segment)
    : BaseForecastModel(horizon, segment), regress_dim_(regress_dim) {}

std::string TimeSeriesKernelReg::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesKernelReg(";
  model_str_builder << "regress_dim = " << regress_dim_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", segment = " << segment_;
  model_str_builder << ")";
  return model_str_builder.str();
}

float TimeSeriesKernelReg::TrainEpoch(matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, regress_dim_, X, y);
  kernel_x_ = X;
  kernel_y_ = y;
  matrix_eig kernel =
      (-EigenUtil::PairwiseEuclideanDist(X, kernel_x_)).array().exp();
  // Eigen doesnt auto-broadcast on division
  matrix_eig norm = kernel.rowwise().sum().replicate(1, y.cols());
  matrix_eig y_hat = (kernel * y).array() / (norm.array());
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesKernelReg::ValidateEpoch(matrix_eig &data,
                                         matrix_eig &test_true,
                                         matrix_eig &test_pred,
                                         bool return_preds) {
  matrix_eig X, y;
  ModelUtil::GenerateFeatureMatrix(this, data, regress_dim_, X, y);
  kernel_x_ = X;
  kernel_y_ = y;
  matrix_eig kernel =
      (-EigenUtil::PairwiseEuclideanDist(X, kernel_x_)).array().exp();
  // Eigen doesnt auto-broadcast on division
  matrix_eig norm = kernel.rowwise().sum().replicate(1, y.cols());
  matrix_eig y_hat = (kernel * y).array() / (norm.array());
  if (return_preds) {
    test_true = y;
    test_pred = y_hat;
  }
  return ModelUtil::MeanSqError(y, y_hat);
}

}  // namespace brain
}  // namespace peloton