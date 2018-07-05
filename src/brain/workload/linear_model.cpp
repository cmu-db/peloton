//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// linear_model.cpp
//
// Identification: src/brain/workload/linear_model.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/linear_model.h"
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

void TimeSeriesLinearReg::Fit(const matrix_eig &X, const matrix_eig &y,
                              UNUSED_ATTRIBUTE int bsz) {
  matrix_eig X_proc;
  ModelUtil::GenerateFeatureMatrix(*this, X, X_proc);
  matrix_eig XTX = X_proc.transpose() * X_proc;
  XTX += matrix_eig::Identity(XTX.rows(), XTX.rows()) * epsilon_;
  XTX = (XTX.inverse() * (X_proc.transpose()));
  matrix_eig y_hat(y.rows(), y.cols());
  for (long label_idx = 0; label_idx < y.cols(); label_idx++) {
    weights_.emplace_back(XTX * y.col(label_idx));
  }
}

matrix_eig TimeSeriesLinearReg::Predict(const matrix_eig &X,
                                        UNUSED_ATTRIBUTE int bsz) const {
  matrix_eig X_proc;
  ModelUtil::GenerateFeatureMatrix(*this, X, X_proc);
  matrix_eig y_hat(X_proc.rows(), weights_.size());
  for (long label_idx = 0; label_idx < y_hat.cols(); label_idx++) {
    y_hat.col(label_idx) = (X_proc * weights_[label_idx]).transpose();
  }
  return y_hat;
}

float TimeSeriesLinearReg::TrainEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::FeatureLabelSplit(*this, data, X, y);
  Fit(X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesLinearReg::ValidateEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::FeatureLabelSplit(*this, data, X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

}  // namespace brain
}  // namespace peloton