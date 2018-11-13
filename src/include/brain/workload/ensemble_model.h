//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ensemble_model.h
//
// Identification: src/include/brain/workload/ensemble_model.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {
class TimeSeriesEnsemble {
 public:
  TimeSeriesEnsemble(std::vector<std::shared_ptr<BaseForecastModel>> models,
                     const std::vector<float> &model_weights, int batch_size);
  float Validate(const matrix_eig &data);
  BaseForecastModel &GetModel(size_t idx);
  size_t ModelsSize() const;

 private:
  std::vector<std::shared_ptr<BaseForecastModel>> models_;
  int batch_size_;
  const vector_t model_weights_;
  // TODO(saatviks): Pass TFModel's bsz here(Find better way of handling this)
};
}  // namespace brain
}  // namespace peloton
