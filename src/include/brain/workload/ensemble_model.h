#pragma once

#include "brain/workload/base_tf.h"

namespace peloton{
namespace brain{
class TimeSeriesEnsemble {
 public:
  TimeSeriesEnsemble(std::vector<std::unique_ptr<BaseForecastModel>>& models,
                     std::vector<int> &num_epochs,
                     std::vector<float> &model_weights,
                     int batch_size);
  float Train(const matrix_eig &data);
  float Validate(const matrix_eig &data);
 private:
  std::vector<std::unique_ptr<BaseForecastModel>> models_;
  std::vector<int> num_epochs_;
  std::vector<float> model_weights_;
  // Pass TFModel's bsz here
  int batch_size_;
};
}
}
