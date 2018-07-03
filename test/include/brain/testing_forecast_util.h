#pragma once

#include "common/internal_types.h"
#include "brain/util/eigen_util.h"
#include "brain/workload/base_tf.h"
#include "brain/workload/ensemble_model.h"

namespace peloton{
namespace test{

enum class WorkloadType{ SimpleSinusoidal, NoisySinusoidal, SimpleLinear, NoisyLinear };

class TestingForecastUtil{
 public:
  static void WorkloadTest(brain::BaseForecastModel& model,
                           WorkloadType w, size_t val_interval,
                           size_t num_samples = 1000,
                           size_t num_feats = 3, float val_split = 0.5,
                           bool normalize = false,
                           float val_loss_thresh = 0.06,
                           size_t early_stop_patience = 10,
                           float early_stop_delta = 0.01);
  static void WorkloadTest(brain::TimeSeriesEnsemble& model,
                           WorkloadType w, size_t val_interval,
                           size_t num_samples = 1000,
                           size_t num_feats = 3, float val_split = 0.5,
                           bool normalize = false,
                           float val_loss_thresh = 0.06,
                           size_t early_stop_patience = 10,
                           float early_stop_delta = 0.01);
 private:
  static matrix_eig GetWorkload(WorkloadType w,
                                size_t num_samples,
                                size_t num_feats);
};

}
}