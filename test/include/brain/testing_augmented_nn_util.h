#pragma once

#include "common/internal_types.h"
#include "brain/util/eigen_util.h"
#include "brain/workload/base_tf.h"
#include "brain/selectivity/augmented_nn.h"

namespace peloton{
namespace test{

enum class DistributionType{ UniformDistribution, SkewedDistribution };

class TestingAugmentedNNUtil{
 public:
  static void Test(brain::AugmentedNN& model,
                   DistributionType d, size_t val_interval,
                   size_t num_samples = 1000,
                   float val_split = 0.5,
                   bool normalize = false,
                   float val_loss_thresh = 0.06,
                   size_t early_stop_patience = 10,
                   float early_stop_delta = 0.01);
// private:
  static matrix_eig GetData(DistributionType d,
                            size_t num_samples, size_t num_tests);
};


}
}
