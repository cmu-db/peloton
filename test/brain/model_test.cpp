//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// linear_models_test.cpp
//
// Identification: test/brain/linear_model_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/linear_model.h"
#include "brain/workload/kernel_model.h"
#include "brain/workload/lstm.h"
#include "brain/workload/workload_defaults.h"
#include <algorithm>
#include "brain/util/model_util.h"
#include "common/harness.h"
#include "util/file_util.h"
#include "brain/testing_forecast_util.h"

namespace peloton {
namespace test {
class ModelTests : public PelotonTest {};

TEST_F(ModelTests, NormalizerTest) {
  auto n = brain::Normalizer();
  matrix_eig X = brain::EigenUtil::ToEigenMat({{1, 2, 3},
                                               {4, 5, 6},
                                               {7, 8, 9}});
  n.Fit(X);
  matrix_eig Xrecon = n.ReverseTransform(n.Transform(X));
  EXPECT_TRUE(Xrecon.isApprox(X));
}

TEST_F(ModelTests, TimeSeriesLSTMTest) {
  auto model = std::unique_ptr<brain::TimeSeriesLSTM>(new brain::TimeSeriesLSTM(
      brain::LSTMWorkloadDefaults::NFEATS,
      brain::LSTMWorkloadDefaults::NENCODED, brain::LSTMWorkloadDefaults::NHID,
      brain::LSTMWorkloadDefaults::NLAYERS, brain::LSTMWorkloadDefaults::LR,
      brain::LSTMWorkloadDefaults::DROPOUT_RATE,
      brain::LSTMWorkloadDefaults::CLIP_NORM,
      brain::LSTMWorkloadDefaults::BATCH_SIZE,
      brain::LSTMWorkloadDefaults::BPTT, brain::CommonWorkloadDefaults::HORIZON,
      brain::CommonWorkloadDefaults::INTERVAL));
  size_t MAX_EPOCHS = 100;
  size_t LOG_INTERVAL = 20;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = false;
  float VAL_THESH = 0.05;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::SimpleSinusoidal,
                                    MAX_EPOCHS, LOG_INTERVAL, NUM_SAMPLES,
                                    NUM_FEATS, VAL_SPLIT, NORMALIZE, VAL_THESH);
}

TEST_F(ModelTests, LinearRegTest) {
    auto model = std::unique_ptr<brain::TimeSeriesLinearReg>(
      new brain::TimeSeriesLinearReg(
          brain::LinearRegWorkloadDefaults::BPTT,
          brain::CommonWorkloadDefaults::HORIZON,
          brain::CommonWorkloadDefaults::INTERVAL));
  size_t MAX_EPOCHS = 1;
  size_t LOG_INTERVAL = 1;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = true;
  float VAL_THESH = 0.1;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::NoisyLinear,
                                    MAX_EPOCHS, LOG_INTERVAL, NUM_SAMPLES,
                                    NUM_FEATS, VAL_SPLIT, NORMALIZE, VAL_THESH);
}

TEST_F(ModelTests, KernelRegTest) {
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(
          brain::LinearRegWorkloadDefaults::BPTT,
          brain::CommonWorkloadDefaults::HORIZON,
          brain::CommonWorkloadDefaults::INTERVAL));
  size_t MAX_EPOCHS = 1;
  size_t LOG_INTERVAL = 1;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = true;
  float VAL_THESH = 0.1;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::NoisyLinear,
                                    MAX_EPOCHS, LOG_INTERVAL, NUM_SAMPLES,
                                    NUM_FEATS, VAL_SPLIT, NORMALIZE, VAL_THESH);
}
}  // namespace test
}  // namespace peloton