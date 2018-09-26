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

#include <algorithm>
#include "brain/testing_forecast_util.h"
#include "brain/util/model_util.h"
#include "brain/workload/kernel_model.h"
#include "brain/workload/linear_model.h"
#include "brain/workload/lstm.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"
#include "util/file_util.h"

namespace peloton {
namespace test {
class ModelTests : public PelotonTest {};

TEST_F(ModelTests, NormalizerTest) {
  auto n = brain::Normalizer();
  matrix_eig X =
      brain::EigenUtil::ToEigenMat({{1, 2, 3}, {4, 5, 6}, {7, 8, 9}});
  n.Fit(X);
  matrix_eig Xrecon = n.ReverseTransform(n.Transform(X));
  EXPECT_TRUE(Xrecon.isApprox(X));
}

// Enable after resolving
TEST_F(ModelTests, DISABLED_TimeSeriesLSTMTest) {
  auto model = std::unique_ptr<brain::TimeSeriesLSTM>(new brain::TimeSeriesLSTM(
      brain::LSTMWorkloadDefaults::NFEATS,
      brain::LSTMWorkloadDefaults::NENCODED, brain::LSTMWorkloadDefaults::NHID,
      brain::LSTMWorkloadDefaults::NLAYERS, brain::LSTMWorkloadDefaults::LR,
      brain::LSTMWorkloadDefaults::DROPOUT_RATE,
      brain::LSTMWorkloadDefaults::CLIP_NORM,
      brain::LSTMWorkloadDefaults::BATCH_SIZE,
      brain::LSTMWorkloadDefaults::BPTT, brain::CommonWorkloadDefaults::HORIZON,
      brain::CommonWorkloadDefaults::INTERVAL,
      brain::LSTMWorkloadDefaults::EPOCHS));
  EXPECT_TRUE(model->IsTFModel());
  size_t LOG_INTERVAL = 20;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = false;
  float VAL_THESH = 0.05;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::SimpleSinusoidal,
                                    LOG_INTERVAL, NUM_SAMPLES, NUM_FEATS,
                                    VAL_SPLIT, NORMALIZE, VAL_THESH,
                                    brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                                    brain::CommonWorkloadDefaults::ESTOP_DELTA);
}

TEST_F(ModelTests, LinearRegTest) {
  auto model = std::unique_ptr<brain::TimeSeriesLinearReg>(
      new brain::TimeSeriesLinearReg(brain::LinearRegWorkloadDefaults::BPTT,
                                     brain::CommonWorkloadDefaults::HORIZON,
                                     brain::CommonWorkloadDefaults::INTERVAL));
  EXPECT_FALSE(model->IsTFModel());
  size_t LOG_INTERVAL = 1;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = true;
  float VAL_THESH = 0.1;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::NoisyLinear,
                                    LOG_INTERVAL, NUM_SAMPLES, NUM_FEATS,
                                    VAL_SPLIT, NORMALIZE, VAL_THESH,
                                    brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                                    brain::CommonWorkloadDefaults::ESTOP_DELTA);
}

TEST_F(ModelTests, KernelRegTest) {
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(brain::KernelRegWorkloadDefaults::BPTT,
                                     brain::CommonWorkloadDefaults::HORIZON,
                                     brain::CommonWorkloadDefaults::INTERVAL));
  EXPECT_FALSE(model->IsTFModel());
  size_t LOG_INTERVAL = 1;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = true;
  float VAL_THESH = 0.1;
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::NoisyLinear,
                                    LOG_INTERVAL, NUM_SAMPLES, NUM_FEATS,
                                    VAL_SPLIT, NORMALIZE, VAL_THESH,
                                    brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                                    brain::CommonWorkloadDefaults::ESTOP_DELTA);
}

TEST_F(ModelTests, DISABLED_TimeSeriesEnsembleTest) {
  auto lr_model = std::make_shared<brain::TimeSeriesLinearReg>(
      brain::LinearRegWorkloadDefaults::BPTT,
      brain::CommonWorkloadDefaults::HORIZON,
      brain::CommonWorkloadDefaults::INTERVAL);
  auto kr_model = std::make_shared<brain::TimeSeriesKernelReg>(
      brain::KernelRegWorkloadDefaults::BPTT,
      brain::CommonWorkloadDefaults::HORIZON,
      brain::CommonWorkloadDefaults::INTERVAL);
  auto lstm_model = std::make_shared<brain::TimeSeriesLSTM>(
      brain::LSTMWorkloadDefaults::NFEATS,
      brain::LSTMWorkloadDefaults::NENCODED, brain::LSTMWorkloadDefaults::NHID,
      brain::LSTMWorkloadDefaults::NLAYERS, brain::LSTMWorkloadDefaults::LR,
      brain::LSTMWorkloadDefaults::DROPOUT_RATE,
      brain::LSTMWorkloadDefaults::CLIP_NORM,
      brain::LSTMWorkloadDefaults::BATCH_SIZE,
      brain::LSTMWorkloadDefaults::BPTT, brain::CommonWorkloadDefaults::HORIZON,
      brain::CommonWorkloadDefaults::INTERVAL,
      brain::LSTMWorkloadDefaults::EPOCHS);

  size_t LOG_INTERVAL = 20;
  size_t NUM_SAMPLES = 1000;
  size_t NUM_FEATS = 3;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = false;
  float VAL_THESH = 0.06;

  auto model =
      std::unique_ptr<brain::TimeSeriesEnsemble>(new brain::TimeSeriesEnsemble(
          {lr_model, kr_model, lstm_model}, {0.33f, 0.33f, 0.33},
          brain::LSTMWorkloadDefaults::BATCH_SIZE));
  TestingForecastUtil::WorkloadTest(*model, WorkloadType::SimpleSinusoidal,
                                    LOG_INTERVAL, NUM_SAMPLES, NUM_FEATS,
                                    VAL_SPLIT, NORMALIZE, VAL_THESH,
                                    brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                                    brain::CommonWorkloadDefaults::ESTOP_DELTA);
}


}  // namespace test
}  // namespace peloton
