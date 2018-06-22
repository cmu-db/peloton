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

#include "brain/workload/linear_models.h"
#include "brain/workload/workload_defaults.h"
#include <algorithm>
#include "brain/util/model_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {
class LinearModelTests : public PelotonTest {};

// Add a test for a linear workload

TEST_F(LinearModelTests, LinearRegSineWaveWorkloadTest) {
  // Sine Wave workload
  int NUM_SAMPLES = 1000;
  int NUM_WAVES = 3;
  matrix_eig data = matrix_eig::Zero(NUM_SAMPLES, NUM_WAVES);
  for (int i = 0; i < NUM_WAVES; i++) {
    data.col(i).setLinSpaced(NUM_SAMPLES, NUM_SAMPLES * i,
                             NUM_SAMPLES * (i + 1) - 1);
    if (i % 2 == 0) {
      data.col(i) = data.col(i).array().sin();
    } else {
      data.col(i) = data.col(i).array().cos();
    }
  }
  // Offsets for Splitting into Train/Test
  int split_point = NUM_SAMPLES / 2;

  // Split into train/test data
  // 0 -> end - train_offset
  matrix_eig train_data = data.topRows(split_point);

  // end - test_offset -> end
  matrix_eig test_data = data.bottomRows(split_point);

  // TimeSeriesLinearReg
  auto model = std::unique_ptr<brain::TimeSeriesLinearReg>(
      new brain::TimeSeriesLinearReg(
          brain::LinearRegWorkloadDefaults::REGRESSION_DIM,
          brain::CommonWorkloadDefaults::HORIZON,
          brain::CommonWorkloadDefaults::SEGMENT));

  LOG_INFO("Building Model: %s", model->ToString().c_str());
  float train_loss = model->TrainEpoch(train_data);
  LOG_DEBUG("Train Loss: %.10f", train_loss);
  matrix_eig _, __;
  float valid_loss = model->ValidateEpoch(test_data, _, __, false);
  LOG_DEBUG("Valid Loss: %.10f", valid_loss);

  // The loss is going down to 0.000005. Given that this is a linear
  // model trying to predict a sine wave - this is quite surprising!
  float LOSS_THRESH = 0.05;
  EXPECT_LE(train_loss, LOSS_THRESH);
  EXPECT_LE(valid_loss, LOSS_THRESH);
}

TEST_F(LinearModelTests, KernelRegSineWaveWorkloadTest) {
  // Sine Wave workload
  int NUM_SAMPLES = 1000;
  int NUM_WAVES = 3;
  matrix_eig data = matrix_eig::Zero(NUM_SAMPLES, NUM_WAVES);
  for (int i = 0; i < NUM_WAVES; i++) {
    data.col(i).setLinSpaced(NUM_SAMPLES, NUM_SAMPLES * i,
                             NUM_SAMPLES * (i + 1) - 1);
    if (i % 2 == 0) {
      data.col(i) = data.col(i).array().sin();
    } else {
      data.col(i) = data.col(i).array().cos();
    }
  }
  // Offsets for Splitting into Train/Test
  int split_point = NUM_SAMPLES / 2;

  // Split into train/test data
  // 0 -> end - train_offset
  matrix_eig train_data = data.topRows(split_point);

  // end - test_offset -> end
  matrix_eig test_data = data.bottomRows(split_point);

  // TimeSeriesLinearReg
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(
          brain::LinearRegWorkloadDefaults::REGRESSION_DIM,
          brain::CommonWorkloadDefaults::HORIZON,
          brain::CommonWorkloadDefaults::SEGMENT));

  LOG_INFO("Building Model: %s", model->ToString().c_str());
  float train_loss = model->TrainEpoch(train_data);
  LOG_DEBUG("Train Loss: %.10f", train_loss);
  matrix_eig _, __;
  float valid_loss = model->ValidateEpoch(test_data, _, __, false);
  LOG_DEBUG("Valid Loss: %.10f", valid_loss);
  //
  // The loss is going down to 0.000005. Given that this is a linear
  // model trying to predict a sine wave - this is quite surprising!
  float LOSS_THRESH = 0.05;
  EXPECT_LE(train_loss, LOSS_THRESH);
  EXPECT_LE(valid_loss, LOSS_THRESH);
}
}  // namespace test
}  // namespace peloton