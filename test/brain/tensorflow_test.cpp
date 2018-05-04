//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tensorflow_test.cpp
//
// Identification: test/brain/tensorflow_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>
#include "brain/util/eigen_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_util.h"
#include "brain/workload/lstm_tf.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class TensorflowTests : public PelotonTest {};

TEST_F(TensorflowTests, BasicTFTest) {
  // Check that the tensorflow library imports and prints version info correctly
  EXPECT_TRUE(brain::TFUtil::GetTFVersion());
}

TEST_F(TensorflowTests, BasicEigenTest) {
  Eigen::MatrixXd m = Eigen::MatrixXd::Random(2, 2);
  EXPECT_EQ(m.rows(), 2);
  EXPECT_EQ(m.cols(), 2);
  EXPECT_TRUE(m.IsRowMajor);
}

TEST_F(TensorflowTests, SineWavePredictionTest) {
  // Sine Wave prediction test works here
  int NUM_SAMPLES = 1000;
  int NUM_WAVES = 3;
  matrix_eig data = matrix_eig::Zero(NUM_SAMPLES, NUM_WAVES);
  for (int i = 0; i < NUM_WAVES; i++) {
    data.col(i).setLinSpaced(NUM_SAMPLES, NUM_SAMPLES * i,
                             NUM_SAMPLES * (i + 1) - 1);
    data.col(i) = data.col(i).array().sin();
  }

  // Offsets for Splitting into Train/Test
  int split_point = NUM_SAMPLES / 2;

  // Split into train/test data
  // 0 -> end - train_offset
  matrix_eig train_data = Eigen::Map<matrix_eig>(
      data.data(), data.rows() - split_point, data.cols());

  // end - test_offset -> end
  matrix_eig test_data = Eigen::Map<matrix_eig>(
      data.data() + (data.rows() - split_point) * data.cols(), split_point,
      data.cols());

  // TimeSeriesLSTM
  int LOG_INTERVAL = 20;
  int epochs = 100;
  auto model = std::unique_ptr<brain::TimeSeriesLSTM>(new brain::TimeSeriesLSTM(
      brain::LSTMWorkloadDefaults::NFEATS,
      brain::LSTMWorkloadDefaults::NENCODED, brain::LSTMWorkloadDefaults::NHID,
      brain::LSTMWorkloadDefaults::NLAYERS, brain::LSTMWorkloadDefaults::LR,
      brain::LSTMWorkloadDefaults::DROPOUT_RATE,
      brain::LSTMWorkloadDefaults::CLIP_NORM,
      brain::LSTMWorkloadDefaults::BATCH_SIZE,
      brain::LSTMWorkloadDefaults::HORIZON, brain::LSTMWorkloadDefaults::BPTT,
      brain::LSTMWorkloadDefaults::SEGMENT));

  // Check that model file has indeed generated
  EXPECT_TRUE(FileUtil::Exists(
      FileUtil::GetRelativeToRootPath("src/brain/modelgen/LSTM.pb")));

  // Initialize model
  model->TFInit();

  // Not applying Normalization, since sine waves are [-1, 1]

  // Variables which will hold true/prediction values
  matrix_eig y, y_hat;

  Eigen::VectorXf train_loss_avg = Eigen::VectorXf::Zero(LOG_INTERVAL);
  float prev_train_loss = 10.0;
  const float VAL_LOSS_THRESH = 0.06;
  float val_loss = VAL_LOSS_THRESH * 2;
  for (int epoch = 1; epoch <= epochs; epoch++) {
    auto train_loss = model->TrainEpoch(train_data);
    int idx = (epoch - 1) % LOG_INTERVAL;
    train_loss_avg(idx) = train_loss;
    if (epoch % LOG_INTERVAL == 0) {
      val_loss = model->ValidateEpoch(test_data, y, y_hat, false);
      train_loss = train_loss_avg.mean();
      // Below check is not advisable - one off failure chance
      // EXPECT_LE(val_loss, prev_valid_loss);
      // An average on the other hand should surely pass
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.5f, Valid Loss: %.5f", train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }
  EXPECT_LE(val_loss, VAL_LOSS_THRESH);
}

}  // namespace test
}  // namespace peloton
