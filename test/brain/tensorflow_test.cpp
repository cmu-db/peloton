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

// define needed to tell Eigen to use Row-major only
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_util.h"
#include "common/harness.h"
#include "util/file_util.h"
#include "brain/util/eigen_util.h"
#include <numeric>
#include "brain/workload/lstm_tf.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class TensorflowTests : public PelotonTest {};

TEST_F(TensorflowTests, BasicTFTest) {
  // Check that the tensorflow library imports and prints version info correctly
  EXPECT_TRUE(brain::TFUtil::GetTFVersion());
  // Check that the model is indeed generated
  auto lstm_model_file =
      FileUtil::GetRelativeToRootPath("/src/brain/modelgen/LSTM.pb");
  EXPECT_TRUE(FileUtil::Exists(lstm_model_file));
}

TEST_F(TensorflowTests, TFSessionEntityTest) {
  auto tf = brain::TfSessionEntity<float, float>();
  EXPECT_TRUE(tf.IsStatusOk());
  auto lstm_model_file =
      FileUtil::GetRelativeToRootPath("/src/brain/modelgen/LSTM.pb");
  // Import a graph and check that everything worked correctly
  tf.ImportGraph(lstm_model_file);
  EXPECT_TRUE(tf.IsStatusOk());
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
  std::string path_to_settings = FileUtil::GetRelativeToRootPath("/src/brain/modelgen/settings.json");
  matrix_eig data = matrix_eig::Zero(NUM_SAMPLES, NUM_WAVES);
  for(int i = 0; i < NUM_WAVES; i++) {
    data.col(i).setLinSpaced(NUM_SAMPLES, NUM_SAMPLES*i, NUM_SAMPLES*(i + 1) - 1);
    data.col(i) = data.col(i).array().sin();
  }

  // Load model/problem specific settings
  auto settings = FileUtil::LoadJSON(FileUtil::GetRelativeToRootPath("/src/brain/modelgen/settings.json"));

  // Offsets for Splitting into Train/Test
  int split_point = NUM_SAMPLES/2;

  // Split into train/test data
  // 0 -> end - train_offset
  matrix_eig train_data = Eigen::Map<matrix_eig>(
      data.data(), data.rows() - split_point, data.cols());

  // end - test_offset -> end
  matrix_eig test_data = Eigen::Map<matrix_eig>(
      data.data() + (data.rows() - split_point) * data.cols(), split_point,
      data.cols());

  // // Seq2Seq LSTM
  float clip_norm = settings.get<float>("models.LSTM.clip_norm");
  float dropout_rate = settings.get<float>("models.LSTM.dropout_ratio");
  int batch_size = settings.get<int>("models.LSTM.batch_size");
  float lr = settings.get<float>("models.LSTM.lr");
  std::string path_to_model = FileUtil::GetRelativeToRootPath("/src/brain/modelgen/LSTM.pb");
  int LOG_INTERVAL = 20;
  // 800 epochs is too much for a test
  int epochs = 100;
  auto model = peloton::brain::Seq2SeqLSTM(
      path_to_model, lr, dropout_rate, clip_norm, batch_size,
      settings.get<int>("horizon"), settings.get<int>("models.LSTM.bptt"),
      settings.get<int>("segment"));

  // Initialize model
  model.TFInit();

  // Not applying Normalization, since sine waves are [-1, 1]

  // Variables which will hold true/prediction values
  matrix_eig y, y_hat;

  Eigen::VectorXf train_loss_avg = Eigen::VectorXf::Zero(LOG_INTERVAL);
  float prev_train_loss = 10.0;
  for (int epoch = 1; epoch <= epochs; epoch++) {
    auto train_loss = model.TrainEpoch(train_data);
    int idx = (epoch - 1) % LOG_INTERVAL;
    train_loss_avg(idx) = train_loss;
    if(epoch % LOG_INTERVAL == 0) {
      UNUSED_ATTRIBUTE auto val_loss = model.ValidateEpoch(test_data, y, y_hat, false);
      train_loss = train_loss_avg.mean();
      // Below check is not advisable - one off failure chance
      // EXPECT_LE(val_loss, prev_valid_loss);
      // An average on the other hand should surely pass
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.5f, Valid Loss: %.5f",
                train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }


}

}  // namespace test
}  // namespace peloton
