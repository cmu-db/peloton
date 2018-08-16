
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
#include "brain/workload/AugmentedNN_tf.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"
#include "util/file_util.h"
#include "stdlib.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class TensorflowTests : public PelotonTest {};

// TODO: Enable this test once tensorflow package supports Python 3.7 (#1448)
TEST_F(TensorflowTests, DISABLED_BasicTFTest) {
  // Check that the tensorflow library imports and prints version info correctly
  EXPECT_TRUE(brain::TFUtil::GetTFVersion());
}

TEST_F(TensorflowTests, BasicEigenTest) {
  Eigen::MatrixXd m = Eigen::MatrixXd::Random(2, 2);
  EXPECT_EQ(m.rows(), 2);
  EXPECT_EQ(m.cols(), 2);
  EXPECT_TRUE(m.IsRowMajor);
}

TEST_F(TensorflowTests, AugmentedNNUniformTest) {
  // generate uniform dataset
  int NUM_X = 1000;
  matrix_eig hist = matrix_eig::Zero(NUM_X + 1, 1);
  matrix_eig sum = matrix_eig::Zero(NUM_X + 1, 1);
  float sum_hist = 0;
  for (int i = 1; i <= NUM_X; i++) {
    hist(i, 0) = 100;
  }

  for (int i = 1; i <= NUM_X; i++) {
    sum(i, 0) = sum(i - 1, 0) + hist(i, 0);
  }
  sum_hist = sum(NUM_X, 0);
  
  // generate training and validating data randomly
  int NUM_QUERIES = 10000;
  matrix_eig data = matrix_eig::Zero(NUM_QUERIES, 3);
  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> dist(1, NUM_X);

  // data: [lowerbound, upperbound, truth selectivity]
  for (int i = 0; i < NUM_QUERIES; i++) {
    int l = dist(rng);
    int u = dist(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    // assume the max&min values of the col are known
    // so here preprocessing([min,max]->[-1,1]) can be done
    data(i, 0) = (2 / (float)NUM_X) * l - 1;
    data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    data(i, 2) = sel;
  }
  // Offsets for Splitting into Train/Test
  int split_point = NUM_QUERIES / 2;

  // Split into train/validate data
  // 0 -> end - train_offset
  matrix_eig train_data = Eigen::Map<matrix_eig>(
      data.data(), data.rows() - split_point, data.cols());

  // end - validate_offset -> end
  matrix_eig validate_data = Eigen::Map<matrix_eig>(
      data.data() + (data.rows() - split_point) * data.cols(), split_point,
      data.cols());

  float HIGH_SEL = 0.8;
  float LOW_SEL = 0.2;
  int NUM_TESTS = brain::AugmentedNNWorkloadDefaults::BATCH_SIZE;

  matrix_eig test_random_data = matrix_eig::Zero(NUM_TESTS, 3);
  matrix_eig test_highsel_data = matrix_eig::Zero(NUM_TESTS, 3);
  matrix_eig test_lowsel_data = matrix_eig::Zero(NUM_TESTS, 3);

  // generate test data with high selectivity
  for (int i = 0; i < NUM_TESTS; i++) {
    int l, u;
    float sel;
    do {
      l = dist(rng);
      u = dist(rng);
      if (l > u) {
        std::swap(l, u);
      }
      sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    } while(sel <= HIGH_SEL);
    test_highsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_highsel_data(i, 1) = (2 / (float)NUM_X) * u - 1;
    test_highsel_data(i, 2) = sel;
  }

  // generate test data with low selectivity
  for (int i = 0; i < NUM_TESTS; i++) {
    int l, u;
    float sel;
    do {
      l = dist(rng);
      u = dist(rng);
      if (l > u) {
        std::swap(l, u);
      }
      sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    } while(sel >= LOW_SEL);
    test_lowsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_lowsel_data(i, 1) = (2 / (float)NUM_X) * u - 1;
    test_lowsel_data(i, 2) = sel;
  }

  // generate test data with random selectivity
  for (int i = 0; i < NUM_TESTS; i++) {
    int l = dist(rng);
    int u = dist(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l - 1,0)) / sum_hist;
    test_random_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_random_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    test_random_data(i, 2) = sel;
  }

  auto model = std::unique_ptr<brain::AugmentedNN>(new brain::AugmentedNN(
      brain::AugmentedNNWorkloadDefaults::NCOL,
      brain::AugmentedNNWorkloadDefaults::ORDER,
      brain::AugmentedNNWorkloadDefaults::NNEURON,
      brain::AugmentedNNWorkloadDefaults::LR,
      brain::AugmentedNNWorkloadDefaults::BATCH_SIZE));
  
  EXPECT_TRUE(FileUtil::Exists(
      FileUtil::GetRelativeToRootPath("src/brain/modelgen/AugmentedNN.pb")));

  model->TFInit();

  int LOG_INTERVAL = 20;
  int epochs = 400;
  // Variables which will hold true/prediction values
  matrix_eig y, y_hat;

  Eigen::VectorXf train_loss_avg = Eigen::VectorXf::Zero(LOG_INTERVAL);
  float prev_train_loss = 10.0;
  const float VAL_LOSS_THRESH = 0.06;
  float val_loss = VAL_LOSS_THRESH * 2;

  for (int epoch = 1; epoch <= epochs; epoch++) {
    auto train_loss = model->TrainEpoch(train_data); // returns a training loss
    int idx = (epoch - 1) % LOG_INTERVAL;
    train_loss_avg(idx) = train_loss; // insert the loss into Vector
    if (epoch % LOG_INTERVAL == 0) {
      val_loss = model->ValidateEpoch(validate_data, y, y_hat, false); // returns a validating loss
      train_loss = train_loss_avg.mean();
      // Below check is not advisable - one off failure chance
      // EXPECT_LE(val_loss, prev_valid_loss);
      // An average on the other hand should surely pass
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.8f, Valid Loss: %.8f", train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }

  int NUM_PRINT = 10;
  float test_loss = 10.0;

  test_loss = model->ValidateEpoch(test_highsel_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test with high selectivity: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f", 
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0) - y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);

  test_loss = model->ValidateEpoch(test_lowsel_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test with low selectivity: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f", 
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0)-y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);

  test_loss = model->ValidateEpoch(test_random_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test with random selectivity: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f", 
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0) - y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);
}

TEST_F(TensorflowTests, AugmentedNNSkewedTest) {
  // generate skewed dataset
  int NUM_X = 1000;
  matrix_eig hist = matrix_eig::Zero(NUM_X + 1, 1);
  matrix_eig sum = matrix_eig::Zero(NUM_X + 1, 1);
  float sum_hist = 0;

  // skewed
  for (int i = 1; i < 100; i++) {
    hist(i, 0) = 2 + std::round(100 * std::exp(-0.001 * std::pow(i - 100.0, 2)));
  }
  for (int i = 100; i <= NUM_X; i++) {
    hist(i, 0) = 2 + std::round(100 * std::exp(-0.00008 * std::pow(i - 100.0, 2)));
  }

  for (int i = 1; i <= NUM_X; i++) {
    sum(i, 0) = sum(i - 1, 0) + hist(i, 0);
  }
  sum_hist = sum(NUM_X, 0);
 
  // generate training and testing data randomly
  int NUM_QUERIES = 10000;
  matrix_eig data = matrix_eig::Zero(NUM_QUERIES, 3);
  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> dist(1, NUM_X);

  // data: [lowerbound, upperbound, truth selectivity]
  for (int i = 0; i < NUM_QUERIES; i++) {
    int l = dist(rng);
    int u = dist(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l-1, 0)) / sum_hist;
    // assume the max&min values of the col are known
    // so here preprocessing([min,max]->[-1,1]) can be done
    data(i, 0) = (2 / (float)NUM_X) * l - 1;
    data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    data(i, 2) = sel;
  }
  // Offsets for Splitting into Train/Test
  int split_point = NUM_QUERIES / 2;

  // Split into train/validate data
  // 0 -> end - train_offset
  matrix_eig train_data = Eigen::Map<matrix_eig>(
      data.data(), data.rows() - split_point, data.cols());

  // end - validate_offset -> end
  matrix_eig validate_data = Eigen::Map<matrix_eig>(
      data.data() + (data.rows() - split_point) * data.cols(), split_point,
      data.cols());

  int NUM_TESTS = brain::AugmentedNNWorkloadDefaults::BATCH_SIZE;
  matrix_eig test_low_data = matrix_eig::Zero(NUM_TESTS, 3);
  matrix_eig test_high_data = matrix_eig::Zero(NUM_TESTS, 3);
  matrix_eig test_random_data = matrix_eig::Zero(NUM_TESTS, 3);
  std::uniform_int_distribution<std::mt19937::result_type> dist_low(300, 999);
  std::uniform_int_distribution<std::mt19937::result_type> dist_high(50, 150);

  // generate test data on the low end
  for (int i = 0; i < NUM_TESTS; i++) {
    int l = dist_low(rng);
    int u = dist_low(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    test_low_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_low_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    test_low_data(i, 2) = sel;
  }

  // generate test data on the high end
  for (int i = 0; i < NUM_TESTS; i++) {
    int l = dist_high(rng);
    int u = dist_high(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    test_high_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_high_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    test_high_data(i, 2) = sel;
  }
  
  // generate test data randomly
  for (int i = 0; i < NUM_TESTS; i++) {
    int l = dist(rng);
    int u = dist(rng);
    if (l > u) {
      std::swap(l, u);
    }
    float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
    test_random_data(i, 0) = (2 / (float)NUM_X) * l - 1;
    test_random_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
    test_random_data(i, 2) = sel;
  }

  auto model = std::unique_ptr<brain::AugmentedNN>(new brain::AugmentedNN(
      brain::AugmentedNNWorkloadDefaults::NCOL,
      brain::AugmentedNNWorkloadDefaults::ORDER,
      brain::AugmentedNNWorkloadDefaults::NNEURON,
      brain::AugmentedNNWorkloadDefaults::LR,
      brain::AugmentedNNWorkloadDefaults::BATCH_SIZE));

  EXPECT_TRUE(FileUtil::Exists(
      FileUtil::GetRelativeToRootPath("src/brain/modelgen/AugmentedNN.pb")));

  model->TFInit();

  int LOG_INTERVAL = 20;
  int epochs = 600;
  // Variables which will hold true/prediction values
  matrix_eig y, y_hat;

  Eigen::VectorXf train_loss_avg = Eigen::VectorXf::Zero(LOG_INTERVAL);
  float prev_train_loss = 10.0;
  const float VAL_LOSS_THRESH = 0.06;
  float val_loss = VAL_LOSS_THRESH * 2;

  for (int epoch = 1; epoch <= epochs; epoch++) {
    auto train_loss = model->TrainEpoch(train_data); // returns a training loss
    int idx = (epoch - 1) % LOG_INTERVAL;
    train_loss_avg(idx) = train_loss; // insert the loss into Vector
    if (epoch % LOG_INTERVAL == 0) {
      val_loss = model->ValidateEpoch(validate_data, y, y_hat, false); // returns a validating loss
      train_loss = train_loss_avg.mean();
      // Below check is not advisable - one off failure chance
      // EXPECT_LE(val_loss, prev_valid_loss);
      // An average on the other hand should surely pass
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.8f, Valid Loss: %.8f", train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }

  int NUM_PRINT = 10;
  float test_loss = 10.0;
 
  test_loss = model->ValidateEpoch(test_high_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test on high selectivity end: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f",
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0) - y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);

  test_loss = model->ValidateEpoch(test_low_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test on low selectivity end: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f", 
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0) - y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);

  test_loss = model->ValidateEpoch(test_random_data, y, y_hat, true);
  LOG_DEBUG("\n");
  LOG_DEBUG("Test random data: ");
  for (int i = 0; i < NUM_PRINT; i++) {
    LOG_DEBUG("Truth: %.4f, Pred: %.4f, AbsError: %.4f", 
        y(i, 0), y_hat(i, 0), std::abs(y(i, 0) - y_hat(i, 0)));
  }
  LOG_DEBUG("AMSE: %.8f", test_loss);
}


// TODO: Enable this test once tensorflow package supports Python 3.7 (#1448)
TEST_F(TensorflowTests, DISABLED_SineWavePredictionTest) {
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
