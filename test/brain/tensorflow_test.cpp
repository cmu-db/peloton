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
#include "include/brain/util/tf_util.h"
#include "common/harness.h"
#include "util/file_util.h"
#include "brain/util/eigen_util.h"
#include <numeric>

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

TEST_F(TensorflowTests, WavePredictionTest) {
  // Sine Wave prediction test works here
  int NUM_SAMPLES = 10;
  int NUM_WAVES = 3;
  std::vector<std::vector<float>> waves(NUM_WAVES);
  for(int i = 0; i < NUM_WAVES; i++) {
    std::vector<float> wave(NUM_SAMPLES);
    std::iota(wave.begin(), wave.end(), 0);
    waves.push_back(wave);
  }
  matrix_eig eig_waves = peloton::brain::EigenUtil::MatrixTToEigenMat(waves);
  matrix_eig sine_waves = eig_waves.array().sin();
  std::cout << sine_waves << std::endl;
}

}  // namespace test
}  // namespace peloton
