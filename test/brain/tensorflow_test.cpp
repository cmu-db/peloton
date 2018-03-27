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

}  // namespace test
}  // namespace peloton
