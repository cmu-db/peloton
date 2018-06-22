//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// model_util_test.cpp
//
// Identification: test/brain/model_util_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/model_util.h"
#include "brain/workload/linear_models.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"

namespace peloton {
namespace test {
class ModelUtilTests : public PelotonTest {};

TEST_F(ModelUtilTests, MeanSquareErrorTest) {
  matrix_eig m1 = brain::EigenUtil::ToEigenMat({{1, 1, 0}, {0, 0, 1}});
  matrix_eig m2 = brain::EigenUtil::ToEigenMat({{0, 1, 0}, {1, 0, 1}});
  EXPECT_FLOAT_EQ(brain::ModelUtil::MeanSqError(m1, m1), 0.0);
  EXPECT_FLOAT_EQ(brain::ModelUtil::MeanSqError(m1, m2), 0.3333333);
}

TEST_F(ModelUtilTests, GenerateFeatureMatrixTest1) {
  int REG_DIM = 2;
  int HZN = 2;
  int SGMT = 1;
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(REG_DIM, HZN, SGMT));
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2},
                                                      {3, 4},
                                                      {5, 6},
                                                      {7, 8},
                                                      {9, 10},
                                                      {11, 12}});
  matrix_eig expected_feat = brain::EigenUtil::ToEigenMat({{1, 2, 3, 4},
                                                           {3, 4, 5, 6},
                                                           {5, 6, 7, 8}});
  matrix_eig expected_fcast = brain::EigenUtil::ToEigenMat({{7, 8},
                                                            {9, 10},
                                                            {11, 12}});
  matrix_eig processed_feats, processed_fcast;
  brain::ModelUtil::GenerateFeatureMatrix(model.get(), workload, REG_DIM,
                                          processed_feats, processed_fcast);
  EXPECT_TRUE(processed_feats.isApprox(expected_feat));
  EXPECT_TRUE(processed_fcast.isApprox(expected_fcast));
}

TEST_F(ModelUtilTests, GenerateFeatureMatrixTest2) {
  int REG_DIM = 1;
  int HZN = 2;
  int SGMT = 1;
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(REG_DIM, HZN, SGMT));
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2},
                                                      {3, 4},
                                                      {5, 6},
                                                      {7, 8},
                                                      {9, 10},
                                                      {11, 12}});
  matrix_eig expected_feat = brain::EigenUtil::ToEigenMat({{1, 2},
                                                           {3, 4},
                                                           {5, 6},
                                                           {7, 8}});
  matrix_eig expected_fcast = brain::EigenUtil::ToEigenMat({{5, 6},
                                                            {7, 8},
                                                            {9, 10},
                                                            {11, 12}});
  matrix_eig processed_feats, processed_fcast;
  brain::ModelUtil::GenerateFeatureMatrix(model.get(), workload, REG_DIM,
                                          processed_feats, processed_fcast);
  EXPECT_TRUE(processed_feats.isApprox(expected_feat));
  EXPECT_TRUE(processed_fcast.isApprox(expected_fcast));
}

TEST_F(ModelUtilTests, GetBatchTest) {
  int UNUSED_REG_DIM = 2;
  int HZN = 2;
  int SGMT = 1;
  int BSZ = 4;
  int BPTT = 2;
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2}, {3, 4},
                                                      {5, 6}, {7, 8},
                                                      {9, 10}, {11, 12},
                                                      {13, 14}, {15, 16},
                                                      {17, 18}, {19, 20},
                                                      {21, 22}, {23, 24},
                                                      {25, 26}, {27, 28},
                                                      {29, 30}, {31, 32},
                                                      {33, 34}, {35, 36},
                                                      {37, 38}, {39, 40},
                                                      {41, 42}, {43, 44},
                                                      {45, 46}, {47, 48},
                                                      {49, 50}, {51, 52},
                                                      {53, 54},  {55, 56},
                                                      {57, 58},  {59, 60},
                                                      {61, 62},  {63, 64}});
  int BATCH_NUMSAMPLES = workload.rows()/BSZ; // = 8 samples
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(UNUSED_REG_DIM, HZN, SGMT));
  std::vector<int> batch_offsets_check{0, 1};
  for(int batch_offset: batch_offsets_check) {
    std::vector<matrix_eig> data_batches, target_batches;
    brain::ModelUtil::GetBatch(model.get(), workload, batch_offset,
                               BSZ, BPTT, data_batches, target_batches);
    // Check correct bsz
    EXPECT_EQ(data_batches.size(), BSZ);
    EXPECT_EQ(target_batches.size(), BSZ);

    for(int i = 0; i < BSZ; i++) {
      // Check correct bptt
      EXPECT_EQ(data_batches[i].rows(), BPTT);
      EXPECT_EQ(target_batches[i].rows(), BPTT);
      // Check correct data slices(should start
      // from BATCH_OFFSET:BATCH_OFFSET + BPTT)
      EXPECT_TRUE(data_batches[i].isApprox(
          workload.middleRows(i*BATCH_NUMSAMPLES + batch_offset, BPTT)));
      // Check correct target slices(should start
      // from BATCH_OFFSET + HZN:BATCH_OFFSET + HZN + BPTT)
      EXPECT_TRUE(target_batches[i].isApprox(
          workload.middleRows(i*BATCH_NUMSAMPLES + batch_offset + HZN, BPTT)));
    }
  }
}

}  // namespace test
}  // namespace peloton