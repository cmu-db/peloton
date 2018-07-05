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
#include "brain/workload/kernel_model.h"
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
  int BPTT = 2;
  int HZN = 2;
  int SGMT = 1;
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, HZN, SGMT));
  matrix_eig workload = brain::EigenUtil::ToEigenMat(
      {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}, {11, 12}});
  matrix_eig expected_feat =
      brain::EigenUtil::ToEigenMat({{1, 2, 3, 4}, {3, 4, 5, 6}, {5, 6, 7, 8}});
  matrix_eig expected_fcast =
      brain::EigenUtil::ToEigenMat({{7, 8}, {9, 10}, {11, 12}});
  matrix_eig feats, fcast;
  brain::ModelUtil::FeatureLabelSplit(*model, workload, feats, fcast);
  matrix_eig processed_feats;
  brain::ModelUtil::GenerateFeatureMatrix(*model, feats, processed_feats);
  EXPECT_TRUE(processed_feats.isApprox(expected_feat));
  EXPECT_TRUE(fcast.isApprox(expected_fcast));
}

TEST_F(ModelUtilTests, GenerateFeatureMatrixTest2) {
  int BPTT = 1;
  int HZN = 2;
  int SGMT = 1;
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, HZN, SGMT));
  matrix_eig workload = brain::EigenUtil::ToEigenMat(
      {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}, {11, 12}});
  matrix_eig expected_feat =
      brain::EigenUtil::ToEigenMat({{1, 2}, {3, 4}, {5, 6}, {7, 8}});
  matrix_eig expected_fcast =
      brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}, {9, 10}, {11, 12}});
  matrix_eig X, processed_fcast, processed_feats;
  brain::ModelUtil::FeatureLabelSplit(*model, workload, X, processed_fcast);
  brain::ModelUtil::GenerateFeatureMatrix(*model, X, processed_feats);
  EXPECT_TRUE(processed_feats.isApprox(expected_feat));
  EXPECT_TRUE(processed_fcast.isApprox(expected_fcast));
}

TEST_F(ModelUtilTests, TimeMajorBatchifyTest) {
  // TODO(saatviks): Needs more rigorous testing
  int HZN = 2;
  int INTERVAL = 1;
  int BSZ = 4;
  int BPTT = 2;
  bool TIME_MAJOR = true;
  matrix_eig workload = brain::EigenUtil::ToEigenMat(
      {{1, 2},   {3, 4},   {5, 6},   {7, 8},   {9, 10},  {11, 12}, {13, 14},
       {15, 16}, {17, 18}, {19, 20}, {21, 22}, {23, 24}, {25, 26}, {27, 28},
       {29, 30}, {31, 32}, {33, 34}, {35, 36}, {37, 38}, {39, 40}, {41, 42},
       {43, 44}, {45, 46}, {47, 48}, {49, 50}, {51, 52}, {53, 54}, {55, 56},
       {57, 58}, {59, 60}, {61, 62}, {63, 64}});
  int BATCH_NUMSAMPLES = workload.rows() / BSZ;  // = 8 samples
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, HZN, INTERVAL));
  std::vector<int> batch_offsets_check{0, 1};
  for (int batch_offset : batch_offsets_check) {
    std::vector<matrix_eig> data_batches, target_batches;
    brain::ModelUtil::GetBatch(*model, workload, batch_offset, BSZ,
                               data_batches, target_batches, TIME_MAJOR);
    // Check correct bsz
    EXPECT_EQ(data_batches.size(), BSZ);
    EXPECT_EQ(target_batches.size(), BSZ);

    for (int i = 0; i < BSZ; i++) {
      // Check correct bptt
      EXPECT_EQ(data_batches[i].rows(), BPTT);
      EXPECT_EQ(target_batches[i].rows(), BPTT);
      // Check correct data slices(should start
      // from BATCH_OFFSET:BATCH_OFFSET + BPTT)
      EXPECT_TRUE(data_batches[i].isApprox(
          workload.middleRows(i * BATCH_NUMSAMPLES + batch_offset, BPTT)));
      // Check correct target slices(should start
      // from BATCH_OFFSET + HZN:BATCH_OFFSET + HZN + BPTT)
      EXPECT_TRUE(target_batches[i].isApprox(workload.middleRows(
          i * BATCH_NUMSAMPLES + batch_offset + HZN, BPTT)));
    }
  }
}

TEST_F(ModelUtilTests, SimpleBatchifyTest) {
  // Batchifying workload only into data batches(no target batches)
  int UNUSED_HZN = 2;
  int UNUSED_INTERVAL = 1;
  int BSZ = 3;
  int BPTT = 2;
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2},
                                                      {3, 4},
                                                      {5, 6},
                                                      {7, 8},
                                                      {9, 10},
                                                      {11, 12},
                                                      {13, 14},
                                                      {15, 16},
                                                      {17, 18},
                                                      {19, 20},
                                                      {21, 22},
                                                      {23, 24},
                                                      {25, 26},
                                                      {27, 28},
                                                      {29, 30},
                                                      {31, 32},
                                                      {33, 34}});
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, UNUSED_HZN, UNUSED_INTERVAL));
  std::vector<std::vector<matrix_eig>> data_batches;
  brain::ModelUtil::GetBatches(*model, workload, BSZ, data_batches);
  int num_batch_exp = 4;
  EXPECT_EQ(data_batches.size(), num_batch_exp);
  std::vector<std::vector<matrix_eig>> data_batches_exp = {
      {brain::EigenUtil::ToEigenMat({{1, 2}, {3, 4}}),
       brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}}),
       brain::EigenUtil::ToEigenMat({{9, 10}, {11, 12}})},
      {brain::EigenUtil::ToEigenMat({{13, 14}, {15, 16}}),
       brain::EigenUtil::ToEigenMat({{17, 18}, {19, 20}}),
       brain::EigenUtil::ToEigenMat({{21, 22}, {23, 24}})},
      {brain::EigenUtil::ToEigenMat({{25, 26}, {27, 28}}),
       brain::EigenUtil::ToEigenMat({{29, 30}, {31, 32}})},
      {brain::EigenUtil::ToEigenMat({{33, 34}})}};
  EXPECT_EQ(data_batches_exp, data_batches);
}

TEST_F(ModelUtilTests, BatchMajorBatchifyTest1) {
  // Entire workload in one batch
  int HZN = 2;
  int INTERVAL = 1;
  int BSZ = 7;
  int BPTT = 2;
  int BATCH_OFFSET = 0;
  bool TIME_MAJOR = false;
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2},
                                                      {3, 4},
                                                      {5, 6},
                                                      {7, 8},
                                                      {9, 10},
                                                      {11, 12},
                                                      {13, 14},
                                                      {15, 16},
                                                      {17, 18},
                                                      {19, 20},
                                                      {21, 22},
                                                      {23, 24},
                                                      {25, 26},
                                                      {27, 28},
                                                      {29, 30},
                                                      {31, 32}});
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, HZN, INTERVAL));
  std::vector<matrix_eig> data_batch, target_batch;
  brain::ModelUtil::GetBatch(*model, workload, BATCH_OFFSET, BSZ, data_batch,
                             target_batch, TIME_MAJOR);

  EXPECT_EQ(data_batch.size(), BSZ);
  EXPECT_EQ(target_batch.size(), BSZ);

  std::vector<matrix_eig> data_batch_exp = {
      brain::EigenUtil::ToEigenMat({{1, 2}, {3, 4}}),
      brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}}),
      brain::EigenUtil::ToEigenMat({{9, 10}, {11, 12}}),
      brain::EigenUtil::ToEigenMat({{13, 14}, {15, 16}}),
      brain::EigenUtil::ToEigenMat({{17, 18}, {19, 20}}),
      brain::EigenUtil::ToEigenMat({{21, 22}, {23, 24}}),
      brain::EigenUtil::ToEigenMat({{25, 26}, {27, 28}})};
  std::vector<matrix_eig> target_batch_exp = {
      brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}}),
      brain::EigenUtil::ToEigenMat({{9, 10}, {11, 12}}),
      brain::EigenUtil::ToEigenMat({{13, 14}, {15, 16}}),
      brain::EigenUtil::ToEigenMat({{17, 18}, {19, 20}}),
      brain::EigenUtil::ToEigenMat({{21, 22}, {23, 24}}),
      brain::EigenUtil::ToEigenMat({{25, 26}, {27, 28}}),
      brain::EigenUtil::ToEigenMat({{29, 30}, {31, 32}})};
  EXPECT_EQ(data_batch_exp, data_batch);
  EXPECT_EQ(target_batch_exp, target_batch);
}

TEST_F(ModelUtilTests, BatchMajorBatchifyTest2) {
  int HZN = 2;
  int INTERVAL = 1;
  int BSZ = 4;
  int BPTT = 2;
  bool TIME_MAJOR = false;
  matrix_eig workload = brain::EigenUtil::ToEigenMat({{1, 2},
                                                      {3, 4},
                                                      {5, 6},
                                                      {7, 8},
                                                      {9, 10},
                                                      {11, 12},
                                                      {13, 14},
                                                      {15, 16},
                                                      {17, 18},
                                                      {19, 20},
                                                      {21, 22},
                                                      {23, 24},
                                                      {25, 26},
                                                      {27, 28},
                                                      {29, 30},
                                                      {31, 32}});
  auto model = std::unique_ptr<brain::TimeSeriesKernelReg>(
      new brain::TimeSeriesKernelReg(BPTT, HZN, INTERVAL));
  std::vector<std::vector<matrix_eig>> data_batches, target_batches;
  brain::ModelUtil::GetBatches(*model, workload, BSZ, data_batches,
                               target_batches, TIME_MAJOR);
  int num_batches_exp = 2;
  EXPECT_EQ(num_batches_exp, data_batches.size());
  EXPECT_EQ(num_batches_exp, target_batches.size());
  std::vector<std::vector<matrix_eig>> data_batches_exp = {
      {brain::EigenUtil::ToEigenMat({{1, 2}, {3, 4}}),
       brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}}),
       brain::EigenUtil::ToEigenMat({{9, 10}, {11, 12}}),
       brain::EigenUtil::ToEigenMat({{13, 14}, {15, 16}})},
      {brain::EigenUtil::ToEigenMat({{17, 18}, {19, 20}}),
       brain::EigenUtil::ToEigenMat({{21, 22}, {23, 24}}),
       brain::EigenUtil::ToEigenMat({{25, 26}, {27, 28}})}};
  std::vector<std::vector<matrix_eig>> target_batches_exp = {
      {brain::EigenUtil::ToEigenMat({{5, 6}, {7, 8}}),
       brain::EigenUtil::ToEigenMat({{9, 10}, {11, 12}}),
       brain::EigenUtil::ToEigenMat({{13, 14}, {15, 16}}),
       brain::EigenUtil::ToEigenMat({{17, 18}, {19, 20}})},
      {brain::EigenUtil::ToEigenMat({{21, 22}, {23, 24}}),
       brain::EigenUtil::ToEigenMat({{25, 26}, {27, 28}}),
       brain::EigenUtil::ToEigenMat({{29, 30}, {31, 32}})}};
  EXPECT_EQ(data_batches, data_batches_exp);
  EXPECT_EQ(target_batches, target_batches_exp);
}

TEST_F(ModelUtilTests, EarlyStopTest) {
  size_t patience = 3;
  float delta = 0.01;
  vector_t empty_set = {};
  vector_t nostop_set = {0.19, 0.08, 0.05, 0.03};
  vector_t stop_set = {0.082, 0.091, 0.085, 0.081};
  vector_t single_set = {0.082};
  EXPECT_FALSE(brain::ModelUtil::EarlyStop(empty_set, patience, delta));
  EXPECT_FALSE(brain::ModelUtil::EarlyStop(nostop_set, patience, delta));
  EXPECT_TRUE(brain::ModelUtil::EarlyStop(stop_set, patience, delta));
}

}  // namespace test
}  // namespace peloton