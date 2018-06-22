//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// model_util.cpp
//
// Identification: src/brain/util/model_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/model_util.h"
#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {
float ModelUtil::MeanSqError(const matrix_eig &ytrue, const matrix_eig &ypred) {
  PELOTON_ASSERT(ytrue.rows() == ypred.rows() && ytrue.cols() == ypred.cols());
  return (ytrue - ypred).array().square().mean();
}

void ModelUtil::GetBatch(const BaseForecastModel *model, const matrix_eig &mat,
                         size_t batch_offset, size_t bsz, size_t bptt,
                         std::vector<matrix_eig> &data,
                         std::vector<matrix_eig> &target) {
  size_t samples_per_batch = mat.rows() / bsz;
  size_t seq_len = std::min<size_t>(
      bptt, samples_per_batch - model->GetHorizon() - batch_offset);
  // bsz vector of <seq_len, feat_len> = (bsz, seq_len, feat_len)
  for (size_t batch_idx = 0; batch_idx < bsz; batch_idx++) {
    size_t block_start = batch_idx * samples_per_batch;
    size_t block_offset = block_start + batch_offset;
    // train mat[row_idx:row_idx + seq_len, :)
    matrix_eig data_batch = mat.block(block_offset, 0, seq_len, mat.cols());
    // target mat[row_idx + horizon_: row_idx + seq_len + horizon_ , :]
    matrix_eig target_batch =
        mat.block(block_offset + model->GetHorizon(), 0, seq_len, mat.cols());
    // Push batches into containers
    data.push_back(data_batch);
    target.push_back(target_batch);
  }
}

void ModelUtil::GenerateFeatureMatrix(const BaseForecastModel *model,
                                      matrix_eig &data, int regress_dim,
                                      matrix_eig &processed_features,
                                      matrix_eig &processed_forecasts) {
  size_t timesteps = data.rows();
  std::vector<matrix_eig> feat_matrix, forecast_matrix;
  for (size_t idx = 0; idx <= timesteps - model->GetHorizon() - regress_dim;
       idx++) {
    feat_matrix.push_back(EigenUtil::ToEigenMat(
        {EigenUtil::Flatten(data.middleRows(idx, regress_dim))}));
    forecast_matrix.emplace_back(
        data.row(idx + regress_dim + model->GetHorizon() - 1));
  }
  processed_features = EigenUtil::VStack(feat_matrix);
  processed_forecasts = EigenUtil::VStack(forecast_matrix);
}

}  // namespace brain
}  // namespace peloton