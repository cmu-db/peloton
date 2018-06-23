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
                         size_t batch_offset, size_t bsz,
                         std::vector<matrix_eig> &data,
                         std::vector<matrix_eig> &target) {
  size_t samples_per_batch = mat.rows() / bsz;
  size_t seq_len = std::min<size_t>(
      model->GetBPTT(), samples_per_batch - model->GetHorizon() - batch_offset);
  // bsz vector of <seq_len, feat_len> = (bsz, seq_len, feat_len)
  for (size_t seq_idx = 0; seq_idx < bsz; seq_idx++) {
    size_t seqblock_start = seq_idx * samples_per_batch;
    size_t seq_offset = seqblock_start + batch_offset;
    // train mat[row_idx:row_idx + seq_len, :)
    matrix_eig data_batch = mat.block(seq_offset, 0, seq_len, mat.cols());
    // target mat[row_idx + horizon_: row_idx + seq_len + horizon_ , :]
    matrix_eig target_batch =
        mat.block(seq_offset + model->GetHorizon(), 0, seq_len, mat.cols());
    // Push batches into containers
    data.push_back(data_batch);
    target.push_back(target_batch);
  }
}

void ModelUtil::GetBatches(const BaseForecastModel *model,
                           const matrix_eig &mat, size_t batch_size,
                           std::vector<std::vector<matrix_eig>> &data,
                           std::vector<std::vector<matrix_eig>> &target) {
  // Obtain relevant metadata
  int max_allowed_bsz = mat.rows() / (model->GetHorizon() + model->GetBPTT());
  int min_allowed_bsz = 1;
  int bsz = std::max(min_allowed_bsz, std::min<int>(batch_size, max_allowed_bsz));
  int samples_per_input = mat.rows() / bsz;
  int num_feats = mat.cols();

  // Trim the data for equal sized inputs per batch
  matrix_eig mat_adjusted = mat.block(0, 0, samples_per_input * bsz, num_feats);

  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < samples_per_input - model->GetHorizon();
       batch_offset += model->GetBPTT()) {
    std::vector<matrix_eig> data_batch_eig, target_batch_eig;
    ModelUtil::GetBatch(model, mat_adjusted, batch_offset, bsz, data_batch_eig,
                        target_batch_eig);
    data.push_back(data_batch_eig);
    target.push_back(target_batch_eig);
  }
}

void ModelUtil::GenerateFeatureMatrix(const BaseForecastModel *model,
                                      const matrix_eig &data,
                                      matrix_eig &processed_features,
                                      matrix_eig &processed_forecasts) {
  size_t timesteps = data.rows();
  std::vector<matrix_eig> feat_matrix, forecast_matrix;
  for (size_t idx = 0; idx <= timesteps - model->GetHorizon() - model->GetBPTT();
       idx++) {
    feat_matrix.push_back(EigenUtil::ToEigenMat(
        {EigenUtil::Flatten(data.middleRows(idx, model->GetBPTT()))}));
    forecast_matrix.emplace_back(
        data.row(idx + model->GetBPTT() + model->GetHorizon() - 1));
  }
  processed_features = EigenUtil::VStack(feat_matrix);
  processed_forecasts = EigenUtil::VStack(forecast_matrix);
}

}  // namespace brain
}  // namespace peloton