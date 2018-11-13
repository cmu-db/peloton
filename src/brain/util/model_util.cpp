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

void ModelUtil::GetBatch(const BaseForecastModel &model, const matrix_eig &mat,
                         size_t batch_offset, size_t bsz,
                         std::vector<matrix_eig> &data,
                         std::vector<matrix_eig> &target, bool time_major) {
  if (time_major) {
    size_t samples_per_batch = mat.rows() / bsz;
    size_t seq_len = std::min<size_t>(
        model.GetBPTT(), samples_per_batch - model.GetHorizon() - batch_offset);
    // bsz vector of <seq_len, feat_len> = (bsz, seq_len, feat_len)
    for (size_t seq_idx = 0; seq_idx < bsz; seq_idx++) {
      size_t seqblock_start = seq_idx * samples_per_batch;
      size_t seq_offset = seqblock_start + batch_offset;
      // train mat[row_idx:row_idx + seq_len, :)
      matrix_eig data_batch = mat.block(seq_offset, 0, seq_len, mat.cols());
      // target mat[row_idx + horizon_: row_idx + seq_len + horizon_ , :]
      matrix_eig target_batch =
          mat.block(seq_offset + model.GetHorizon(), 0, seq_len, mat.cols());
      // Push batches into containers
      data.push_back(data_batch);
      target.push_back(target_batch);
    }
  } else {
    size_t seq_len = model.GetBPTT();
    // bsz vector of <seq_len, feat_len> = (bsz, seq_len, feat_len)
    for (size_t seq_idx = 0; seq_idx < bsz; seq_idx++) {
      size_t seq_start = seq_idx * seq_len + batch_offset;
      matrix_eig data_batch = mat.block(seq_start, 0, seq_len, mat.cols());
      matrix_eig target_batch =
          mat.block(seq_start + model.GetHorizon(), 0, seq_len, mat.cols());
      data.push_back(data_batch);
      target.push_back(target_batch);
    }
  }
}

void ModelUtil::GetBatches(const BaseForecastModel &model,
                           const matrix_eig &mat, size_t batch_size,
                           std::vector<std::vector<matrix_eig>> &data,
                           std::vector<std::vector<matrix_eig>> &target,
                           bool time_major) {
  if (time_major) {
    // Obtain relevant metadata
    int max_allowed_bsz = mat.rows() / (model.GetHorizon() + model.GetBPTT());
    int min_allowed_bsz = 1;
    int bsz =
        std::max(min_allowed_bsz, std::min<int>(batch_size, max_allowed_bsz));
    int samples_per_input = mat.rows() / bsz;
    int num_feats = mat.cols();

    // Trim the data for equal sized inputs per batch
    matrix_eig mat_adjusted =
        mat.block(0, 0, samples_per_input * bsz, num_feats);

    for (int batch_offset = 0;
         batch_offset < samples_per_input - model.GetHorizon();
         batch_offset += model.GetBPTT()) {
      std::vector<matrix_eig> data_batch_eig, target_batch_eig;
      ModelUtil::GetBatch(model, mat_adjusted, batch_offset, bsz,
                          data_batch_eig, target_batch_eig);
      data.push_back(data_batch_eig);
      target.push_back(target_batch_eig);
    }
  } else {
    int max_rows_in = mat.rows() - model.GetHorizon();
    int num_samples = max_rows_in / model.GetBPTT();
    // Obtain batch size
    int max_allowed_bsz = num_samples;
    int min_allowed_bsz = 1;
    int bsz =
        std::max(min_allowed_bsz, std::min<int>(batch_size, max_allowed_bsz));
    int samples_per_batch = bsz * model.GetBPTT();
    int samples_per_seq = model.GetBPTT();
    int batch_offset = 0;
    for (batch_offset = 0; batch_offset < max_rows_in - samples_per_batch;
         batch_offset += samples_per_batch) {
      std::vector<matrix_eig> data_batch_eig, target_batch_eig;
      ModelUtil::GetBatch(model, mat, batch_offset, bsz, data_batch_eig,
                          target_batch_eig, time_major);
      data.push_back(data_batch_eig);
      target.push_back(target_batch_eig);
    }
    int rem_bsz = (max_rows_in - batch_offset) / samples_per_seq;
    if (rem_bsz > 0) {
      std::vector<matrix_eig> data_batch_eig, target_batch_eig;
      ModelUtil::GetBatch(model, mat, batch_offset, rem_bsz, data_batch_eig,
                          target_batch_eig, time_major);
      data.push_back(data_batch_eig);
      target.push_back(target_batch_eig);
    }
  }
}

void ModelUtil::GetBatches(const BaseForecastModel &model,
                           const matrix_eig &mat, size_t batch_size,
                           std::vector<std::vector<matrix_eig>> &data_batches) {
  int num_seq = mat.rows() / model.GetBPTT();
  // Obtain batch size
  int max_allowed_bsz = num_seq;
  int min_allowed_bsz = 1;
  int bsz =
      std::max(min_allowed_bsz, std::min<int>(batch_size, max_allowed_bsz));
  int samples_per_batch = bsz * model.GetBPTT();
  int samples_per_seq = model.GetBPTT();
  int batch_offset = 0;
  for (batch_offset = 0; batch_offset < mat.rows() - samples_per_batch;
       batch_offset += samples_per_batch) {
    std::vector<matrix_eig> data_batch;
    for (int seq_idx = 0; seq_idx < bsz; seq_idx++) {
      int seq_offset = batch_offset + seq_idx * samples_per_seq;
      data_batch.push_back(mat.middleRows(seq_offset, samples_per_seq));
    }
    data_batches.push_back(data_batch);
  }
  // Push remaining samples into smaller batch
  int rem_bsz = (mat.rows() - batch_offset) / samples_per_seq;
  if (rem_bsz > 0) {
    std::vector<matrix_eig> data_batch;
    for (int seq_idx = 0; seq_idx < rem_bsz; seq_idx++) {
      int seq_offset = batch_offset + seq_idx * samples_per_seq;
      data_batch.push_back(mat.middleRows(seq_offset, samples_per_seq));
    }
    data_batches.push_back(data_batch);
  }
  batch_offset += rem_bsz * samples_per_seq;
  int rem_seq_len = mat.rows() - batch_offset;
  // Push anything further remaining into a single batch of size < BPTT
  data_batches.push_back({mat.bottomRows(rem_seq_len)});
}

void ModelUtil::FeatureLabelSplit(const BaseForecastModel &model,
                                  const matrix_eig &data, matrix_eig &X,
                                  matrix_eig &y) {
  size_t offset_train = data.rows() - model.GetHorizon();
  X = data.topRows(offset_train);
  size_t offset_label = model.GetBPTT() + model.GetHorizon() - 1;
  y = data.middleRows(offset_label, data.rows() - offset_label);
}

void ModelUtil::GenerateFeatureMatrix(const BaseForecastModel &model,
                                      const matrix_eig &data,
                                      matrix_eig &processed_features) {
  size_t timesteps = data.rows();
  std::vector<matrix_eig> feat_matrix;
  for (size_t idx = 0; idx <= timesteps - model.GetBPTT(); idx++) {
    feat_matrix.push_back(EigenUtil::ToEigenMat(
        {EigenUtil::Flatten(data.middleRows(idx, model.GetBPTT()))}));
  }
  processed_features = EigenUtil::VStack(feat_matrix);
}

bool ModelUtil::EarlyStop(vector_t val_losses, size_t patience, float delta) {
  // Check for edge cases
  PELOTON_ASSERT(patience > 1);
  PELOTON_ASSERT(delta > 0);
  if (val_losses.size() < patience) return false;
  float cur_loss = val_losses[val_losses.size() - 1];
  float pat_loss = val_losses[val_losses.size() - patience];
  // Loss should have at least dropped by delta at this point
  return (pat_loss - cur_loss) < delta;
}

}  // namespace brain
}  // namespace peloton