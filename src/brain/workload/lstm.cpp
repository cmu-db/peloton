//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lstm.cpp
//
// Identification: src/brain/workload/lstm.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/lstm.h"
#include "brain/util/model_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"
#include "util/file_util.h"

namespace peloton {
namespace brain {

TimeSeriesLSTM::TimeSeriesLSTM(int nfeats, int nencoded, int nhid, int nlayers,
                               float learn_rate, float dropout_ratio,
                               float clip_norm, int batch_size, int bptt,
                               int horizon, int interval, int epochs)
    : BaseTFModel("src/brain/modelgen", "src/brain/modelgen/LSTM.py",
                  "src/brain/modelgen/LSTM.pb"),
      BaseForecastModel(bptt, horizon, interval, epochs),
      nfeats_(nfeats),
      nencoded_(nencoded),
      nhid_(nhid),
      nlayers_(nlayers),
      learn_rate_(learn_rate),
      dropout_ratio_(dropout_ratio),
      clip_norm_(clip_norm),
      batch_size_(batch_size) {
  GenerateModel(ConstructModelArgsString());
  // Import the Model
  tf_session_entity_->ImportGraph(graph_path_);
  // Initialize the model
  TFInit();
}

std::string TimeSeriesLSTM::ConstructModelArgsString() const {
  std::stringstream args_str_builder;
  args_str_builder << " --nfeats " << nfeats_;
  args_str_builder << " --nencoded " << nencoded_;
  args_str_builder << " --nhid " << nhid_;
  args_str_builder << " --nlayers " << nlayers_;
  args_str_builder << " --lr " << learn_rate_;
  args_str_builder << " --dropout_ratio " << dropout_ratio_;
  args_str_builder << " --clip_norm " << clip_norm_;
  args_str_builder << " " << modelgen_path_;
  return args_str_builder.str();
}

std::string TimeSeriesLSTM::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesLSTM(";
  model_str_builder << "nfeats = " << nfeats_;
  model_str_builder << ", nencoded = " << nencoded_;
  model_str_builder << ", nhid = " << nhid_;
  model_str_builder << ", nlayers = " << nlayers_;
  model_str_builder << ", lr = " << learn_rate_;
  model_str_builder << ", dropout_ratio = " << dropout_ratio_;
  model_str_builder << ", clip_norm = " << clip_norm_;
  model_str_builder << ", bsz = " << batch_size_;
  model_str_builder << ", bptt = " << bptt_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", interval = " << interval_;
  model_str_builder << ")";
  return model_str_builder.str();
}

void TimeSeriesLSTM::Fit(const matrix_eig &X, const matrix_eig &y, int bsz) {
  auto data_batch = EigenUtil::Flatten(X);
  auto target_batch = EigenUtil::Flatten(y);
  int seq_len = data_batch.size() / (bsz * nfeats_);
  std::vector<int64_t> dims{bsz, seq_len, nfeats_};
  std::vector<TfFloatIn *> inputs_optimize{
      new TfFloatIn(data_batch.data(), dims, "data_"),
      new TfFloatIn(target_batch.data(), dims, "target_"),
      new TfFloatIn(dropout_ratio_, "dropout_ratio_"),
      new TfFloatIn(learn_rate_, "learn_rate_"),
      new TfFloatIn(clip_norm_, "clip_norm_")};
  tf_session_entity_->Eval(inputs_optimize, "optimizeOp_");
  std::for_each(inputs_optimize.begin(), inputs_optimize.end(), TFIO_Delete);
}

float TimeSeriesLSTM::TrainEpoch(const matrix_eig &data) {
  std::vector<float> losses;
  std::vector<std::vector<matrix_eig>> data_batches, target_batches;

  ModelUtil::GetBatches(*this, data, batch_size_, data_batches, target_batches);

  PELOTON_ASSERT(data_batches.size() == target_batches.size());

  // Run through each batch and compute loss/apply backprop
  std::vector<matrix_eig> y_batch, y_hat_batch;
  for (size_t i = 0; i < data_batches.size(); i++) {
    std::vector<matrix_eig> &data_batch_eig = data_batches[i];
    std::vector<matrix_eig> &target_batch_eig = target_batches[i];
    matrix_eig X_batch = EigenUtil::VStack(data_batch_eig);
    int bsz = static_cast<int>(data_batch_eig.size());
    // Fit
    Fit(X_batch, EigenUtil::VStack(target_batch_eig), bsz);
    // Predict
    matrix_eig y_hat_eig = Predict(X_batch, bsz);
    y_hat_batch.push_back(y_hat_eig);
    y_batch.push_back(EigenUtil::VStack(target_batch_eig));
  }
  matrix_eig y = EigenUtil::VStack(y_batch);
  matrix_eig y_hat = EigenUtil::VStack(y_hat_batch);
  return ModelUtil::MeanSqError(y, y_hat);
}

matrix_eig TimeSeriesLSTM::Predict(const matrix_eig &X, int bsz) const {
  auto data_batch = EigenUtil::Flatten(X);
  int seq_len = data_batch.size() / (bsz * nfeats_);
  std::vector<int64_t> dims{bsz, seq_len, nfeats_};
  std::vector<TfFloatIn *> inputs_predict{
      new TfFloatIn(data_batch.data(), dims, "data_"),
      new TfFloatIn(1.0, "dropout_ratio_")};
  auto output_predict = new TfFloatOut("pred_");
  // Obtain predicted values
  auto out = tf_session_entity_->Eval(inputs_predict, output_predict);
  std::vector<matrix_eig> y_hat;
  int idx = 0;
  for (int seq_idx = 0; seq_idx < bsz; seq_idx++) {
    matrix_t seq;
    for (int samp_idx = 0; samp_idx < seq_len; samp_idx++) {
      vector_t feat_vec;
      for (int feat_idx = 0; feat_idx < nfeats_; feat_idx++) {
        feat_vec.push_back(out[idx++]);
      }
      seq.push_back(feat_vec);
    }
    y_hat.push_back(EigenUtil::ToEigenMat(seq));
  }
  std::for_each(inputs_predict.begin(), inputs_predict.end(), TFIO_Delete);
  TFIO_Delete(output_predict);
  return EigenUtil::VStack(y_hat);
}

float TimeSeriesLSTM::ValidateEpoch(const matrix_eig &data) {
  std::vector<float> losses;
  std::vector<std::vector<matrix_eig>> data_batches, target_batches;
  ModelUtil::GetBatches(*this, data, batch_size_, data_batches, target_batches);

  PELOTON_ASSERT(data_batches.size() == target_batches.size());

  // Run through each batch and compute loss/apply backprop
  std::vector<matrix_eig> y_hat_batch, y_batch;
  for (size_t i = 0; i < data_batches.size(); i++) {
    std::vector<matrix_eig> &data_batch_eig = data_batches[i];
    std::vector<matrix_eig> &target_batch_eig = target_batches[i];
    matrix_eig y_hat_i = Predict(EigenUtil::VStack(data_batch_eig),
                                 static_cast<int>(data_batch_eig.size()));
    y_hat_batch.push_back(y_hat_i);
    y_batch.push_back(EigenUtil::VStack(target_batch_eig));
  }
  matrix_eig y = EigenUtil::VStack(y_batch);
  matrix_eig y_hat = EigenUtil::VStack(y_hat_batch);
  return ModelUtil::MeanSqError(y, y_hat);
}
}  // namespace brain
}  // namespace peloton
