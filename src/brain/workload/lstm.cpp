//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lstm_tf.cpp
//
// Identification: src/brain/workload/lstm_tf.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/tf_session_entity/tf_session_entity_output.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/workload/lstm.h"
#include "util/file_util.h"

namespace peloton {
namespace brain {

TimeSeriesLSTM::TimeSeriesLSTM(int nfeats, int nencoded, int nhid, int nlayers,
                               float learn_rate, float dropout_ratio,
                               float clip_norm, int batch_size, int bptt,
                               int horizon, int segment)
    : BaseTFModel("src/brain/modelgen",
                  "src/brain/modelgen/LSTM.py",
                  "src/brain/modelgen/LSTM.pb"),
      BaseForecastModel(horizon, segment),
      nfeats_(nfeats),
      nencoded_(nencoded),
      nhid_(nhid),
      nlayers_(nlayers),
      learn_rate_(learn_rate),
      dropout_ratio_(dropout_ratio),
      clip_norm_(clip_norm),
      batch_size_(batch_size),
      bptt_(bptt) {
  GenerateModel(ConstructModelArgsString());
  // Import the Model
  tf_session_entity_->ImportGraph(graph_path_);
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
  model_str_builder << "LSTM(";
  model_str_builder << "nfeats = " << nfeats_;
  model_str_builder << ", nencoded = " << nencoded_;
  model_str_builder << ", nhid = " << nhid_;
  model_str_builder << ", nlayers = " << nlayers_;
  model_str_builder << ", lr = " << learn_rate_;
  model_str_builder << ", dropout_ratio = " << dropout_ratio_;
  model_str_builder << ", clip_norm = " << clip_norm_;
  model_str_builder << ", bsz = " << batch_size_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", bptt = " << bptt_;
  model_str_builder << ", segment = " << segment_;
  model_str_builder << ")";
  return model_str_builder.str();
}

float TimeSeriesLSTM::TrainEpoch(matrix_eig &data) {
  std::vector<float> losses;
  // Obtain relevant metadata
  int max_allowed_bsz = data.rows() / (horizon_ + bptt_);
  int min_allowed_bsz = 1;
  int bsz = std::max(min_allowed_bsz, std::min(batch_size_, max_allowed_bsz));
  int samples_per_input = data.rows() / bsz;
  int num_feats = data.cols();

  // Trim the data for equal sized inputs per batch
  data = data.block(0, 0, samples_per_input * bsz, data.cols());

  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < samples_per_input - horizon_;
       batch_offset += bptt_) {
    std::vector<matrix_eig> data_batch_eig, target_batch_eig;
    GetBatch(data, batch_offset, bsz, bptt_, data_batch_eig, target_batch_eig);
    auto data_batch = EigenUtil::Flatten(data_batch_eig);
    auto target_batch = EigenUtil::Flatten(target_batch_eig);
    int seq_len = data_batch.size() / (bsz * num_feats);
    std::vector<int64_t> dims{bsz, seq_len, num_feats};
    std::vector<TfFloatIn *> inputs_optimize{
        new TfFloatIn(data_batch.data(), dims, "data_"),
        new TfFloatIn(target_batch.data(), dims, "target_"),
        new TfFloatIn(dropout_ratio_, "dropout_ratio_"),
        new TfFloatIn(learn_rate_, "learn_rate_"),
        new TfFloatIn(clip_norm_, "clip_norm_")};
    std::vector<TfFloatIn *> inputs_loss{
        new TfFloatIn(data_batch.data(), dims, "data_"),
        new TfFloatIn(target_batch.data(), dims, "target_"),
        new TfFloatIn(1.0, "dropout_ratio_")};
    auto output_loss = new TfFloatOut("lossOp_");
    auto out = tf_session_entity_->Eval(inputs_loss, output_loss);
    tf_session_entity_->Eval(inputs_optimize, "optimizeOp_");
    losses.push_back(out[0]);
    std::for_each(inputs_optimize.begin(), inputs_optimize.end(), TFIO_Delete);
    std::for_each(inputs_loss.begin(), inputs_loss.end(), TFIO_Delete);
    TFIO_Delete(output_loss);
  }
  return std::accumulate(losses.begin(), losses.end(), 0.0) / losses.size();
}

//void TimeSeriesLSTM::Forecast(peloton::matrix_eig &data, peloton::matrix_eig &forecasted) const {
//
//}

float TimeSeriesLSTM::ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                                    matrix_eig &test_pred, bool return_preds) {
  std::vector<float> y_hat, y;

  // Obtain relevant metadata
  int max_allowed_bsz = data.rows() / (horizon_ + bptt_);
  int min_allowed_bsz = 1;
  int bsz = std::max(min_allowed_bsz, std::min(batch_size_, max_allowed_bsz));
  int samples_per_input = data.rows() / bsz;
  int num_feats = data.cols();

  // Trim the data for equal sized inputs per batch
  data = data.block(0, 0, samples_per_input * bsz, data.cols());

  // Apply Validation
  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < samples_per_input - horizon_;
       batch_offset += bptt_) {
    std::vector<matrix_eig> data_batch_eig, target_batch_eig;
    GetBatch(data, batch_offset, bsz, bptt_, data_batch_eig, target_batch_eig);
    auto data_batch = EigenUtil::Flatten(data_batch_eig);
    auto target_batch = EigenUtil::Flatten(target_batch_eig);
    int seq_len = data_batch.size() / (bsz * num_feats);
    std::vector<int64_t> dims{bsz, seq_len, num_feats};
    std::vector<TfFloatIn *> inputs_predict{
        new TfFloatIn(data_batch.data(), dims, "data_"),
        new TfFloatIn(1.0, "dropout_ratio_")};
    auto output_predict = new TfFloatOut("pred_");

    // Obtain predicted values
    auto out = tf_session_entity_->Eval(inputs_predict, output_predict);

    // Flattened predicted/true values
    for (size_t i = 0; i < data_batch.size(); i++) {
      y_hat.push_back(out[i]);
    }
    y.insert(y.end(), target_batch.begin(), target_batch.end());
    std::for_each(inputs_predict.begin(), inputs_predict.end(), TFIO_Delete);
    TFIO_Delete(output_predict);
  }

  // Select the correct time window for the true values(fn of horizon and
  // segment)
  int segment_offset = segment_ * num_feats;
  y = std::vector<float>(y.end() - segment_offset, y.end());
  y_hat = std::vector<float>(y_hat.end() - segment_offset, y_hat.end());

  // Compute MSE
  std::vector<float> sq_err;
  auto sq_err_fn = [=](float y_i, float y_hat_i) {
    return (y_i - y_hat_i) * (y_i - y_hat_i);
  };
  std::transform(y.begin(), y.end(), y_hat.begin(), std::back_inserter(sq_err),
                 sq_err_fn);
  float loss =
      std::accumulate(sq_err.begin(), sq_err.end(), 0.0) / sq_err.size();

  // Optionally return true/predicted values in form num_samples x num_feats
  if (return_preds) {
    matrix_t test_true_vec, test_pred_vec;
    for (int i = 0; i < segment_offset / num_feats; i++) {
      test_true_vec.emplace_back(std::vector<float>(
          y.begin() + i * num_feats, y.begin() + (i + 1) * num_feats));
      test_pred_vec.emplace_back(std::vector<float>(
          y_hat.begin() + i * num_feats, y_hat.begin() + (i + 1) * num_feats));
    }
    test_true = peloton::brain::EigenUtil::MatrixTToEigenMat(test_true_vec);
    test_pred = peloton::brain::EigenUtil::MatrixTToEigenMat(test_pred_vec);
  }
  return loss;
}
}  // namespace brain
}  // namespace peloton
