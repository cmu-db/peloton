#include "brain/workload/lstm_tf.h"

namespace peloton {
namespace brain {

Seq2SeqLSTM::Seq2SeqLSTM(const std::string &graph_path, float learn_rate,
                         float dropout_ratio, float clip_norm, int batch_size,
                         int horizon, int bptt, int segment)
    : BaseTFModel(graph_path),
      learn_rate_(learn_rate),
      dropout_ratio_(dropout_ratio),
      clip_norm_(clip_norm),
      batch_size_(batch_size),
      horizon_(horizon),
      segment_(segment),
      bptt_(bptt) {}

void Seq2SeqLSTM::GetBatch(const matrix_eig &mat, size_t batch_offset, size_t bsz,
                           std::vector<float> &data,
                           std::vector<float> &target) {
  size_t samples_per_input = mat.rows() / bsz;
  size_t seq_len =
      std::min<size_t>(bptt_, samples_per_input - horizon_ - batch_offset);
  auto data_ptr = mat.data();

  for (size_t input_idx = 0; input_idx < bsz; input_idx++) {
    size_t row_idx = input_idx * samples_per_input;
    auto data_batch = peloton::brain::EigenUtil::FlattenMatrix(
        Eigen::Map<const matrix_eig>(data_ptr + row_idx * mat.cols(), seq_len, mat.cols()));
    auto target_batch = peloton::brain::EigenUtil::FlattenMatrix(
        Eigen::Map<const matrix_eig>(data_ptr + (row_idx + horizon_) * mat.cols(), seq_len, mat.cols()));
    data.insert(data.end(), data_batch.begin(), data_batch.end());
    target.insert(target.end(), target_batch.begin(), target_batch.end());
  }
}

/**
 * Train the Tensorflow model
 * @param data: Contiguous time-series data
 * @return: Average Training loss
 * Given a continuous sequence of data,
 * this function:
 * 1. breaks the data into batches('Batchify')
 * 2. prepares tensorflow-entity inputs/outputs
 * 3. computes loss and applies backprop
 * Finally the average training loss over all the
 * batches is returned.
 */
float Seq2SeqLSTM::TrainEpoch(matrix_eig &data) {
  std::vector<float> losses;
  // Obtain relevant metadata
  int max_allowed_bsz = data.rows() / (horizon_ + bptt_);
  int min_allowed_bsz = 1;
  int bsz = std::max(min_allowed_bsz, std::min(batch_size_, max_allowed_bsz));
  int samples_per_input = data.rows() / bsz;
  int num_feats = data.cols();

  // Trim the data for equal sized inputs per batch
  data =
      Eigen::Map<matrix_eig>(data.data(), samples_per_input * bsz, data.cols());

  // Run through each batch and compute loss/apply backprop
  for (size_t batch_offset = 0; batch_offset < samples_per_input - horizon_;
       batch_offset += bptt_) {
    std::vector<float> data_batch, target_batch;
    GetBatch(data, batch_offset, bsz, data_batch, target_batch);
    int seq_len = data_batch.size() / (bsz * num_feats);
    std::vector<int64_t> dims{bsz, seq_len, num_feats};
    std::vector<TfFloatIn> inputs_optimize{
        TfFloatIn(data_batch.data(), dims, "data_"),
        TfFloatIn(target_batch.data(), dims, "target_"),
        TfFloatIn(dropout_ratio_, "dropout_ratio_"),
        TfFloatIn(learn_rate_, "learn_rate_"),
        TfFloatIn(clip_norm_, "clip_norm_")};
    std::vector<TfFloatIn> inputs_loss{
        TfFloatIn(data_batch.data(), dims, "data_"),
        TfFloatIn(target_batch.data(), dims, "target_"),
        TfFloatIn(1.0, "dropout_ratio_")};
    std::vector<TfFloatOut> outputs{TfFloatOut("lossOp_")};
    auto out = this->tf_session_entity_->Eval(inputs_loss, outputs);
    this->tf_session_entity_->Eval(inputs_optimize, "optimizeOp_");
    losses.push_back(out[0]);
  }
  return std::accumulate(losses.begin(), losses.end(), 0.0) / losses.size();
}

/**
 * @param data: Contiguous time-series data
 * @param test_true: reference variable to which true values are returned
 * @param test_pred: reference variable to which predicted values are returned
 * @param return_preds: Whether to return predicted/true values
 * @return: Average Validation Loss
 * This applies the same set of steps as TrainEpoch.
 * However instead of applying backprop it obtains predicted values.
 * Then the validation loss is calculated for the relevant sequence
 * - this is a function of segment and horizon.
 */
float Seq2SeqLSTM::ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                                 matrix_eig &test_pred, bool return_preds) {
  std::vector<float> y_hat, y;

  // Obtain relevant metadata
  int max_allowed_bsz = data.rows() / (horizon_ + bptt_);
  int min_allowed_bsz = 1;
  int bsz = std::max(min_allowed_bsz, std::min(batch_size_, max_allowed_bsz));
  int samples_per_input = data.rows() / bsz;
  int num_feats = data.cols();

  // Trim the data for equal sized inputs per batch
  data =
      Eigen::Map<matrix_eig>(data.data(), samples_per_input * bsz, data.cols());

  // Apply Validation
  // Run through each batch and compute loss/apply backprop
  for (size_t batch_offset = 0; batch_offset < samples_per_input - horizon_;
       batch_offset += bptt_) {
    std::vector<float> data_batch, target_batch;
    GetBatch(data, batch_offset, bsz, data_batch, target_batch);
    int seq_len = data_batch.size() / (bsz * num_feats);
    std::vector<int64_t> dims{bsz, seq_len, num_feats};
    std::vector<TfFloatIn> inputs_predict{
        TfFloatIn(data_batch.data(), dims, "data_"),
        TfFloatIn(1.0, "dropout_ratio_")};
    std::vector<TfFloatOut> outputs{TfFloatOut("pred_")};

    // Obtain predicted values
    auto out = this->tf_session_entity_->Eval(inputs_predict, outputs);

    // Flattened predicted/true values
    for (int i = 0; i < data_batch.size(); i++) {
      y_hat.push_back(out[i]);
    }
    y.insert(y.end(), target_batch.begin(), target_batch.end());
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
