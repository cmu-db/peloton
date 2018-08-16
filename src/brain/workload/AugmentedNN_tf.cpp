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

#include "brain/workload/AugmentedNN_tf.h"
#include "util/file_util.h"

namespace peloton {
namespace brain {

AugmentedNN::AugmentedNN(int ncol, int order, int nneuron,
                               float learn_rate, int batch_size)
    : BaseTFModel(),
      learn_rate_(learn_rate),
      batch_size_(batch_size) {
  // First set the model information to use for generation and imports
  SetModelInfo();

  // Generate the Model
  std::string args_str = ConstructModelArgsString(
      ncol, order, nneuron, learn_rate);
  GenerateModel(args_str);
  // Import the Model
  tf_session_entity_->ImportGraph(graph_path_);
}

void AugmentedNN::SetModelInfo() {
  modelgen_path_ =
      peloton::FileUtil::GetRelativeToRootPath("src/brain/modelgen");
  pymodel_path_ =
      peloton::FileUtil::GetRelativeToRootPath("src/brain/modelgen/AugmentedNN.py");
  graph_path_ =
      peloton::FileUtil::GetRelativeToRootPath("src/brain/modelgen/AugmentedNN.pb");
}

std::string AugmentedNN::ConstructModelArgsString(int ncol, int order,
                                                     int nneuron, float learn_rate) {
  std::stringstream args_str_builder;
  args_str_builder << " --ncol " << ncol;
  args_str_builder << " --order " << order;
  args_str_builder << " --nneuron " << nneuron;
  args_str_builder << " --lr " << learn_rate;
  args_str_builder << " " << this->modelgen_path_;
  return args_str_builder.str();
}

void AugmentedNN::GetBatch(const matrix_eig &mat, size_t batch_offset,
                              size_t bsz, std::vector<float> &data,
                              std::vector<float> &target) {
  matrix_eig mat_data = mat.block(0, 0, mat.rows(), mat.cols() - 1);
  matrix_eig mat_target = mat.block(0, mat.cols() - 1, mat.rows(), 1);

  auto data_ptr = mat_data.data();
  auto target_ptr = mat_target.data();

  size_t row_idx = batch_offset * bsz;
  auto data_batch =
      peloton::brain::EigenUtil::FlattenMatrix(Eigen::Map<const matrix_eig>(
          data_ptr + row_idx * mat_data.cols(), bsz, mat_data.cols()));
  auto target_batch =
      peloton::brain::EigenUtil::FlattenMatrix(Eigen::Map<const matrix_eig>(
          target_ptr + row_idx * mat_target.cols(), bsz, mat_target.cols()));
  data.insert(data.end(), data_batch.begin(), data_batch.end());
  target.insert(target.end(), target_batch.begin(), target_batch.end());
}


float AugmentedNN::TrainEpoch(matrix_eig &mat) {
  std::vector<float> losses;
  // Obtain relevant metadata
  int min_allowed_bsz = 1;
  int bsz = std::max(batch_size_, min_allowed_bsz);
  int number_of_batches = mat.rows() / bsz;
  int num_cols = mat.cols() - 1;

  // Trim the data for equal sized inputs per batch
  //mat =
  //    Eigen::Map<matrix_eig>(mat.data(), number_of_batches * bsz, mat.cols());

  matrix_eig mat_target = mat.block(0, mat.cols() - 1, mat.rows(), 1);

  // LOG_DEBUG("mat_target rows: %u, cols: %u\n", (unsigned) mat_target.rows(), (unsigned) mat_target.cols());

  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < number_of_batches;
       ++batch_offset) {
    std::vector<float> data_batch, target_batch;
    GetBatch(mat, batch_offset, bsz, data_batch, target_batch);

    // LOG_DEBUG("data_batch size: %u, tatget_batch size: %u\n", (unsigned) data_batch.size(), (unsigned) target_batch.size());

    std::vector<int64_t> dims_data{bsz, num_cols};
    std::vector<int64_t> dims_target{bsz, 1};

    std::vector<TfFloatIn *> inputs_optimize{
        new TfFloatIn(data_batch.data(), dims_data, "data_"),
        new TfFloatIn(target_batch.data(), dims_target, "target_"),
        new TfFloatIn(learn_rate_, "learn_rate_")};

    std::vector<TfFloatIn *> inputs_loss{
        new TfFloatIn(data_batch.data(), dims_data, "data_"),
        new TfFloatIn(target_batch.data(), dims_target, "target_")};

    auto output_loss = new TfFloatOut("lossOp_");
    auto out = this->tf_session_entity_->Eval(inputs_loss, output_loss);
    this->tf_session_entity_->Eval(inputs_optimize, "optimizeOp_");
    losses.push_back(out[0]);

    std::for_each(inputs_optimize.begin(), inputs_optimize.end(), TFIO_Delete);
    std::for_each(inputs_loss.begin(), inputs_loss.end(), TFIO_Delete);
    TFIO_Delete(output_loss);
  }
  return std::accumulate(losses.begin(), losses.end(), 0.0) / losses.size();
}

float AugmentedNN::ValidateEpoch(matrix_eig &mat, matrix_eig &test_true,
                                    matrix_eig &test_pred, bool return_preds) {
  std::vector<float> y_hat, y;

  // Obtain relevant metadata
  int min_allowed_bsz = 1;
  int bsz = std::max(batch_size_, min_allowed_bsz);
  int number_of_batches = mat.rows() / bsz;
  int num_cols = mat.cols() - 1;

  // Trim the data for equal sized inputs per batch
  //mat =
  //    Eigen::Map<matrix_eig>(mat.data(), number_of_batches * bsz, mat.cols());

  // Apply Validation
  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < number_of_batches;
       ++batch_offset) {
    std::vector<float> data_batch, target_batch;
    GetBatch(mat, batch_offset, bsz, data_batch, target_batch);
    
    //LOG_DEBUG("data_batch size: %u, target_batch size: %u\n", (unsigned) data_batch.size(), (unsigned) target_batch.size());

    std::vector<int64_t> dims_data{bsz, num_cols};
    std::vector<int64_t> dims_target{bsz, 1};

    //LOG_DEBUG("HERE!\n");

    std::vector<TfFloatIn *> inputs_predict{
        new TfFloatIn(data_batch.data(), dims_data, "data_")};
    //    new TfFloatIn(target_batch.data(), dims_target, "target_")};
    
    //auto output_predict = new TfFloatOut("pred_");
    auto output_predict = new TfFloatOut(dims_target, "pred_");

    //LOG_DEBUG("HERE2!\n");
    
    // Obtain predicted values
    auto out = this->tf_session_entity_->Eval(inputs_predict, output_predict);
    
//    for (size_t i = 0; i < data_batch.size(); i++)
//      LOG_DEBUG("HERE3 %f!\n", out[i]);

    // Flattened predicted/true values
    for (size_t i = 0; i < target_batch.size(); i++) {
      y_hat.push_back(out[i]);
    }
    
    // LOG_DEBUG("HERE4!\n");

    y.insert(y.end(), target_batch.begin(), target_batch.end());
    std::for_each(inputs_predict.begin(), inputs_predict.end(), TFIO_Delete);


    TFIO_Delete(output_predict);

  }
    
  //LOG_DEBUG("HERE4!\n");

  // Compute MSE
  std::vector<float> sq_err;
  auto sq_err_fn = [=](float y_i, float y_hat_i) {
    return (y_i - y_hat_i) * (y_i - y_hat_i);
  };
  std::transform(y.begin(), y.end(), y_hat.begin(), std::back_inserter(sq_err),
                 sq_err_fn);
  float loss =
      std::accumulate(sq_err.begin(), sq_err.end(), 0.0) / sq_err.size();

  // Optionally return true/predicted values
  if (return_preds) {
    matrix_t test_true_vec, test_pred_vec;
    for (int i = 0; i < mat.rows(); i++) {
      test_true_vec.emplace_back(std::vector<float>(
          y.begin() + i, y.begin() + i + 1));
      test_pred_vec.emplace_back(std::vector<float>(
          y_hat.begin() + i, y_hat.begin() + i + 1));
    }
    test_true = peloton::brain::EigenUtil::MatrixTToEigenMat(test_true_vec);
    test_pred = peloton::brain::EigenUtil::MatrixTToEigenMat(test_pred_vec);
  }
  return loss;
}
}  // namespace brain
}  // namespace peloton
