//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// augmented_nn.cpp
//
// Identification: src/brain/workload/augmented_nn.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/selectivity/augmented_nn.h"
#include "brain/util/model_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"
#include "util/file_util.h"

namespace peloton {
namespace brain {

AugmentedNN::AugmentedNN(int column_num, int order, int neuron_num,
                            float learn_rate, int batch_size, int epochs)
    : BaseTFModel("src/brain/modelgen", "src/brain/modelgen/augmented_nn.py",
                  "src/brain/modelgen/augmented_nn.pb"),
      column_num_(column_num),
      order_(order),
      neuron_num_(neuron_num),
      learn_rate_(learn_rate),
      batch_size_(batch_size),
      epochs_(epochs) {
  GenerateModel(ConstructModelArgsString());
  // Import the Model
  tf_session_entity_->ImportGraph(graph_path_);
  // Initialize the model
  TFInit();
}

std::string AugmentedNN::ConstructModelArgsString() const {
  std::stringstream args_str_builder;
  args_str_builder << " --column_num " << column_num_;
  args_str_builder << " --order " << order_;
  args_str_builder << " --neuron_num " << neuron_num_;
  args_str_builder << " --lr " << learn_rate_;
  args_str_builder << " " << this->modelgen_path_;
  return args_str_builder.str();
}

std::string AugmentedNN::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "augmented_nn(";
  model_str_builder << "column_num = " << column_num_;
  model_str_builder << ", order = " << order_;
  model_str_builder << ", neuron_num = " << neuron_num_;
  model_str_builder << ", lr = " << learn_rate_;
  model_str_builder << ", batch_size = " << batch_size_;
  model_str_builder << ")";
  return model_str_builder.str();
}

// returns a batch
void AugmentedNN::GetBatch(const matrix_eig &mat, size_t batch_offset,
                              size_t bsz, matrix_eig &data,
                              matrix_eig &target) {
  size_t row_idx = batch_offset * bsz;
  data = mat.block(row_idx, 0, bsz, mat.cols() - 1);
  target = mat.block(row_idx, mat.cols() - 1, bsz, 1);
}

// backpropagate once
void AugmentedNN::Fit(const matrix_eig &X, const matrix_eig &y, int bsz) {
  auto data_batch = EigenUtil::Flatten(X);
  auto target_batch = EigenUtil::Flatten(y);
  std::vector<int64_t> dims_data{bsz, X.cols()};
  std::vector<int64_t> dims_target{bsz, 1};
  std::vector<TfFloatIn *> inputs_optimize{
      new TfFloatIn(data_batch.data(), dims_data, "data_"),
      new TfFloatIn(target_batch.data(), dims_target, "target_"),
      new TfFloatIn(learn_rate_, "learn_rate_")};
  tf_session_entity_->Eval(inputs_optimize, "optimizeOp_");
  std::for_each(inputs_optimize.begin(), inputs_optimize.end(), TFIO_Delete);
}

float AugmentedNN::TrainEpoch(const matrix_eig &mat) {
  std::vector<float> losses;
  // Obtain relevant metadata
  int min_allowed_bsz = 1;
  int bsz = std::min((int)mat.rows(), std::max(batch_size_, min_allowed_bsz));
  int number_of_batches = mat.rows() / bsz;
  int num_cols = mat.cols() - 1;

  std::vector<matrix_eig> y_batch, y_hat_batch;
  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < number_of_batches;
       ++batch_offset) {
    matrix_eig data_batch, target_batch;
    GetBatch(mat, batch_offset, bsz, data_batch, target_batch);

    std::vector<int64_t> dims_data{bsz, num_cols};
    std::vector<int64_t> dims_target{bsz, 1};

    Fit(data_batch, target_batch, bsz);

    matrix_eig y_hat_eig = Predict(data_batch, bsz);
    y_hat_batch.push_back(y_hat_eig);
    y_batch.push_back(target_batch);
  }
  matrix_eig y = EigenUtil::VStack(y_batch);
  matrix_eig y_hat = EigenUtil::VStack(y_hat_batch);
  return ModelUtil::MeanSqError(y, y_hat);

}

// x: [bsz, 2]
// return: [bsz, 1]
matrix_eig AugmentedNN::Predict(const matrix_eig &X, int bsz) const {
  auto data_batch = EigenUtil::Flatten(X);
  std::vector<int64_t> dims_data{bsz, X.cols()};
  std::vector<int64_t> dims_target{bsz, 1};

  std::vector<TfFloatIn *> inputs_predict{
      new TfFloatIn(data_batch.data(), dims_data, "data_")}; 
  auto output_predict = new TfFloatOut(dims_target, "pred_");
  // Obtain predicted values
  auto out = tf_session_entity_->Eval(inputs_predict, output_predict);
  
  matrix_t y_hat;
  for (int res_idx = 0; res_idx < bsz; res_idx++) {
    vector_t res = {out[res_idx]};
    y_hat.push_back(res);
  }
  std::for_each(inputs_predict.begin(), inputs_predict.end(), TFIO_Delete);
  TFIO_Delete(output_predict);
  return EigenUtil::ToEigenMat(y_hat);
}

float AugmentedNN::ValidateEpoch(const matrix_eig &mat) {
  // Obtain relevant metadata
  int min_allowed_bsz = 1;
  int bsz = std::min((int)mat.rows(), std::max(batch_size_, min_allowed_bsz));
  int number_of_batches = mat.rows() / bsz;
  int num_cols = mat.cols() - 1;

  std::vector<matrix_eig> y_batch, y_hat_batch;
  // Apply Validation
  // Run through each batch and compute loss/apply backprop
  for (int batch_offset = 0; batch_offset < number_of_batches;
       ++batch_offset) {
    matrix_eig data_batch, target_batch;
    GetBatch(mat, batch_offset, bsz, data_batch, target_batch);

    std::vector<int64_t> dims_data{bsz, num_cols};
    std::vector<int64_t> dims_target{bsz, 1};

    matrix_eig y_hat_eig = Predict(data_batch, bsz);
    y_hat_batch.push_back(y_hat_eig);
    y_batch.push_back(target_batch);
  }
  matrix_eig y = EigenUtil::VStack(y_batch);
  matrix_eig y_hat = EigenUtil::VStack(y_hat_batch);
  return ModelUtil::MeanSqError(y, y_hat);

}
}  // namespace brain
}  // namespace peloton

