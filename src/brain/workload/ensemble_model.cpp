#include "brain/workload/ensemble_model.h"
#include "brain/util/model_util.h
#include <numeric>

namespace peloton{
namespace brain{
TimeSeriesEnsemble::TimeSeriesEnsemble(std::vector<std::unique_ptr<BaseForecastModel>> &models,
                                       std::vector<int> &num_epochs,
                                       std::vector<float> &model_weights,
                                       int batch_size)
    :models_(models),
     num_epochs_(num_epochs),
     batch_size_(batch_size),
     model_weights_(model_weights) {}

float TimeSeriesEnsemble::Train(const matrix_eig &data) {
  vector_t losses;
  for(int i = 0; i < models_.size(); i++) {
    auto &model = models_[i];
    int epochs = num_epochs_[i];
    float loss = 0.0;
    for(int epoch = 1; epoch <= epochs; epoch++) {
      loss = model->TrainEpoch(data);
    }
    losses.push_back(loss);
  }
  return static_cast<float>(std::accumulate(losses.begin(), losses.end(), 0.0)/losses.size());
}

float TimeSeriesEnsemble::Validate(const matrix_eig &data) {
  std::vector<float> losses;
  std::vector<std::vector<matrix_eig>> data_batches, target_batches;
  // TODO(saatviks): Hacky!!
  ModelUtil::GetBatches(*models_[0], data, batch_size_, data_batches, target_batches);

  PELOTON_ASSERT(data_batches.size() == target_batches.size());

  // Run through each batch and compute loss/apply backprop
  std::vector<matrix_eig> y_hat_batch, y_batch;
  for(size_t i = 0; i < data_batches.size(); i++) {
    int bsz = data_batches[i].size();
    matrix_eig data_batch_eig = EigenUtil::VStack(data_batches[i]);
    matrix_eig target_batch_eig = EigenUtil::VStack(target_batches[i]);
    matrix_eig y_hat_ensemble_i = matrix_eig::Zero(data_batch_eig.rows(), data_batch_eig.cols());
    for(int model_idx = 0; model_idx < models_.size(); model_idx++) {
      y_hat_ensemble_i += models_[model_idx]->Predict(data_batch_eig, bsz)*model_weights_[i];
    }

    y_hat_batch.push_back(y_hat_ensemble_i);
    y_batch.push_back(target_batch_eig);
  }
  matrix_eig y = EigenUtil::VStack(y_batch);
  matrix_eig y_hat = EigenUtil::VStack(y_hat_batch);
  return ModelUtil::MeanSqError(y, y_hat);
}

}
}
