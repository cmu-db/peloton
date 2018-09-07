#include "brain/testing_forecast_util.h"
#include <limits>
#include "brain/util/model_util.h"
#include "brain/util/eigen_util.h"
#include "common/harness.h"
#include <random>

namespace peloton {
namespace test {

void TestingForecastUtil::WorkloadTest(
    brain::BaseForecastModel &model, WorkloadType w, size_t val_interval,
    size_t num_samples, size_t num_feats, float val_split, bool normalize,
    float val_loss_thresh, size_t early_stop_patience, float early_stop_delta) {
  LOG_INFO("Using Model: %s", model.ToString().c_str());

  matrix_eig data = GetWorkload(w, num_samples, num_feats);
  brain::Normalizer n(normalize);
  val_interval = std::min<size_t>(val_interval, model.GetEpochs());

  // Determine the split point
  size_t split_point =
      data.rows() - static_cast<size_t>(data.rows() * val_split);

  // Split into train/test data
  matrix_eig train_data = data.topRows(split_point);
  n.Fit(train_data);
  train_data = n.Transform(train_data);
  matrix_eig test_data = 
      n.Transform(data.bottomRows(
          static_cast<size_t>(data.rows() - split_point)));

  vector_eig train_loss_avg = vector_eig::Zero(val_interval);
  float prev_train_loss = std::numeric_limits<float>::max();
  float val_loss = val_loss_thresh * 2;
  std::vector<float> val_losses;
  for (int epoch = 1; epoch <= model.GetEpochs() &&
                      !brain::ModelUtil::EarlyStop(
                          val_losses, early_stop_patience, early_stop_delta);
       epoch++) {
    auto train_loss = model.TrainEpoch(train_data);
    size_t idx = (epoch - 1) % val_interval;
    train_loss_avg(idx) = train_loss;
    if (epoch % val_interval == 0) {
      val_loss = model.ValidateEpoch(test_data);
      train_loss = train_loss_avg.mean();
      // Below check is not advisable - one off failure chance
      // EXPECT_LE(val_loss, prev_valid_loss);
      // An average on the other hand should surely pass
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.10f, Valid Loss: %.10f", train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }
  EXPECT_LE(val_loss, val_loss_thresh);
}

void TestingForecastUtil::WorkloadTest(
    brain::TimeSeriesEnsemble &model, WorkloadType w, size_t val_interval,
    size_t num_samples, size_t num_feats, float val_split, bool normalize,
    float val_loss_thresh, size_t early_stop_patience, float early_stop_delta) {
  for (size_t i = 0; i < model.ModelsSize(); i++) {
    WorkloadTest(model.GetModel(i), w, val_interval, num_samples, num_feats,
                 val_split, normalize, val_loss_thresh, early_stop_patience,
                 early_stop_delta);
  }
  matrix_eig valid_data = GetWorkload(w, num_samples, num_feats);
  float ensemble_loss = model.Validate(valid_data);
  LOG_DEBUG("Ensemble Loss: %.10f", ensemble_loss);
  EXPECT_LE(ensemble_loss, val_loss_thresh);
}

matrix_eig TestingForecastUtil::GetWorkload(WorkloadType w, size_t num_samples,
                                            size_t num_feats) {
  matrix_eig data;
  switch (w) {
    case WorkloadType::SimpleSinusoidal:
      data = matrix_eig::Zero(num_samples, num_feats);
      // Mixed workload of Sine and Cosine waves
      for (size_t i = 0; i < num_feats; i++) {
        data.col(i).setLinSpaced(num_samples, num_samples * i,
                                 num_samples * (i + 1) - 1);
        if (i % 2 == 0) {
          data.col(i) = data.col(i).array().sin();
        } else {
          data.col(i) = data.col(i).array().cos();
        }
      }
      LOG_INFO("Generating a Sinusoidal workload of dims: %ld x %ld",
               num_samples, num_feats);
      break;
    case WorkloadType::NoisySinusoidal:
      data =
          GetWorkload(WorkloadType::SimpleSinusoidal, num_samples, num_feats) +
          brain::EigenUtil::GaussianNoise(num_samples, num_feats, 0.5, 1);
      LOG_INFO("Adding Gaussian Noise(Mean=0.5, Std = 1.0)");
      break;
    case WorkloadType::SimpleLinear:
      // y=m*x
      data = vector_eig::LinSpaced(num_samples, 0, num_samples - 1)
                 .replicate(1, num_feats);
      for (size_t i = 0; i < num_feats; i++) {
        data.col(i) *= (3 * i);
      }
      LOG_INFO("Generating a Linear workload of dims: %ld x %ld", num_samples,
               num_feats);
      break;
    case WorkloadType::NoisyLinear:
      data = GetWorkload(WorkloadType::SimpleLinear, num_samples, num_feats) +
             brain::EigenUtil::GaussianNoise(num_samples, num_feats, 0.5, 1);
      LOG_INFO("Adding Gaussian Noise(Mean=0.5, Std = 1.0)");
      break;
  }
  return data;
}


}  // namespace test
}  // namespace peloton
