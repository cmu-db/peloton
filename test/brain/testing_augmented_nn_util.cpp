#include "brain/testing_augmented_nn_util.h"
#include <limits>
#include "brain/util/model_util.h"
#include "brain/util/eigen_util.h"
#include "common/harness.h"
#include <random>

namespace peloton {
namespace test {

void TestingAugmentedNNUtil::Test(
    brain::AugmentedNN &model, DistributionType d, 
    size_t val_interval, size_t num_samples, 
    float val_split, bool normalize, float val_loss_thresh, 
    size_t early_stop_patience, float early_stop_delta) {
  LOG_INFO("Using Model: %s", model.ToString().c_str());
  size_t num_tests = model.GetBatchsize();
  matrix_eig all_data = GetData(d, num_samples, num_tests);

  matrix_eig test_data = all_data.bottomRows(num_tests*3);
  matrix_eig data = all_data.topRows(all_data.rows() - num_tests*3);

  brain::Normalizer n(normalize);
  val_interval = std::min<size_t>(val_interval, model.GetEpochs());

  // Determine the split point
  size_t split_point =
      data.rows() - static_cast<size_t>(data.rows() * val_split);

  // Split into train/validate data
  matrix_eig train_data = data.topRows(split_point);
  n.Fit(train_data);
  train_data = n.Transform(train_data);
  matrix_eig validate_data = 
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
      val_loss = model.ValidateEpoch(validate_data);
      train_loss = train_loss_avg.mean();
      EXPECT_LE(train_loss, prev_train_loss);
      LOG_DEBUG("Train Loss: %.10f, Valid Loss: %.10f", train_loss, val_loss);
      prev_train_loss = train_loss;
    }
  }
  EXPECT_LE(val_loss, val_loss_thresh);

  matrix_eig check_data = 
      test_data.block(0, 0, test_data.rows(), test_data.cols() - 1);
  matrix_eig check_target_data = 
      test_data.block(0, test_data.cols() - 1, test_data.rows(), 1);

  matrix_eig test_res = model.Predict(check_data, num_tests*3);

  LOG_INFO("Test with on high end: ");
  for (size_t i = 0; i < 10; i++) {
    LOG_INFO("Truth: %.8f, Pred: %.8f", 
              check_target_data(i,0), test_res(i,0));
  }
  float test_loss = peloton::brain::ModelUtil::MeanSqError(
      check_target_data.topRows(num_tests), 
      test_res.topRows(num_tests)); 
  LOG_INFO("AMSE: %.8f", test_loss);

  LOG_INFO("Test with on low end: ");
  for (size_t i = num_tests; i < num_tests + 10; i++) {
    LOG_INFO("Truth: %.8f, Pred: %.8f", 
              check_target_data(i,0), test_res(i,0));
  }
  test_loss = peloton::brain::ModelUtil::MeanSqError(
      check_target_data.middleRows(num_tests, num_tests), 
      test_res.middleRows(num_tests, num_tests)); 
  LOG_INFO("AMSE: %.8f", test_loss);

  LOG_INFO("Test randomly: ");
  for (size_t i = 2 * num_tests; i < 2 * num_tests + 10; i++) {
    LOG_INFO("Truth: %.8f, Pred: %.8f", 
              check_target_data(i,0), test_res(i,0));
  }
  test_loss = peloton::brain::ModelUtil::MeanSqError(
      check_target_data.bottomRows(num_tests), 
      test_res.bottomRows(num_tests)); 
  LOG_INFO("AMSE: %.8f", test_loss);

}


matrix_eig TestingAugmentedNNUtil::GetData(DistributionType d, 
                                           size_t num_samples, 
                                           size_t num_tests) {
  matrix_eig data;
  switch (d) {
    case DistributionType::UniformDistribution: {
      int NUM_X = 1000;
      matrix_eig hist = matrix_eig::Zero(NUM_X + 1, 1);
      matrix_eig sum = matrix_eig::Zero(NUM_X + 1, 1);
      float sum_hist = 0;
      for (int i = 1; i <= NUM_X; i++) {
        hist(i, 0) = 100;
      }

      for (int i = 1; i <= NUM_X; i++) {
        sum(i, 0) = sum(i - 1, 0) + hist(i, 0);
      }
      sum_hist = sum(NUM_X, 0);

      // generate training and validating data randomly
      data = matrix_eig::Zero(num_samples, 3); //3:lowerbound, upperbound, sel
      std::mt19937 rng;
      rng.seed(std::random_device()());
      std::uniform_int_distribution<std::mt19937::result_type> dist(1, NUM_X);

      // data: [lowerbound, upperbound, truth selectivity]
      for (size_t i = 0; i < num_samples; i++) {
        int l = dist(rng);
        int u = dist(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        // assume the max&min values of the col are known
        // so here preprocessing([min,max]->[-1,1]) can be done
        data(i, 0) = (2 / (float)NUM_X) * l - 1;
        data(i, 1) = (2 / (float)NUM_X) * u - 1;
        data(i, 2) = sel;
      }
   
      float HIGH_SEL = 0.8;
      float LOW_SEL = 0.2;

      matrix_eig test_random_data = matrix_eig::Zero(num_tests, 3);
      matrix_eig test_highsel_data = matrix_eig::Zero(num_tests, 3);
      matrix_eig test_lowsel_data = matrix_eig::Zero(num_tests, 3);

      // generate test data with high selectivity
      for (size_t i = 0; i < num_tests; i++) {
        int l, u;
        float sel;
        do {
          l = dist(rng);
          u = dist(rng);
          if (l > u) {
            std::swap(l, u);
          }
          sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        } while(sel <= HIGH_SEL);
        test_highsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_highsel_data(i, 1) = (2 / (float)NUM_X) * u - 1;
        test_highsel_data(i, 2) = sel;
      }

      // generate test data with low selectivity
      for (size_t i = 0; i < num_tests; i++) {
        int l, u;
        float sel;
        do {
          l = dist(rng);
          u = dist(rng);
          if (l > u) {
            std::swap(l, u);
          }
          sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        } while(sel >= LOW_SEL);
        test_lowsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_lowsel_data(i, 1) = (2 / (float)NUM_X) * u - 1;
        test_lowsel_data(i, 2) = sel;
      }

      // generate test data with random selectivity
      for (size_t i = 0; i < num_tests; i++) {
        int l = dist(rng);
        int u = dist(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1,0)) / sum_hist;
        test_random_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_random_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
        test_random_data(i, 2) = sel;
      }

      std::vector<matrix_eig> data_vec = {data, test_highsel_data,
                                          test_lowsel_data, test_random_data};
      data = peloton::brain::EigenUtil::VStack(data_vec);

      break;
    }
    case DistributionType::SkewedDistribution: {
      // generate skewed dataset
      int NUM_X = 1000;
      matrix_eig hist = matrix_eig::Zero(NUM_X + 1, 1);
      matrix_eig sum = matrix_eig::Zero(NUM_X + 1, 1);
      float sum_hist = 0;

      // skewed
      for (int i = 1; i < 100; i++) {
        hist(i, 0) = 2 + std::round(100 * 
                              std::exp(-0.001 * std::pow(i - 100.0, 2)));
      }
      for (int i = 100; i <= NUM_X; i++) {
        hist(i, 0) = 2 + std::round(100 * 
                              std::exp(-0.00008 * std::pow(i - 100.0, 2)));
      }

      for (int i = 1; i <= NUM_X; i++) {
        sum(i, 0) = sum(i - 1, 0) + hist(i, 0);
      }
      sum_hist = sum(NUM_X, 0);

      // generate training and testing data randomly
      data = matrix_eig::Zero(num_samples, 3);
      std::mt19937 rng;
      rng.seed(std::random_device()());
      std::uniform_int_distribution<std::mt19937::result_type> dist(1, NUM_X);

      // data: [lowerbound, upperbound, truth selectivity]
      for (size_t i = 0; i < num_samples; i++) {
        int l = dist(rng);
        int u = dist(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        // assume the max&min values of the col are known
        // so here preprocessing([min,max]->[-1,1]) can be done
        data(i, 0) = (2 / (float)NUM_X) * l - 1;
        data(i, 1) = (2 / (float)NUM_X) * u - 1;
        data(i, 2) = sel;
      }
      matrix_eig test_lowsel_data = matrix_eig::Zero(num_tests, 3);
      matrix_eig test_highsel_data = matrix_eig::Zero(num_tests, 3);
      matrix_eig test_random_data = matrix_eig::Zero(num_tests, 3);
      std::uniform_int_distribution<std::mt19937::result_type> dist_low(300, 999);
      std::uniform_int_distribution<std::mt19937::result_type> dist_high(50, 150);

      // generate test data on the low end
      for (size_t i = 0; i < num_tests; i++) {
        int l = dist_low(rng);
        int u = dist_low(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        test_lowsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_lowsel_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
        test_lowsel_data(i, 2) = sel;
      }

      // generate test data on the high end
      for (size_t i = 0; i < num_tests; i++) {
        int l = dist_high(rng);
        int u = dist_high(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        test_highsel_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_highsel_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
        test_highsel_data(i, 2) = sel;
      }
  
      // generate test data randomly
      for (size_t i = 0; i < num_tests; i++) {
        int l = dist(rng);
        int u = dist(rng);
        if (l > u) {
          std::swap(l, u);
        }
        float sel = (sum(u, 0) - sum(l - 1, 0)) / sum_hist;
        test_random_data(i, 0) = (2 / (float)NUM_X) * l - 1;
        test_random_data(i, 1) = (2 / (float)NUM_X) * u - 1; 
        test_random_data(i, 2) = sel;
      }

      std::vector<matrix_eig> data_vec = {data, test_highsel_data,
                                          test_lowsel_data, test_random_data};
      data = peloton::brain::EigenUtil::VStack(data_vec);
      break;
    }
  }
  return data;
}


}  // namespace test
}  // namespace peloton
