//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// model_util.h
//
// Identification: src/include/brain/util/model_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/util/eigen_util.h"

namespace peloton {
namespace brain {

// Forward declarations
class BaseForecastModel;

class ModelUtil {
 public:
  static float MeanSqError(const matrix_eig &ytrue, const matrix_eig &ypred);

  /**
   * Utility function to create a batch of the given data
   * , generally useful to be fed into a TF model.
   * The method of creating batches is specialized for Time series
   * forecasting, hence placed here.
   * Batches are in batch-major format. => (Bsz, SeqLen, FeatLen)
   * The way of creating batches closely follows QB5000 code, except that
   * the time-major format there is converted to this Batch-major one.
   * Refer to ModelUtilTest to better understand whats happening here.
   */
  static void GetBatch(const BaseForecastModel *model, const matrix_eig &mat,
                       size_t batch_offset, size_t bsz,
                       std::vector<matrix_eig> &data,
                       std::vector<matrix_eig> &target);

  /**
   * Converts a dataset/workload(upto RAM sized) passed to it into (X, y) batches
   * useful for either training/validation of models working on batches of data.
   * This is only meant for forecastable models
   * The produced batches are of form [num_batches(outer std::vector), 
   * bsz/num_seq(inner std::vector), (seq_len, feat_len)(eigen matrix)]
   */
  static void GetBatches(const BaseForecastModel *model, const matrix_eig &mat,
                         size_t batch_size, 
                         std::vector<std::vector<matrix_eig>> &data,
                         std::vector<std::vector<matrix_eig>> &target);

  /**
   * Given data of the form (timesteps, feat_len), this function generates
   * a feature matrix appropriate for learning a forecast model.
   * Useful for Linear and Kernel Regression Models
   */
  static void GenerateFeatureMatrix(const BaseForecastModel *model,
                                    matrix_eig &data,
                                    matrix_eig &processed_features,
                                    matrix_eig &processed_forecasts);
};
}  // namespace brain
}  // namespace peloton