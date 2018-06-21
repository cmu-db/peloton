#pragma once

#include "brain/util/eigen_util.h"


namespace peloton{
namespace brain{

// Forward declarations
class BaseForecastModel;

class ModelUtil{
 public:
  static float MeanSqError(const matrix_eig& ytrue, const matrix_eig& ypred);

  /**
   * Utility functions to create batches of the given data
   * , generally useful to be fed into a TF model.
   * The method of creating batches is specialized for Time series
   * forecasting, hence placed here.
   * Batches are in batch-major format. => (Bsz, SeqLen, FeatLen)
   * The way of creating batches closely follows QB5000 code, except that
   * the time-major format there is converted to this Batch-major one.
   * // TODO(saatviks): This part is a little confusing - I should document
   * it in more detail.
   */
  static void GetBatch(const BaseForecastModel* model,
                       const matrix_eig &mat, size_t batch_offset,
                       size_t bsz, size_t bptt,
                       std::vector<matrix_eig> &data,
                       std::vector<matrix_eig> &target);

  /**
   * Given data of the form (timesteps, feat_len), this function generates
   * a feature matrix appropriate for learning a forecast model.
   * Useful for Linear and Kernel Regression Models
   */
  static void GenerateFeatureMatrix(const BaseForecastModel* model,
                                    matrix_eig &data, int regress_dim,
                                    matrix_eig &processed_features,
                                    matrix_eig &processed_forecasts);
};
}
}