//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// augmentedNN.h
//
// Identification: src/include/brain/workload/augmentedNN.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <numeric>
#include <string>
#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {

template <typename Type>
class TfSessionEntityInput;
template <typename Type>
class TfSessionEntityOutput;

using TfFloatIn = TfSessionEntityInput<float>;
using TfFloatOut = TfSessionEntityOutput<float>;

class AugmentedNN : public BaseTFModel {
 public:
  AugmentedNN(int ncol, int order, int nneuron,
              float learn_rate, int batch_size, int epochs);
  /**
   * Train the Tensorflow model
   * @param mat: Contiguous time-series data
   * @return: Average Training loss
   * Given a continuous sequence of data,
   * this function:
   * 1. breaks the data into batches('Batchify')
   * 2. prepares tensorflow-entity inputs/outputs
   * 3. computes loss and applies backprop
   * Finally the average training loss over all the
   * batches is returned.
   */
  float TrainEpoch(const matrix_eig &mat);

  /**
   * @param mat: Contiguous time-series data
   * @param test_true: reference variable to which true values are returned
   * @param test_pred: reference variable to which predicted values are returned
   * @param return_preds: Whether to return predicted/true values
   * @return: Average Validation Loss
   * This applies the same set of steps as TrainEpoch.
   * However instead of applying backprop it obtains predicted values.
   * Then the validation loss is calculated for the relevant sequence
   * - this is a function of segment and horizon.
   */
  float ValidateEpoch(const matrix_eig &mat); 

  void Fit(const matrix_eig &X, const matrix_eig &y, int bsz) override;
  matrix_eig Predict(const matrix_eig &X, int bsz) const override;

  /**
   * @return std::string representing model object
   */
  std::string ToString() const override;
  int GetEpochs() const { return epochs_; }
  int GetBatchsize() const { return batch_size_; }
 private:
  /**
   * Utility function to create batches from the given data
   * to be fed into the LSTM model
   */
  void GetBatch(const matrix_eig &mat, size_t batch_offset, size_t bsz,
                matrix_eig &data, matrix_eig &target);
  // Function to generate the args string to feed the python model
  std::string ConstructModelArgsString() const;
  // Attributes needed for the Seq2Seq LSTM model(set by the user/settings.json)
  int ncol_;
  int order_;
  int nneuron_;
  float learn_rate_;
  int batch_size_;
  int epochs_;
};
}  // namespace brain
}  // namespace peloton
