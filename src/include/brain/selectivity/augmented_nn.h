//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// augmented_nn.h
//
// Identification: src/include/brain/workload/augmented_nn.h
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

/**
 * AugmentedNN is used to predict selectivity or cardinality
 * for range predicates, e.g, 
 *   SELECT * FROM table WHERE c1 >= l1 AND c1 <= u1
 *                         AND c2 >= l2 AND c2 <= u2
 *                         AND ... 
 * Input is [l1, u1, l2, u2, ...]
 * Output is selectivity or cardinality
 */
class AugmentedNN : public BaseTFModel {
 public:
  AugmentedNN(int ncol, int order, int nneuron,
              float learn_rate, int batch_size, int epochs);
  /**
   * Train the Tensorflow model
   * @param mat: Training data
   * @return: Average Training loss
   * Given a matrix of training data,
   * this function:
   * 1. breaks the data into batches('Batchify')
   * 2. prepares tensorflow-entity inputs/outputs
   * 3. computes loss and applies backprop
   * Finally the average training loss over all the
   * batches is returned.
   */
  float TrainEpoch(const matrix_eig &mat);

  /**
   * @param mat: Validating data
   * @return: Average Validation Loss
   * This applies the same set of steps as TrainEpoch.
   * However instead of applying backprop it obtains predicted values.
   * Then the validation loss is calculated.
   */
  float ValidateEpoch(const matrix_eig &mat); 

  /**
   * @param X: Input for the model
   * @param y: Ground truth output corresponding to X
   * @param bsz: Batchsize
   * This function applies backprop once.
   */ 
  void Fit(const matrix_eig &X, const matrix_eig &y, int bsz) override;

  /**
   * @param X: Input for the model
   * @param bsz: Batchsize
   * This function gets prediction for the input from the model.
   */
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

  // Attributes needed for the AugmentedNN model
  // number of columns used as input
  int column_num_;
  // max order for activation function
  int order_;
  // number of neurons in hidden layer
  int neuron_num_;
  float learn_rate_;
  int batch_size_;
  int epochs_;
};
}  // namespace brain
}  // namespace peloton
