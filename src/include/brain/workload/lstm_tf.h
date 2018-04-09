//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lstm_tf.h
//
// Identification: src/include/brain/workload/lstm_tf.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>
#include <string>
#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {

using TfFloatIn = TfSessionEntityInput<float>;
using TfFloatOut = TfSessionEntityOutput<float>;

class TimeSeriesLSTM : public BaseTFModel {
 public:
  TimeSeriesLSTM(int nfeats, int nencoded, int nhid, int nlayers,
                 float learn_rate, float dropout_ratio, float clip_norm,
                 int batch_size, int horizon, int bptt, int segment);
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
  float TrainEpoch(matrix_eig &data) override;

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
  float ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                      matrix_eig &test_pred, bool return_preds) override;

 private:
  /**
   * Utility function to create batches from the given data
   * to be fed into the LSTM model
   */
  void GetBatch(const matrix_eig &mat, size_t batch_offset, size_t bsz,
                std::vector<float> &data, std::vector<float> &target);
  // Function to generate the args string to feed the python model
  std::string ConstructModelArgsString(int nfeats, int nencoded, int nhid,
                                       int nlayers, float learn_rate,
                                       float dropout_ratio, float clip_norm);
  // Attributes needed for the Seq2Seq LSTM model(set by the user/settings.json)
  float learn_rate_;
  float dropout_ratio_;
  float clip_norm_;
  int batch_size_;
  int horizon_;
  int segment_;
  int bptt_;
  void SetModelInfo() override;
};
}  // namespace brain
}  // namespace peloton