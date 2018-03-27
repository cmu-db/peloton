#include <numeric>
#include <string>
#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {

using TfFloatIn = TfSessionEntityInput<float>;
using TfFloatOut = TfSessionEntityOutput<float>;

class Seq2SeqLSTM : public BaseTFModel {
 public:
  Seq2SeqLSTM(const std::string &graph_path, float learn_rate,
              float dropout_ratio, float clip_norm, int batch_size, int horizon,
              int bptt, int segment);
  // Fn for training the Seq2Seq LSTM
  float TrainEpoch(matrix_eig &data) override;
  // Fn for validating the Seq2Seq LSTM
  float ValidateEpoch(matrix_eig &data, matrix_eig &test_true, matrix_eig &test_pred,
                      bool return_preds) override;

 private:
  void GetBatch(const matrix_eig &mat, size_t batch_offset, size_t bsz,
                std::vector<float> &data, std::vector<float> &target);
  // Attributes needed for the Seq2Seq LSTM model(set by the user/settings.json)
  float learn_rate_;
  float dropout_ratio_;
  float clip_norm_;
  int batch_size_;
  int horizon_;
  int segment_;
  int bptt_;
};
}  // namespace brain
}  // namespace peloton