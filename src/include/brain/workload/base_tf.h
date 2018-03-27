#pragma once

#include <memory>
#include "brain/util/eigen_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"

namespace peloton {
namespace brain {

/**
 * BaseTFModel is an abstract class defining constructor/basic operations
 * that needed to be supported by a Tensorflow model for sequence/workload
 * prediction.
 * Note that the base assumption is that the graph would be generated in
 * tensorflow-python
 * which is a well developed API. This model graph and its Ops can be serialized
 * to
 * `protobuf` format and imported on the C++ end. The constructor will always
 * accept a string
 * path to the serialized graph and import the same.
 */
class BaseTFModel {
 public:
  void TFInit();
  virtual float TrainEpoch(matrix_eig &data) = 0;
  virtual float ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                              matrix_eig &test_pred, bool return_preds) = 0;
  explicit BaseTFModel(const std::string &graph_path);

 protected:
  std::unique_ptr<TfSessionEntity<float, float>> tf_session_entity_;
};
}  // namespace brain
}  // namespace peloton