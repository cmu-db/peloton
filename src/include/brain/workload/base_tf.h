//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// base_tf.h
//
// Identification: src/include/brain/workload/base_tf.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "brain/util/eigen_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"

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
  // Constructor - sets up session object
  BaseTFModel();
  // Destructor - cleans up any generated model files
  ~BaseTFModel();

  /**
   * Global variable initialization
   * Should be called (1) before training for the first time OR (2) when there
   * is
   * a need for re-training the model.
   */
  void TFInit();

  virtual float TrainEpoch(matrix_eig &data) = 0;
  virtual float ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                              matrix_eig &test_pred, bool return_preds) = 0;

 protected:
  std::unique_ptr<TfSessionEntity<float, float>> tf_session_entity_;
  // Path to the working directory to use to write graph - Must be set in child
  // constructors
  std::string modelgen_path_;
  // Path to the Python TF model to use - Must be set in child constructors
  std::string pymodel_path_;
  // Path to the written graph - Must be set in child constructors
  std::string graph_path_;
  // Function to Generate the tensorflow model graph.
  void GenerateModel(const std::string &args_str);
  // Child classes should set the name of the python model and
  // corresponding protobuf graph path in this function
  // Lambda function for deleting input/output objects
  std::function<void(TfSessionEntityIOBase<float> *)> TFIO_Delete =
      [&](TfSessionEntityIOBase<float> *ptr) { delete ptr; };
  virtual void SetModelInfo() = 0;
};
}  // namespace brain
}  // namespace peloton