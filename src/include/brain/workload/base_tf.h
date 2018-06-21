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
#include "brain/util/tf_session_entity/tf_session_entity_io.h"
#include "brain/util/eigen_util.h"

namespace peloton {
namespace brain {

/**
 * Base Abstract class to inherit for writing ML models
 */
class BaseModel{
 public:
  virtual std::string ToString() const = 0;
};

/**
 * Base Abstract class to inherit for writing forecasting ML models
 */
class BaseForecastModel: public BaseModel {
 public:
  BaseForecastModel(int horizon, int segment)
      : BaseModel(),
        horizon_(horizon),
        segment_(segment) {};
  virtual float TrainEpoch(matrix_eig &data) = 0;
  virtual float ValidateEpoch(matrix_eig &data, matrix_eig &test_true,
                              matrix_eig &test_pred, bool return_preds) = 0;
  int GetHorizon() const { return horizon_; }
  int GetSegment() const { return segment_; }


 protected:
  int horizon_;
  int segment_;
  // TODO(saatviks): Add paddling days and aggregate
};

template <typename InputType, typename OutputType>
class TfSessionEntity;

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
class BaseTFModel: public BaseModel {
 public:
  // Constructor - sets up session object
  BaseTFModel(const std::string& modelgen_path,
              const std::string& pymodel_path,
              const std::string& graph_path);
  // Destructor - cleans up any generated model files
  ~BaseTFModel();

  /**
   * Global variable initialization
   * Should be called (1) before training for the first time OR (2) when there
   * is
   * a need for re-training the model.
   */
  void TFInit();

 protected:
  std::unique_ptr<TfSessionEntity<float, float>> tf_session_entity_;
  // Path to the working directory to use to write graph - Must be set in child
  // constructors
  std::string modelgen_path_;
  // Path to the Python TF model to use - Relative path must be passed in constructor
  std::string pymodel_path_;
  // Path to the written graph - Relative path must be passed in constructor
  std::string graph_path_;
  // Function to Generate the tensorflow model graph.
  void GenerateModel(const std::string &args_str);
  // Lambda function for deleting input/output objects
  std::function<void(TfSessionEntityIOBase<float> *)> TFIO_Delete =
      [&](TfSessionEntityIOBase<float> *ptr) { delete ptr; };

};
}  // namespace brain
}  // namespace peloton