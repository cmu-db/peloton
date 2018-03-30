//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// base_tf.cpp
//
// Identification: src/brain/workload/base_tf.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {

/**
 * Constructor for any Tensorflow model
 * @param graph_path: path to the Tensorflow model graph
 */
BaseTFModel::BaseTFModel(const std::string &graph_path) {
  tf_session_entity_ = std::unique_ptr<TfSessionEntity<float, float>>(
      new TfSessionEntity<float, float>());
  tf_session_entity_->ImportGraph(graph_path);
  assert(tf_session_entity_->IsStatusOk());
}

/**
 * Global variable initialization
 * Should be called (1) before training for the first time OR (2) when there is
 * a need for re-training the model.
 */
void BaseTFModel::TFInit() {
  tf_session_entity_->Eval("init");
  assert(tf_session_entity_->IsStatusOk());
}
}
}