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

BaseTFModel::BaseTFModel() {
  tf_session_entity_ = std::unique_ptr<TfSessionEntity<float, float>>(
      new TfSessionEntity<float, float>());
}

BaseTFModel::~BaseTFModel() { remove(graph_path_.c_str()); }

void BaseTFModel::TFInit() {
  tf_session_entity_->Eval("init");
  PELOTON_ASSERT(tf_session_entity_->IsStatusOk());
}

void BaseTFModel::GenerateModel(const std::string &args_str) {
  std::string cmd = "python3 \"" + pymodel_path_ + "\" " + args_str;
  LOG_DEBUG("Executing command: %s", cmd.c_str());
  UNUSED_ATTRIBUTE bool succ = system(cmd.c_str());
  PELOTON_ASSERT(succ == 0);
}
}
}