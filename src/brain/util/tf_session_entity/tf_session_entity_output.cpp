//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_output.cpp
//
// Identification: src/brain/util/tf_session_entity/tf_session_entity_output.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/tf_session_entity/tf_session_entity_output.h"

namespace peloton {
namespace brain {

TFSEOUT_TEMPLATE_ARGUMENTS
TFSEOUT_TYPE::TfSessionEntityOutput(const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  this->tensor_ =
      TF_AllocateTensor(this->data_type_, nullptr, 0, sizeof(OutputType));
}

TFSEOUT_TEMPLATE_ARGUMENTS
TFSEOUT_TYPE::TfSessionEntityOutput(const std::vector<int64_t> &dims,
                                    const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  int64_t num_elems = 1;
  for (auto elem : dims) {
    num_elems *= elem;
  }
  this->tensor_ = TF_AllocateTensor(this->data_type_, dims.data(), dims.size(),
                                    sizeof(OutputType) * num_elems);
}

// Explicit template Initialization
template class TfSessionEntityOutput<float>;
}  // namespace brain
}  // namespace peloton
