//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_io.cpp
//
// Identification: src/brain/util/tf_session_entity/tf_session_entity_io.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/util/tf_session_entity/tf_session_entity_io.h"

namespace peloton {
namespace brain {

TFSEIO_BASE_TEMPLATE_ARGUMENTS
TFSEIO_BASE_TYPE::~TfSessionEntityIOBase() { TF_DeleteTensor(this->tensor_); }

TFSEIO_BASE_TEMPLATE_ARGUMENTS
void TFSEIO_BASE_TYPE::DetermineDataType() {
  if (std::is_same<N, int64_t>::value) {
    data_type_ = TF_INT64;
  } else if (std::is_same<N, int32_t>::value) {
    data_type_ = TF_INT32;
  } else if (std::is_same<N, int16_t>::value) {
    data_type_ = TF_INT16;
  } else if (std::is_same<N, int8_t>::value) {
    data_type_ = TF_INT8;
  } else if (std::is_same<N, int>::value) {
    data_type_ = TF_INT32;
  } else if (std::is_same<N, float>::value) {
    data_type_ = TF_FLOAT;
  } else if (std::is_same<N, double>::value) {
    data_type_ = TF_DOUBLE;
  }
}

TFSEIO_BASE_TEMPLATE_ARGUMENTS
std::string TFSEIO_BASE_TYPE::GetPlaceholderName() { return placeholder_name_; }

TFSEIO_BASE_TEMPLATE_ARGUMENTS
TF_Tensor *&TFSEIO_BASE_TYPE::GetTensor() { return tensor_; }

// Explicit template Initialization
template class TfSessionEntityIOBase<float>;

}  // namespace brain
}  // namespace peloton
