//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_io.h
//
// Identification:
// src/include/brain/util/tf_session_entity/tf_session_entity_io.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tensorflow/c/c_api.h>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <type_traits>
#include <vector>
#include "common/logger.h"
#include "common/macros.h"

#define TFSEIO_BASE_TEMPLATE_ARGUMENTS template <typename N>
#define TFSEIO_BASE_TYPE TfSessionEntityIOBase<N>

namespace peloton {
namespace brain {

/**
 * The `TfSessionEntityIOBase` is a base class
 * containing common member variables/functions
 * for the Input/Output wrapper classes to inherit and use.
 * Thus the member variables have been marked as `protected`.
 */
TFSEIO_BASE_TEMPLATE_ARGUMENTS
class TfSessionEntityIOBase {
 public:
  std::string GetPlaceholderName();
  TF_Tensor *&GetTensor();
  ~TfSessionEntityIOBase();

 protected:
  // name of io placeholder
  std::string placeholder_name_;
  // data type of io
  TF_DataType data_type_;
  // tensor holding/allocated for data
  TF_Tensor *tensor_;
  // fn to autodetermine the TF_ data type
  void DetermineDataType();
};
}  // namespace brain
}  // namespace peloton
