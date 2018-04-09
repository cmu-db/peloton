//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_input.h
//
// Identification:
// src/include/brain/util/tf_session_entity/tf_session_entity_input.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "tf_session_entity_io.h"

#define TFSEIN_TEMPLATE_ARGUMENTS template <typename InputType>
#define TFSEIN_TYPE TfSessionEntityInput<InputType>

namespace peloton {
namespace brain {

/**
 * TfSessionEntityInput is a wrapper class to make passing inputs
 * to TFHelper/Tensorflow-C much simpler. It can
 * take commonly used data structures(currently constants,
 * std::vector both 1d and 2d) which are processed to
 * native type flattened arrays needed by by the C-API.
 * Alternatively itll accept native type flattened arrays directly.
 * It minimized user inputs and attempts to maximize the work
 * of filling in the blanks for the TF-C API function calls
 * The TfSessionEntity::Eval requires a std::vector of TfSessionEntityInputs
 * TODO: Add support for other commonly used std containers
 */
TFSEIN_TEMPLATE_ARGUMENTS
class TfSessionEntityInput : public TfSessionEntityIOBase<InputType> {
 public:
  // Const Input
  TfSessionEntityInput(const InputType &input, const std::string &op);
  // 1d vector
  TfSessionEntityInput(const std::vector<InputType> &input,
                       const std::string &op);
  // 2d vector
  TfSessionEntityInput(const std::vector<std::vector<InputType>> &input,
                       const std::string &op);
  // raw flattened input
  TfSessionEntityInput(InputType *input, const std::vector<int64_t> &dims,
                       const std::string &op);

 private:
  // Flattens 2d inputs
  InputType *Flatten(const std::vector<std::vector<InputType>> &elems);
};
}  // namespace brain
}  // namespace peloton
