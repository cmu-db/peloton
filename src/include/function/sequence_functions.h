//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_functions.h
//
// Identification: src/include/function/sequence_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "type/value.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace function {

class SequenceFunctions {
 public:

  // Nextval will return the next value of the given sequence
  static uint32_t Nextval(executor::ExecutorContext &ctx, const char *sequence_name);

  // Currval will return the current value of the given sequence
  static uint32_t Currval(executor::ExecutorContext &ctx, const char *sequence_name);

  static type::Value _Nextval(const std::vector<type::Value> &args);
  static type::Value _Currval(const std::vector<type::Value> &args);
};

}  // namespace function
}  // namespace peloton
