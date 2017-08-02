//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions.h
//
// Identification: src/include/expression/decimal_functions.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>

#include "type/value.h"

namespace peloton {
namespace function {
class DecimalFunctions {
 public:
  static type::Value Sqrt(const std::vector<type::Value>& args);
};
}  // namespace expression
}  // namespace peloton
