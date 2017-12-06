//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions.h
//
// Identification: src/include/function/decimal_functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "type/value.h"

namespace peloton {
namespace function {

class DecimalFunctions {
 public:
  static type::Value _Sqrt(const std::vector<type::Value>& args);
  static type::Value _Floor(const std::vector<type::Value>& args);
  static double Floor(const double val);
  static double Sqrt(double arg);
  static double SqrtInt(int64_t arg);
};

}  // namespace function
}  // namespace peloton
