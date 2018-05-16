//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_functions.h
//
// Identification: src/include/function/numeric_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

namespace peloton {

namespace codegen {
namespace type {
class Type;
}  // namespace type
}  // namespace codegen

namespace type {
class Value;
}  // namespace value

namespace function {

class NumericFunctions {
 public:
  // Abs
  static double Abs(double arg);
  static type::Value _Abs(const std::vector<type::Value> &args);

  // Sqrt
  static double ISqrt(uint32_t num);
  static double DSqrt(double num);
  static type::Value Sqrt(const std::vector<type::Value> &args);

  // Floor
  static double Floor(double val);
  static type::Value _Floor(const std::vector<type::Value> &args);

  // Round
  static double Round(double arg);
  static type::Value _Round(const std::vector<type::Value> &args);

  // Ceil
  static double Ceil(double args);
  static type::Value _Ceil(const std::vector<type::Value> &args);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Input functions
  ///
  //////////////////////////////////////////////////////////////////////////////

  static bool InputBoolean(const codegen::type::Type &type, const char *ptr,
                           uint32_t len);

  static int8_t InputTinyInt(const codegen::type::Type &type, const char *ptr,
                             uint32_t len);

  static int16_t InputSmallInt(const codegen::type::Type &type, const char *ptr,
                               uint32_t len);

  static int32_t InputInteger(const codegen::type::Type &type, const char *ptr,
                              uint32_t len);

  static int64_t InputBigInt(const codegen::type::Type &type, const char *ptr,
                             uint32_t len);

  static double InputDecimal(const codegen::type::Type &type, const char *ptr,
                             uint32_t len);
};

}  // namespace function
}  // namespace peloton
