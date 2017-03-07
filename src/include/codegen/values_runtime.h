//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.h
//
// Identification: src/include/codegen/values_runtime.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/value.h"

namespace peloton {
namespace codegen {

class ValuesRuntime {
 public:
  //
  static void outputTinyInt(char* values, uint32_t idx, int8_t val);
  //
  static void outputSmallInt(char* values, uint32_t idx, int16_t val);
  //
  static void outputInteger(char* values, uint32_t idx, int32_t val);
  //
  static void outputBigInt(char* values, uint32_t idx, int64_t val);
  //
  static void outputDouble(char* values, uint32_t idx, double val);
  //
  static void outputVarchar(char* values, uint32_t idx, char* str,
                            uint32_t len);
};

}  // namespace codegen
}  // namespace peloton