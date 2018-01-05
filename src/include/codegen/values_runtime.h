//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.h
//
// Identification: src/include/codegen/values_runtime.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {
namespace codegen {

class ValuesRuntime {
 public:
  // Write out the given boolean value into the array at the provided index
  static void OutputBoolean(char *values, uint32_t idx, bool val, bool is_null);

  // Write out the given tinyint value into the array at the provided index
  static void OutputTinyInt(char *values, uint32_t idx, int8_t val);

  // Write out the given smallint value into the array at the provided index
  static void OutputSmallInt(char *values, uint32_t idx, int16_t val);

  // Write out the given integer value into the array at the provided index
  static void OutputInteger(char *values, uint32_t idx, int32_t val);

  // Write out the given bigint value into the array at the provided index
  static void OutputBigInt(char *values, uint32_t idx, int64_t val);

  // Write out the given date value into the array at the provided index
  static void OutputDate(char *values, uint32_t idx, int32_t val);

  // Write out the given timestamp value into the array at the provided index
  static void OutputTimestamp(char *values, uint32_t idx, int64_t val);

  // Write out the given decimal value into the array at the provided index
  static void OutputDecimal(char *values, uint32_t idx, double val);

  // Write out the given varchar value into the array at the provided index
  static void OutputVarchar(char *values, uint32_t idx, const char *str,
                            uint32_t len);

  // Write out the given varbinary value into the array at the provided index
  static void OutputVarbinary(char *values, uint32_t idx, const char *str,
                              uint32_t len);

  static int32_t CompareStrings(const char *str1, uint32_t len1,
                                const char *str2, uint32_t len2);
};

}  // namespace codegen
}  // namespace peloton
