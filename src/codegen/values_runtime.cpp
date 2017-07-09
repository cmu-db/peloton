//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.cpp
//
// Identification: src/codegen/values_runtime.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/values_runtime.h"

#include "type/type_util.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

void ValuesRuntime::OutputBoolean(char *values, uint32_t idx, bool val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetBooleanValue(val);
}

void ValuesRuntime::OutputTinyInt(char *values, uint32_t idx, int8_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetTinyIntValue(val);
}

void ValuesRuntime::OutputSmallInt(char *values, uint32_t idx, int16_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetSmallIntValue(val);
}

void ValuesRuntime::OutputInteger(char *values, uint32_t idx, int32_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetIntegerValue(val);
}

void ValuesRuntime::OutputBigInt(char *values, uint32_t idx, int64_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetBigIntValue(val);
}

void ValuesRuntime::OutputDate(char *values, uint32_t idx, int32_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetDateValue(val);
}

void ValuesRuntime::OutputTimestamp(char *values, uint32_t idx, int64_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetTimestampValue(val);
}

void ValuesRuntime::OutputDecimal(char *values, uint32_t idx, double val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetDecimalValue(val);
}

void ValuesRuntime::OutputVarchar(char *values, uint32_t idx, char *str,
                                  UNUSED_ATTRIBUTE uint32_t len) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  // There are two types of VARCHAR: one is from storage and the other is from
  // the query, e.g. CASE. In the latter case, the variable 'len' does not
  // include a null terminator at the end of the array, while it does in the
  // former. This function is used for both of them, so computes based on 'str'.
  vals[idx] = type::ValueFactory::GetVarcharValue(str, false);
}

void ValuesRuntime::OutputVarbinary(char *values, uint32_t idx, char *ptr,
                                    uint32_t len) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  const auto *bin_ptr = reinterpret_cast<unsigned char *>(ptr);
  vals[idx] = type::ValueFactory::GetVarbinaryValue(bin_ptr, len, false);
}

int32_t ValuesRuntime::CompareStrings(const char *str1, uint32_t len1,
                                      const char *str2, uint32_t len2) {
  return type::TypeUtil::CompareStrings(str1, len1, str2, len2);
}

}  // namespace codegen
}  // namespace peloton
