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

static inline void SetValue(type::Value *addr, type::Value value) {
  new (addr) type::Value(std::move(value));
}

void ValuesRuntime::OutputBoolean(char *values, uint32_t idx, bool val,
                                  bool is_null) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  if (is_null) {
    SetValue(addr,
             type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN));
  } else {
    SetValue(addr, type::ValueFactory::GetBooleanValue(val));
  }
}

void ValuesRuntime::OutputTinyInt(char *values, uint32_t idx, int8_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetTinyIntValue(val));
}

void ValuesRuntime::OutputSmallInt(char *values, uint32_t idx, int16_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetSmallIntValue(val));
}

void ValuesRuntime::OutputInteger(char *values, uint32_t idx, int32_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetIntegerValue(val));
}

void ValuesRuntime::OutputBigInt(char *values, uint32_t idx, int64_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetBigIntValue(val));
}

void ValuesRuntime::OutputDate(char *values, uint32_t idx, int32_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetDateValue(val));
}

void ValuesRuntime::OutputTimestamp(char *values, uint32_t idx, int64_t val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetTimestampValue(val));
}

void ValuesRuntime::OutputDecimal(char *values, uint32_t idx, double val) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetDecimalValue(val));
}

void ValuesRuntime::OutputVarchar(char *values, uint32_t idx, char *str,
                                  UNUSED_ATTRIBUTE uint32_t len) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  SetValue(addr, type::ValueFactory::GetVarcharValue(str, len, false));
}

void ValuesRuntime::OutputVarbinary(char *values, uint32_t idx, char *ptr,
                                    uint32_t len) {
  auto addr = &reinterpret_cast<type::Value *>(values)[idx];
  const auto *bin_ptr = reinterpret_cast<unsigned char *>(ptr);
  SetValue(addr, type::ValueFactory::GetVarbinaryValue(bin_ptr, len, false));
}

int32_t ValuesRuntime::CompareStrings(const char *str1, uint32_t len1,
                                      const char *str2, uint32_t len2) {
  return type::TypeUtil::CompareStrings(str1, len1, str2, len2);
}

}  // namespace codegen
}  // namespace peloton
