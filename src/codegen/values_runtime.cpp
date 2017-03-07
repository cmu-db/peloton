//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.cpp
//
// Identification: src/codegen/values_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/values_runtime.h"

#include <cstdint>

#include "common/logger.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

void ValuesRuntime::outputTinyInt(char *values, uint32_t idx, int8_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetTinyIntValue(val);
}

void ValuesRuntime::outputSmallInt(char *values, uint32_t idx, int16_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetSmallIntValue(val);
}

void ValuesRuntime::outputInteger(char *values, uint32_t idx, int32_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetIntegerValue(val);
}

void ValuesRuntime::outputBigInt(char *values, uint32_t idx, int64_t val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  LOG_DEBUG("Got big integer: %ld", val);
  vals[idx] = type::ValueFactory::GetBigIntValue(val);
}

void ValuesRuntime::outputDouble(char *values, uint32_t idx, double val) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  vals[idx] = type::ValueFactory::GetDecimalValue(val);
}

void ValuesRuntime::outputVarchar(char *values, uint32_t idx, char *str,
                                  uint32_t len) {
  type::Value *vals = reinterpret_cast<type::Value *>(values);
  std::string string_val{str, len};
  vals[idx] = type::ValueFactory::GetVarcharValue(string_val);
}

}  // namespace codegen
}  // namespace peloton