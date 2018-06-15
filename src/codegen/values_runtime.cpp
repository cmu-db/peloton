//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.cpp
//
// Identification: src/codegen/values_runtime.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/values_runtime.h"

#include "codegen/runtime_functions.h"
#include "codegen/type/type.h"
#include "type/abstract_pool.h"
#include "type/value.h"
#include "type/type_util.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Output functions
///
////////////////////////////////////////////////////////////////////////////////

namespace {

inline void SetValue(peloton::type::Value *val_ptr,
                     peloton::type::Value &&val) {
  new (val_ptr) peloton::type::Value(val);
}

}  // namespace

void ValuesRuntime::OutputBoolean(char *values, uint32_t idx, bool val,
                                  bool is_null) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  if (is_null) {
    SetValue(&vals[idx], peloton::type::ValueFactory::GetNullValueByType(
                             peloton::type::TypeId::BOOLEAN));
  } else {
    SetValue(&vals[idx], peloton::type::ValueFactory::GetBooleanValue(val));
  }
}

void ValuesRuntime::OutputTinyInt(char *values, uint32_t idx, int8_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetTinyIntValue(val));
}

void ValuesRuntime::OutputSmallInt(char *values, uint32_t idx, int16_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetSmallIntValue(val));
}

void ValuesRuntime::OutputInteger(char *values, uint32_t idx, int32_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetIntegerValue(val));
}

void ValuesRuntime::OutputBigInt(char *values, uint32_t idx, int64_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetBigIntValue(val));
}

void ValuesRuntime::OutputDate(char *values, uint32_t idx, int32_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetDateValue(val));
}

void ValuesRuntime::OutputTimestamp(char *values, uint32_t idx, int64_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetTimestampValue(val));
}

void ValuesRuntime::OutputDecimal(char *values, uint32_t idx, double val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetDecimalValue(val));
}

void ValuesRuntime::OutputVarchar(char *values, uint32_t idx, const char *str,
                                  uint32_t len) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx],
           peloton::type::ValueFactory::GetVarcharValue(str, len, false));
}

void ValuesRuntime::OutputVarbinary(char *values, uint32_t idx, const char *ptr,
                                    uint32_t len) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  const auto *bin_ptr = reinterpret_cast<const unsigned char *>(ptr);
  SetValue(&vals[idx],
           peloton::type::ValueFactory::GetVarbinaryValue(bin_ptr, len, false));
}

}  // namespace codegen
}  // namespace peloton
