//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value_factory.h
//
// Identification: src/backend/common/value_factory.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Value generator
//===--------------------------------------------------------------------===//

class ValueFactory {
 public:
  static inline Value Clone(const Value &src, VarlenPool *dataPool = nullptr) {
    return Value::Clone(src, dataPool);
  }

  static inline Value GetInvalidValue() { return Value::GetInvalidValue(); }

  static inline Value GetTinyIntValue(int8_t value) {
    return Value::GetTinyIntValue(value);
  }

  static inline Value GetSmallIntValue(int16_t value) {
    return Value::GetSmallIntValue(value);
  }

  static inline Value GetIntegerValue(int32_t value) {
    return Value::GetIntegerValue(value);
  }

  static inline Value GetBigIntValue(int64_t value) {
    return Value::GetBigIntValue(value);
  }

  static inline Value GetTimestampValue(int64_t value) {
    return Value::GetTimestampValue(value);
  }

  static inline Value GetDoubleValue(double value) {
    return Value::GetDoubleValue(value);
  }

  static inline Value GetStringValue(std::string value,
                                     VarlenPool *data_pool = nullptr) {
    return Value::GetStringValue(value, data_pool);
  }

  static inline Value GetNullStringValue() {
    return Value::GetNullStringValue();
  }

  static inline Value GetBinaryValue(std::string value,
                                     VarlenPool *data_pool = nullptr) {
    /// uses hex encoding
    return Value::GetBinaryValue(value, data_pool);
  }

  static inline Value GetBinaryValue(unsigned char *value, int32_t len,
                                     VarlenPool *data_pool) {
    return Value::GetBinaryValue(value, len, data_pool);
  }

  static inline Value GetNullBinaryValue() {
    return Value::GetNullBinaryValue();
  }

  /// Returns value of type = VALUE_TYPE_NULL. Careful with this!
  static inline Value GetNullValue() { return Value::GetNullValue(); }

  static inline Value GetDecimalValueFromString(const std::string &txt) {
    return Value::GetDecimalValueFromString(txt);
  }

  static inline Value GetAddressValue(void *address) {
    return Value::GetAddressValue(address);
  }

  static inline Value GetTrue() { return Value::GetTrue(); }

  static inline Value GetFalse() { return Value::GetFalse(); }

  //===--------------------------------------------------------------------===//
  // Testing helpers
  //===--------------------------------------------------------------------===//

  static inline Value CastAsBigInt(Value value) { return value.CastAsBigInt(); }

  static inline Value CastAsInteger(Value value) {
    return value.CastAsInteger();
  }

  static inline Value CastAsSmallInt(Value value) {
    return value.CastAsSmallInt();
  }

  static inline Value CastAsTinyInt(Value value) {
    return value.CastAsTinyInt();
  }

  static inline Value CastAsDouble(Value value) { return value.CastAsDouble(); }

  static inline Value CastAsDecimal(Value value) {
    return value.CastAsDecimal();
  }

  static inline Value CastAsString(Value value) { return value.CastAsString(); }
};

}  // End peloton namespace
