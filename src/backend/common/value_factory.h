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
// Value factory
//===--------------------------------------------------------------------===//

class ValueFactory {
 public:

  static inline Value Clone(const Value &src, VarlenPool *dataPool) {
    return Value::Clone(src, dataPool);
  }

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

  static inline Value GetBooleanValue(bool value) {
    return Value::GetBooleanValue(value);
  }

  /// Constructs a value copied into long-lived pooled memory (or the heap)
  /// that will require an explicit Value::free.
  static inline Value GetStringValue(const char *value, VarlenPool* pool = NULL) {
    return Value::GetAllocatedValue(VALUE_TYPE_VARCHAR, value, (size_t)(value ? strlen(value) : 0), pool);
  }

  /// Constructs a value copied into long-lived pooled memory (or the heap)
  /// that will require an explicit Value::free.
  static inline Value GetStringValue(const std::string value, VarlenPool* pool = NULL) {
    return Value::GetAllocatedValue(VALUE_TYPE_VARCHAR, value.c_str(), value.length(), pool);
  }

  /// Constructs a value copied into temporary thread-local storage.
  static inline Value GetTempStringValue(const std::string value) {
    return Value::GetAllocatedValue(VALUE_TYPE_VARCHAR, value.c_str(), value.length(), nullptr);
  }

  static inline Value GetNullStringValue() {
    return Value::GetNullStringValue();
  }

  /// Constructs a value copied into long-lived pooled memory (or the heap)
  /// that will require an explicit Value::free.
  /// Assumes hex-encoded input
  static inline Value GetBinaryValue(const std::string& value, VarlenPool* pool = NULL) {
    size_t rawLength = value.length() / 2;
    unsigned char rawBuf[rawLength];
    HexDecodeToBinary(rawBuf, value.c_str());
    return GetBinaryValue(rawBuf, (int32_t)rawLength, pool);
  }

  /// Constructs a value copied into temporary string pool
  /// Assumes hex-encoded input
  static inline Value GetTempBinaryValue(const std::string& value) {
    size_t rawLength = value.length() / 2;
    unsigned char rawBuf[rawLength];
    HexDecodeToBinary(rawBuf, value.c_str());
    return Value::GetAllocatedValue(VALUE_TYPE_VARBINARY, reinterpret_cast<const char*>(rawBuf), (size_t)rawLength,  nullptr);
  }

  /// Constructs a value copied into long-lived pooled memory (or the heap)
  /// that will require an explicit Value::free.
  /// Assumes raw byte input
  static inline Value GetBinaryValue(const unsigned char* rawBuf, int32_t rawLength, VarlenPool* pool = NULL) {
    return Value::GetAllocatedValue(VALUE_TYPE_VARBINARY, reinterpret_cast<const char*>(rawBuf), (size_t)rawLength, pool);
  }

  static inline Value GetNullBinaryValue() {
    return Value::GetNullBinaryValue();
  }

  /** Returns valuetype = VALUE_TYPE_NULL. Careful with this! */
  static inline Value GetNullValue() {
    return Value::GetNullValue();
  }

  static inline Value GetDecimalValueFromString(const std::string &txt) {
    return Value::GetDecimalValueFromString(txt);
  }

  static Value GetArrayValueFromSizeAndType(size_t elementCount, ValueType elementType)
  {
    return Value::GetAllocatedArrayValueFromSizeAndType(elementCount, elementType);
  }

  static inline Value GetAddressValue(void *address) {
    return Value::GetAddressValue(address);
  }

  // What follows exists for test only!

  static inline Value CastAsBigInt(Value value) {
    if (value.IsNull()) {
      return Value::GetNullValue(VALUE_TYPE_BIGINT);
    }
    return value.CastAsBigInt();
  }

  static inline Value CastAsInteger(Value value) {
    if (value.IsNull()) {
      Value retval(VALUE_TYPE_INTEGER);
      retval.SetNull();
      return retval;
    }

    return value.CastAsInteger();
  }

  static inline Value CastAsSmallInt(Value value) {
    if (value.IsNull()) {
      Value retval(VALUE_TYPE_SMALLINT);
      retval.SetNull();
      return retval;
    }

    return value.CastAsSmallInt();
  }

  static inline Value CastAsTinyInt(Value value) {
    if (value.IsNull()) {
      Value retval(VALUE_TYPE_TINYINT);
      retval.SetNull();
      return retval;
    }

    return value.CastAsTinyInt();
  }

  static inline Value CastAsDouble(Value value) {
    if (value.IsNull()) {
      Value retval(VALUE_TYPE_DOUBLE);
      retval.SetNull();
      return retval;
    }

    return value.CastAsDouble();
  }

  static inline Value CastAsDecimal(Value value) {
    if (value.IsNull()) {
      Value retval(VALUE_TYPE_DECIMAL);
      retval.SetNull();
      return retval;
    }
    return value.CastAsDecimal();
  }

  static inline Value CastAsString(Value value) {
    return value.CastAsString();
  }

  static Value ValueFromSQLDefaultType(const ValueType type, const std::string &value, VarlenPool* pool) {
    switch (type) {
      case VALUE_TYPE_NULL:
      {
        return GetNullValue();
      }
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
      {
        Value retval(VALUE_TYPE_BIGINT);
        int64_t ival = atol(value.c_str());
        retval = GetBigIntValue(ival);
        return retval.CastAs(type);
      }
      case VALUE_TYPE_DECIMAL:
      {
        return GetDecimalValueFromString(value);
      }
      case VALUE_TYPE_DOUBLE:
      {
        double dval = atof(value.c_str());
        return GetDoubleValue(dval);
      }
      case VALUE_TYPE_VARCHAR:
      {
        return GetStringValue(value.c_str(), pool);
      }
      case VALUE_TYPE_VARBINARY:
      {
        return GetBinaryValue(value, pool);
      }
      default:
      {
        // skip to throw
      }
    }

    throw Exception("Default value parsing error.");
  }

};

}  // End peloton namespace
