//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bithise_functions.h
//
// Identification: src/backend/expression/bithise_functions.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>
#include <string>
#include <limits.h>

#include "backend/common/value.h"

namespace peloton {

template<> inline Value Value::CallUnary<FUNC_VOLT_BITNOT>() const {
  if (GetValueType() != VALUE_TYPE_BIGINT) {
    // The parser should enforce this for us, but just in case...
    throw Exception("unsupported non-BigInt type for SQL BITNOT function");
  }

  if (IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t result = ~(GetBigInt());
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BITNOT would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }

  return GetBigIntValue(result);
}

template<> inline Value Value::CallUnary<FUNC_VOLT_HEX>() const {
  if (GetValueType() != VALUE_TYPE_BIGINT) {
    // The parser should enforce this for us, but just in case...
    throw Exception("unsupported non-BigInt type for SQL HEX function");
  }

  if (IsNull()) {
    return GetNullStringValue();
  }
  int64_t inputDecimal = GetBigInt();

  std::stringstream ss;
  ss << std::hex << std::uppercase << inputDecimal; // decimal_value
  std::string res (ss.str());
  return GetTempStringValue(res.c_str(),res.length());
}

template<> inline Value Value::CallUnary<FUNC_VOLT_BIN>() const {
  if (GetValueType() != VALUE_TYPE_BIGINT) {
    // The parser should enforce this for us, but just in case...
    throw Exception("unsupported non-BigInt type for SQL BIN function");
  }

  if (IsNull()) {
    return GetNullStringValue();
  }
  uint64_t inputDecimal = uint64_t(GetBigInt());

  std::stringstream ss;
  const size_t uint64_size = sizeof(inputDecimal)*CHAR_BIT;
  uint64_t mask = 0x1ULL << (uint64_size - 1);
  int idx = int(uint64_size - 1);
  for (;0 <= idx && (inputDecimal & mask) == 0; idx -= 1) {
    mask >>= 1;
  }
  for (; 0 <= idx; idx -= 1) {
    ss << ((inputDecimal & mask) ? '1' : '0');
    mask >>= 1;
  }
  std::string res (ss.str());
  if (res.size() == 0) {
    res = std::string("0");
  }
  return GetTempStringValue(res.c_str(),res.length());
}

template<> inline Value Value::Call<FUNC_BITAND>(const std::vector<Value>& arguments) {
  assert(arguments.size() == 2);
  const Value& lval = arguments[0];
  const Value& rval = arguments[1];
  if (lval.GetValueType() != VALUE_TYPE_BIGINT || rval.GetValueType() != VALUE_TYPE_BIGINT) {
    throw Exception("unsupported non-BigInt type for SQL BITAND function");
  }

  if (lval.IsNull() || rval.IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t lv = lval.GetBigInt();
  int64_t rv = rval.GetBigInt();

  int64_t result = lv & rv;
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BITAND would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }
  return GetBigIntValue(result);
}


template<> inline Value Value::Call<FUNC_BITOR>(const std::vector<Value>& arguments) {
  assert(arguments.size() == 2);
  const Value& lval = arguments[0];
  const Value& rval = arguments[1];
  if (lval.GetValueType() != VALUE_TYPE_BIGINT || rval.GetValueType() != VALUE_TYPE_BIGINT) {
    throw Exception("unsupported non-BigInt type for SQL BITOR function");
  }

  if (lval.IsNull() || rval.IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t lv = lval.GetBigInt();
  int64_t rv = rval.GetBigInt();

  int64_t result = lv | rv;
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BITOR would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }
  return GetBigIntValue(result);
}


template<> inline Value Value::Call<FUNC_BITXOR>(const std::vector<Value>& arguments) {
  assert(arguments.size() == 2);
  const Value& lval = arguments[0];
  const Value& rval = arguments[1];
  if (lval.GetValueType() != VALUE_TYPE_BIGINT || rval.GetValueType() != VALUE_TYPE_BIGINT) {
    throw Exception("unsupported non-BigInt type for SQL BITXOR function");
  }

  if (lval.IsNull() || rval.IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t lv = lval.GetBigInt();
  int64_t rv = rval.GetBigInt();

  int64_t result = lv ^ rv;
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BITXOR would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }
  return GetBigIntValue(result);
}


template<> inline Value Value::Call<FUNC_VOLT_BIT_SHIFT_LEFT>(const std::vector<Value>& arguments) {
  assert(arguments.size() == 2);
  const Value& lval = arguments[0];
  if (lval.GetValueType() != VALUE_TYPE_BIGINT) {
    throw Exception("unsupported non-BigInt type for SQL BIT_SHIFT_LEFT function");
  }

  const Value& rval = arguments[1];

  if (lval.IsNull() || rval.IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t lv = lval.GetBigInt();
  int64_t shifts = rval.CastAsBigIntAndGetValue();
  if (shifts < 0) {
    throw Exception(
        "unsupported negative value for bit shifting");
  }
  // shifting by more than 63 bits.Is undefined behavior
  if (shifts > 63) {
    return GetBigIntValue(0);
  }

  int64_t result = lv << shifts;
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BIT_SHIFT_LEFT would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }

  return GetBigIntValue(result);
}

template<> inline Value Value::Call<FUNC_VOLT_BIT_SHIFT_RIGHT>(const std::vector<Value>& arguments) {
  assert(arguments.size() == 2);
  const Value& lval = arguments[0];
  if (lval.GetValueType() != VALUE_TYPE_BIGINT) {
    throw Exception("unsupported non-BigInt type for SQL BIT_SHIFT_RIGHT function");
  }

  const Value& rval = arguments[1];

  if (lval.IsNull() || rval.IsNull()) {
    return GetNullValue(VALUE_TYPE_BIGINT);
  }

  int64_t lv = lval.GetBigInt();
  int64_t shifts = rval.CastAsBigIntAndGetValue();
  if (shifts < 0) {
    throw Exception(
        "unsupported negative value for bit shifting");
  }
  // shifting by more than 63 bits.Is undefined behavior
  if (shifts > 63) {
    return GetBigIntValue(0);
  }

  // right logical shift, padding 0s without taking care of sign bits
  int64_t result = (int64_t)(((uint64_t) lv) >> shifts);
  if (result == INT64_NULL) {
    throw Exception(
        "Application of bithise function BIT_SHIFT_RIGHT would produce INT64_MIN, "
        "which.Is reserved for SQL NULL values.");
  }

  return GetBigIntValue(result);
}

}  // End peloton namespace


