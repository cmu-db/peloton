//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// value_peeker.h
//
// Identification: src/backend/common/value_peeker.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"
#include "backend/common/types.h"

#include <cassert>

namespace peloton {

/**
 * A class for Peeking into an Value and converting its data to a
 * native C++ type.
 *
 * It is necessary for some classes to have access to the actual value
 * in order to serialize, format for printing, or run tests.
 * Moving the functionality for accessing the private data into
 * these static methods allows Value to define ValuePeeker as its
 * only friend class. Anything that uses this class is a possible
 * candidate for having its functionality moved into Value to ensure
 * consistency.
 */
class ValuePeeker {
  friend class Value;

public:
  static inline ValueType PeekValueType(const Value value) {
    return value.GetValueType();
  }

  static inline int8_t PeekTinyInt(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_TINYINT);
    return value.GetTinyInt();
  }

  static inline int16_t PeekSmallInt(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_SMALLINT);
    return value.GetSmallInt();
  }

  static inline int32_t PeekInteger(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_INTEGER);
    return value.GetInteger();
  }

  // Cast as Int and Peek at value.
  // This is used by index code that need a real number from a tuple.
  static inline int32_t PeekAsInteger(const Value value) {
    return value.CastAsInteger().GetInteger();
  }

  static inline double PeekDouble(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_DOUBLE);
    return value.GetDouble();
  }

  static inline int64_t PeekBigInt(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_BIGINT);
    return value.GetBigInt();
  }

  // Cast as BigInt and Peek at value.
  // This is used by index code that need a real number from a tuple.
  static inline int64_t PeekAsBigInt(const Value value) {
    return value.CastAsBigIntAndGetValue();
  }

  static inline int64_t PeekTimestamp(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_TIMESTAMP);
    return value.GetTimestamp();
  }

  static inline TTInt PeekDecimal(const Value value) {
    return value.GetDecimal();
  }

  /// Exists for testing
  static inline std::string PeekDecimalString(const Value value) {
    return value.CreateStringFromDecimal();
  }

  static inline void *PeekObjectValue(const Value value) {
    assert(value.GetValueType() == VALUE_TYPE_VARCHAR);
    return value.GetObjectValue();
  }

  static inline int32_t PeekObjectLength(const Value value) {
    assert((value.GetValueType() == VALUE_TYPE_VARCHAR) ||
           (value.GetValueType() == VALUE_TYPE_VARBINARY));
    return value.GetObjectLength();
  }

  static inline int64_t PeekAsRawInt64(const Value value) {
    return value.CastAsRawInt64AndGetValue();
  }
};

} // End peloton namespace
