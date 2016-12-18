//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker.h
//
// Identification: src/backend/type/value_peeker.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/boolean_type.h"
#include "type/array_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

//===--------------------------------------------------------------------===//
// Value Peeker
//===--------------------------------------------------------------------===//

class ValuePeeker {
 public:
  static inline int32_t PeekParameterOffset(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::PARAMETER_OFFSET);
    return value.GetAs<int32_t>();
  }
  
  static inline bool PeekBoolean(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::BOOLEAN);
    return ((bool)value.GetAs<int8_t>());
  }

  static inline int8_t PeekTinyInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::TINYINT);
    return value.GetAs<int8_t>();
  }

  static inline int16_t PeekSmallInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::SMALLINT);
    return value.GetAs<int16_t>();
  }

  static inline int32_t PeekInteger(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::INTEGER);
    return value.GetAs<int32_t>();
  }

  static inline int64_t PeekBigInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::BIGINT);
    return value.GetAs<int64_t>();
  }

  static inline double PeekDouble(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::DECIMAL);
    return value.GetAs<double>();
  }

  static inline uint64_t PeekTimestamp(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::TIMESTAMP);
    return value.GetAs<uint64_t>();
  }

  static inline const char *PeekVarchar(const Value &value) {
    PL_ASSERT(value.GetTypeId() == Type::VARCHAR);
    return value.GetData();
  }
};


}  // namespace type
}  // namespace peloton
