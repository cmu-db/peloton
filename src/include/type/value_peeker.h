//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker.h
//
// Identification: src/include/type/value_peeker.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
    PL_ASSERT(value.GetTypeId() == TypeId::PARAMETER_OFFSET);
    return value.GetAs<int32_t>();
  }
  
  static inline bool PeekBoolean(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::BOOLEAN);
    return ((bool)value.GetAs<int8_t>());
  }

  static inline int8_t PeekTinyInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::TINYINT);
    return value.GetAs<int8_t>();
  }

  static inline int16_t PeekSmallInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::SMALLINT);
    return value.GetAs<int16_t>();
  }

  static inline int32_t PeekInteger(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::INTEGER);
    return value.GetAs<int32_t>();
  }

  static inline int64_t PeekBigInt(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::BIGINT);
    return value.GetAs<int64_t>();
  }

  static inline double PeekDouble(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::DECIMAL);
    return value.GetAs<double>();
  }

  static inline int32_t PeekDate(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::DATE);
    return value.GetAs<uint32_t>();
  }

  static inline uint64_t PeekTimestamp(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::TIMESTAMP);
    return value.GetAs<uint64_t>();
  }

  static inline const char *PeekVarchar(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::VARCHAR);
    return value.GetData();
  }

  static inline const char *PeekVarbinary(const Value &value) {
    PL_ASSERT(value.GetTypeId() == TypeId::VARBINARY);
    return value.GetData();
  }
};


}  // namespace type
}  // namespace peloton
