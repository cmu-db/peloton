//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker.h
//
// Identification: src/backend/common/value_peeker.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/numeric_value.h"
#include "common/boolean_value.h"
#include "common/decimal_value.h"
#include "common/varlen_value.h"
#include "common/array_value.h"
#include "common/timestamp_value.h"

namespace peloton {
namespace common {

//===--------------------------------------------------------------------===//
// Value Peeker
//===--------------------------------------------------------------------===//

class ValuePeeker {
 public:
  static inline int32_t PeekParameterOffset(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::PARAMETER_OFFSET);
    return value->GetAs<int32_t>(); 
  }
  
  static inline bool PeekBoolean(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::BOOLEAN);
    return ((bool)value->GetAs<int8_t>());
  }

  static inline int8_t PeekTinyInt(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::TINYINT);
    return value->GetAs<int8_t>();
  }

  static inline int16_t PeekSmallInt(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::SMALLINT);
    return value->GetAs<int16_t>();
  }

  static inline int32_t PeekInteger(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::INTEGER);
    return value->GetAs<int32_t>();
  }

  static inline int64_t PeekBigInt(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::BIGINT);
    return value->GetAs<int64_t>();
  }

  static inline double PeekDouble(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::DECIMAL);
    return value->GetAs<double>();
  }

  static inline uint64_t PeekTimestamp(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::TIMESTAMP);
    return value->GetAs<uint64_t>();
  }

  static inline const char *PeekVarchar(const Value *value) {
    PL_ASSERT(value->GetTypeId() == Type::VARCHAR);
    return ((VarlenValue *)value)->GetData();
  }
};


}  // namespace common
}  // namespace peloton
