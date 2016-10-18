//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value.h
//
// Identification: src/backend/common/boolean_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/boolean_value.h"
#include "common/varlen_value.h"
#include <iostream>

namespace peloton {
namespace common {

BooleanValue::BooleanValue(int8_t val)
  : Value(Type::GetInstance(Type::BOOLEAN)) {
  value_.boolean = (int8_t)val;
}

Value *BooleanValue::CompareEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean == o.GetAs<int8_t>());
}

Value *BooleanValue::CompareNotEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean != o.GetAs<int8_t>());
}

Value *BooleanValue::CompareLessThan(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean < o.GetAs<int8_t>());
}

Value *BooleanValue::CompareLessThanEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean <= o.GetAs<int8_t>());
}

Value *BooleanValue::CompareGreaterThan(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean > o.GetAs<int8_t>());
}

Value *BooleanValue::CompareGreaterThanEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(value_.boolean >= o.GetAs<int8_t>());
}

std::string BooleanValue::ToString() const {
  if (IsTrue())
    return "true";
  if (IsFalse())
    return "false";
  return "boolean_null";
}

size_t BooleanValue::Hash() const {
  return std::hash<int8_t>{}(value_.boolean);
}

void BooleanValue::HashCombine(size_t &seed) const {
  hash_combine<int8_t>(seed, value_.boolean);
}

Value *BooleanValue::Copy() const {
  return new BooleanValue(value_.boolean);
}

Value *BooleanValue::CastAs(const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::BOOLEAN:
      return Copy();
    case Type::VARCHAR:
      if (IsNull())
        return new VarlenValue(nullptr, 0, false);
      return new VarlenValue(ToString(), false);
    default:
      break;
  }
  throw Exception("BOOLEAN is not coercable to "
      + Type::GetInstance(type_id).ToString());
}

}  // namespace peloton
}  // namespace common
