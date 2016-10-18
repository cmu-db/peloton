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

#include "common/boolean_type.h"

#include <iostream>
#include "common/varlen_type.h"
#include "common/value_factory.h"

namespace peloton {
namespace common {

BooleanType::BooleanType()
  : Type(Type::BOOLEAN) {
}

Value BooleanType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean == right.GetAs<int8_t>());
}

Value BooleanType::CompareNotEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean != right.GetAs<int8_t>());
}

Value BooleanType::CompareLessThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean < right.GetAs<int8_t>());
}

Value BooleanType::CompareLessThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean <= right.GetAs<int8_t>());
}

Value BooleanType::CompareGreaterThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean > right.GetAs<int8_t>());
}

Value BooleanType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::BOOLEAN);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
  return ValueFactory::GetBooleanValue(left.value_.boolean >= right.GetAs<int8_t>());
}

std::string BooleanType::ToString(const Value& val) const {
  if (val.IsTrue())
    return "true";
  if (val.IsFalse())
    return "false";
  return "boolean_null";
}

size_t BooleanType::Hash(const Value& val) const {
  return std::hash<int8_t>{}(val.value_.boolean);
}

void BooleanType::HashCombine(const Value& val, size_t &seed) const {
  val.hash_combine<int8_t>(seed, val.value_.boolean);
}

Value BooleanType::Copy(const Value& val) const {
  return Value(type_id_, val.value_.boolean);
}

Value BooleanType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::BOOLEAN:
      return val.Copy();
    case Type::VARCHAR:
      if (val.IsNull())
        return ValueFactory::GetVarcharValue(nullptr, 0);
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("BOOLEAN is not coercable to "
      + Type::GetInstance(type_id)->ToString());
}

}  // namespace peloton
}  // namespace common
