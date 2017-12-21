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

#include "type/boolean_type.h"

#include "type/value_factory.h"
#include "common/logger.h"

namespace peloton {
namespace type {

#define BOOLEAN_COMPARE_FUNC(OP) \
  GetCmpBool(left.value_.boolean \
             OP \
             right.CastAs(TypeId::BOOLEAN).value_.boolean)

BooleanType::BooleanType() : Type(TypeId::BOOLEAN) {}

CmpBool BooleanType::CompareEquals(const Value& left,
                                   const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(==);
}

CmpBool BooleanType::CompareNotEquals(const Value& left,
                                      const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(!=);
}

CmpBool BooleanType::CompareLessThan(const Value& left,
                                     const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(<);
}

CmpBool BooleanType::CompareLessThanEquals(const Value& left,
                                           const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(<=);
}

CmpBool BooleanType::CompareGreaterThan(const Value& left,
                                        const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(>);
}

CmpBool BooleanType::CompareGreaterThanEquals(const Value& left,
                                              const Value& right) const {
  PL_ASSERT(GetTypeId() == TypeId::BOOLEAN);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return BOOLEAN_COMPARE_FUNC(>=);
}

Value BooleanType::Min(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareLessThan(right) == CmpBool::TRUE) return left.Copy();
  return right.Copy();
}

Value BooleanType::Max(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareGreaterThanEquals(right) == CmpBool::TRUE) return left.Copy();
  return right.Copy();
}

std::string BooleanType::ToString(const Value& val) const {
  if (val.IsTrue()) return "true";
  if (val.IsFalse()) return "false";
  return "boolean_null";
}

size_t BooleanType::Hash(const Value& val) const {
  return std::hash<int8_t>{}(val.value_.boolean);
}

void BooleanType::HashCombine(const Value& val, size_t& seed) const {
  val.hash_combine<int8_t>(seed, val.value_.boolean);
}

void BooleanType::SerializeTo(const Value& val, SerializeOutput& out) const {
  out.WriteByte(val.value_.boolean);
  return;
}

void BooleanType::SerializeTo(const Value& val, char* storage,
                              bool inlined UNUSED_ATTRIBUTE,
                              AbstractPool* pool UNUSED_ATTRIBUTE) const {
  *reinterpret_cast<int8_t*>(storage) = val.value_.boolean;
  return;
}

// Deserialize a value of the given type from the given storage space.
Value BooleanType::DeserializeFrom(const char* storage,
                                   const bool inlined UNUSED_ATTRIBUTE,
                                   AbstractPool* pool UNUSED_ATTRIBUTE) const {
  int8_t val = *reinterpret_cast<const int8_t*>(storage);
  return Value(type_id_, val);
}
Value BooleanType::DeserializeFrom(SerializeInput& in UNUSED_ATTRIBUTE,
                                   AbstractPool* pool UNUSED_ATTRIBUTE) const {
  return Value(type_id_, in.ReadByte());
}

Value BooleanType::Copy(const Value& val) const {
  return Value(type_id_, val.value_.boolean);
}

Value BooleanType::CastAs(const Value& val, const TypeId type_id) const {
  switch (type_id) {
    case TypeId::BOOLEAN:
      return val.Copy();
    case TypeId::VARCHAR:
      if (val.IsNull()) return ValueFactory::GetVarcharValue(nullptr, 0);
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("BOOLEAN is not coercable to " +
                  Type::GetInstance(type_id)->ToString());
}

}  // namespace type
}  // namespace peloton
