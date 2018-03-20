//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_type.cpp
//
// Identification: src/type/date_type.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/date_type.h"

#include "type/value_factory.h"

namespace peloton {
namespace type {

DateType::DateType() : Type(TypeId::DATE) {}

CmpBool DateType::CompareEquals(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() == right.GetAs<int32_t>());
}

CmpBool DateType::CompareNotEquals(const Value& left,
                                   const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() != right.GetAs<int32_t>());
}

CmpBool DateType::CompareLessThan(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() < right.GetAs<int32_t>());
}

CmpBool DateType::CompareLessThanEquals(const Value& left,
                                        const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() <= right.GetAs<int32_t>());
}

CmpBool DateType::CompareGreaterThan(const Value& left,
                                     const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() > right.GetAs<int32_t>());
}

CmpBool DateType::CompareGreaterThanEquals(const Value& left,
                                           const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() >= right.GetAs<int32_t>());
}

Value DateType::Min(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareLessThan(right) == CmpBool::TRUE) return left.Copy();
  return right.Copy();
}

Value DateType::Max(const Value& left, const Value& right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareGreaterThan(right) == CmpBool::TRUE) return left.Copy();
  return right.Copy();
}

// Debug
std::string DateType::ToString(const Value& val) const {
  if (val.IsNull()) return "date_null";
  int32_t tm = val.value_.date;
  tm /= 1000000;
  tm /= 100000;
  uint16_t year = tm % 10000;
  tm /= 10000;
  int tz = tm % 27;
  tz -= 12;
  tm /= 27;
  uint16_t day = tm % 32;
  tm /= 32;
  uint16_t month = tm;
  char str[30];
  char zone[5];
  sprintf(str, "%04d-%02d-%02d", year, month, day);
  if (tz >= 0) {
    str[26] = '+';
  } else
    str[26] = '-';
  if (tz < 0) tz = -tz;
  sprintf(zone, "%02d", tz);
  str[27] = 0;
  return std::string(std::string(str) + std::string(zone));
}

// Compute a hash value
size_t DateType::Hash(const Value& val) const {
  return std::hash<int32_t>{}(val.value_.date);
}

void DateType::HashCombine(const Value& val, size_t& seed) const {
  val.hash_combine<int32_t>(seed, val.value_.date);
}

void DateType::SerializeTo(const Value& val, SerializeOutput& out) const {
  out.WriteInt(val.value_.date);
}

void DateType::SerializeTo(const Value& val, char* storage,
                           bool inlined UNUSED_ATTRIBUTE,
                           AbstractPool* pool UNUSED_ATTRIBUTE) const {
  *reinterpret_cast<int32_t*>(storage) = val.value_.date;
}

// Deserialize a value of the given type from the given storage space.
Value DateType::DeserializeFrom(const char* storage,
                                const bool inlined UNUSED_ATTRIBUTE,
                                AbstractPool* pool UNUSED_ATTRIBUTE) const {
  int32_t val = *reinterpret_cast<const int32_t*>(storage);
  return Value(type_id_, static_cast<int32_t>(val));
}
Value DateType::DeserializeFrom(SerializeInput& in UNUSED_ATTRIBUTE,
                                AbstractPool* pool UNUSED_ATTRIBUTE) const {
  return Value(type_id_, in.ReadInt());
}

// Create a copy of this value
Value DateType::Copy(const Value& val) const { return Value(val); }

Value DateType::CastAs(const Value& val, const TypeId type_id) const {
  switch (type_id) {
    case TypeId::DATE:
      return Copy(val);
    case TypeId::VARCHAR:
      if (val.IsNull()) return ValueFactory::GetVarcharValue(nullptr, 0);
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("Date is not coercable to " +
                  Type::GetInstance(type_id)->ToString());
}

}  // namespace type
}  // namespace peloton