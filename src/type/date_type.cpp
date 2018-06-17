//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_type.cpp
//
// Identification: src/type/date_type.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/date_type.h"

#include "function/date_functions.h"
#include "type/value_factory.h"

namespace peloton {
namespace type {

DateType::DateType() : Type(TypeId::DATE) {}

CmpBool DateType::CompareEquals(const Value &left, const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() == right.GetAs<int32_t>());
}

CmpBool DateType::CompareNotEquals(const Value &left,
                                   const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() != right.GetAs<int32_t>());
}

CmpBool DateType::CompareLessThan(const Value &left, const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() < right.GetAs<int32_t>());
}

CmpBool DateType::CompareLessThanEquals(const Value &left,
                                        const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() <= right.GetAs<int32_t>());
}

CmpBool DateType::CompareGreaterThan(const Value &left,
                                     const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() > right.GetAs<int32_t>());
}

CmpBool DateType::CompareGreaterThanEquals(const Value &left,
                                           const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CmpBool::NULL_;
  return GetCmpBool(left.GetAs<int32_t>() >= right.GetAs<int32_t>());
}

Value DateType::Min(const Value &left, const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareLessThan(right) == CmpBool::CmpTrue) return left.Copy();
  return right.Copy();
}

Value DateType::Max(const Value &left, const Value &right) const {
  PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);
  if (left.CompareGreaterThan(right) == CmpBool::CmpTrue) return left.Copy();
  return right.Copy();
}

std::string DateType::ToString(const Value &val) const {
  // Null
  if (val.IsNull()) {
    return "date_null";
  }

  int32_t year, month, day;
  function::DateFunctions::JulianToDate(val.value_.date, year, month, day);
  return StringUtil::Format("%04d-%02d-%02d", year, month, day);
}

// Compute a hash value
size_t DateType::Hash(const Value &val) const {
  return std::hash<int32_t>{}(val.value_.date);
}

void DateType::HashCombine(const Value &val, size_t &seed) const {
  val.hash_combine<int32_t>(seed, val.value_.date);
}

void DateType::SerializeTo(const Value &val, SerializeOutput &out) const {
  out.WriteInt(val.value_.date);
}

void DateType::SerializeTo(const Value &val, char *storage,
                           bool inlined UNUSED_ATTRIBUTE,
                           AbstractPool *pool UNUSED_ATTRIBUTE) const {
  *reinterpret_cast<int32_t *>(storage) = val.value_.date;
}

// Deserialize a value of the given type from the given storage space.
Value DateType::DeserializeFrom(const char *storage,
                                const bool inlined UNUSED_ATTRIBUTE,
                                AbstractPool *pool UNUSED_ATTRIBUTE) const {
  int32_t val = *reinterpret_cast<const int32_t *>(storage);
  return Value(type_id_, static_cast<int32_t>(val));
}
Value DateType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                                AbstractPool *pool UNUSED_ATTRIBUTE) const {
  return Value(type_id_, in.ReadInt());
}

// Create a copy of this value
Value DateType::Copy(const Value &val) const { return Value(val); }

Value DateType::CastAs(const Value &val, const TypeId type_id) const {
  switch (type_id) {
    case TypeId::DATE:
      return Copy(val);
    case TypeId::VARCHAR:
      if (val.IsNull()) return ValueFactory::GetVarcharValue(nullptr, 0);
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("Date is not coercible to " +
                  Type::GetInstance(type_id)->ToString());
}

}  // namespace type
}  // namespace peloton