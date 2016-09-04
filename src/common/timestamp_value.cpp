//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value.h
//
// Identification: src/backend/common/timestamp_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/timestamp_value.h"
#include "common/boolean_value.h"
#include "common/varlen_value.h"

namespace peloton {
namespace common {

TimestampValue::TimestampValue(uint64_t val)
  : Value(Type::GetInstance(Type::TIMESTAMP)) {
  value_.timestamp = val;
}

Value *TimestampValue::CompareEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<uint64_t>() == o.GetAs<uint64_t>());
}

Value *TimestampValue::CompareNotEquals(const Value &o) const {
  CheckComparable(o);
  if (o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<uint64_t>() != o.GetAs<uint64_t>());
}

Value *TimestampValue::CompareLessThan(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<uint64_t>() < o.GetAs<uint64_t>());
}

Value *TimestampValue::CompareLessThanEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<uint64_t>() <= o.GetAs<uint64_t>());
}

Value *TimestampValue::CompareGreaterThan(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<int64_t>() > o.GetAs<int64_t>());
}

Value *TimestampValue::CompareGreaterThanEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  return new BooleanValue(GetAs<uint64_t>() >= o.GetAs<uint64_t>());
}

// Debug
std::string TimestampValue::ToString() const {
  if (IsNull())
    return "timestamp_null";
  uint64_t tm = value_.timestamp;
  uint32_t micro = tm % 1000000;
  tm /= 1000000;
  uint32_t second = tm % 100000;
  uint16_t sec = second % 60;
  second /= 60;
  uint16_t min = second % 60;
  second /= 60;
  uint16_t hour = second % 24;
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
  sprintf(str, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
    year, month, day, hour, min, sec, micro);
  if(tz >= 0) {
    str[26] = '+';
  }
  else
    str[26] = '-';
  if (tz < 0)
    tz = -tz;
  sprintf(zone, "%02d", tz);
  str[27] = 0;
  return std::string(std::string(str) + std::string(zone));
}

// Compute a hash value
size_t TimestampValue::Hash() const {
  return std::hash<uint64_t>{}(value_.timestamp);
}

void TimestampValue::HashCombine(size_t &seed) const {
  hash_combine<int64_t>(seed, value_.timestamp);
}

void TimestampValue::SerializeTo(SerializeOutput &out) const {
  out.WriteLong(value_.timestamp);
}

void TimestampValue::SerializeTo(char *storage, bool inlined UNUSED_ATTRIBUTE,
                                 VarlenPool *pool UNUSED_ATTRIBUTE) const {
  *reinterpret_cast<uint64_t *>(storage) = value_.timestamp;
}

// Create a copy of this value
Value *TimestampValue::Copy() const {
  return new TimestampValue(value_.timestamp);
}

Value *TimestampValue::CastAs(const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::TIMESTAMP:
      return Copy();
    case Type::VARCHAR:
      if (IsNull())
        return new VarlenValue(nullptr, 0);
      return new VarlenValue(ToString());
    default:
      break;
  }
  throw Exception("TIMESTAMP is not coercable to "
      + Type::GetInstance(type_id).ToString());
}

}  // namespace peloton
}  // namespace common
