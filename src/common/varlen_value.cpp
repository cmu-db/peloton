//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_value.h
//
// Identification: src/backend/common/varlen_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/varlen_value.h"
#include "common/boolean_value.h"
#include "common/value_factory.h"
#include <boost/functional/hash_fwd.hpp>

namespace peloton {
namespace common {

VarlenType::VarlenType(TypeId type) :
    Type(type) {
}

VarlenType::~VarlenType() {

}

// Access the raw variable length data
const char *VarlenType::GetData(const Value& val) const {
  return (val.value_.ptr + sizeof(uint32_t));
}

// Get the length of the variable length data
uint32_t VarlenType::GetLength(const Value& val) const {
  return *((uint32_t *) val.value_.ptr);
}

Value *VarlenType::CompareEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() == right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) == 0);
}

Value *VarlenType::CompareNotEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() != right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) != 0);
}

Value *VarlenType::CompareLessThan(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() < right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) < 0);
}

Value *VarlenType::CompareLessThanEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() <= right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) <= 0);
}

Value *VarlenType::CompareGreaterThan(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() > right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) > 0);
}

Value *VarlenType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new Value(Type::BOOLEAN, left.GetLength() >= right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return new Value(Type::BOOLEAN, strcmp(str1, str2) >= 0);
}

std::string VarlenType::ToString(const Value& val) const {
  if (val.IsNull())
    return "varlen_null";
  if (val.GetLength() == PELOTON_VARCHAR_MAX_LEN)
    return "varlen_max";
  if (GetTypeId() == Type::VARBINARY)
    return std::string(val.GetData(), val.GetLength());
  return std::string(val.GetData(), val.GetLength() - 1);
}

size_t VarlenType::Hash(const Value& val) const {
  return std::hash<std::string>()(val.ToString());
}

void VarlenType::HashCombine(const Value& val, size_t &seed) const {
  std::string str = val.ToString();
  hash_combine<std::string>(seed, str);
}

void VarlenType::SerializeTo(const Value& val, SerializeOutput &out) const {
  out.WriteInt(val.GetLength());
  if (val.GetLength() > 0) {
    out.WriteBytes(val.GetData(), val.GetLength());
  }
}

void VarlenType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    VarlenPool *pool) const {
  if (pool == nullptr) {
    uint32_t size = val.GetLength() + sizeof(uint32_t);
    char *data = new char[size];
    PL_ASSERT(data != nullptr);
    *reinterpret_cast<const char **>(storage) = data;
    PL_MEMCPY(data, val.value_.ptr, size);
  } else {
    uint32_t size = val.GetLength() + sizeof(uint32_t);
    char *data = (char *) pool->Allocate(size);
    PL_ASSERT(data != nullptr);
    *reinterpret_cast<const char **>(storage) = data;
    PL_MEMCPY(data, val.value_.ptr, size);
  }
}

Value *VarlenType::Copy(const Value& val) const {
  uint32_t len = val.GetLength();
  return new Value(val.GetTypeId(), val.GetData(), len);
}

Value *VarlenType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
  case Type::BOOLEAN:
    return ValueFactory::CastAsBoolean(val);
  case Type::TINYINT:
    return ValueFactory::CastAsTinyInt(val);
  case Type::SMALLINT:
    return ValueFactory::CastAsSmallInt(val);
  case Type::INTEGER:
    return ValueFactory::CastAsInteger(val);
  case Type::TIMESTAMP:
    return ValueFactory::CastAsTimestamp(val);
  case Type::VARCHAR:
  case Type::VARBINARY:
    return val.Copy();
  default:
    break;
  }
  throw Exception(
      "VARCHAR is not coercable to " + Type::GetInstance(type_id).ToString());
}

}  // namespace common
}  // namespace peloton
