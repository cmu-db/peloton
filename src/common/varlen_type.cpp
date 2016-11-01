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

#include "common/varlen_type.h"

#include "common/value_factory.h"
#include <boost/functional/hash_fwd.hpp>
#include "common/boolean_type.h"

namespace peloton {
namespace common {

VarlenType::VarlenType(TypeId type) :
    Type(type) {
}

VarlenType::~VarlenType() {

}

inline static int CompareStrings(const char *str1, int len1, const char *str2, int len2){
  int ret = strncmp(str1, str2, std::min(len1, len2));
  if (ret == 0 &&len1 != len2){
    ret = len1-len2;
  }
  return ret;
}

// Access the raw variable length data
const char *VarlenType::GetData(const Value& val) const {
  return val.value_.varlen.data;
}

// Get the length of the variable length data
uint32_t VarlenType::GetLength(const Value& val) const {
  return val.value_.varlen.len;
}

Value VarlenType::CompareEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() == right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) == 0);
}

Value VarlenType::CompareNotEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() != right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) != 0);
}

Value VarlenType::CompareLessThan(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() < right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) < 0);
}

Value VarlenType::CompareLessThanEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() <= right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) <= 0);
}

Value VarlenType::CompareGreaterThan(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() > right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) > 0);
}

Value VarlenType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  if (left.GetLength() == PELOTON_VARCHAR_MAX_LEN
      || right.GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return ValueFactory::GetBooleanValue(left.GetLength() >= right.GetLength());
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return ValueFactory::GetBooleanValue(CompareStrings(str1, left.GetLength(), str2, right.GetLength()) >= 0);
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
  val.hash_combine<std::string>(seed, str);
}

void VarlenType::SerializeTo(const Value& val, SerializeOutput &out) const {
  out.WriteInt(val.GetLength());
  if (val.GetLength() > 0) {
    out.WriteBytes(val.GetData(), val.GetLength());
  }
}

void VarlenType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    VarlenPool *pool) const {
  uint32_t size = val.GetLength() + sizeof(uint32_t);
  char *data = (pool == nullptr ) ? new char[size] : (char *) pool->Allocate(size);
  *reinterpret_cast<const char **>(storage) = data;
  PL_MEMCPY(data, &val.value_.varlen.len, sizeof(uint32_t));
  PL_MEMCPY(data + sizeof(uint32_t), val.value_.varlen.data, size-sizeof(uint32_t));
}

// Deserialize a value of the given type from the given storage space.
Value VarlenType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, VarlenPool *pool UNUSED_ATTRIBUTE) const{
  const char *ptr = *reinterpret_cast<const char * const *>(storage);
  if (ptr == nullptr)
  return Value(type_id_, nullptr, 0);
  uint32_t len = *reinterpret_cast<const uint32_t *>(ptr);
  return Value(type_id_, ptr + sizeof(uint32_t), len);
}
Value VarlenType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              VarlenPool *pool UNUSED_ATTRIBUTE) const{
  uint32_t len = in.ReadInt();
  const char *data = (char *) in.getRawPointer(len);
  return Value(type_id_, data, len);
}

Value VarlenType::Copy(const Value& val) const {
  uint32_t len = val.GetLength();
  return Value(val.GetTypeId(), val.GetData(), len);
}

Value VarlenType::CastAs(const Value& val, const Type::TypeId type_id) const {
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
      "VARCHAR is not coercable to " + Type::GetInstance(type_id)->ToString());
}

}  // namespace common
}  // namespace peloton
