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

VarlenValue::VarlenValue(const char *data, uint32_t len)
                            : Value(Type::GetInstance(Type::VARCHAR)) {
  if (len == PELOTON_VARCHAR_MAX_LEN) {
    value_.ptr = (char *) malloc(sizeof(uint32_t));
    assert(value_.ptr != nullptr);
    (*(uint32_t *) value_.ptr) = len;
  }
  else {
    value_.ptr = (char *) malloc(len + sizeof(uint32_t));
    assert(value_.ptr != nullptr);
    (*(uint32_t *) value_.ptr) = len;
    char *dest = value_.ptr + sizeof(uint32_t);
    memcpy(dest, data, len);
  }
}

VarlenValue::VarlenValue(const std::string &data)
                            : Value(Type::GetInstance(Type::VARCHAR)) {
  uint32_t len = data.length();
  value_.ptr = (char *) malloc(len + 1 + sizeof(uint32_t));
  assert(value_.ptr != nullptr);
  (*(uint32_t *) value_.ptr) = len + 1;
  char *dest = value_.ptr + sizeof(uint32_t);
  memcpy(dest, data.c_str(), len + 1);
}

VarlenValue::VarlenValue(const Varlen *varlen)
                            : Value(Type::GetInstance(Type::VARCHAR)) {
  value_.ptr = (char *) malloc(varlen->GetSize() + sizeof(uint32_t));
  (*(uint32_t *) value_.ptr) = varlen->GetSize();
  char *dest = value_.ptr;
  memcpy(dest, varlen->GetRaw(), varlen->GetSize());
}

VarlenValue::~VarlenValue() {
  free(value_.ptr);
}

// Access the raw variable length data
const char *VarlenValue::GetData() const {
  return (value_.ptr + sizeof(uint32_t));
}

// Get the length of the variable length data
uint32_t VarlenValue::GetLength() const {
  return *((uint32_t *) value_.ptr);
}

Value *VarlenValue::CompareEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() == ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) == 0);
}

Value *VarlenValue::CompareNotEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() != ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) != 0);
}

Value *VarlenValue::CompareLessThan(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() < ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) < 0);
}

Value *VarlenValue::CompareLessThanEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() <= ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) <= 0);
}

Value *VarlenValue::CompareGreaterThan(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() > ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) > 0);
}

Value *VarlenValue::CompareGreaterThanEquals(const Value &o) const {
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN
      || ((VarlenValue &)o).GetLength() == PELOTON_VARCHAR_MAX_LEN) {
    return new BooleanValue(GetLength() >= ((VarlenValue &)o).GetLength());
  }
  const char *str1 = GetData();
  const char *str2 = ((VarlenValue &)o).GetData();
  return new BooleanValue(strcmp(str1, str2) >= 0);
}

std::string VarlenValue::ToString() const {
  if (IsNull())
    return "varlen_null";
  if (GetLength() == PELOTON_VARCHAR_MAX_LEN)
    return "varlen_max";
  return std::string(GetData(), GetLength() - 1);
}

size_t VarlenValue::Hash() const {
  return std::hash<std::string>()(ToString()); 
}

void VarlenValue::HashCombine(size_t &seed) const {
  std::string str = ToString();
  hash_combine<std::string>(seed, str);
}

void VarlenValue::SerializeTo(SerializeOutput &out) const {
  out.WriteInt(GetLength());
  if (GetLength() > 0) {
    out.WriteBytes(GetData(), GetLength());
  }
}

void VarlenValue::SerializeTo(char *storage, bool inlined UNUSED_ATTRIBUTE,
                              VarlenPool *pool) const {
  if (pool == nullptr) {
    uint32_t size = GetLength() + sizeof(uint32_t);
    char *data = (char *)malloc(size);
    assert(data != nullptr);
    *reinterpret_cast<const char **>(storage) = data;
    memcpy(data, value_.ptr, size);
  }
  else {
    uint32_t size = GetLength() + sizeof(uint32_t);
    char *data = (char *)pool->Allocate(size);
    assert(data != nullptr);
    *reinterpret_cast<const char **>(storage) = data;
    memcpy(data, value_.ptr, size);
  }
}

Value *VarlenValue::Copy() const {
  uint32_t len = *(value_.ptr);
  return new VarlenValue(GetData(), len);
}

Value *VarlenValue::CastAs(const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::BOOLEAN:
      return ValueFactory::CastAsBoolean(*this);
    case Type::TINYINT:
      return ValueFactory::CastAsTinyInt(*this);
    case Type::SMALLINT:
      return ValueFactory::CastAsSmallInt(*this);
    case Type::INTEGER:
      return ValueFactory::CastAsInteger(*this);
    case Type::TIMESTAMP:
      return ValueFactory::CastAsTimestamp(*this);
    case Type::VARCHAR:
      return Copy();
    default:
      break;
  }
  throw Exception("VARCHAR is not coercable to "
      + Type::GetInstance(type_id).ToString());
}

}  // namespace common
}  // namespace peloton
