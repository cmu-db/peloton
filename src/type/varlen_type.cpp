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

#include "type/varlen_type.h"

#include <boost/functional/hash_fwd.hpp>
#include "type/boolean_type.h"
#include "type/type_util.h"
#include "type/value_factory.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace type {

VarlenType::VarlenType(TypeId type) : Type(type) {}

VarlenType::~VarlenType() {}

// Access the raw variable length data
const char *VarlenType::GetData(const Value &val) const {
  return val.value_.varlen;
}

// Get the length of the variable length data
uint32_t VarlenType::GetLength(const Value &val) const { return val.size_.len; }

// Access the raw varlen data stored from the tuple storage
char *VarlenType::GetData(char *storage) {
  char *ptr = *reinterpret_cast<char **>(storage);
  return ptr;
}

CmpBool VarlenType::CompareEquals(const Value &left, const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) == GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) == 0);
}

CmpBool VarlenType::CompareNotEquals(const Value &left,
                                     const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) != GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) != 0);
}

CmpBool VarlenType::CompareLessThan(const Value &left,
                                    const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) < GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) < 0);
}

CmpBool VarlenType::CompareLessThanEquals(const Value &left,
                                          const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) <= GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) <= 0);
}

CmpBool VarlenType::CompareGreaterThan(const Value &left,
                                       const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) > GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) > 0);
}

CmpBool VarlenType::CompareGreaterThanEquals(const Value &left,
                                             const Value &right) const {
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) >= GetLength(right));
  }
  const char *str1 = left.GetData();
  const char *str2 = right.GetData();
  // TODO strcmp does not work on binary values
  return GetCmpBool(TypeUtil::CompareStrings(str1, GetLength(left), str2,
                                             GetLength(right)) >= 0);
}

Value VarlenType::Min(const Value& left, const Value& right) const {
    PL_ASSERT(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
        return left.OperateNull(right);
    if (left.CompareLessThan(right) == CMP_TRUE)
        return left.Copy();
    return right.Copy();
}

Value VarlenType::Max(const Value& left, const Value& right) const {
    PL_ASSERT(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
        return left.OperateNull(right);
    if (left.CompareGreaterThan(right) == CMP_TRUE)
        return left.Copy();
    return right.Copy();
}

std::string VarlenType::ToString(const Value &val) const {
  uint32_t len = GetLength(val);

  if (val.IsNull()) return "varlen_null";
  if (len == PELOTON_VARCHAR_MAX_LEN) return "varlen_max";
  if (len == 0) {
    return "";
  }
  if (GetTypeId() == Type::VARBINARY) return std::string(GetData(val), len);
  return std::string(GetData(val), len - 1);
}

size_t VarlenType::Hash(const Value &val) const {
  return std::hash<std::string>()(val.ToString());
}

void VarlenType::HashCombine(const Value &val, size_t &seed) const {
  std::string str = val.ToString();
  val.hash_combine<std::string>(seed, str);
}

void VarlenType::SerializeTo(const Value &val, SerializeOutput &out) const {
  uint32_t len = GetLength(val);
  out.WriteInt(len);
  if (len > 0 && len < PELOTON_VALUE_NULL) {
    out.WriteBytes(val.GetData(), len);
  }
}

void VarlenType::SerializeTo(const Value &val, char *storage,
                             bool inlined UNUSED_ATTRIBUTE,
                             AbstractPool *pool) const {
  uint32_t len = GetLength(val);
  char *data;
  if (len == PELOTON_VALUE_NULL) {
    data = nullptr;
  } else {
    uint32_t size = len + sizeof(uint32_t);
    data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
    PL_MEMCPY(data, &len, sizeof(uint32_t));
    PL_MEMCPY(data + sizeof(uint32_t), val.value_.varlen,
              size - sizeof(uint32_t));
  }
  *reinterpret_cast<const char **>(storage) = data;
}

// Deserialize a value of the given type from the given storage space.
Value VarlenType::DeserializeFrom(const char *storage,
                                  const bool inlined UNUSED_ATTRIBUTE,
                                  AbstractPool *pool UNUSED_ATTRIBUTE) const {
  const char *ptr = *reinterpret_cast<const char *const *>(storage);
  if (ptr == nullptr) {
    return Value(type_id_, nullptr, 0, false);
  }
  uint32_t len = *reinterpret_cast<const uint32_t *>(ptr);
  return Value(type_id_, ptr + sizeof(uint32_t), len, false);
}
Value VarlenType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                                  AbstractPool *pool UNUSED_ATTRIBUTE) const {
  uint32_t len = in.ReadInt();
  if (len == PELOTON_VALUE_NULL) {
    return Value(type_id_, nullptr, 0, false);
  } else {
    const char *data = (char *)in.getRawPointer(len);
    return Value(type_id_, data, len, false);
  }
}

Value VarlenType::Copy(const Value &val) const { return Value(val); }

Value VarlenType::CastAs(const Value &val, const Type::TypeId type_id) const {
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
  throw Exception("VARCHAR is not coercable to " +
                  Type::GetInstance(type_id)->ToString());
}

}  // namespace type
}  // namespace peloton
