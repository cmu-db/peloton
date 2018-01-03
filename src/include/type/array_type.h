//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value.h
//
// Identification: src/backend/type/array_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "type/type.h"
#include "common/exception.h"
#include "type/value_factory.h"

#include <vector>

namespace peloton {
namespace type {

class ValueFactory;

class ArrayType : public Type {
 public:
  ArrayType(TypeId type);
  ~ArrayType() {}

  // Get the element at a given index in this array
  Value GetElementAt(const Value& val, uint64_t idx) const override;

  TypeId GetElementType(const Value& val UNUSED_ATTRIBUTE) const override;

  // Does this value exist in this array?
  Value InList(const Value& list, const Value &object) const override;

  CmpBool CompareEquals(const Value& left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value& left, const Value &right) const override;
  CmpBool CompareLessThan(const Value& left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThan(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  Value CastAs(const Value& val, const TypeId type_id) const override;

  bool IsInlined(const Value& val UNUSED_ATTRIBUTE) const override { return false; }
  std::string ToString(const Value& val) const override;
  size_t Hash(const Value& val UNUSED_ATTRIBUTE) const override { return 0; }
  void HashCombine(const Value& val UNUSED_ATTRIBUTE, size_t &seed UNUSED_ATTRIBUTE) const override {}

  void SerializeTo(const Value& val UNUSED_ATTRIBUTE, SerializeOutput &out UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize array types to storage");
  }
  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override;

  void SerializeTo(const Value& val UNUSED_ATTRIBUTE, char *storage UNUSED_ATTRIBUTE,
                   bool inlined UNUSED_ATTRIBUTE,
                   AbstractPool *pool UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize array types to storage");
  }
  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage,
                                const bool inlined, AbstractPool *pool = nullptr) const override;

  // Create a copy of this value
  Value Copy(const Value &val) const override;

  // Get the number of elements in the array
  uint32_t GetLength(const Value& val) const override;

};

}  // namespace peloton
}  // namespace type
