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
  ArrayType() : Type(TypeId::ARRAY) {}
  ArrayType(Type *elem_type) : Type(TypeId::ARRAY, elem_type) {}
  ~ArrayType() {}

  // Get the element at a given index in this array
  Value GetElementAt(const Value &val, uint64_t idx) const override;

  std::shared_ptr<Type> GetElemType(const Value &val UNUSED_ATTRIBUTE) const;

  // Does this value exist in this array?
  Value InList(const Value &list, const Value &object) const override;

  CmpBool CompareEquals(const Value &left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value &left,
                           const Value &right) const override;
  CmpBool CompareLessThan(const Value &left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value &left,
                                const Value &right) const override;
  CmpBool CompareGreaterThan(const Value &left,
                             const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value &left,
                                   const Value &right) const override;

  Value CastAs(const Value &val, const TypeId type_id) const override;

  bool IsInlined(const Value &val UNUSED_ATTRIBUTE) const override {
    return false;
  }
  std::string ToString(const Value &val) const override;
  size_t Hash(const Value &val UNUSED_ATTRIBUTE) const override { return 0; }
  void HashCombine(const Value &val UNUSED_ATTRIBUTE,
                   size_t &seed UNUSED_ATTRIBUTE) const override {}

  // Serialize this value into the given storage space
  void SerializeTo(const Value &val, SerializeOutput &out) const override;
  void SerializeTo(const Value &val, char *storage, bool inlined,
                   AbstractPool *pool) const override;

  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage, const bool inlined,
                        AbstractPool *pool = nullptr) const override;
  Value DeserializeFrom(SerializeInput &in,
                        AbstractPool *pool = nullptr) const override;

  // Create a copy of this value
  Value Copy(const Value &val) const override;

  // Get the number of elements in the array
  uint32_t GetLength(const Value &val) const override;
};

}  // namespace peloton
}  // namespace type
