//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value.h
//
// Identification: src/backend/common/array_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "common/type.h"
#include "common/exception.h"
#include "common/value_factory.h"

#include <vector>

namespace peloton {
namespace common {

class ValueFactory;

class ArrayType : public Type {
 public:
  ArrayType(): Type(Type::ARRAY){}
  ~ArrayType() {}

  // Get the element at a given index in this array
  Value GetElementAt(const Value& val, uint64_t idx) const;

  Type::TypeId GetElementType(const Value& val UNUSED_ATTRIBUTE) const;

  // Does this value exist in this array?
  Value InList(const Value& list, const Value &object) const;

  Value CompareEquals(const Value& left, const Value &right) const override;
  Value CompareNotEquals(const Value& left, const Value &right) const override;
  Value CompareLessThan(const Value& left, const Value &right) const override;
  Value CompareLessThanEquals(const Value& left, const Value &right) const override;
  Value CompareGreaterThan(const Value& left, const Value &right) const override;
  Value CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  Value CastAs(const Value& val, const Type::TypeId type_id) const override;

  bool IsInlined(const Value& val UNUSED_ATTRIBUTE) const override { return false; }
  std::string ToString(const Value& val UNUSED_ATTRIBUTE) const override { return ""; }
  size_t Hash(const Value& val UNUSED_ATTRIBUTE) const override { return 0; }
  void HashCombine(const Value& val UNUSED_ATTRIBUTE, size_t &seed UNUSED_ATTRIBUTE) const override {}

  void SerializeTo(const Value& val UNUSED_ATTRIBUTE, SerializeOutput &out UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize array types to storage");
  }

  void SerializeTo(const Value& val UNUSED_ATTRIBUTE, char *storage UNUSED_ATTRIBUTE,
                   bool inlined UNUSED_ATTRIBUTE,
                   VarlenPool *pool UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize array types to storage");
  }

  Value Copy(const Value& val UNUSED_ATTRIBUTE) const override { return ValueFactory::GetNullValueByType(type_id_); }

};

}  // namespace peloton
}  // namespace common
