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
  ArrayType(): Type(Type::ARRAY){}
  ~ArrayType() {}

  // Get the element at a given index in this array
  Value GetElementAt(const Value& val, uint64_t idx) const;

  Type::TypeId GetElementType(const Value& val UNUSED_ATTRIBUTE) const;

  // Does this value exist in this array?
  Value InList(const Value& list, const Value &object) const;

  CmpBool CompareEquals(const Value& left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value& left, const Value &right) const override;
  CmpBool CompareLessThan(const Value& left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThan(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override;

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
                   AbstractPool *pool UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize array types to storage");
  }
  // Access the raw variable length data
  const char *GetData(const Value &val) const {
    return val.value_.array;
  }

  Value Copy(const Value& val UNUSED_ATTRIBUTE) const override {
    switch (val.GetElementType()) {
      case INTEGER: {
        return ValueFactory::GetArrayValue<int32_t>(val.GetTypeId(),*(std::vector<int32_t>*)GetData(val));
      }
      case VARCHAR: {
        return ValueFactory::GetArrayValue<std::string>(val.GetTypeId(),*(std::vector<std::string>*)GetData(val));
      }
      case DECIMAL: {
        return ValueFactory::GetArrayValue<int64_t>(val.GetTypeId(),*(std::vector<int64_t>*)GetData(val));
      }
      default:
        throw NotImplementedException(StringUtil::Format(
            "Value type %d not supported in Copy yet...\n", val.GetTypeId()));
    }

  }

};

}  // namespace peloton
}  // namespace type
