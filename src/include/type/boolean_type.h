//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value.h
//
// Identification: src/backend/common/boolean_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/type.h"
#include "type/value.h"
#include "common/exception.h"

namespace peloton {
namespace type {

// A boolean value isn't a real SQL type, but we treat it as one to keep
// consistent in the expression subsystem.
class BooleanType : public Type {
 public:
  ~BooleanType() {}
  BooleanType();

  // Comparison functions
  CmpBool CompareEquals(const Value& left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value& left, const Value &right) const override;
  CmpBool CompareLessThan(const Value& left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThan(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  // Decimal types are always inlined
  bool IsInlined(const Value&) const override { return true; }

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value&, SerializeOutput &) const override{
    throw Exception("Can't serialize boolean types to storage");
  }

  void SerializeTo(const Value& , char *, bool ,
                   AbstractPool *) const override{
    throw Exception("Can't serialize boolean types to storage");
  }

  // Create a copy of this value
  Value Copy(const Value& val) const override;

  Value CastAs(const Value& val, const Type::TypeId type_id) const override;
};

}  // namespace type
}  // namespace peloton
