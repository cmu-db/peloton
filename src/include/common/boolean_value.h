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

#include "common/value.h"

namespace peloton {
namespace common {

// A boolean value isn't a real SQL type, but we treat it as one to keep
// consistent in the expression subsystem.
class BooleanType : public Type {
 public:
  ~BooleanType() {}
  BooleanType();

  // Comparison functions
  Value *CompareEquals(const Value& left, const Value &right) const override;
  Value *CompareNotEquals(const Value& left, const Value &right) const override;
  Value *CompareLessThan(const Value& left, const Value &right) const override;
  Value *CompareLessThanEquals(const Value& left, const Value &right) const override;
  Value *CompareGreaterThan(const Value& left, const Value &right) const override;
  Value *CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  Value *CastAs(const Type::TypeId type_id) const override;

  // Decimal types are always inlined
  bool IsInlined(const Value& val) const override { return true; }

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override{
    throw Exception("Can't serialize boolean types to storage");
  }

  void SerializeTo(const Value& val, char *storage, bool inlined,
                   VarlenPool *pool) const override{
    throw Exception("Can't serialize boolean types to storage");
  }

  // Create a copy of this value
  Value *Copy(const Value& val) const override;

  Value *CastAs(Value& val, const Type::TypeId type_id);
};

}  // namespace peloton
}  // namespace common
