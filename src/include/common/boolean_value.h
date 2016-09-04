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
class BooleanValue : public Value {
 public:
  ~BooleanValue() {}
  BooleanValue(int8_t val);

  // Comparison functions
  Value *CompareEquals(const Value &o) const override;
  Value *CompareNotEquals(const Value &o) const override;
  Value *CompareLessThan(const Value &o) const override;
  Value *CompareLessThanEquals(const Value &o) const override;
  Value *CompareGreaterThan(const Value &o) const override;
  Value *CompareGreaterThanEquals(const Value &o) const override;

  Value *CastAs(const Type::TypeId type_id) const override;

  // Integer types are always inlined
  bool IsInlined() const override { return true; }

  // Debug
  std::string ToString() const override;

  // Compute a hash value
  size_t Hash() const override;
  void HashCombine(size_t &seed) const override;

  // Boolean values aren't real SQL types, and hence, should not be serialized
  void SerializeTo(SerializeOutput &out UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize boolean types to storage");
  }
  void SerializeTo(char *storage UNUSED_ATTRIBUTE,
                   bool inlined UNUSED_ATTRIBUTE,
                   VarlenPool *pool UNUSED_ATTRIBUTE) const override {
    throw Exception("Can't serialize boolean types to storage");
  }

  // Create a copy of this value
  Value *Copy() const override;
};

}  // namespace peloton
}  // namespace common
