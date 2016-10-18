//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// integer_parent_type.h
//
// Identification: src/backend/common/numeric_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/integer_parent_type.h"

namespace peloton {
namespace common {

// An integer value of the common sizes.
class BigintType : public IntegerParentType {
 public:
  ~BigintType() {}

  BigintType();

  // Other mathematical functions
  Value Add(const Value& left, const Value &right) const override;
  Value Subtract(const Value& left, const Value &right) const override;
  Value Multiply(const Value& left, const Value &right) const override;
  Value Divide(const Value& left, const Value &right) const override;
  Value Modulo(const Value& left, const Value &right) const override;
  Value Sqrt(const Value& val) const override;

  // Comparison functions
  Value CompareEquals(const Value& left, const Value &right) const override;
  Value CompareNotEquals(const Value& left, const Value &right) const override;
  Value CompareLessThan(const Value& left, const Value &right) const override;
  Value CompareLessThanEquals(const Value& left, const Value &right) const override;
  Value CompareGreaterThan(const Value& left, const Value &right) const override;
  Value CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  Value CastAs(const Value& val, const Type::TypeId type_id) const override;

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override;
  void SerializeTo(const Value& val, char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage,
                                const bool inlined, VarlenPool *pool = nullptr) const override;
  Value DeserializeFrom(SerializeInput &in,
                                VarlenPool *pool = nullptr) const override;

  // Create a copy of this value
  Value Copy(const Value& val) const override;

 protected:

  Value OperateNull(const Value& left, const Value &right) const override;

  bool IsZero(const Value& val) const override;
};

}  // namespace common
}  // namespace peloton
