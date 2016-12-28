//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_value.h
//
// Identification: src/backend/common/decimal_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/numeric_type.h"

namespace peloton {
namespace type {

class DecimalType : public NumericType {
 public:

  DecimalType();
  //DecimalValue(DecDef definition);

  // Other mathematical functions
  Value Add(const Value& left, const Value &right) const override;
  Value Subtract(const Value& left, const Value &right) const override;
  Value Multiply(const Value& left, const Value &right) const override;
  Value Divide(const Value& left, const Value &right) const override;
  Value Modulo(const Value& left, const Value &right) const override;
  Value Min(const Value& left, const Value &right) const override;
  Value Max(const Value& left, const Value &right) const override;
  Value Sqrt(const Value& val) const override;
  bool IsZero(const Value& val) const override;

  // Comparison functions
  CmpBool CompareEquals(const Value& left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value& left, const Value &right) const override;
  CmpBool CompareLessThan(const Value& left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThan(const Value& left, const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override;

  Value CastAs(const Value& val, const Type::TypeId type_id) const override;

  // Decimal types are always inlined
  bool IsInlined(const Value&) const override { return true; }

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override;
  void SerializeTo(const Value& val, char *storage, bool inlined,
                   AbstractPool *pool) const override;

  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage,
                                const bool inlined, AbstractPool *pool = nullptr) const override;
  Value DeserializeFrom(SerializeInput &in,
                                AbstractPool *pool = nullptr) const override;

  // Create a copy of this value
  Value Copy(const Value& val) const override;

 private:
  Value OperateNull(const Value& left, const Value &right) const override;
};

}  // namespace peloton
}  // namespace type
