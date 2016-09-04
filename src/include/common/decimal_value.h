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

#include "common/numeric_value.h"

namespace peloton {
namespace common {

class DecimalValue : public NumericValue {
 public:
  //struct DecDef {
  //  int ndigits;
  //  int weight;
  //  int sign;
  //  int scale;
  //};

  DecimalValue(double d);
  DecimalValue(float f);
  //DecimalValue(DecDef definition);

  // Other mathematical functions
  Value *Add(const Value &o) const override;
  Value *Subtract(const Value &o) const override;
  Value *Multiply(const Value &o) const override;
  Value *Divide(const Value &o) const override;
  Value *Modulo(const Value &o) const override;
  Value *Min(const Value &o) const override;
  Value *Max(const Value &o) const override;
  Value *Sqrt() const override;

  // Decimal comparisons will be much more involved than integer comparisons
  Value *CompareEquals(const Value &o) const override;
  Value *CompareNotEquals(const Value &o) const override;
  Value *CompareLessThan(const Value &o) const override;
  Value *CompareLessThanEquals(const Value &o) const override;
  Value *CompareGreaterThan(const Value &o) const override;
  Value *CompareGreaterThanEquals(const Value &o) const override;

  Value *CastAs(const Type::TypeId type_id) const override;

  bool IsInlined() const override { return true; }
  bool IsZero() const override;

  // Debug
  std::string ToString() const override;

  // Compute a hash value
  size_t Hash() const override;
  void HashCombine(size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(SerializeOutput &out) const override;
  void SerializeTo(char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Create a copy of this value
  Value *Copy() const override;

 private:
  Value *OperateNull(const Value &o) const override;
};

}  // namespace peloton
}  // namespace common
