//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.h
//
// Identification: src/backend/common/numeric_value.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/value.h"

namespace peloton {
namespace common {

// A numeric value is an abstract type representing a number. Numerics can be
// either integral or non-integral (decimal), but must provide arithmetic
// operations on its value.
class NumericValue : public Value {
 public:
  NumericValue(Type &type) : Value(type) {}
  virtual ~NumericValue() = 0;

  // Other mathematical functions
  virtual Value *Add(const Value &o) const = 0;
  virtual Value *Subtract(const Value &o) const = 0;
  virtual Value *Multiply(const Value &o) const = 0;
  virtual Value *Divide(const Value &o) const = 0;
  virtual Value *Modulo(const Value &o) const = 0;
  virtual Value *Min(const Value &o) const = 0;
  virtual Value *Max(const Value &o) const = 0;
  virtual Value *Sqrt() const = 0;
  virtual Value *OperateNull(const Value &o) const = 0;
  virtual bool IsZero() const = 0;
};

// An integer value of the common sizes.
class IntegerValue : public NumericValue {
 public:
  ~IntegerValue() {}

  IntegerValue(int8_t i);
  IntegerValue(int16_t i);
  IntegerValue(int32_t i);
  IntegerValue(int32_t i, bool is_offset);
  IntegerValue(int64_t i);
  
  // Other mathematical functions
  Value *Add(const Value &o) const override;
  Value *Subtract(const Value &o) const override;
  Value *Multiply(const Value &o) const override;
  Value *Divide(const Value &o) const override;
  Value *Modulo(const Value &o) const override;
  Value *Min(const Value &o) const override;
  Value *Max(const Value &o) const override;
  Value *Sqrt() const override;

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

  // Serialize this value into the given storage space
  void SerializeTo(SerializeOutput &out) const override;
  void SerializeTo(char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Create a copy of this value
  Value *Copy() const override;

 private:
  template<class T1, class T2>
  Value *AddValue(const Value &o) const;
  template<class T1, class T2>
  Value *SubtractValue(const Value &o) const;
  template<class T1, class T2>
  Value *MultiplyValue(const Value &o) const;
  template<class T1, class T2>
  Value *DivideValue(const Value &o) const;
  template<class T1, class T2>
  Value *ModuloValue(const Value &o) const;

  Value *OperateNull(const Value &o) const override;

  bool IsZero() const override;
};

}  // namespace common
}  // namespace peloton
