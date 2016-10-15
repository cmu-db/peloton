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
class NumericType : public Type {
 public:
  NumericType(Type::TypeId type) : Type(type) {}
  virtual ~NumericType() = 0;

  // Other mathematical functions
  virtual Value *Add(const Value& left, const Value &right) const = 0;
  virtual Value *Subtract(const Value& left, const Value &right) const = 0;
  virtual Value *Multiply(const Value& left, const Value &right) const = 0;
  virtual Value *Divide(const Value& left, const Value &right) const = 0;
  virtual Value *Modulo(const Value& left, const Value &right) const = 0;
  virtual Value *Min(const Value& left, const Value &right) const = 0;
  virtual Value *Max(const Value& left, const Value &right) const = 0;
  virtual Value *Sqrt(const Value& val) const = 0;
  virtual Value *OperateNull(const Value& left, const Value &right) const = 0;
  virtual bool IsZero(const Value& val) const = 0;
};

// An integer value of the common sizes.
class IntegerType : public NumericType {
 public:
  ~IntegerType() {}

  IntegerType(bool is_offset);
  
  // Other mathematical functions
  Value *Add(const Value& left, const Value &right) const override;
  Value *Subtract(const Value& left, const Value &right) const override;
  Value *Multiply(const Value& left, const Value &right) const override;
  Value *Divide(const Value& left, const Value &right) const override;
  Value *Modulo(const Value& left, const Value &right) const override;
  Value *Min(const Value& left, const Value &right) const override;
  Value *Max(const Value& left, const Value &right) const override;
  Value *Sqrt(const Value& val) const override;

  // Comparison functions
  Value *CompareEquals(const Value& left, const Value &right) const override;
  Value *CompareNotEquals(const Value& left, const Value &right) const override;
  Value *CompareLessThan(const Value& left, const Value &right) const override;
  Value *CompareLessThanEquals(const Value& left, const Value &right) const override;
  Value *CompareGreaterThan(const Value& left, const Value &right) const override;
  Value *CompareGreaterThanEquals(const Value& left, const Value &right) const override;
  
  Value *CastAs(const Type::TypeId type_id) const override;

  // Integer types are always inlined
  bool IsInlined(const Value& val) const override { return true; }

  // Debug
  std::string ToString(const Value& val) const override;

  // Compute a hash value
  size_t Hash(const Value& val) const override;
  void HashCombine(const Value& val, size_t &seed) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value& val, SerializeOutput &out) const override;
  void SerializeTo(const Value& val, char *storage, bool inlined,
                   VarlenPool *pool) const override;

  // Create a copy of this value
  Value *Copy(const Value& val) const override;

 private:
  template<class T1, class T2>
  Value *AddValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value *SubtractValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value *MultiplyValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value *DivideValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value *ModuloValue(const Value& left, const Value &right) const;

  Value *OperateNull(const Value& left, const Value &right) const override;

  bool IsZero(const Value& val) const override;
};

}  // namespace common
}  // namespace peloton
