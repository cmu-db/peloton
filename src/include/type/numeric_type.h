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

#include <cmath>
#include "type/value.h"

namespace peloton {
namespace type {

// A numeric value is an abstract type representing a number. Numerics can be
// either integral or non-integral (decimal), but must provide arithmetic
// operations on its value.
class NumericType : public Type {
 public:
  NumericType(Type::TypeId type) : Type(type) {}
  virtual ~NumericType() = 0;

  // Other mathematical functions
  virtual Value Add(const Value& left, const Value &right) const = 0;
  virtual Value Subtract(const Value& left, const Value &right) const = 0;
  virtual Value Multiply(const Value& left, const Value &right) const = 0;
  virtual Value Divide(const Value& left, const Value &right) const = 0;
  virtual Value Modulo(const Value& left, const Value &right) const = 0;
  virtual Value Min(const Value& left, const Value &right) const = 0;
  virtual Value Max(const Value& left, const Value &right) const = 0;
  virtual Value Sqrt(const Value& val) const = 0;
  virtual Value OperateNull(const Value& left, const Value &right) const = 0;
  virtual bool IsZero(const Value& val) const = 0;

 protected:
  static inline double ValMod(double x, double y) {
    return x - trunc((double)x / (double)y) * y;
  }
};

}  // namespace type
}  // namespace peloton
