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

#include "type/numeric_type.h"
#include "common/exception.h"

namespace peloton {
namespace type {

// An integer value of the common sizes.
class IntegerParentType : public NumericType {
 public:
  ~IntegerParentType() {}

  IntegerParentType(TypeId type);

  // Other mathematical functions
  virtual Value Add(const Value& left, const Value &right) const override = 0;
  virtual Value Subtract(const Value& left, const Value &right) const override = 0;
  virtual Value Multiply(const Value& left, const Value &right) const override = 0;
  virtual Value Divide(const Value& left, const Value &right) const override = 0;
  virtual Value Modulo(const Value& left, const Value &right) const override = 0;
  Value Min(const Value& left, const Value &right) const override;
  Value Max(const Value& left, const Value &right) const override;
  virtual Value Sqrt(const Value& val) const override = 0;

  // Comparison functions
  virtual CmpBool CompareEquals(const Value& left, const Value &right) const override = 0;
  virtual CmpBool CompareNotEquals(const Value& left, const Value &right) const override = 0;
  virtual CmpBool CompareLessThan(const Value& left, const Value &right) const override = 0;
  virtual CmpBool CompareLessThanEquals(const Value& left, const Value &right) const override = 0;
  virtual CmpBool CompareGreaterThan(const Value& left, const Value &right) const override = 0;
  virtual CmpBool CompareGreaterThanEquals(const Value& left, const Value &right) const override = 0;

  virtual Value CastAs(const Value& val, const TypeId type_id) const override = 0;

  // Integer types are always inlined
  bool IsInlined(const Value&) const override { return true; }

  // Debug
  virtual std::string ToString(const Value& val) const override = 0;

  // Compute a hash value
  virtual size_t Hash(const Value& val) const override = 0;
  virtual void HashCombine(const Value& val, size_t &seed) const override = 0;

  // Serialize this value into the given storage space
  virtual void SerializeTo(const Value& val, SerializeOutput &out) const override = 0;
  virtual void SerializeTo(const Value& val, char *storage, bool inlined,
                   AbstractPool *pool) const override = 0;

  // Deserialize a value of the given type from the given storage space.
  virtual Value DeserializeFrom(const char *storage,
                                const bool inlined, AbstractPool *pool = nullptr) const override = 0;
  virtual Value DeserializeFrom(SerializeInput &in,
                                AbstractPool *pool = nullptr) const override = 0;

  // Create a copy of this value
  virtual Value Copy(const Value& val) const override = 0;

 protected:
  template<class T1, class T2>
  Value AddValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value SubtractValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value MultiplyValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value DivideValue(const Value& left, const Value &right) const;
  template<class T1, class T2>
  Value ModuloValue(const Value& left, const Value &right) const;

  virtual Value OperateNull(const Value& left, const Value &right) const override = 0;

  virtual bool IsZero(const Value& val) const override = 0;
};

template<class T1, class T2>
Value IntegerParentType::AddValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);

  if ((x + y) != sum1 && (x + y) != sum2) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0)) {
      throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return Value(left.GetTypeId(), sum1);
  }
  if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0)) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return Value(right.GetTypeId(), sum2);
}

template<class T1, class T2>
Value IntegerParentType::SubtractValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  if ((x - y) != diff1 && (x - y) != diff2) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0)) {
      throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return Value(left.GetTypeId(), diff1);
  }
  if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0)) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return Value(right.GetTypeId(), diff2);
}

template<class T1, class T2>
Value IntegerParentType::MultiplyValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  if ((x * y) != prod1 && (x * y) != prod2) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((y != 0 && prod1 / y != x)) {
      throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return Value(left.GetTypeId(), prod1);
  }
  if (y != 0 && prod2 / y != x) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return Value(right.GetTypeId(), prod2);
}

template<class T1, class T2>
Value IntegerParentType::DivideValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  if (y == 0) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x / y);
  T2 quot2 = (T2)(x / y);
  if (sizeof(x) >= sizeof(y)) {
    return Value(left.GetTypeId(), quot1);
  }
  return Value(right.GetTypeId(), quot2);
}

template<class T1, class T2>
Value IntegerParentType::ModuloValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  if (y == 0) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x % y);
  T2 quot2 = (T2)(x % y);
  if (sizeof(x) >= sizeof(y)) {
    return Value(left.GetTypeId(), quot1);
  }
  return Value(right.GetTypeId(), quot2);
}

}  // namespace type
}  // namespace peloton
