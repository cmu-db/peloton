//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.cpp
//
// Identification: src/backend/common/numeric_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/numeric_type.h"

#include <cmath>
#include <iostream>
#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/varlen_type.h"

namespace peloton {
namespace common {

static inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

NumericType::~NumericType() {}

IntegerType::IntegerType(TypeId type)
                     : NumericType(type) {
}

bool IntegerType::IsZero(const Value& val) const {
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      return (val.value_.tinyint == 0);
    case Type::SMALLINT:
      return (val.value_.smallint == 0);
    case Type::PARAMETER_OFFSET:
    case Type::INTEGER:
      return (val.value_.integer == 0);
    case Type::BIGINT:
      return (val.value_.bigint == 0);
    default:
      break;
  }
  std::string msg = Type::GetInstance(val.GetTypeId())->ToString()
                  + " is not an integer type";
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
}

template<class T1, class T2>
Value *IntegerType::AddValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);

  if ((x + y) != sum1 && (x + y) != sum2) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0)) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return new Value(left.GetTypeId(), sum1);
  }
  if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0)) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new Value(right.GetTypeId(), sum2);
}

template<class T1, class T2>
Value *IntegerType::SubtractValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  if ((x - y) != diff1 && (x - y) != diff2) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0)) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return new Value(left.GetTypeId(), diff1);
  }
  if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0)) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new Value(right.GetTypeId(), diff2);
}

template<class T1, class T2>
Value *IntegerType::MultiplyValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  if ((x * y) != prod1 && (x * y) != prod2) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  // Overflow detection
  if (sizeof(x) >= sizeof(y)) {
    if ((y != 0 && prod1 / y != x)) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
    }
    return new Value(left.GetTypeId(), prod1);
  }
  if (y != 0 && prod2 / y != x) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new Value(right.GetTypeId(), prod2);
}

template<class T1, class T2>
Value *IntegerType::DivideValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  if (y == 0) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x / y);
  T2 quot2 = (T2)(x / y);
  if (sizeof(x) >= sizeof(y)) {
    return new Value(left.GetTypeId(), quot1);
  }
  return new Value(right.GetTypeId(), quot2);
}

template<class T1, class T2>
Value *IntegerType::ModuloValue(const Value& left, const Value &right) const {
  T1 x = left.GetAs<T1>();
  T2 y = right.GetAs<T2>();
  if (y == 0) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x % y);
  T2 quot2 = (T2)(x % y);
  if (sizeof(x) >= sizeof(y)) {
    return new Value(left.GetTypeId(), quot1);
  }
  return new Value(right.GetTypeId(), quot2);
}

Value *IntegerType::Add(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int8_t, int8_t>(left, right);
        case Type::SMALLINT:
          return AddValue<int8_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int8_t, int32_t>(left, right);
        case Type::BIGINT:
          return AddValue<int8_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.tinyint + right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int16_t, int8_t>(left, right);
        case Type::SMALLINT:
          return AddValue<int16_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int16_t, int32_t>(left, right);
        case Type::BIGINT:
          return AddValue<int16_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.smallint + right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int32_t, int8_t>(left, right);
        case Type::SMALLINT:
          return AddValue<int32_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int32_t, int32_t>(left, right);
        case Type::BIGINT:
          return AddValue<int32_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.integer + right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int64_t, int8_t>(left, right);
        case Type::SMALLINT:
          return AddValue<int64_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int64_t, int32_t>(left, right);
        case Type::BIGINT:
          return AddValue<int64_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.bigint + right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Subtract(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int8_t, int8_t>(left, right);
        case Type::SMALLINT:
          return SubtractValue<int8_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int8_t, int32_t>(left, right);
        case Type::BIGINT:
          return SubtractValue<int8_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.tinyint - right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int16_t, int8_t>(left, right);
        case Type::SMALLINT:
          return SubtractValue<int16_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int16_t, int32_t>(left, right);
        case Type::BIGINT:
          return SubtractValue<int16_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.smallint - right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int32_t, int8_t>(left, right);
        case Type::SMALLINT:
          return SubtractValue<int32_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int32_t, int32_t>(left, right);
        case Type::BIGINT:
          return SubtractValue<int32_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.integer - right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int64_t, int8_t>(left, right);
        case Type::SMALLINT:
          return SubtractValue<int64_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int64_t, int32_t>(left, right);
        case Type::BIGINT:
          return SubtractValue<int64_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.bigint - right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Multiply(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int8_t, int8_t>(left, right);
        case Type::SMALLINT:
          return MultiplyValue<int8_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int8_t, int32_t>(left, right);
        case Type::BIGINT:
          return MultiplyValue<int8_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.tinyint * right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int16_t, int8_t>(left, right);
        case Type::SMALLINT:
          return MultiplyValue<int16_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int16_t, int32_t>(left, right);
        case Type::BIGINT:
          return MultiplyValue<int16_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.smallint * right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int32_t, int8_t>(left, right);
        case Type::SMALLINT:
          return MultiplyValue<int32_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int32_t, int32_t>(left, right);
        case Type::BIGINT:
          return MultiplyValue<int32_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.integer * right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int64_t, int8_t>(left, right);
        case Type::SMALLINT:
          return MultiplyValue<int64_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int64_t, int32_t>(left, right);
        case Type::BIGINT:
          return MultiplyValue<int64_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.bigint * right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Divide(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zerright.");
  }
  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int8_t, int8_t>(left, right);
        case Type::SMALLINT:
          return DivideValue<int8_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int8_t, int32_t>(left, right);
        case Type::BIGINT:
          return DivideValue<int8_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.tinyint / right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int16_t, int8_t>(left, right);
        case Type::SMALLINT:
          return DivideValue<int16_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int16_t, int32_t>(left, right);
        case Type::BIGINT:
          return DivideValue<int16_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.smallint / right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int32_t, int8_t>(left, right);
        case Type::SMALLINT:
          return DivideValue<int32_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int32_t, int32_t>(left, right);
        case Type::BIGINT:
          return DivideValue<int32_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.integer / right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int64_t, int8_t>(left, right);
        case Type::SMALLINT:
          return DivideValue<int64_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int64_t, int32_t>(left, right);
        case Type::BIGINT:
          return DivideValue<int64_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, left.value_.bigint / right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Modulo(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zerright.");
  }
  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int8_t, int8_t>(left, right);
        case Type::SMALLINT:
          return ModuloValue<int8_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int8_t, int32_t>(left, right);
        case Type::BIGINT:
          return ModuloValue<int8_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, ValMod(left.value_.tinyint, right.GetAs<double>()));
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int16_t, int8_t>(left, right);
        case Type::SMALLINT:
          return ModuloValue<int16_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int16_t, int32_t>(left, right);
        case Type::BIGINT:
          return ModuloValue<int16_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, ValMod(left.value_.smallint, right.GetAs<double>()));
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int32_t, int8_t>(left, right);
        case Type::SMALLINT:
          return ModuloValue<int32_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int32_t, int32_t>(left, right);
        case Type::BIGINT:
          return ModuloValue<int32_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, ValMod(left.value_.integer, right.GetAs<double>()));
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int64_t, int8_t>(left, right);
        case Type::SMALLINT:
          return ModuloValue<int64_t, int16_t>(left, right);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int64_t, int32_t>(left, right);
        case Type::BIGINT:
          return ModuloValue<int64_t, int64_t>(left, right);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, ValMod(left.value_.bigint, right.GetAs<double>()));
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Min(const Value& left, const Value &right) const {
 left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  std::unique_ptr<Value> cmp(left.CompareGreaterThanEquals(right));
  if (cmp->IsTrue())
    return left.Copy();
  return right.Copy();
}

Value *IntegerType::Max(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  std::unique_ptr<Value> cmp(left.CompareGreaterThanEquals(right));
  if (cmp->IsTrue())
    return left.Copy();
  return right.Copy();
}

Value *IntegerType::Sqrt(const Value& val) const {
  val.CheckInteger();
  if (val.IsNull())
    return new Value(Type::DECIMAL, PELOTON_DECIMAL_NULL);

  switch(val.GetTypeId()) {
    case Type::TINYINT:
      if (val.value_.tinyint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new Value(Type::DECIMAL, sqrt(val.value_.tinyint));
    case Type::SMALLINT:
      if (val.value_.smallint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new Value(Type::DECIMAL, sqrt(val.value_.smallint));
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      if (val.value_.integer < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new Value(Type::DECIMAL, sqrt(val.value_.integer));
    case Type::BIGINT:
      if (val.value_.bigint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new Value(Type::DECIMAL, sqrt(val.value_.bigint));
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::OperateNull(const Value& left, const Value &right) const {
  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(right.GetTypeId(), (int8_t)PELOTON_INT8_NULL);
        case Type::SMALLINT:
          return new Value(right.GetTypeId(), (int16_t)PELOTON_INT16_NULL);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(right.GetTypeId(), (int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new Value(right.GetTypeId(), (int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, (double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
          return new Value(right.GetTypeId(), (int16_t)PELOTON_INT16_NULL);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(right.GetTypeId(), (int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new Value(right.GetTypeId(), (int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, (double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(right.GetTypeId(), (int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new Value(right.GetTypeId(), (int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, (double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
        case Type::BIGINT:
          return new Value(right.GetTypeId(), (int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, (double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareEquals(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);

  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint == right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint == right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint == right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint == right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint == right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint == right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint == right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint == right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint == right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint == right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer == right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer == right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer == right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer == right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer == right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint == right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint == right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint == right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint == right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint == right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareNotEquals(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint != right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint != right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint != right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint != right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint != right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint != right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint != right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint != right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint != right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint != right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer != right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer != right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer != right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer != right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer != right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint != right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint != right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint != right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint != right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint != right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareLessThan(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint < right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint < right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint < right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint < right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint < right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint < right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint < right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint < right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint < right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint < right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer < right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer < right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer < right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer < right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer < right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint < right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint < right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint < right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint < right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint < right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareLessThanEquals(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint <= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint <= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint <= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint <= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint <= right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint <= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint <= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint <= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint <= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint <= right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer <= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer <= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer <= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer <= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer <= right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint <= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint <= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint <= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint <= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint <= right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareGreaterThan(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);

  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint > right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint > right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint > right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint > right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint > right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint > right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint > right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint > right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint > right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint > right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer > right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer > right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer > right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer > right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer > right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint > right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint > right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint > right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint > right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint > right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
    
  switch (left.GetTypeId()) {
    case Type::TINYINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint >= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint >= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.tinyint >= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.tinyint >= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.tinyint >= right.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.smallint >= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.smallint >= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.smallint >= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.smallint >= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.smallint >= right.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.integer >= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.integer >= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.integer >= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.integer >= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.integer >= right.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (right.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BOOLEAN, left.value_.bigint >= right.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BOOLEAN, left.value_.bigint >= right.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new Value(Type::BOOLEAN, left.value_.bigint >= right.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BOOLEAN, left.value_.bigint >= right.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::BOOLEAN, left.value_.bigint >= right.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

std::string IntegerType::ToString(const Value& val) const {
  val.CheckInteger();
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      if (val.IsNull())
        return "tinyint_null";
      return std::to_string(val.value_.tinyint);
    case Type::SMALLINT:
      if (val.IsNull())
        return "smallint_null";
      return std::to_string(val.value_.smallint);
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      if (val.IsNull())
        return "integer_null";
      return std::to_string(val.value_.integer);
    case Type::BIGINT:
      if (val.IsNull())
        return "bigint_null";
      return std::to_string(val.value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

size_t IntegerType::Hash(const Value& val) const {
  val.CheckInteger();
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      return std::hash<int8_t>{}(val.value_.tinyint);
    case Type::SMALLINT:
      return std::hash<int16_t>{}(val.value_.smallint);
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      return std::hash<int32_t>{}(val.value_.integer);
    case Type::BIGINT:
      return std::hash<int64_t>{}(val.value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

void IntegerType::HashCombine(const Value& val, size_t &seed) const {
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      val.hash_combine<int8_t>(seed, val.value_.tinyint);
      break;
    case Type::SMALLINT:
      val.hash_combine<int16_t>(seed, val.value_.smallint);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      val.hash_combine<int32_t>(seed, val.value_.integer);
      break;
    case Type::BIGINT:
      val.hash_combine<int64_t>(seed, val.value_.bigint);
      break;
    default:
      break;
  }
}

void IntegerType::SerializeTo(const Value& val, SerializeOutput &out) const {
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      out.WriteByte(val.value_.tinyint);
      return;
    case Type::SMALLINT:
      out.WriteShort(val.value_.smallint);
      return;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      out.WriteInt(val.value_.integer);
      return;
    case Type::BIGINT:
      out.WriteLong(val.value_.bigint);
      return;
    default:
      return;
  }
  throw Exception("type error");
}

void IntegerType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
                               VarlenPool *pool UNUSED_ATTRIBUTE) const {
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      *reinterpret_cast<int8_t *>(storage) = val.value_.tinyint;
      return;
    case Type::SMALLINT:
      *reinterpret_cast<int16_t *>(storage) = val.value_.smallint;
      return;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      *reinterpret_cast<int32_t *>(storage) = val.value_.integer;
      return;
    case Type::BIGINT:
      *reinterpret_cast<int32_t *>(storage) = val.value_.bigint;
      return;
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::Copy(const Value& val) const {
  val.CheckInteger();
  switch(val.GetTypeId()) {
    case Type::TINYINT:
      return new Value(Type::TINYINT, val.value_.tinyint);
    case Type::SMALLINT:
      return new Value(Type::SMALLINT, val.value_.smallint);
    case Type::INTEGER:
      return new Value(Type::INTEGER, val.value_.integer);
    case Type::PARAMETER_OFFSET:
      return new Value(Type::PARAMETER_OFFSET, val.value_.integer);
    case Type::BIGINT:
      return new Value(Type::BIGINT, val.value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::TINYINT: {
      if (val.IsNull())
        return new Value(Type::TINYINT, PELOTON_INT8_NULL);
      switch(val.GetTypeId()) {
        // tinyint to tinyint
        case Type::TINYINT: {
          return val.Copy();
        }
        // smallint to tinyint
        case Type::SMALLINT: {
          if (val.GetAs<int16_t>() > PELOTON_INT8_MAX
           || val.GetAs<int16_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)val.GetAs<int16_t>());
        }
        // integer to tinyint
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET: {
          if (val.GetAs<int32_t>() > PELOTON_INT8_MAX
           || val.GetAs<int32_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)val.GetAs<int32_t>());
        }
        // bigint to tinyint
        case Type::BIGINT: {
          if (val.GetAs<int64_t>() > PELOTON_INT8_MAX
           || val.GetAs<int64_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)val.GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::SMALLINT: {
      if (val.IsNull())
        return new Value(Type::SMALLINT, PELOTON_INT16_NULL);
      switch(val.GetTypeId()) {
        // tinyint to smallint
        case Type::TINYINT: {
          return new Value(Type::SMALLINT, (int16_t)val.GetAs<int8_t>());
        }
        // smallint to smallint
        case Type::SMALLINT: {
          return val.Copy();
        }
        // integer to smallint
        case Type::INTEGER: {
          if (val.GetAs<int32_t>() > PELOTON_INT16_MAX
           || val.GetAs<int32_t>() < PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::SMALLINT, (int16_t)val.GetAs<int32_t>());
        }
        // bigint to smallint
        case Type::BIGINT: {
          if (val.GetAs<int64_t>() > PELOTON_INT16_MAX
           || val.GetAs<int64_t>() < PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::SMALLINT, (int16_t)val.GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET: {
      if (val.IsNull())
        return new Value(Type::INTEGER, PELOTON_INT32_NULL);
      switch(val.GetTypeId()) {
        // tinyint to integer
        case Type::TINYINT: {
          new Value(Type::INTEGER, (int32_t)val.GetAs<int8_t>());
        }
        // smallint to integer
        case Type::SMALLINT: {
          new Value(Type::INTEGER, (int32_t)val.GetAs<int16_t>());
       }
       // integer to integer
        case Type::INTEGER: {
          return val.Copy();
        }
        case Type::PARAMETER_OFFSET: {
          return new Value(Type::INTEGER, (int32_t)val.GetAs<int32_t>());
        }
        // bigint to integer
        case Type::BIGINT: {
          if (val.GetAs<int64_t>() > PELOTON_INT32_MAX
           || val.GetAs<int64_t>() < PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new Value(Type::INTEGER, (int32_t)val.GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::BIGINT: {
      if (val.IsNull())
        return new Value(Type::BIGINT, PELOTON_INT64_NULL);
      switch(val.GetTypeId()) {
        // tinyint to bigint
        case Type::TINYINT: {
          return new Value(Type::BIGINT, (int64_t)val.GetAs<int8_t>());
        }
        // smallint to bigint
        case Type::SMALLINT: {
          return new Value(Type::BIGINT, (int64_t)val.GetAs<int16_t>());
       }
       // integer to bigint
        case Type::INTEGER: 
        case Type::PARAMETER_OFFSET: {
          return new Value(Type::BIGINT, (int64_t)val.GetAs<int32_t>());
        }
        // bigint to bigint
        case Type::BIGINT: {
          return val.Copy();
        }
        default:
          break;
      }
    }
    case Type::DECIMAL: {
      if (val.IsNull())
        return new Value(Type::DECIMAL, PELOTON_DECIMAL_NULL);
      switch(val.GetTypeId()) {
        // tinyint to decimal
        case Type::TINYINT: {
          return new Value(Type::DECIMAL, (double)val.GetAs<int8_t>());
        }
        // smallint to decimal
        case Type::SMALLINT: {
          return new Value(Type::DECIMAL, (double)val.GetAs<int16_t>());
       }
       // integer to decimal
        case Type::INTEGER: 
        case Type::PARAMETER_OFFSET: {
          return new Value(Type::DECIMAL, (double)val.GetAs<int32_t>());
        }
        // bigint to decimal
        case Type::BIGINT: {
          return new Value(Type::DECIMAL, (double)val.GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::VARCHAR:
      if (val.IsNull())
        return new Value(Type::VARCHAR, nullptr, 0);
      return new Value(Type::VARCHAR, val.ToString());
    default:
      break;
  }
  throw Exception(Type::GetInstance(val.GetTypeId())->ToString()
      + " is not coercable to "
      + Type::GetInstance(type_id)->ToString());
}


}  // namespace common
}  // namespace peloton
