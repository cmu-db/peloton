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

#include "common/numeric_value.h"
#include "common/decimal_value.h"
#include "common/boolean_value.h"
#include "common/varlen_value.h"
#include <cmath>
#include <iostream>

namespace peloton {
namespace common {

static inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

NumericValue::~NumericValue() {}

IntegerValue::IntegerValue(int8_t i)
                            : NumericValue(Type::GetInstance(Type::TINYINT)) {
  value_.tinyint = i;
}

IntegerValue::IntegerValue(int16_t i)
                            : NumericValue(Type::GetInstance(Type::SMALLINT)) {
  value_.smallint = i;
}

IntegerValue::IntegerValue(int32_t i, UNUSED_ATTRIBUTE bool is_offset)
                     : NumericValue(Type::GetInstance(Type::PARAMETER_OFFSET)) {
  PL_ASSERT(is_offset);
  value_.integer = i;
}

IntegerValue::IntegerValue(int32_t i)
                            : NumericValue(Type::GetInstance(Type::INTEGER)) {
  value_.integer = i;
}

IntegerValue::IntegerValue(int64_t i)
                            : NumericValue(Type::GetInstance(Type::BIGINT)) {
  value_.bigint = i;
}

bool IntegerValue::IsZero() const {
  switch(GetTypeId()) {
    case Type::TINYINT:
      return (value_.tinyint == 0);
    case Type::SMALLINT:
      return (value_.smallint == 0);
    case Type::PARAMETER_OFFSET:
    case Type::INTEGER:
      return (value_.integer == 0);
    case Type::BIGINT:
      return (value_.bigint == 0);
    default:
      break;
  }
  std::string msg = Type::GetInstance(GetTypeId()).ToString()
                  + " is not an integer type";
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
}

template<class T1, class T2>
Value *IntegerValue::AddValue(const Value &o) const {
  T1 x = GetAs<T1>();
  T2 y = o.GetAs<T2>();
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
    return new IntegerValue(sum1);
  }
  if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0)) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new IntegerValue(sum2);
}

template<class T1, class T2>
Value *IntegerValue::SubtractValue(const Value &o) const {
  T1 x = GetAs<T1>();
  T2 y = o.GetAs<T2>();
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
    return new IntegerValue(diff1);
  }
  if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0)) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new IntegerValue(diff2);
}

template<class T1, class T2>
Value *IntegerValue::MultiplyValue(const Value &o) const {
  T1 x = GetAs<T1>();
  T2 y = o.GetAs<T2>();
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
    return new IntegerValue(prod1);
  }
  if (y != 0 && prod2 / y != x) {
    throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                    "Numeric value out of range.");
  }
  return new IntegerValue(prod2);
}

template<class T1, class T2>
Value *IntegerValue::DivideValue(const Value &o) const {
  T1 x = GetAs<T1>();
  T2 y = o.GetAs<T2>();
  if (y == 0) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x / y);
  T2 quot2 = (T2)(x / y);
  if (sizeof(x) >= sizeof(y)) {
    return new IntegerValue(quot1);
  }
  return new IntegerValue(quot2);
}

template<class T1, class T2>
Value *IntegerValue::ModuloValue(const Value &o) const {
  T1 x = GetAs<T1>();
  T2 y = o.GetAs<T2>();
  if (y == 0) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  T1 quot1 = (T1)(x % y);
  T2 quot2 = (T2)(x % y);
  if (sizeof(x) >= sizeof(y)) {
    return new IntegerValue(quot1);
  }
  return new IntegerValue(quot2);
}

Value *IntegerValue::Add(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int8_t, int8_t>(o);
        case Type::SMALLINT:
          return AddValue<int8_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int8_t, int32_t>(o);
        case Type::BIGINT:
          return AddValue<int8_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.tinyint + o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int16_t, int8_t>(o);
        case Type::SMALLINT:
          return AddValue<int16_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int16_t, int32_t>(o);
        case Type::BIGINT:
          return AddValue<int16_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.smallint + o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int32_t, int8_t>(o);
        case Type::SMALLINT:
          return AddValue<int32_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int32_t, int32_t>(o);
        case Type::BIGINT:
          return AddValue<int32_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.integer + o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return AddValue<int64_t, int8_t>(o);
        case Type::SMALLINT:
          return AddValue<int64_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return AddValue<int64_t, int32_t>(o);
        case Type::BIGINT:
          return AddValue<int64_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.bigint + o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Subtract(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int8_t, int8_t>(o);
        case Type::SMALLINT:
          return SubtractValue<int8_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int8_t, int32_t>(o);
        case Type::BIGINT:
          return SubtractValue<int8_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.tinyint - o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int16_t, int8_t>(o);
        case Type::SMALLINT:
          return SubtractValue<int16_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int16_t, int32_t>(o);
        case Type::BIGINT:
          return SubtractValue<int16_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.smallint - o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int32_t, int8_t>(o);
        case Type::SMALLINT:
          return SubtractValue<int32_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int32_t, int32_t>(o);
        case Type::BIGINT:
          return SubtractValue<int32_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.integer - o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return SubtractValue<int64_t, int8_t>(o);
        case Type::SMALLINT:
          return SubtractValue<int64_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return SubtractValue<int64_t, int32_t>(o);
        case Type::BIGINT:
          return SubtractValue<int64_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.bigint - o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Multiply(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int8_t, int8_t>(o);
        case Type::SMALLINT:
          return MultiplyValue<int8_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int8_t, int32_t>(o);
        case Type::BIGINT:
          return MultiplyValue<int8_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.tinyint * o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int16_t, int8_t>(o);
        case Type::SMALLINT:
          return MultiplyValue<int16_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int16_t, int32_t>(o);
        case Type::BIGINT:
          return MultiplyValue<int16_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.smallint * o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int32_t, int8_t>(o);
        case Type::SMALLINT:
          return MultiplyValue<int32_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int32_t, int32_t>(o);
        case Type::BIGINT:
          return MultiplyValue<int32_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.integer * o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return MultiplyValue<int64_t, int8_t>(o);
        case Type::SMALLINT:
          return MultiplyValue<int64_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return MultiplyValue<int64_t, int32_t>(o);
        case Type::BIGINT:
          return MultiplyValue<int64_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.bigint * o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Divide(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  if (((NumericValue &)o).IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int8_t, int8_t>(o);
        case Type::SMALLINT:
          return DivideValue<int8_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int8_t, int32_t>(o);
        case Type::BIGINT:
          return DivideValue<int8_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.tinyint / o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int16_t, int8_t>(o);
        case Type::SMALLINT:
          return DivideValue<int16_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int16_t, int32_t>(o);
        case Type::BIGINT:
          return DivideValue<int16_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.smallint / o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int32_t, int8_t>(o);
        case Type::SMALLINT:
          return DivideValue<int32_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int32_t, int32_t>(o);
        case Type::BIGINT:
          return DivideValue<int32_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.integer / o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return DivideValue<int64_t, int8_t>(o);
        case Type::SMALLINT:
          return DivideValue<int64_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return DivideValue<int64_t, int32_t>(o);
        case Type::BIGINT:
          return DivideValue<int64_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(value_.bigint / o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Modulo(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  if (((NumericValue &)o).IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int8_t, int8_t>(o);
        case Type::SMALLINT:
          return ModuloValue<int8_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int8_t, int32_t>(o);
        case Type::BIGINT:
          return ModuloValue<int8_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(ValMod(value_.tinyint, o.GetAs<double>()));
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int16_t, int8_t>(o);
        case Type::SMALLINT:
          return ModuloValue<int16_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int16_t, int32_t>(o);
        case Type::BIGINT:
          return ModuloValue<int16_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(ValMod(value_.smallint, o.GetAs<double>()));
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int32_t, int8_t>(o);
        case Type::SMALLINT:
          return ModuloValue<int32_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int32_t, int32_t>(o);
        case Type::BIGINT:
          return ModuloValue<int32_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(ValMod(value_.integer, o.GetAs<double>()));
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return ModuloValue<int64_t, int8_t>(o);
        case Type::SMALLINT:
          return ModuloValue<int64_t, int16_t>(o);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return ModuloValue<int64_t, int32_t>(o);
        case Type::BIGINT:
          return ModuloValue<int64_t, int64_t>(o);
        case Type::DECIMAL:
          return new DecimalValue(ValMod(value_.bigint, o.GetAs<double>()));
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Min(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  std::unique_ptr<Value> cmp(CompareGreaterThanEquals(o));
  if (cmp->IsTrue())
    return Copy();
  return o.Copy();
}

Value *IntegerValue::Max(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  std::unique_ptr<Value> cmp(CompareGreaterThanEquals(o));
  if (cmp->IsTrue())
    return Copy();
  return o.Copy();
}

Value *IntegerValue::Sqrt() const {
  CheckInteger();
  if (IsNull())
    return new DecimalValue(PELOTON_DECIMAL_NULL);

  switch(GetTypeId()) {
    case Type::TINYINT:
      if (value_.tinyint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new DecimalValue(sqrt(value_.tinyint));
    case Type::SMALLINT:
      if (value_.smallint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new DecimalValue(sqrt(value_.smallint));
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      if (value_.integer < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new DecimalValue(sqrt(value_.integer));
    case Type::BIGINT:
      if (value_.bigint < 0) {
        throw Exception(EXCEPTION_TYPE_DECIMAL,
                        "Cannot take square root of a negative number.");
      }
      return new DecimalValue(sqrt(value_.bigint));
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::OperateNull(const Value &o) const {
  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new IntegerValue((int8_t)PELOTON_INT8_NULL);
        case Type::SMALLINT:
          return new IntegerValue((int16_t)PELOTON_INT16_NULL);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new IntegerValue((int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new IntegerValue((int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new DecimalValue((double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
          return new IntegerValue((int16_t)PELOTON_INT16_NULL);
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new IntegerValue((int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new IntegerValue((int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new DecimalValue((double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new IntegerValue((int32_t)PELOTON_INT32_NULL);
        case Type::BIGINT:
          return new IntegerValue((int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new DecimalValue((double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
        case Type::BIGINT:
          return new IntegerValue((int64_t)PELOTON_INT64_NULL);
        case Type::DECIMAL:
          return new DecimalValue((double)PELOTON_DECIMAL_NULL);
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareEquals(const Value &o) const {
  CheckInteger();
  CheckComparable(o);

  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint == o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint == o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint == o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint == o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint == o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint == o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint == o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint == o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint == o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint == o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer == o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer == o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer == o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer == o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer == o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint == o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint == o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint == o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint == o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint == o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareNotEquals(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint != o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint != o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint != o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint != o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint != o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint != o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint != o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint != o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint != o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint != o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer != o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer != o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer != o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer != o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer != o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint != o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint != o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint != o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint != o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint != o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareLessThan(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint < o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint < o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint < o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint < o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint < o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint < o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint < o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint < o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint < o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint < o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer < o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer < o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer < o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer < o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer < o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint < o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint < o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint < o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint < o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint < o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareLessThanEquals(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint <= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint <= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint <= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint <= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint <= o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint <= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint <= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint <= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint <= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint <= o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer <= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer <= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer <= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer <= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer <= o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint <= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint <= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint <= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint <= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint <= o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareGreaterThan(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint > o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint > o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint > o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint > o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint > o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint > o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint > o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint > o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint > o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint > o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer > o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer > o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer > o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer > o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer > o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint > o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint > o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint > o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint > o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint > o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CompareGreaterThanEquals(const Value &o) const {
  CheckInteger();
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
    
  switch (GetTypeId()) {
    case Type::TINYINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.tinyint >= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.tinyint >= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.tinyint >= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.tinyint >= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.tinyint >= o.GetAs<double>());
        default:
          break;
      }
    case Type::SMALLINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.smallint >= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.smallint >= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.smallint >= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.smallint >= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.smallint >= o.GetAs<double>());
        default:
          break;
      }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.integer >= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.integer >= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.integer >= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.integer >= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.integer >= o.GetAs<double>());
        default:
          break;
      }
    case Type::BIGINT:
      switch (o.GetTypeId()) {
        case Type::TINYINT:
          return new BooleanValue(value_.bigint >= o.GetAs<int8_t>());
        case Type::SMALLINT:
          return new BooleanValue(value_.bigint >= o.GetAs<int16_t>());
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET:
          return new BooleanValue(value_.bigint >= o.GetAs<int32_t>());
        case Type::BIGINT:
          return new BooleanValue(value_.bigint >= o.GetAs<int64_t>());
        case Type::DECIMAL:
          return new BooleanValue(value_.bigint >= o.GetAs<double>());
        default:
          break;
      }
    default:
      break;
  }
  throw Exception("type error");
}

std::string IntegerValue::ToString() const {
  CheckInteger();
  switch(GetTypeId()) {
    case Type::TINYINT:
      if (IsNull())
        return "tinyint_null";
      return std::to_string(value_.tinyint);
    case Type::SMALLINT:
      if (IsNull())
        return "smallint_null";
      return std::to_string(value_.smallint);
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      if (IsNull())
        return "integer_null";
      return std::to_string(value_.integer);
    case Type::BIGINT:
      if (IsNull())
        return "bigint_null";
      return std::to_string(value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

size_t IntegerValue::Hash() const {
  CheckInteger();
  switch(GetTypeId()) {
    case Type::TINYINT:
      return std::hash<int8_t>{}(value_.tinyint);
    case Type::SMALLINT:
      return std::hash<int16_t>{}(value_.smallint);
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      return std::hash<int32_t>{}(value_.integer);
    case Type::BIGINT:
      return std::hash<int64_t>{}(value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

void IntegerValue::HashCombine(size_t &seed) const {
  switch(GetTypeId()) {
    case Type::TINYINT:
      hash_combine<int8_t>(seed, value_.tinyint);
      break;
    case Type::SMALLINT:
      hash_combine<int16_t>(seed, value_.smallint);
      break;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      hash_combine<int32_t>(seed, value_.integer);
      break;
    case Type::BIGINT:
      hash_combine<int64_t>(seed, value_.bigint);
      break;
    default:
      break;
  }
}

void IntegerValue::SerializeTo(SerializeOutput &out) const {
  switch(GetTypeId()) {
    case Type::TINYINT:
      out.WriteByte(value_.tinyint);
      return;
    case Type::SMALLINT:
      out.WriteShort(value_.smallint);
      return;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      out.WriteInt(value_.integer);
      return;
    case Type::BIGINT:
      out.WriteLong(value_.bigint);
      return;
    default:
      return;
  }
  throw Exception("type error");
}

void IntegerValue::SerializeTo(char *storage, bool inlined UNUSED_ATTRIBUTE,
                               VarlenPool *pool UNUSED_ATTRIBUTE) const {
  switch(GetTypeId()) {
    case Type::TINYINT:
      *reinterpret_cast<int8_t *>(storage) = value_.tinyint;
      return;
    case Type::SMALLINT:
      *reinterpret_cast<int16_t *>(storage) = value_.smallint;
      return;
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET:
      *reinterpret_cast<int32_t *>(storage) = value_.integer;
      return;
    case Type::BIGINT:
      *reinterpret_cast<int32_t *>(storage) = value_.bigint;
      return;
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::Copy() const {
  CheckInteger();
  switch(GetTypeId()) {
    case Type::TINYINT:
      return new IntegerValue(value_.tinyint);
    case Type::SMALLINT:
      return new IntegerValue(value_.smallint);
    case Type::INTEGER:
      return new IntegerValue(value_.integer);
    case Type::PARAMETER_OFFSET:
      return new IntegerValue(value_.integer, true);
    case Type::BIGINT:
      return new IntegerValue(value_.bigint);
    default:
      break;
  }
  throw Exception("type error");
}

Value *IntegerValue::CastAs(const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::TINYINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT8_NULL);
      switch(GetTypeId()) {
        // tinyint to tinyint
        case Type::TINYINT: {
          return Copy();
        }
        // smallint to tinyint
        case Type::SMALLINT: {
          if (GetAs<int16_t>() > PELOTON_INT8_MAX
           || GetAs<int16_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int8_t)GetAs<int16_t>());
        }
        // integer to tinyint
        case Type::INTEGER:
        case Type::PARAMETER_OFFSET: {
          if (GetAs<int32_t>() > PELOTON_INT8_MAX
           || GetAs<int32_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int8_t)GetAs<int32_t>());
        }
        // bigint to tinyint
        case Type::BIGINT: {
          if (GetAs<int64_t>() > PELOTON_INT8_MAX
           || GetAs<int64_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int8_t)GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::SMALLINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT16_NULL);
      switch(GetTypeId()) {
        // tinyint to smallint
        case Type::TINYINT: {
          return new IntegerValue((int16_t)GetAs<int8_t>());
        }
        // smallint to smallint
        case Type::SMALLINT: {
          return Copy();
        }
        // integer to smallint
        case Type::INTEGER: {
          if (GetAs<int32_t>() > PELOTON_INT16_MAX
           || GetAs<int32_t>() < PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int16_t)GetAs<int32_t>());
        }
        // bigint to smallint
        case Type::BIGINT: {
          if (GetAs<int64_t>() > PELOTON_INT16_MAX
           || GetAs<int64_t>() < PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int16_t)GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::INTEGER:
    case Type::PARAMETER_OFFSET: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT32_NULL);
      switch(GetTypeId()) {
        // tinyint to integer
        case Type::TINYINT: {
          return new IntegerValue((int32_t)GetAs<int8_t>());
        }
        // smallint to integer
        case Type::SMALLINT: {
          return new IntegerValue((int32_t)GetAs<int16_t>());
       }
       // integer to integer
        case Type::INTEGER: {
          return Copy();
        }
        case Type::PARAMETER_OFFSET: {
          return new IntegerValue((int32_t)GetAs<int32_t>());
        }
        // bigint to integer
        case Type::BIGINT: {
          if (GetAs<int64_t>() > PELOTON_INT32_MAX
           || GetAs<int64_t>() < PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
              "Numeric value out of range.");
          return new IntegerValue((int32_t)GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::BIGINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT64_NULL);
      switch(GetTypeId()) {
        // tinyint to bigint
        case Type::TINYINT: {
          return new IntegerValue((int64_t)GetAs<int8_t>());
        }
        // smallint to bigint
        case Type::SMALLINT: {
          return new IntegerValue((int64_t)GetAs<int16_t>());
       }
       // integer to bigint
        case Type::INTEGER: 
        case Type::PARAMETER_OFFSET: {
          return new IntegerValue((int64_t)GetAs<int32_t>());
        }
        // bigint to bigint
        case Type::BIGINT: {
          return Copy();
        }
        default:
          break;
      }
    }
    case Type::DECIMAL: {
      if (IsNull())
        return new DecimalValue(PELOTON_DECIMAL_NULL);
      switch(GetTypeId()) {
        // tinyint to decimal
        case Type::TINYINT: {
          return new DecimalValue((double)GetAs<int8_t>());
        }
        // smallint to decimal
        case Type::SMALLINT: {
          return new DecimalValue((double)GetAs<int16_t>());
       }
       // integer to decimal
        case Type::INTEGER: 
        case Type::PARAMETER_OFFSET: {
          return new DecimalValue((double)GetAs<int32_t>());
        }
        // bigint to decimal
        case Type::BIGINT: {
          return new DecimalValue((double)GetAs<int64_t>());
        }
        default:
          break;
      }
    }
    case Type::VARCHAR:
      if (IsNull())
        return new VarlenValue(nullptr, 0);
      return new VarlenValue(ToString());
    default:
      break;
  }
  throw Exception(Type::GetInstance(GetTypeId()).ToString()
      + " is not coercable to "
      + Type::GetInstance(type_id).ToString());
}


}  // namespace common
}  // namespace peloton
