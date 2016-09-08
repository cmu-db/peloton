//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_value.cpp
//
// Identification: src/backend/common/decimal_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/decimal_value.h"
#include "common/boolean_value.h"
#include "common/varlen_value.h"
#include <iostream>
#include <cmath>

namespace peloton {
namespace common {

static inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

DecimalValue::DecimalValue(double d)
  : NumericValue(Type::GetInstance(Type::DECIMAL)) {
  value_.decimal = d;
}

DecimalValue::DecimalValue(float f)
  : NumericValue(Type::GetInstance(Type::DECIMAL)) {
  value_.decimal = f;
}

bool DecimalValue::IsZero() const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  return (value_.decimal == 0);
}

Value *DecimalValue::Add(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new DecimalValue(value_.decimal + o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new DecimalValue(value_.decimal + o.GetAs<int16_t>());
    case Type::INTEGER:
      return new DecimalValue(value_.decimal + o.GetAs<int32_t>());
    case Type::BIGINT:
      return new DecimalValue(value_.decimal + o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new DecimalValue(value_.decimal + o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::Subtract(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);
  
  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new DecimalValue(value_.decimal - o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new DecimalValue(value_.decimal - o.GetAs<int16_t>());
    case Type::INTEGER:
      return new DecimalValue(value_.decimal - o.GetAs<int32_t>());
    case Type::BIGINT:
      return new DecimalValue(value_.decimal - o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new DecimalValue(value_.decimal - o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::Multiply(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new DecimalValue(value_.decimal * o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new DecimalValue(value_.decimal * o.GetAs<int16_t>());
    case Type::INTEGER:
      return new DecimalValue(value_.decimal * o.GetAs<int32_t>());
    case Type::BIGINT:
      return new DecimalValue(value_.decimal * o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new DecimalValue(value_.decimal * o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::Divide(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  if (((NumericValue &)o).IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
    return nullptr;
  }
  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new DecimalValue(value_.decimal / o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new DecimalValue(value_.decimal / o.GetAs<int16_t>());
    case Type::INTEGER:
      return new DecimalValue(value_.decimal / o.GetAs<int32_t>());
    case Type::BIGINT:
      return new DecimalValue(value_.decimal / o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new DecimalValue(value_.decimal / o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::Modulo(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);
  
  if (((NumericValue &)o).IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new DecimalValue(ValMod(value_.decimal, o.GetAs<int8_t>()));
    case Type::SMALLINT:
      return new DecimalValue(ValMod(value_.decimal, o.GetAs<int16_t>()));
    case Type::INTEGER:
      return new DecimalValue(ValMod(value_.decimal, o.GetAs<int32_t>()));
    case Type::BIGINT:
      return new DecimalValue(ValMod(value_.decimal, o.GetAs<int64_t>()));
    case Type::DECIMAL:
      return new DecimalValue(ValMod(value_.decimal, o.GetAs<double>()));
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::Min(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  std::unique_ptr<Value> cmp(CompareLessThanEquals(o));
  if (cmp->IsTrue())
    return Copy();
  return o.Copy();
}

Value *DecimalValue::Max(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return OperateNull(o);

  std::unique_ptr<Value> cmp(CompareGreaterThanEquals(o));
  if (cmp->IsTrue())
    return Copy();
  return o.Copy();
}

Value *DecimalValue::Sqrt() const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  if (IsNull())
    return new DecimalValue(PELOTON_DECIMAL_NULL);
  if (value_.decimal < 0) {
    throw Exception(EXCEPTION_TYPE_DECIMAL,
                    "Cannot take square root of a negative number.");
  }
  return new DecimalValue(sqrt(value_.decimal));
}

Value *DecimalValue::OperateNull(const Value &o UNUSED_ATTRIBUTE) const {
  return new DecimalValue(PELOTON_DECIMAL_NULL);
}

Value *DecimalValue::CompareEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
    
  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal == o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal == o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal == o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal == o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal == o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CompareNotEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal != o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal != o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal != o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal != o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal != o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CompareLessThan(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal < o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal < o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal < o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal < o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal < o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CompareLessThanEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal <= o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal <= o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal <= o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal <= o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal <= o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CompareGreaterThan(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);

  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal > o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal > o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal > o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal > o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal > o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CompareGreaterThanEquals(const Value &o) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  CheckComparable(o);
  if (IsNull() || o.IsNull())
    return new BooleanValue(PELOTON_BOOLEAN_NULL);
    
  switch(o.GetTypeId()) {
    case Type::TINYINT:
      return new BooleanValue(value_.decimal >= o.GetAs<int8_t>());
    case Type::SMALLINT:
      return new BooleanValue(value_.decimal >= o.GetAs<int16_t>());
    case Type::INTEGER:
      return new BooleanValue(value_.decimal >= o.GetAs<int32_t>());
    case Type::BIGINT:
      return new BooleanValue(value_.decimal >= o.GetAs<int64_t>());
    case Type::DECIMAL:
      return new BooleanValue(value_.decimal >= o.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value *DecimalValue::CastAs(const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::TINYINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT8_NULL);
      if (GetAs<double>() > PELOTON_INT8_MAX
       || GetAs<double>() < PELOTON_INT8_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return new IntegerValue((int8_t)GetAs<double>());
    }
    case Type::SMALLINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT16_NULL);
      if (GetAs<double>() > PELOTON_INT16_MAX
       || GetAs<double>() < PELOTON_INT16_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return new IntegerValue((int16_t)GetAs<double>());
    }
    case Type::INTEGER: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT32_NULL);
      if (GetAs<double>() > PELOTON_INT32_MAX
       || GetAs<double>() < PELOTON_INT32_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return new IntegerValue((int32_t)GetAs<double>());
    }
    case Type::BIGINT: {
      if (IsNull())
        return new IntegerValue(PELOTON_INT64_NULL);
      if (GetAs<double>() > PELOTON_INT64_MAX
       || GetAs<double>() < PELOTON_INT64_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return new IntegerValue((int64_t)GetAs<double>());
    }
    case Type::DECIMAL:
      return Copy();
    case Type::VARCHAR:
      if (IsNull())
        return new VarlenValue(nullptr, 0);
      return new VarlenValue(ToString());
    default:
      break;
  }
  throw Exception("DECIMAL is not coercable to "
      + Type::GetInstance(type_id).ToString());
}

std::string DecimalValue::ToString() const {
  if (IsNull())
    return "decimal_null";
  return std::to_string(value_.decimal);
}

size_t DecimalValue::Hash() const {
  return std::hash<double>{}(value_.decimal);
}

void DecimalValue::HashCombine(size_t &seed) const {
  hash_combine<double>(seed, value_.decimal);
}

void DecimalValue::SerializeTo(SerializeOutput &out) const {
  out.WriteDouble(value_.decimal);
}

void DecimalValue::SerializeTo(char *storage, bool inlined UNUSED_ATTRIBUTE,
                               VarlenPool *pool UNUSED_ATTRIBUTE) const {
  (void)inlined;
  (void)pool;
  *reinterpret_cast<double *>(storage) = value_.decimal;
}

Value *DecimalValue::Copy() const {
  return new DecimalValue(value_.decimal);
}

}  // namespace peloton
}  // namespace common
