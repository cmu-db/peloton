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

#include "common/decimal_type.h"

#include <iostream>
#include <cmath>
#include "common/boolean_type.h"
#include "common/varlen_type.h"
#include "common/value_factory.h"

namespace peloton {
namespace common {

static inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

DecimalType::DecimalType()
  : NumericType(Type::DECIMAL) {
}

bool DecimalType::IsZero(const Value& val) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  return (val.value_.decimal == 0);
}

Value DecimalType::Add(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal + right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal + right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetDoubleValue(left.value_.decimal + right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal + right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetDoubleValue(left.value_.decimal + right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);
  
  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal - right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal - right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetDoubleValue(left.value_.decimal - right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal - right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetDoubleValue(left.value_.decimal - right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal * right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal * right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetDoubleValue(left.value_.decimal * right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal * right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetDoubleValue(left.value_.decimal * right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal / right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal / right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetDoubleValue(left.value_.decimal / right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetDoubleValue(left.value_.decimal / right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetDoubleValue(left.value_.decimal / right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::Modulo(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);
  
  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO,
                    "Division by zero.");
  }
  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetDoubleValue(ValMod(left.value_.decimal, right.GetAs<int8_t>()));
    case Type::SMALLINT:
      return ValueFactory::GetDoubleValue(ValMod(left.value_.decimal, right.GetAs<int16_t>()));
    case Type::INTEGER:
      return ValueFactory::GetDoubleValue(ValMod(left.value_.decimal, right.GetAs<int32_t>()));
    case Type::BIGINT:
      return ValueFactory::GetDoubleValue(ValMod(left.value_.decimal, right.GetAs<int64_t>()));
    case Type::DECIMAL:
      return ValueFactory::GetDoubleValue(ValMod(left.value_.decimal, right.GetAs<double>()));
    default:
      throw Exception("type error");
  }
}

Value DecimalType::Min(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  Value cmp = (left.CompareLessThanEquals(right));
  if (cmp.IsTrue())
    return left.Copy();
  return right.Copy();
}

Value DecimalType::Max(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  Value cmp = (left.CompareGreaterThanEquals(right));
  if (cmp.IsTrue())
    return left.Copy();
  return right.Copy();
}

Value DecimalType::Sqrt(const Value& val) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  if (val.IsNull())
    return ValueFactory::GetDoubleValue(PELOTON_DECIMAL_NULL);
  if (val.value_.decimal < 0) {
    throw Exception(EXCEPTION_TYPE_DECIMAL,
                    "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDoubleValue(sqrt(val.value_.decimal));
}

Value DecimalType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right UNUSED_ATTRIBUTE) const {
  return ValueFactory::GetDoubleValue(PELOTON_DECIMAL_NULL);
}

Value DecimalType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
    
  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal == right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal == right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal == right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal == right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal == right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CompareNotEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal != right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal != right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal != right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal != right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal != right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CompareLessThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal < right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal < right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal < right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal < right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal < right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CompareLessThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal <= right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal <= right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal <= right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal <= right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal <= right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CompareGreaterThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal > right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal > right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal > right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal > right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal > right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::DECIMAL);
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
    
  switch(right.GetTypeId()) {
    case Type::TINYINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal >= right.GetAs<int8_t>());
    case Type::SMALLINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal >= right.GetAs<int16_t>());
    case Type::INTEGER:
      return ValueFactory::GetBooleanValue(left.value_.decimal >= right.GetAs<int32_t>());
    case Type::BIGINT:
      return ValueFactory::GetBooleanValue(left.value_.decimal >= right.GetAs<int64_t>());
    case Type::DECIMAL:
      return ValueFactory::GetBooleanValue(left.value_.decimal >= right.GetAs<double>());
    default:
      throw Exception("type error");
  }
}

Value DecimalType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
    case Type::TINYINT: {
      if (val.IsNull())
        return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
      if (val.GetAs<double>() > PELOTON_INT8_MAX
       || val.GetAs<double>() < PELOTON_INT8_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetTinyIntValue((int8_t)val.GetAs<double>());
    }
    case Type::SMALLINT: {
      if (val.IsNull())
        return ValueFactory::GetSmallIntValue(PELOTON_INT16_NULL);
      if (val.GetAs<double>() > PELOTON_INT16_MAX
       || val.GetAs<double>() < PELOTON_INT16_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetSmallIntValue((int16_t)val.GetAs<double>());
    }
    case Type::INTEGER: {
      if (val.IsNull())
        return ValueFactory::GetIntegerValue(PELOTON_INT32_NULL);
      if (val.GetAs<double>() > PELOTON_INT32_MAX
       || val.GetAs<double>() < PELOTON_INT32_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetIntegerValue((int32_t)val.GetAs<double>());
    }
    case Type::BIGINT: {
      if (val.IsNull())
        return ValueFactory::GetBigIntValue(PELOTON_INT64_NULL);
      if (val.GetAs<double>() > PELOTON_INT64_MAX
       || val.GetAs<double>() < PELOTON_INT64_MIN)
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetBigIntValue((int64_t)val.GetAs<double>());
    }
    case Type::DECIMAL:
      return val.Copy();
    case Type::VARCHAR:
      if (val.IsNull())
        return ValueFactory::GetVarcharValue(nullptr, 0);
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("DECIMAL is not coercable to "
      + Type::GetInstance(type_id)->ToString());
}

std::string DecimalType::ToString(const Value& val) const {
  if (val.IsNull())
    return "decimal_null";
  return std::to_string(val.value_.decimal);
}

size_t DecimalType::Hash(const Value& val) const {
  return std::hash<double>{}(val.value_.decimal);
}

void DecimalType::HashCombine(const Value& val, size_t &seed) const {
  val.hash_combine<double>(seed, val.value_.decimal);
}

void DecimalType::SerializeTo(const Value& val, SerializeOutput &out) const {
  out.WriteDouble(val.value_.decimal);
}

void DecimalType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
                               VarlenPool *pool UNUSED_ATTRIBUTE) const {
  (void)inlined;
  (void)pool;
  *reinterpret_cast<double *>(storage) = val.value_.decimal;
}

// Deserialize a value of the given type from the given storage space.
Value DecimalType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, VarlenPool *pool UNUSED_ATTRIBUTE) const{
  double val = *reinterpret_cast<const double *>(storage);
  return Value(type_id_, val);
}
Value DecimalType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              VarlenPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadDouble());
}

Value DecimalType::Copy(const Value& val) const {
  return ValueFactory::GetDoubleValue(val.value_.decimal);
}

}  // namespace peloton
}  // namespace common
