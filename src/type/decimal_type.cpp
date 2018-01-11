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

#include "type/decimal_type.h"
#include "type/boolean_type.h"
#include "type/varlen_type.h"
#include "type/value_factory.h"

#include <iostream>
#include <cmath>

namespace peloton {
namespace type {

#define DECIMAL_COMPARE_FUNC(OP) \
  switch (right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return GetCmpBool(left.value_.decimal OP right.GetAs<int8_t>()); \
    case TypeId::SMALLINT: \
      return GetCmpBool(left.value_.decimal OP right.GetAs<int16_t>()); \
    case TypeId::INTEGER: \
    case TypeId::PARAMETER_OFFSET: \
      return GetCmpBool(left.value_.decimal OP right.GetAs<int32_t>()); \
  case TypeId::BIGINT: \
    return GetCmpBool(left.value_.decimal OP right.GetAs<int64_t>()); \
  case TypeId::DECIMAL: \
    return GetCmpBool(left.value_.decimal OP right.GetAs<double>()); \
  case TypeId::VARCHAR: { \
    auto r_value = right.CastAs(TypeId::DECIMAL); \
    return GetCmpBool(left.value_.decimal OP r_value.GetAs<double>()); \
  } \
  default: \
    break; \
  } // SWITCH

#define DECIMAL_MODIFY_FUNC(OP) \
  switch(right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP right.GetAs<int8_t>()); \
    case TypeId::SMALLINT: \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP right.GetAs<int16_t>()); \
    case TypeId::INTEGER: \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP right.GetAs<int32_t>()); \
    case TypeId::BIGINT: \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP right.GetAs<int64_t>()); \
    case TypeId::DECIMAL: \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP right.GetAs<double>()); \
    case TypeId::VARCHAR: { \
      auto r_value = right.CastAs(TypeId::DECIMAL); \
      return ValueFactory::GetDecimalValue(left.value_.decimal OP r_value.GetAs<double>()); \
    } \
    default: \
      break; \
  } // SWITCH


inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

DecimalType::DecimalType()
  : NumericType(TypeId::DECIMAL) {
}

bool DecimalType::IsZero(const Value& val) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  return (val.value_.decimal == 0);
}

Value DecimalType::Add(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  DECIMAL_MODIFY_FUNC(+);

  throw Exception("type error");
}

Value DecimalType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  DECIMAL_MODIFY_FUNC(-);

  throw Exception("type error");
}

Value DecimalType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  DECIMAL_MODIFY_FUNC(*);

  throw Exception("type error");
}

Value DecimalType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero on right-hand side");
  }

  DECIMAL_MODIFY_FUNC(/);

  throw Exception("type error");

}

Value DecimalType::Modulo(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);
  
  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero on right-hand side");
  }
  switch(right.GetTypeId()) {
    case TypeId::TINYINT:
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, right.GetAs<int8_t>()));
    case TypeId::SMALLINT:
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, right.GetAs<int16_t>()));
    case TypeId::INTEGER:
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, right.GetAs<int32_t>()));
    case TypeId::BIGINT:
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, right.GetAs<int64_t>()));
    case TypeId::DECIMAL:
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, right.GetAs<double>()));
    case TypeId::VARCHAR: {
      auto r_value = right.CastAs(TypeId::DECIMAL);
      return ValueFactory::GetDecimalValue(ValMod(left.value_.decimal, r_value.GetAs<double>()));
    }
    default:
      break;
  }
  throw Exception("type error");
}

Value DecimalType::Min(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (left.CompareLessThanEquals(right) == CmpBool::TRUE)
    return left.Copy();
  return right.Copy();
}

Value DecimalType::Max(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (left.CompareGreaterThanEquals(right) == CmpBool::TRUE)
    return left.Copy();
  return right.Copy();
}

Value DecimalType::Sqrt(const Value& val) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  if (val.IsNull())
    return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);
  if (val.value_.decimal < 0) {
    throw Exception(ExceptionType::DECIMAL,
                    "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDecimalValue(sqrt(val.value_.decimal));
}

Value DecimalType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right UNUSED_ATTRIBUTE) const {
  return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);
}

CmpBool DecimalType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;
    
  DECIMAL_COMPARE_FUNC(==);

  throw Exception("type error");
}

CmpBool DecimalType::CompareNotEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  DECIMAL_COMPARE_FUNC(!=);

  throw Exception("type error");
}

CmpBool DecimalType::CompareLessThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  DECIMAL_COMPARE_FUNC(<);

  throw Exception("type error");
}

CmpBool DecimalType::CompareLessThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  DECIMAL_COMPARE_FUNC(<=);

  throw Exception("type error");
}

CmpBool DecimalType::CompareGreaterThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  DECIMAL_COMPARE_FUNC(>);

  throw Exception("type error");
}

CmpBool DecimalType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == TypeId::DECIMAL);
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  DECIMAL_COMPARE_FUNC(>=);

  throw Exception("type error");
}

Value DecimalType::CastAs(const Value& val, const TypeId type_id) const {
  switch (type_id) {
    case TypeId::TINYINT: {
      if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
      if (val.GetAs<double>() > PELOTON_INT8_MAX ||
          val.GetAs<double>() < PELOTON_INT8_MIN)
        throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetTinyIntValue((int8_t)val.GetAs<double>());
    }
    case TypeId::SMALLINT: {
      if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
      if (val.GetAs<double>() > PELOTON_INT16_MAX ||
          val.GetAs<double>() < PELOTON_INT16_MIN)
        throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetSmallIntValue((int16_t)val.GetAs<double>());
    }
    case TypeId::INTEGER: {
      if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
      if (val.GetAs<double>() > PELOTON_INT32_MAX ||
          val.GetAs<double>() < PELOTON_INT32_MIN)
        throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetIntegerValue((int32_t)val.GetAs<double>());
    }
    case TypeId::BIGINT: {
      if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
      if (val.GetAs<double>() > PELOTON_INT64_MAX ||
          val.GetAs<double>() < PELOTON_INT64_MIN)
        throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
      return ValueFactory::GetBigIntValue((int64_t)val.GetAs<double>());
    }
    case TypeId::DECIMAL:
      return val.Copy();
    case TypeId::VARCHAR:
      if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
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

  // PAVLO: 2017-04-02
  // Using '%g' makes it mimic Postgres better by not showing
  // any leading zeros.
  return StringUtil::Format("%g", val.value_.decimal);
  // return std::to_string(val.value_.decimal);
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
                               AbstractPool *pool UNUSED_ATTRIBUTE) const {
  (void)inlined;
  (void)pool;
  *reinterpret_cast<double *>(storage) = val.value_.decimal;
}

// Deserialize a value of the given type from the given storage space.
Value DecimalType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, AbstractPool *pool UNUSED_ATTRIBUTE) const{
  double val = *reinterpret_cast<const double *>(storage);
  return Value(type_id_, val);
}
Value DecimalType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              AbstractPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadDouble());
}

Value DecimalType::Copy(const Value& val) const {
  return ValueFactory::GetDecimalValue(val.value_.decimal);
}

}  // namespace peloton
}  // namespace type
