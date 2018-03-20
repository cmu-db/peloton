//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.cpp
//
// Identification: src/backend/type/numeric_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/bigint_type.h"

#include "type/value_factory.h"

namespace peloton {
namespace type {

#define BIGINT_COMPARE_FUNC(OP) \
  switch (right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return GetCmpBool(left.value_.bigint OP right.GetAs<int8_t>()); \
    case TypeId::SMALLINT: \
      return GetCmpBool(left.value_.bigint OP right.GetAs<int16_t>()); \
    case TypeId::INTEGER: \
    case TypeId::PARAMETER_OFFSET: \
      return GetCmpBool(left.value_.bigint OP right.GetAs<int32_t>()); \
  case TypeId::BIGINT: \
    return GetCmpBool(left.value_.bigint OP right.GetAs<int64_t>()); \
  case TypeId::DECIMAL: \
    return GetCmpBool(left.value_.bigint OP right.GetAs<double>()); \
  case TypeId::VARCHAR: { \
    auto r_value = right.CastAs(TypeId::BIGINT); \
    return GetCmpBool(left.value_.bigint OP r_value.GetAs<int64_t>()); \
  } \
  default: \
    break; \
  } // SWITCH

#define BIGINT_MODIFY_FUNC(METHOD, OP) \
  switch (right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return METHOD<int64_t, int8_t>(left, right); \
    case TypeId::SMALLINT: \
      return METHOD<int64_t, int16_t>(left, right); \
    case TypeId::INTEGER: \
    case TypeId::PARAMETER_OFFSET: \
      return METHOD<int64_t, int32_t>(left, right); \
    case TypeId::BIGINT: \
      return METHOD<int64_t, int64_t>(left, right); \
    case TypeId::DECIMAL: \
      return ValueFactory::GetDecimalValue( \
                left.value_.bigint OP right.GetAs<double>()); \
    case TypeId::VARCHAR: { \
      auto r_value = right.CastAs(TypeId::BIGINT); \
      return METHOD<int64_t, int64_t>(left, r_value); \
    } \
    default: \
      break; \
  } // SWITCH

BigintType::BigintType() :
    IntegerParentType(TypeId::BIGINT) {
}

bool BigintType::IsZero(const Value& val) const {
  return (val.value_.bigint == 0);

}

Value BigintType::Add(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  BIGINT_MODIFY_FUNC(AddValue, +);

  throw Exception("type error");
}

Value BigintType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  BIGINT_MODIFY_FUNC(SubtractValue, -);

  throw Exception("type error");
}

Value BigintType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  BIGINT_MODIFY_FUNC(MultiplyValue, *);

  throw Exception("type error");
}

Value BigintType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero on right-hand side");
  }

  BIGINT_MODIFY_FUNC(DivideValue, /);

  throw Exception("type error");
}

Value BigintType::Modulo(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero on right-hand side");
  }

  switch (right.GetTypeId()) {
  case TypeId::TINYINT:
    return ModuloValue<int64_t, int8_t>(left, right);
  case TypeId::SMALLINT:
    return ModuloValue<int64_t, int16_t>(left, right);
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET:
    return ModuloValue<int64_t, int32_t>(left, right);
  case TypeId::BIGINT:
    return ModuloValue<int64_t, int64_t>(left, right);
  case TypeId::DECIMAL:
    return ValueFactory::GetDecimalValue(
        ValMod(left.value_.bigint, right.GetAs<double>()));
  case TypeId::VARCHAR: {
    auto r_value = right.CastAs(TypeId::BIGINT);
    return ModuloValue<int64_t, int64_t>(left, r_value);
  }
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::Sqrt(const Value& val) const {
  PL_ASSERT(val.CheckInteger());
  if (val.IsNull())
    return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);

  if (val.value_.bigint < 0) {
    throw Exception(ExceptionType::DECIMAL,
        "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDecimalValue(sqrt(val.value_.bigint));

}

Value BigintType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right) const {

  switch (right.GetTypeId()) {
  case TypeId::TINYINT:
  case TypeId::SMALLINT:
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET:
  case TypeId::BIGINT:
    return ValueFactory::GetBigIntValue((int64_t) PELOTON_INT64_NULL);
  case TypeId::DECIMAL:
    return ValueFactory::GetDecimalValue((double) PELOTON_DECIMAL_NULL);
  default:
    break;
  }
  throw Exception("type error");
}

CmpBool BigintType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));

  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(==);

  throw Exception("type error");
}

CmpBool BigintType::CompareNotEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(!=);

  throw Exception("type error");
}

CmpBool BigintType::CompareLessThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(<);

  throw Exception("type error");
}

CmpBool BigintType::CompareLessThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(<=);

  throw Exception("type error");
}

CmpBool BigintType::CompareGreaterThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(>);

  throw Exception("type error");
}

CmpBool BigintType::CompareGreaterThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  BIGINT_COMPARE_FUNC(>=);

  throw Exception("type error");
}

std::string BigintType::ToString(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  if (val.IsNull())
    return "bigint_null";
  return std::to_string(val.value_.bigint);

}

size_t BigintType::Hash(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  return std::hash<int64_t> { }(val.value_.bigint);

}

void BigintType::HashCombine(const Value& val, size_t &seed) const {

  val.hash_combine<int64_t>(seed, val.value_.bigint);

}

void BigintType::SerializeTo(const Value& val, SerializeOutput &out) const {

  out.WriteLong(val.value_.bigint);

}

void BigintType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    AbstractPool *pool UNUSED_ATTRIBUTE) const {

  *reinterpret_cast<int64_t *>(storage) = val.value_.bigint;

}

// Deserialize a value of the given type from the given storage space.
Value BigintType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, AbstractPool *pool UNUSED_ATTRIBUTE) const{
  int64_t val = *reinterpret_cast<const int64_t *>(storage);
  return Value(type_id_, val);
}
Value BigintType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              AbstractPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadLong());
}

Value BigintType::Copy(const Value& val) const {

  return ValueFactory::GetBigIntValue(val.value_.bigint);

}

Value BigintType::CastAs(const Value& val, const TypeId type_id) const {

  switch (type_id) {
  case TypeId::TINYINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    if (val.GetAs<int64_t>() > PELOTON_INT8_MAX ||
        val.GetAs<int64_t>() < PELOTON_INT8_MIN)
      throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetTinyIntValue((int8_t) val.GetAs<int64_t>());
  }
  case TypeId::SMALLINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    if (val.GetAs<int64_t>() > PELOTON_INT16_MAX ||
        val.GetAs<int64_t>() < PELOTON_INT16_MIN)
      throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetSmallIntValue((int16_t) val.GetAs<int64_t>());
  }
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    if (val.GetAs<int64_t>() > PELOTON_INT32_MAX ||
        val.GetAs<int64_t>() < PELOTON_INT32_MIN)
      throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
    return Value(type_id, (int32_t) val.GetAs<int64_t>());

  }
  case TypeId::BIGINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return val.Copy();
  }
  case TypeId::DECIMAL: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return ValueFactory::GetDecimalValue((double) val.GetAs<int64_t>());
  }
  case TypeId::VARCHAR:
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return ValueFactory::GetVarcharValue(val.ToString());
  default:
    break;
  }
  throw Exception(
      Type::GetInstance(val.GetTypeId())->ToString() + " is not coercable to "
          + Type::GetInstance(type_id)->ToString());
}

}  // namespace type
}  // namespace peloton
