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

#include "type/integer_type.h"

#include "type/value_factory.h"

namespace peloton {
namespace type {

#define INT_COMPARE_FUNC(OP) \
  switch (right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return GetCmpBool(left.value_.integer OP right.GetAs<int8_t>()); \
    case TypeId::SMALLINT: \
      return GetCmpBool(left.value_.integer OP right.GetAs<int16_t>()); \
    case TypeId::INTEGER: \
    case TypeId::PARAMETER_OFFSET: \
      return GetCmpBool(left.value_.integer OP right.GetAs<int32_t>()); \
  case TypeId::BIGINT: \
    return GetCmpBool(left.value_.integer OP right.GetAs<int64_t>()); \
  case TypeId::DECIMAL: \
    return GetCmpBool(left.value_.integer OP right.GetAs<double>()); \
  case TypeId::VARCHAR: { \
    auto r_value = right.CastAs(TypeId::INTEGER); \
    return GetCmpBool(left.value_.integer OP r_value.GetAs<int32_t>()); \
  } \
  default: \
    break; \
  } // SWITCH

#define INT_MODIFY_FUNC(METHOD, OP) \
  switch (right.GetTypeId()) { \
    case TypeId::TINYINT: \
      return METHOD<int32_t, int8_t>(left, right); \
    case TypeId::SMALLINT: \
      return METHOD<int32_t, int16_t>(left, right); \
    case TypeId::INTEGER: \
    case TypeId::PARAMETER_OFFSET: \
      return METHOD<int32_t, int32_t>(left, right); \
    case TypeId::BIGINT: \
      return METHOD<int32_t, int64_t>(left, right); \
    case TypeId::DECIMAL: \
      return ValueFactory::GetDecimalValue( \
                left.value_.integer OP right.GetAs<double>()); \
    case TypeId::VARCHAR: { \
      auto r_value = right.CastAs(TypeId::INTEGER); \
      return METHOD<int32_t, int32_t>(left, r_value); \
    } \
    default: \
      break; \
  } // SWITCH

IntegerType::IntegerType(TypeId type) :
    IntegerParentType(type) {
}

bool IntegerType::IsZero(const Value& val) const {

  return (val.value_.integer == 0);

}

Value IntegerType::Add(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  INT_MODIFY_FUNC(AddValue, +);

  throw Exception("type error");
}

Value IntegerType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  INT_MODIFY_FUNC(SubtractValue, -);

  throw Exception("type error");
}

Value IntegerType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  INT_MODIFY_FUNC(MultiplyValue, *);

  throw Exception("type error");
}

Value IntegerType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO,
                    "Division by zero on right-hand side");
  }

  INT_MODIFY_FUNC(DivideValue, /);

  throw Exception("type error");
}

Value IntegerType::Modulo(const Value& left, const Value &right) const {
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
    return ModuloValue<int32_t, int8_t>(left, right);
  case TypeId::SMALLINT:
    return ModuloValue<int32_t, int16_t>(left, right);
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET:
    return ModuloValue<int32_t, int32_t>(left, right);
  case TypeId::BIGINT:
    return ModuloValue<int32_t, int64_t>(left, right);
  case TypeId::DECIMAL:
    return ValueFactory::GetDecimalValue(
        ValMod(left.value_.integer, right.GetAs<double>()));
  case TypeId::VARCHAR: {
      auto r_value = right.CastAs(TypeId::INTEGER);
      return ModuloValue<int32_t, int32_t>(left, r_value);
  }
  default:
    break;
  }

  throw Exception("type error");
}

Value IntegerType::Sqrt(const Value& val) const {
  PL_ASSERT(val.CheckInteger());
  if (val.IsNull())
    return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);

  if (val.value_.integer < 0) {
    throw Exception(ExceptionType::DECIMAL,
        "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDecimalValue(sqrt(val.value_.integer));

}

Value IntegerType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right) const {

  switch (right.GetTypeId()) {
  case TypeId::TINYINT:
  case TypeId::SMALLINT:
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET:
    return ValueFactory::GetIntegerValue((int32_t) PELOTON_INT32_NULL);
  case TypeId::BIGINT:
    return ValueFactory::GetBigIntValue((int64_t) PELOTON_INT64_NULL);
  case TypeId::DECIMAL:
    return ValueFactory::GetDecimalValue((double) PELOTON_DECIMAL_NULL);
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));

  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(==);

  throw Exception("type error");
}

CmpBool IntegerType::CompareNotEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(!=);

  throw Exception("type error");
}

CmpBool IntegerType::CompareLessThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(<);

  throw Exception("type error");
}

CmpBool IntegerType::CompareLessThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(<=);

  throw Exception("type error");
}

CmpBool IntegerType::CompareGreaterThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(>);

  throw Exception("type error");
}

CmpBool IntegerType::CompareGreaterThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CmpBool::NULL_;

  INT_COMPARE_FUNC(>=);

  throw Exception("type error");
}

std::string IntegerType::ToString(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  if (val.IsNull())
    return "integer_null";
  return std::to_string(val.value_.integer);

}

size_t IntegerType::Hash(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  return std::hash<int32_t> { }(val.value_.integer);

}

void IntegerType::HashCombine(const Value& val, size_t &seed) const {
  val.hash_combine<int32_t>(seed, val.value_.integer);
}

void IntegerType::SerializeTo(const Value& val, SerializeOutput &out) const {

  out.WriteInt(val.value_.integer);
  return;

}

void IntegerType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    AbstractPool *pool UNUSED_ATTRIBUTE) const {

  *reinterpret_cast<int32_t *>(storage) = val.value_.integer;
  return;

  throw Exception("type error");
}

// Deserialize a value of the given type from the given storage space.
Value IntegerType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, AbstractPool *pool UNUSED_ATTRIBUTE) const{
  int32_t val = *reinterpret_cast<const int32_t *>(storage);
  return Value(type_id_, val);
}
Value IntegerType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              AbstractPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadInt());
}

Value IntegerType::Copy(const Value& val) const {
  PL_ASSERT(val.CheckInteger());
  return Value(val.GetTypeId(), val.value_.integer);
}

Value IntegerType::CastAs(const Value& val, const TypeId type_id) const {

  switch (type_id) {
  case TypeId::TINYINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    if (val.GetAs<int32_t>() > PELOTON_INT8_MAX ||
        val.GetAs<int32_t>() < PELOTON_INT8_MIN)
      throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetTinyIntValue((int8_t) val.GetAs<int32_t>());
  }
  case TypeId::SMALLINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    if (val.GetAs<int32_t>() > PELOTON_INT16_MAX  ||
        val.GetAs<int32_t>() < PELOTON_INT16_MIN)
      throw Exception(ExceptionType::OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetSmallIntValue((int16_t) val.GetAs<int32_t>());
  }
  case TypeId::INTEGER:
  case TypeId::PARAMETER_OFFSET: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return Value(type_id, (int32_t) val.GetAs<int32_t>());
  }
  case TypeId::BIGINT: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return ValueFactory::GetBigIntValue((int64_t) val.GetAs<int32_t>());
  }
  case TypeId::DECIMAL: {
    if (val.IsNull()) return ValueFactory::GetNullValueByType(type_id);
    return ValueFactory::GetDecimalValue((double) val.GetAs<int32_t>());
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
