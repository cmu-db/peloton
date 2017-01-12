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

#include <cmath>
#include <iostream>
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

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
    return ValueFactory::GetDecimalValue(left.value_.integer + right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value IntegerType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDecimalValue(left.value_.integer - right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value IntegerType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDecimalValue(left.value_.integer * right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value IntegerType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDecimalValue(left.value_.integer / right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value IntegerType::Modulo(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDecimalValue(
        ValMod(left.value_.integer, right.GetAs<double>()));
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
    throw Exception(EXCEPTION_TYPE_DECIMAL,
        "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDecimalValue(sqrt(val.value_.integer));

}

Value IntegerType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right) const {

  switch (right.GetTypeId()) {
  case Type::TINYINT:
  case Type::SMALLINT:
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return ValueFactory::GetIntegerValue((int32_t) PELOTON_INT32_NULL);
  case Type::BIGINT:
    return ValueFactory::GetBigIntValue((int64_t) PELOTON_INT64_NULL);
  case Type::DECIMAL:
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
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.integer == right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer == right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer == right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer == right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.integer == right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareNotEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.integer != right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer != right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer != right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer != right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.integer != right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareLessThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(left.value_.integer < right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer < right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer < right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer < right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(left.value_.integer < right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareLessThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.integer <= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer <= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer <= right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer <= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.integer <= right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareGreaterThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(left.value_.integer > right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer > right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer >  right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer > right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(left.value_.integer > right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool IntegerType::CompareGreaterThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.integer >= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.integer >= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.integer >= right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.integer >= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.integer >= right.GetAs<double>());
  default:
    break;
  }
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

Value IntegerType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
  case Type::TINYINT: {
    if (val.IsNull())
      return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
    if (val.GetAs<int32_t>() > PELOTON_INT8_MAX
        || val.GetAs<int32_t>() < PELOTON_INT8_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetTinyIntValue((int8_t) val.GetAs<int32_t>());
  }
  case Type::SMALLINT: {
    if (val.IsNull())
      return ValueFactory::GetSmallIntValue(PELOTON_INT16_NULL);

    if (val.GetAs<int32_t>() > PELOTON_INT16_MAX
        || val.GetAs<int32_t>() < PELOTON_INT16_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetSmallIntValue((int16_t) val.GetAs<int32_t>());
  }
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET: {
    if (val.IsNull())
      return Value(type_id, PELOTON_INT32_NULL);
    return Value(type_id, (int32_t) val.GetAs<int32_t>());

  }
  case Type::BIGINT: {
    if (val.IsNull())
      return ValueFactory::GetBigIntValue(PELOTON_INT64_NULL);
    return ValueFactory::GetBigIntValue((int64_t) val.GetAs<int32_t>());
  }
  case Type::DECIMAL: {
    if (val.IsNull())
      return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);
    return ValueFactory::GetDecimalValue((double) val.GetAs<int32_t>());
  }
  case Type::VARCHAR:
    if (val.IsNull())
      return ValueFactory::GetVarcharValue(nullptr, 0);
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
