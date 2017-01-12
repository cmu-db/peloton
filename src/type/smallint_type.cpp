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

#include "type/smallint_type.h"

#include <cmath>
#include <iostream>
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/varlen_type.h"
#include "type/value_factory.h"

namespace peloton {
namespace type {

SmallintType::SmallintType() :
    IntegerParentType(Type::SMALLINT) {
}

bool SmallintType::IsZero(const Value& val) const {
  return (val.value_.smallint == 0);
}

Value SmallintType::Add(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDecimalValue(
        left.value_.smallint + right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value SmallintType::Subtract(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDecimalValue(
        left.value_.smallint - right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value SmallintType::Multiply(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDecimalValue(
        left.value_.smallint * right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value SmallintType::Divide(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDecimalValue(
        left.value_.smallint / right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value SmallintType::Modulo(const Value& left, const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDecimalValue(
        ValMod(left.value_.smallint, right.GetAs<double>()));
  default:
    break;
  }

  throw Exception("type error");
}

Value SmallintType::Sqrt(const Value& val) const {
  PL_ASSERT(val.CheckInteger());
  if (val.IsNull())
    return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);

  if (val.value_.smallint < 0) {
    throw Exception(EXCEPTION_TYPE_DECIMAL,
        "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDecimalValue(sqrt(val.value_.smallint));

  throw Exception("type error");
}

Value SmallintType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right) const {

  switch (right.GetTypeId()) {
  case Type::TINYINT:
  case Type::SMALLINT:
    return ValueFactory::GetSmallIntValue((int16_t) PELOTON_INT16_NULL);
  case Type::INTEGER:
    return ValueFactory::GetIntegerValue((int32_t) PELOTON_INT32_NULL);
  case Type::PARAMETER_OFFSET:
    return ValueFactory::GetParameterOffsetValue((int32_t) PELOTON_INT32_NULL);
  case Type::BIGINT:
    return ValueFactory::GetBigIntValue((int64_t) PELOTON_INT64_NULL);
  case Type::DECIMAL:
    return ValueFactory::GetDecimalValue((double) PELOTON_DECIMAL_NULL);
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool SmallintType::CompareEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));

  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint == right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint == right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint == right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint == right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint == right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool SmallintType::CompareNotEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint != right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint != right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint != right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint != right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint != right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool SmallintType::CompareLessThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint < right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint < right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint < right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint < right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint < right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool SmallintType::CompareLessThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint <= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint <= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint <= right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint <= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint <= right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

CmpBool SmallintType::CompareGreaterThan(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint > right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint > right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint > right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint > right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint > right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

CmpBool SmallintType::CompareGreaterThanEquals(const Value& left,
    const Value &right) const {
  PL_ASSERT(left.CheckInteger());
  PL_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return GetCmpBool(
        left.value_.smallint >= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return GetCmpBool(
        left.value_.smallint >= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return GetCmpBool(
        left.value_.smallint >= right.GetAs<int32_t>());
  case Type::BIGINT:
    return GetCmpBool(
        left.value_.smallint >= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return GetCmpBool(
        left.value_.smallint >= right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

std::string SmallintType::ToString(const Value& val) const {
  PL_ASSERT(val.CheckInteger());
  switch (val.GetTypeId()) {
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

size_t SmallintType::Hash(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  return std::hash<int16_t> { }(val.value_.smallint);

}

void SmallintType::HashCombine(const Value& val, size_t &seed) const {

  val.hash_combine<int16_t>(seed, val.value_.smallint);

}

void SmallintType::SerializeTo(const Value& val, SerializeOutput &out) const {

  out.WriteShort(val.value_.smallint);
  return;

}

void SmallintType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    AbstractPool *pool UNUSED_ATTRIBUTE) const {

  *reinterpret_cast<int16_t *>(storage) = val.value_.smallint;
  return;

}

// Deserialize a value of the given type from the given storage space.
Value SmallintType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, AbstractPool *pool UNUSED_ATTRIBUTE) const{
  int16_t val = *reinterpret_cast<const int16_t *>(storage);
  return Value(type_id_, val);
}
Value SmallintType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              AbstractPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadShort());
}

Value SmallintType::Copy(const Value& val) const {
  PL_ASSERT(val.CheckInteger());

  return ValueFactory::GetSmallIntValue(val.value_.smallint);

  throw Exception("type error");
}

Value SmallintType::CastAs(const Value& val,
    const Type::TypeId type_id) const {
  switch (type_id) {
  case Type::TINYINT: {
    if (val.IsNull())
      return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
    if (val.GetAs<int16_t>() > PELOTON_INT8_MAX
        || val.GetAs<int16_t>() < PELOTON_INT8_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetTinyIntValue((int8_t) val.GetAs<int16_t>());

  }
  case Type::SMALLINT: {
    if (val.IsNull())
      return ValueFactory::GetSmallIntValue(PELOTON_INT16_NULL);
    return val.Copy();
  }
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET: {
    if (val.IsNull())
      return Value(type_id, PELOTON_INT32_NULL);

    Value(type_id, (int32_t) val.GetAs<int16_t>());

  }
  case Type::BIGINT: {
    if (val.IsNull())
      return ValueFactory::GetBigIntValue(PELOTON_INT64_NULL);

    return ValueFactory::GetBigIntValue((int64_t) val.GetAs<int16_t>());

  }
  case Type::DECIMAL: {
    if (val.IsNull())
      return ValueFactory::GetDecimalValue(PELOTON_DECIMAL_NULL);

    return ValueFactory::GetDecimalValue((double) val.GetAs<int16_t>());
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
