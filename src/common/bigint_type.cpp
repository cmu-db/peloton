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

#include "common/bigint_type.h"

#include <cmath>
#include <iostream>
#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/varlen_type.h"

namespace peloton {
namespace common {

BigintType::BigintType() :
    IntegerParentType(BIGINT) {
}

bool BigintType::IsZero(const Value& val) const {
  return (val.value_.bigint == 0);

}

Value BigintType::Add(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDoubleValue(left.value_.bigint + right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::Subtract(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDoubleValue(left.value_.bigint - right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::Multiply(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

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
    return ValueFactory::GetDoubleValue(left.value_.bigint * right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value BigintType::Divide(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDoubleValue(left.value_.bigint / right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::Modulo(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (right.IsZero()) {
    throw Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, "Division by zerright.");
  }

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
    return ValueFactory::GetDoubleValue(
        ValMod(left.value_.bigint, right.GetAs<double>()));
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::Sqrt(const Value& val) const {
  val.CheckInteger();
  if (val.IsNull())
    return ValueFactory::GetDoubleValue(PELOTON_DECIMAL_NULL);

  if (val.value_.bigint < 0) {
    throw Exception(EXCEPTION_TYPE_DECIMAL,
        "Cannot take square root of a negative number.");
  }
  return ValueFactory::GetDoubleValue(sqrt(val.value_.bigint));

}

Value BigintType::OperateNull(const Value& left UNUSED_ATTRIBUTE, const Value &right) const {

  switch (right.GetTypeId()) {
  case Type::TINYINT:
  case Type::SMALLINT:
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
  case Type::BIGINT:
    return ValueFactory::GetBigIntValue((int64_t) PELOTON_INT64_NULL);
  case Type::DECIMAL:
    return ValueFactory::GetDoubleValue((double) PELOTON_DECIMAL_NULL);
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::CompareEquals(const Value& left, const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);

  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint == right.GetAs<int8_t>());
  case Type::SMALLINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint == right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return Value(Type::BOOLEAN,
        left.value_.bigint == right.GetAs<int32_t>());
  case Type::BIGINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint == right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint == right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::CompareNotEquals(const Value& left,
    const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint != right.GetAs<int8_t>());
  case Type::SMALLINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint != right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return Value(Type::BOOLEAN,
        left.value_.bigint != right.GetAs<int32_t>());
  case Type::BIGINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint != right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint != right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::CompareLessThan(const Value& left,
    const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint < right.GetAs<int8_t>());
  case Type::SMALLINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint < right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return ValueFactory::GetBooleanValue(left.value_.bigint < right.GetAs<int32_t>());
  case Type::BIGINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint < right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint < right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::CompareLessThanEquals(const Value& left,
    const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint <= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint <= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return Value(Type::BOOLEAN,
        left.value_.bigint <= right.GetAs<int32_t>());
  case Type::BIGINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint <= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint <= right.GetAs<double>());
  default:
    break;
  }
  throw Exception("type error");
}

Value BigintType::CompareGreaterThan(const Value& left,
    const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint > right.GetAs<int8_t>());
  case Type::SMALLINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint > right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return ValueFactory::GetBooleanValue(left.value_.bigint > right.GetAs<int32_t>());
  case Type::BIGINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint > right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint > right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

Value BigintType::CompareGreaterThanEquals(const Value& left,
    const Value &right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);

  switch (right.GetTypeId()) {
  case Type::TINYINT:
    return ValueFactory::GetBooleanValue(left.value_.bigint >= right.GetAs<int8_t>());
  case Type::SMALLINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint >= right.GetAs<int16_t>());
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET:
    return Value(Type::BOOLEAN,
        left.value_.bigint >= right.GetAs<int32_t>());
  case Type::BIGINT:
    return Value(Type::BOOLEAN,
        left.value_.bigint >= right.GetAs<int64_t>());
  case Type::DECIMAL:
    return ValueFactory::GetBooleanValue(left.value_.bigint >= right.GetAs<double>());
  default:
    break;
  }

  throw Exception("type error");
}

std::string BigintType::ToString(const Value& val) const {
  val.CheckInteger();

  if (val.IsNull())
    return "bigint_null";
  return std::to_string(val.value_.bigint);

}

size_t BigintType::Hash(const Value& val) const {
  val.CheckInteger();

  return std::hash<int64_t> { }(val.value_.bigint);

}

void BigintType::HashCombine(const Value& val, size_t &seed) const {

  val.hash_combine<int64_t>(seed, val.value_.bigint);

}

void BigintType::SerializeTo(const Value& val, SerializeOutput &out) const {

  out.WriteLong(val.value_.bigint);

}

void BigintType::SerializeTo(const Value& val, char *storage, bool inlined UNUSED_ATTRIBUTE,
    VarlenPool *pool UNUSED_ATTRIBUTE) const {

  *reinterpret_cast<int32_t *>(storage) = val.value_.bigint;

}

// Deserialize a value of the given type from the given storage space.
Value BigintType::DeserializeFrom(const char *storage ,
                              const bool inlined UNUSED_ATTRIBUTE, VarlenPool *pool UNUSED_ATTRIBUTE) const{
  int64_t val = *reinterpret_cast<const int64_t *>(storage);
  return Value(type_id_, val);
}
Value BigintType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              VarlenPool *pool UNUSED_ATTRIBUTE) const{
  return Value(type_id_, in.ReadLong());
}

Value BigintType::Copy(const Value& val) const {

  return ValueFactory::GetBigIntValue(val.value_.bigint);

}

Value BigintType::CastAs(const Value& val, const Type::TypeId type_id) const {
  switch (type_id) {
  case Type::TINYINT: {
    if (val.IsNull())
      return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
    if (val.GetAs<int64_t>() > PELOTON_INT8_MAX
        || val.GetAs<int64_t>() < PELOTON_INT8_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetTinyIntValue((int8_t) val.GetAs<int64_t>());
  }
  case Type::SMALLINT: {
    if (val.IsNull())
      return ValueFactory::GetSmallIntValue(PELOTON_INT16_NULL);
    if (val.GetAs<int64_t>() > PELOTON_INT16_MAX
        || val.GetAs<int64_t>() < PELOTON_INT16_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return ValueFactory::GetSmallIntValue((int16_t) val.GetAs<int64_t>());
  }
  case Type::INTEGER:
  case Type::PARAMETER_OFFSET: {
    if (val.IsNull())
      return Value(type_id, PELOTON_INT32_NULL);

    if (val.GetAs<int64_t>() > PELOTON_INT32_MAX
        || val.GetAs<int64_t>() < PELOTON_INT32_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
          "Numeric value out of range.");
    return Value(type_id, (int32_t) val.GetAs<int64_t>());

  }
  case Type::BIGINT: {
    if (val.IsNull())
      return ValueFactory::GetBigIntValue(PELOTON_INT64_NULL);

    return val.Copy();
  }
  case Type::DECIMAL: {
    if (val.IsNull())
      return ValueFactory::GetDoubleValue(PELOTON_DECIMAL_NULL);

    return ValueFactory::GetDoubleValue((double) val.GetAs<int64_t>());
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

}  // namespace common
}  // namespace peloton
