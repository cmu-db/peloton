//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/backend/common/type.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/type.h"
#include "common/value.h"
#include "common/numeric_value.h"
#include "common/decimal_value.h"
#include "common/boolean_value.h"
#include "common/varlen_value.h"
#include "common/timestamp_value.h"
#include "common/exception.h"

namespace peloton {
namespace common {

Type Type::kTypes[] = {
  Type(Type::INVALID),
  Type(Type::PARAMETER_OFFSET),
  Type(Type::BOOLEAN),
  Type(Type::TINYINT),
  Type(Type::SMALLINT),
  Type(Type::INTEGER),
  Type(Type::BIGINT),
  Type(Type::DECIMAL),
  Type(Type::TIMESTAMP),
  Type(Type::DATE),
  Type(Type::VARCHAR),
  Type(Type::VARBINARY),
  Type(Type::ARRAY),
  Type(Type::UDT),
};

// Is this type equivalent to the other
bool Type::Equals(const Type &other) const {
  return (type_id_ == other.type_id_);
}

// Get the size of this data type in bytes
uint64_t Type::GetTypeSize(const TypeId type_id) {
  switch (type_id) {
    case BOOLEAN:
    case TINYINT:
      return 1;
    case SMALLINT:
      return 2;
    case INTEGER:
    case PARAMETER_OFFSET:
      return 4;
    case BIGINT:
      return 8;
    case DECIMAL:
      return 8;
    case TIMESTAMP:
      return 8;
    case VARCHAR:
    case VARBINARY:
    case ARRAY:
      return 0;
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

// Is this type coercable from the other type
bool Type::IsCoercableFrom(const TypeId type_id) const {
  switch (type_id_) {
    case INVALID:
      return 0;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      switch (type_id) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case VARCHAR:
          return 1;
        default:
          return 0;
      }
    case TIMESTAMP:
      return (type_id == VARCHAR || type_id == TIMESTAMP);
    case VARCHAR:
      switch (type_id) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case TIMESTAMP:
        case VARCHAR:
          return 1;
        default:
          return 0;
      }
    default:
      return (type_id == type_id_);
  }
}

std::string Type::ToString() const {
  switch (type_id_) {
    case INVALID:
      return "INVALID";
    case PARAMETER_OFFSET:
      return "PARAMETER_OFFSET";
    case BOOLEAN:
      return "BOOLEAN";
    case TINYINT:
      return "TINYINT";
    case SMALLINT:
      return "SMALLINT";
    case INTEGER:
      return "INTEGER";
    case BIGINT:
      return "BIGINT";
    case DECIMAL:
      return "DECIMAL";
    case TIMESTAMP:
      return "TIMESTAMP";
    case DATE:
      return "DATE";
    case VARCHAR:
      return "VARCHAR";
    case VARBINARY:
      return "VARBINARY";
    case ARRAY:
      return "ARRAY";
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
}

Value *Type::GetMinValue(TypeId type_id) {
  switch (type_id) {
    case BOOLEAN:
      return new BooleanValue(0);
    case TINYINT:
      return new IntegerValue((int8_t) PELOTON_INT8_MIN);
    case SMALLINT:
      return new IntegerValue((int16_t) PELOTON_INT16_MIN);
    case INTEGER:
      return new IntegerValue((int32_t) PELOTON_INT32_MIN);
    case BIGINT:
      return new IntegerValue((int64_t) PELOTON_INT64_MIN);
    case DECIMAL:
      return new DecimalValue(PELOTON_DECIMAL_MIN);
    case TIMESTAMP:
      return new TimestampValue(0);
    case VARCHAR:
      return new VarlenValue("");
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, "Cannot get minimal value.");
}

Value *Type::GetMaxValue(TypeId type_id) {
  switch (type_id) {
    case BOOLEAN:
      return new BooleanValue(1);
    case TINYINT:
      return new IntegerValue((int8_t) PELOTON_INT8_MAX);
    case SMALLINT:
      return new IntegerValue((int16_t) PELOTON_INT16_MAX);
    case INTEGER:
      return new IntegerValue((int32_t) PELOTON_INT32_MAX);
    case BIGINT:
      return new IntegerValue((int64_t) PELOTON_INT64_MAX);
    case DECIMAL:
      return new DecimalValue(PELOTON_DECIMAL_MAX);
    case TIMESTAMP:
      return new TimestampValue(PELOTON_TIMESTAMP_MAX);
    case VARCHAR:
      return new VarlenValue(nullptr, 0);
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, "Cannot get maximal value.");
}

Type &Type::GetInstance(TypeId type_id) {
  return kTypes[type_id];
}

Type::TypeId Type::GetTypeId() const {
  return type_id_;
}

}  // namespace common
}  // namespace peloton