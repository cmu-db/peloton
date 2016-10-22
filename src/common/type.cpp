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

#include "common/array_type.h"
#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/numeric_type.h"
#include "common/timestamp_type.h"
#include "common/varlen_type.h"
#include "common/value.h"
#include "common/exception.h"
#include "common/varlen_pool.h"
#include "common/tinyint_type.h"
#include "common/smallint_type.h"
#include "common/integer_type.h"
#include "common/bigint_type.h"

namespace peloton {
namespace common {

Type* Type::kTypes[] = {
  new Type(Type::INVALID),
  new IntegerType(Type::PARAMETER_OFFSET),
  new BooleanType(),
  new TinyintType(),
  new SmallintType(),
  new IntegerType(Type::INTEGER),
  new BigintType(),
  new DecimalType(),
  new TimestampType(),
  new Type(Type::DATE), // not yet implemented
  new VarlenType(Type::VARCHAR),
  new VarlenType(Type::VARBINARY),
  new ArrayType(),
  new Type(Type::UDT), // not yet implemented
};

//// Is this type equivalent to the other
//bool Type::Equals(const Type &other) const {
//  return (type_id_ == other.type_id_);
//}

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
      break;
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
      break;
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

Value Type::GetMinValue(TypeId type_id) {
  switch (type_id) {
    case BOOLEAN:
      return Value(type_id, 0);
    case TINYINT:
      return Value(type_id, (int8_t) PELOTON_INT8_MIN);
    case SMALLINT:
      return Value(type_id, (int16_t) PELOTON_INT16_MIN);
    case INTEGER:
      return Value(type_id, (int32_t) PELOTON_INT32_MIN);
    case BIGINT:
      return Value(type_id, (int64_t) PELOTON_INT64_MIN);
    case DECIMAL:
      return Value(type_id, PELOTON_DECIMAL_MIN);
    case TIMESTAMP:
      return Value(type_id, 0);
    case VARCHAR:
      return Value(type_id, "", false);
    case VARBINARY:
      return Value(type_id, "", true);
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, "Cannot get minimal value.");
}

Value Type::GetMaxValue(TypeId type_id) {
  switch (type_id) {
    case BOOLEAN:
      return Value(type_id, 1);
    case TINYINT:
      return Value(type_id, (int8_t) PELOTON_INT8_MAX);
    case SMALLINT:
      return Value(type_id, (int16_t) PELOTON_INT16_MAX);
    case INTEGER:
      return Value(type_id, (int32_t) PELOTON_INT32_MAX);
    case BIGINT:
      return Value(type_id, (int64_t) PELOTON_INT64_MAX);
    case DECIMAL:
      return Value(type_id,PELOTON_DECIMAL_MAX);
    case TIMESTAMP:
      return Value(type_id,PELOTON_TIMESTAMP_MAX);
    case VARCHAR:
      return Value(type_id, nullptr, 0);
    case VARBINARY:
      return Value(type_id, nullptr, 0);
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, "Cannot get maximal value.");
}

Type *Type::GetInstance(TypeId type_id) {
  return kTypes[type_id];
}

Type::TypeId Type::GetTypeId() const {
  return type_id_;
}

Value Type::CompareEquals(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::CompareNotEquals(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::CompareLessThan(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::CompareLessThanEquals(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::CompareGreaterThan(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::CompareGreaterThanEquals(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Other mathematical functions
Value Type::Add(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Subtract(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Multiply(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Divide(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Modulo(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Min(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Max(const Value& left UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::Sqrt(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::OperateNull(const Value& val UNUSED_ATTRIBUTE, const Value& right UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
bool Type::IsZero(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Is the data inlined into this classes storage space, or must it be accessed
// through an indirection/pointer?
bool Type::IsInlined(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Return a stringified version of this value
std::string Type::ToString(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Compute a hash value
size_t Type::Hash(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
void Type::HashCombine(const Value& val UNUSED_ATTRIBUTE, size_t &seed UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Serialize this value into the given storage space. The inlined parameter
// indicates whether we are allowed to inline this value into the storage
// space, or whether we must store only a reference to this value. If inlined
// is false, we may use the provided data pool to allocate space for this
// value, storing a reference into the allocated pool space in the storage.
void Type::SerializeTo(const Value& val UNUSED_ATTRIBUTE, char *storage UNUSED_ATTRIBUTE, bool inlined UNUSED_ATTRIBUTE,
                         VarlenPool *pool UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
void Type::SerializeTo(const Value& val UNUSED_ATTRIBUTE, SerializeOutput &out UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Deserialize a value of the given type from the given storage space.
Value Type::DeserializeFrom(const char *storage UNUSED_ATTRIBUTE,
                              const bool inlined UNUSED_ATTRIBUTE, VarlenPool *pool UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}
Value Type::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                              VarlenPool *pool UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Create a copy of this value
Value Type::Copy(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

Value Type::CastAs(const Value& val UNUSED_ATTRIBUTE, const Type::TypeId type_id UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Access the raw variable length data
const char *Type::GetData(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Get the length of the variable length data
uint32_t Type::GetLength(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

// Get the element at a given index in this array
Value Type::GetElementAt(const Value& val UNUSED_ATTRIBUTE, uint64_t idx UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

Type::TypeId Type::GetElementType(const Value& val UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

  // Does this value exist in this array?
Value Type::InList(const Value& list UNUSED_ATTRIBUTE, const Value &object UNUSED_ATTRIBUTE) const{
  throw new Exception(EXCEPTION_TYPE_INVALID, "invalid type");
}

}  // namespace common
}  // namespace peloton
