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

#include "type/type.h"

#include "common/exception.h"
#include "type/abstract_pool.h"
#include "type/array_type.h"
#include "type/bigint_type.h"
#include "type/boolean_type.h"
#include "type/date_type.h"
#include "type/decimal_type.h"
#include "type/integer_type.h"
#include "type/numeric_type.h"
#include "type/smallint_type.h"
#include "type/timestamp_type.h"
#include "type/tinyint_type.h"
#include "type/value.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

Type* Type::kTypes[] = {
    new Type(TypeId::INVALID),
    new IntegerType(TypeId::PARAMETER_OFFSET),
    new BooleanType(),
    new TinyintType(),
    new SmallintType(),
    new IntegerType(TypeId::INTEGER),
    new BigintType(),
    new DecimalType(),
    new TimestampType(),
    new DateType(),  // not yet implemented
    new VarlenType(TypeId::VARCHAR),
    new VarlenType(TypeId::VARBINARY),
    new ArrayType(),
    new Type(TypeId::UDT),  // not yet implemented
};

// Get the size of this data type in bytes
uint64_t Type::GetTypeSize(const TypeId type_id) {
  switch (type_id) {
    case TypeId::BOOLEAN:
    case TypeId::TINYINT:
      return 1;
    case TypeId::SMALLINT:
      return 2;
    case TypeId::INTEGER:
    case TypeId::PARAMETER_OFFSET:
    case TypeId::DATE:
      return 4;
    case TypeId::BIGINT:
      return 8;
    case TypeId::DECIMAL:
      return 8;
    case TypeId::TIMESTAMP:
      return 8;
    case TypeId::VARCHAR:
    case TypeId::VARBINARY:
    case TypeId::ARRAY:
      return 0;
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Unknown type.");
}

// Is this type coercable from the other type
bool Type::IsCoercableFrom(const TypeId type_id) const {
  switch (type_id_) {
    case TypeId::INVALID:
      return false;
    case TypeId::BOOLEAN:
      return true;
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
    case TypeId::DECIMAL:
      switch (type_id) {
        case TypeId::TINYINT:
        case TypeId::SMALLINT:
        case TypeId::INTEGER:
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::VARCHAR:
          return true;
        default:
          return false;
      }
      break;
    case TypeId::DATE:
      return (type_id == TypeId::DATE || type_id == TypeId::VARCHAR);
    case TypeId::TIMESTAMP:
      return (type_id == TypeId::VARCHAR || type_id == TypeId::TIMESTAMP);
    case TypeId::VARCHAR:
      switch (type_id) {
        case TypeId::BOOLEAN:
        case TypeId::TINYINT:
        case TypeId::SMALLINT:
        case TypeId::INTEGER:
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::TIMESTAMP:
        case TypeId::VARCHAR:
          return true;
        default:
          return false;
      }
      break;
    default:
      return (type_id == type_id_);
  }
}

std::string Type::ToString() const { return (TypeIdToString(type_id_)); }

Value Type::GetMinValue(TypeId type_id) {
  switch (type_id) {
    case TypeId::BOOLEAN:
      return Value(type_id, 0);
    case TypeId::TINYINT:
      return Value(type_id, (int8_t)PELOTON_INT8_MIN);
    case TypeId::SMALLINT:
      return Value(type_id, (int16_t)PELOTON_INT16_MIN);
    case TypeId::INTEGER:
      return Value(type_id, (int32_t)PELOTON_INT32_MIN);
    case TypeId::BIGINT:
      return Value(type_id, (int64_t)PELOTON_INT64_MIN);
    case TypeId::DECIMAL:
      return Value(type_id, PELOTON_DECIMAL_MIN);
    case TypeId::TIMESTAMP:
      return Value(type_id, PELOTON_TIMESTAMP_MIN);
    case TypeId::DATE:
      return Value(type_id, PELOTON_DATE_MIN);
    case TypeId::VARCHAR:
      return Value(type_id, "");
    case TypeId::VARBINARY:
      return Value(type_id, "");
    default:
      break;
  }
  throw Exception(ExceptionType::MISMATCH_TYPE, "Cannot get minimal value.");
}

Value Type::GetMaxValue(TypeId type_id) {
  switch (type_id) {
    case TypeId::BOOLEAN:
      return Value(type_id, 1);
    case TypeId::TINYINT:
      return Value(type_id, (int8_t)PELOTON_INT8_MAX);
    case TypeId::SMALLINT:
      return Value(type_id, (int16_t)PELOTON_INT16_MAX);
    case TypeId::INTEGER:
      return Value(type_id, (int32_t)PELOTON_INT32_MAX);
    case TypeId::BIGINT:
      return Value(type_id, (int64_t)PELOTON_INT64_MAX);
    case TypeId::DECIMAL:
      return Value(type_id, PELOTON_DECIMAL_MAX);
    case TypeId::TIMESTAMP:
      return Value(type_id, PELOTON_TIMESTAMP_MAX);
    case TypeId::DATE:
      return Value(type_id, PELOTON_DATE_MAX);
    case TypeId::VARCHAR:
      return Value(type_id, nullptr, 0, false);
    case TypeId::VARBINARY:
      return Value(type_id, nullptr, 0, false);
    default:
      break;
  }
  throw Exception(ExceptionType::MISMATCH_TYPE, "Cannot get maximal value.");
}

CmpBool Type::CompareEquals(const Value& left UNUSED_ATTRIBUTE,
                            const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("CompareEquals not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
CmpBool Type::CompareNotEquals(const Value& left UNUSED_ATTRIBUTE,
                               const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("CompareNotEquals not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
CmpBool Type::CompareLessThan(const Value& left UNUSED_ATTRIBUTE,
                              const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("CompareLessThan not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
CmpBool Type::CompareLessThanEquals(const Value& left UNUSED_ATTRIBUTE,
                                    const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("CompareLessThanEquals not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
CmpBool Type::CompareGreaterThan(const Value& left UNUSED_ATTRIBUTE,
                                 const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("CompareGreaterThan not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
CmpBool Type::CompareGreaterThanEquals(const Value& left UNUSED_ATTRIBUTE,
                                       const Value& right
                                           UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format(
      "CompareGreaterThanEquals not implemented for type '%s'",
      TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Other mathematical functions
Value Type::Add(const Value& left UNUSED_ATTRIBUTE,
                const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Add not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Subtract(const Value& left UNUSED_ATTRIBUTE,
                     const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Subtract not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Multiply(const Value& left UNUSED_ATTRIBUTE,
                     const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Multiply not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Divide(const Value& left UNUSED_ATTRIBUTE,
                   const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Divide not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Modulo(const Value& left UNUSED_ATTRIBUTE,
                   const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Modulo not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Min(const Value& left UNUSED_ATTRIBUTE,
                const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Min not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Max(const Value& left UNUSED_ATTRIBUTE,
                const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Max not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::Sqrt(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Sqrt not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::OperateNull(const Value& val UNUSED_ATTRIBUTE,
                        const Value& right UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("OperateNull not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
bool Type::IsZero(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("IsZero not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Is the data inlined into this classes storage space, or must it be accessed
// through an indirection/pointer?
bool Type::IsInlined(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("IsLined not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Return a stringified version of this value
std::string Type::ToString(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("ToString not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Compute a hash value
size_t Type::Hash(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Hash not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
void Type::HashCombine(const Value& val UNUSED_ATTRIBUTE,
                       size_t& seed UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("HashCombine not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Serialize this value into the given storage space. The inlined parameter
// indicates whether we are allowed to inline this value into the storage
// space, or whether we must store only a reference to this value. If inlined
// is false, we may use the provided data pool to allocate space for this
// value, storing a reference into the allocated pool space in the storage.
void Type::SerializeTo(const Value& val UNUSED_ATTRIBUTE,
                       char* storage UNUSED_ATTRIBUTE,
                       bool inlined UNUSED_ATTRIBUTE,
                       AbstractPool* pool UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("SerializeTo not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
void Type::SerializeTo(const Value& val UNUSED_ATTRIBUTE,
                       SerializeOutput& out UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("SerializeTo not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Deserialize a value of the given type from the given storage space.
Value Type::DeserializeFrom(const char* storage UNUSED_ATTRIBUTE,
                            const bool inlined UNUSED_ATTRIBUTE,
                            AbstractPool* pool UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("DeserializeFrom not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}
Value Type::DeserializeFrom(SerializeInput& in UNUSED_ATTRIBUTE,
                            AbstractPool* pool UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("DeserializeFrom not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Create a copy of this value
Value Type::Copy(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("Copy not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

Value Type::CastAs(const Value& val UNUSED_ATTRIBUTE,
                   const TypeId type_id UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("CastAs not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Access the raw variable length data
const char* Type::GetData(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("GetData not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Get the length of the variable length data
uint32_t Type::GetLength(const Value& val UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("GetLength not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Access the raw varlen data stored from the tuple storage
char* Type::GetData(char* storage UNUSED_ATTRIBUTE) {
  std::string msg = StringUtil::Format("GetData not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

// Get the element at a given index in this array
Value Type::GetElementAt(const Value& val UNUSED_ATTRIBUTE,
                         uint64_t idx UNUSED_ATTRIBUTE) const {
  std::string msg =
      StringUtil::Format("GetElementAt not implemented for type '%s'",
                         TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

TypeId Type::GetElementType(const Value& val UNUSED_ATTRIBUTE) const {
  return type_id_;
}

// Does this value exist in this array?
Value Type::InList(const Value& list UNUSED_ATTRIBUTE,
                   const Value& object UNUSED_ATTRIBUTE) const {
  std::string msg = StringUtil::Format("InList not implemented for type '%s'",
                                       TypeIdToString(type_id_).c_str());
  throw peloton::NotImplementedException(msg);
}

}  // namespace type
}  // namespace peloton
