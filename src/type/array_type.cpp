//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value.h
//
// Identification: src/backend/common/array_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/type/value.h>
#include "type/array_type.h"
#include <sstream>

#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/type.h"
#include "type/varlen_type.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace type {

ArrayType::ArrayType(TypeId type) :
    Type(type) {
}

// Get the element at a given index in this array
Value ArrayType::GetElementAt(const Value &val, uint64_t idx) const {
  switch (val.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec = *(std::vector<bool> *)(val.value_.array);
      return ValueFactory::GetBooleanValue(vec.at(idx));
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec = *(std::vector<int8_t> *)(val.value_.array);
      return ValueFactory::GetTinyIntValue((int8_t)vec.at(idx));
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec =
          *(std::vector<int16_t> *)(val.value_.array);
      return ValueFactory::GetSmallIntValue((int16_t)vec.at(idx));
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec =
          *(std::vector<int32_t> *)(val.value_.array);
      return ValueFactory::GetIntegerValue((int32_t)vec.at(idx));
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec =
          *(std::vector<int64_t> *)(val.value_.array);
      return ValueFactory::GetBigIntValue((int64_t)vec.at(idx));
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec = *(std::vector<double> *)(val.value_.array);
      return ValueFactory::GetDecimalValue((double)vec.at(idx));
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec =
          *(std::vector<uint64_t> *)(val.value_.array);
      return ValueFactory::GetTimestampValue((uint64_t)vec.at(idx));
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec =
          *(std::vector<std::string> *)(val.value_.array);
      return ValueFactory::GetVarcharValue(vec.at(idx));
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

// Does this value exist in this array?
Value ArrayType::InList(const Value &list, const Value &object) const {
  Value ele = (list.GetElementAt(0));
  PL_ASSERT(ele.CheckComparable(object));
  if (object.IsNull()) return ValueFactory::GetNullValueByType(TypeId::BOOLEAN);
  switch (list.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec = *(std::vector<bool> *)(list.value_.array);
      std::vector<bool>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(ValueFactory::GetBooleanValue(*it).CompareEquals(object));
        if (ValueFactory::GetBooleanValue(*it).CompareEquals(object) == CmpBool::TRUE) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec =
          *(std::vector<int8_t> *)(list.value_.array);
      std::vector<int8_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(Value(TypeId::TINYINT, *it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec =
          *(std::vector<int16_t> *)(list.value_.array);
      std::vector<int16_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(Value(TypeId::SMALLINT, *it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec =
          *(std::vector<int32_t> *)(list.value_.array);
      std::vector<int32_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(ValueFactory::GetIntegerValue(*it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec =
          *(std::vector<int64_t> *)(list.value_.array);
      std::vector<int64_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(ValueFactory::GetBigIntValue(*it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec =
          *(std::vector<double> *)(list.value_.array);
      std::vector<double>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(ValueFactory::GetDecimalValue(*it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec =
          *(std::vector<uint64_t> *)(list.value_.array);
      std::vector<uint64_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(ValueFactory::GetTimestampValue(*it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec =
          *(std::vector<std::string> *)(list.value_.array);
      std::vector<std::string>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(Value(TypeId::VARCHAR, *it).CompareEquals(object));
        if (res.IsTrue()) return res;
      }
      return ValueFactory::GetBooleanValue(false);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareEquals(const Value &left, const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec1 =
          *(std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> vec2 =
          *(std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 == vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareNotEquals(const Value &left, const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec1 =
          *(std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> vec2 =
          *(std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 != vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareLessThan(const Value &left, const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> *vec1 =
          (std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> *vec2 =
          (std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 < vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareLessThanEquals(const Value &left,
                                       const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec1 =
          *(std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> vec2 =
          *(std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 <= vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareGreaterThan(const Value &left,
                                    const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec1 =
          *(std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> vec2 =
          *(std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 > vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

CmpBool ArrayType::CompareGreaterThanEquals(const Value &left,
                                          const Value &right) const {
  PL_ASSERT(IsArrayType());
  PL_ASSERT(left.CheckComparable(right));
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(right.GetElementType())->ToString() +
                      " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(ExceptionType::MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case TypeId::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::TINYINT: {
      std::vector<int8_t> vec1 =
          *(std::vector<int8_t> *)(left.value_.array);
      std::vector<int8_t> vec2 =
          *(std::vector<int8_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::SMALLINT: {
      std::vector<int16_t> vec1 =
          *(std::vector<int16_t> *)(left.value_.array);
      std::vector<int16_t> vec2 =
          *(std::vector<int16_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::INTEGER: {
      std::vector<int32_t> vec1 =
          *(std::vector<int32_t> *)(left.value_.array);
      std::vector<int32_t> vec2 =
          *(std::vector<int32_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::BIGINT: {
      std::vector<int64_t> vec1 =
          *(std::vector<int64_t> *)(left.value_.array);
      std::vector<int64_t> vec2 =
          *(std::vector<int64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::DECIMAL: {
      std::vector<double> vec1 =
          *(std::vector<double> *)(left.value_.array);
      std::vector<double> vec2 =
          *(std::vector<double> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::TIMESTAMP: {
      std::vector<uint64_t> vec1 =
          *(std::vector<uint64_t> *)(left.value_.array);
      std::vector<uint64_t> vec2 =
          *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    case TypeId::VARCHAR: {
      std::vector<std::string> vec1 =
          *(std::vector<std::string> *)(left.value_.array);
      std::vector<std::string> vec2 =
          *(std::vector<std::string> *)(right.GetAs<char *>());
      return GetCmpBool(vec1 >= vec2);
    }
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Element type is invalid.");
}

TypeId ArrayType::GetElementType(const Value &val) const {
  TypeId type_id = val.GetTypeId();
  switch (type_id) {
    case TypeId::INTEGERARRAY: 
      return TypeId::INTEGER;
    case TypeId::DECIMALARRAY: 
      return TypeId::DECIMAL;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%d' for Array GetElementType method",
                             static_cast<int>(type_id));
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value ArrayType::CastAs(const Value &val UNUSED_ATTRIBUTE,
                        UNUSED_ATTRIBUTE const TypeId type_id) const {
  PL_ASSERT(false);
  throw Exception(ExceptionType::INCOMPATIBLE_TYPE,
                  "Cannot cast array values.");
}

// Create a copy of this value
Value ArrayType::Copy(const Value& val) const { return Value(val); }

void ArrayType::SerializeTo(const Value& val UNUSED_ATTRIBUTE, SerializeOutput &out UNUSED_ATTRIBUTE) const {
  throw Exception("Can't serialize array types to storage");
}

void ArrayType::SerializeTo(const Value& val, char *storage,
                 bool inlined UNUSED_ATTRIBUTE,
                 AbstractPool *pool) const {
  TypeId type_id = val.GetTypeId();
  uint32_t len, size;
  char* data = nullptr;
  switch (type_id) {
    case TypeId::INTEGERARRAY: {
      std::vector<int32_t> *int32_vec = 
          reinterpret_cast<std::vector<int32_t> *>(val.value_.array);
      len = int32_vec->size();
      size = len * sizeof(int32_t) + sizeof(uint32_t);
      data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
      PL_MEMCPY(data, &len, sizeof(uint32_t));
      if(len > 0){
        PL_MEMCPY(data + sizeof(uint32_t), int32_vec->data(),
                  size - sizeof(uint32_t));
      }
      break;
    }
    case TypeId::DECIMALARRAY: {
      std::vector<double> *double_vec = 
          reinterpret_cast<std::vector<double> *>(val.value_.array);
      len = double_vec->size();
      size = len * sizeof(double) + sizeof(uint32_t);
      data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
      PL_MEMCPY(data, &len, sizeof(uint32_t));
      if(len > 0){
        PL_MEMCPY(data + sizeof(uint32_t), double_vec->data(),
                  size - sizeof(uint32_t));
      }
      break;
    }
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%d' for Array SerializeTo method",
                             static_cast<int>(type_id));
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
  *reinterpret_cast<const char **>(storage) = data;
}

// Deserialize a value of the given type from the given storage space.
Value ArrayType::DeserializeFrom(const char *storage,
                                  const bool inlined UNUSED_ATTRIBUTE,
                                  AbstractPool *pool UNUSED_ATTRIBUTE) const {
  const char *ptr = *reinterpret_cast<const char *const *>(storage);
  uint32_t len = *reinterpret_cast<const uint32_t *>(ptr);
  switch (type_id_) {
    case TypeId::INTEGERARRAY:
      if(len == 0) {
        auto int32_vec = new std::vector<int32_t>();
        return Value(type_id_, *int32_vec, false);
      } else {
        const int32_t *int32_begin = reinterpret_cast<const int32_t *>(ptr + 
            sizeof(uint32_t));
        auto int32_vec = new std::vector<int32_t>(int32_begin, int32_begin + len);
        return Value(type_id_, *int32_vec, false);
      }
      break;
    case TypeId::DECIMALARRAY:
      if(len == 0) {
        auto double_vec = new std::vector<double>();
        return Value(type_id_, *double_vec, false);
      } else {
        const double *double_begin = reinterpret_cast<const double *>(ptr + 
            sizeof(uint32_t));
        auto double_vec = new std::vector<double>(double_begin, double_begin + len);
        return Value(type_id_, *double_vec, false);
      }
      break;
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%d' for Array DeserializeFrom method",
                             static_cast<int>(type_id_));
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
}

Value ArrayType::DeserializeFrom(SerializeInput &in UNUSED_ATTRIBUTE,
                                  AbstractPool *pool UNUSED_ATTRIBUTE) const {
  throw Exception("Can't deserialize array types from storage");
}

std::string ArrayType::ToString(const Value &val) const {
  uint32_t len = GetLength(val);
  std::ostringstream os;
  os << "{";
  for (uint32_t idx = 0; idx < len; idx++) {
    if(idx != 0) {
      os << ",";
    }
    os << GetElementAt(val, idx).ToString();
  }
  os << "}";
  return os.str();
}

uint32_t ArrayType::GetLength(const Value &val) const {
  TypeId type_id = val.GetTypeId();
  uint32_t len;
  switch (type_id) {
    case TypeId::INTEGERARRAY: {
      std::vector<int32_t> *int32_vec = 
          reinterpret_cast<std::vector<int32_t> *>(val.value_.array);
      len = int32_vec->size();
      break;
    }
    case TypeId::DECIMALARRAY: {
      std::vector<double> *double_vec = 
          reinterpret_cast<std::vector<double> *>(val.value_.array);
      len = double_vec->size();
    }
    default: {
      std::string msg =
          StringUtil::Format("Invalid Type '%d' for Array GetLength method",
                             static_cast<int>(type_id));
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg);
    }
  }
  return len;
}

}  // namespace type
}  // namespace peloton
