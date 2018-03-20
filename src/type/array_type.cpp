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

#include "type/value.h"
#include "type/array_type.h"

#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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
  PL_ASSERT(GetTypeId() == TypeId::ARRAY);
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

Value ArrayType::CastAs(const Value &val UNUSED_ATTRIBUTE,
                        UNUSED_ATTRIBUTE const TypeId type_id) const {
  PL_ASSERT(false);
  throw Exception(ExceptionType::INCOMPATIBLE_TYPE,
                  "Cannot cast array values.");
}

TypeId ArrayType::GetElementType(
    const Value &val UNUSED_ATTRIBUTE) const {
  return val.size_.elem_type_id;
}

}  // namespace type
}  // namespace peloton
