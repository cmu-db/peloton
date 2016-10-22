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

#include "common/array_type.h"

#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/numeric_type.h"
#include "common/timestamp_type.h"
#include "common/varlen_type.h"
#include "common/type.h"

namespace peloton {
namespace common {

// Get the element at a given index in this array
Value ArrayType::GetElementAt(const Value& val, uint64_t idx) const {
  switch (val.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec = *(std::vector<bool> *)(val.value_.array.data);
      return ValueFactory::GetBooleanValue(vec.at(idx));
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec = *(std::vector<int8_t> *)(val.value_.array.data);
      return ValueFactory::GetTinyIntValue((int8_t)vec.at(idx));
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec = *(std::vector<int16_t> *)(val.value_.array.data);
      return ValueFactory::GetSmallIntValue((int16_t)vec.at(idx));
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec = *(std::vector<int32_t> *)(val.value_.array.data);
      return ValueFactory::GetIntegerValue((int32_t)vec.at(idx));
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec = *(std::vector<int64_t> *)(val.value_.array.data);
      return ValueFactory::GetBigIntValue((int64_t)vec.at(idx));
    }
    case Type::DECIMAL: {
      std::vector<double> vec = *(std::vector<double> *)(val.value_.array.data);
      return ValueFactory::GetDoubleValue((double)vec.at(idx));
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec = *(std::vector<uint64_t> *)(val.value_.array.data);
      return ValueFactory::GetTimestampValue((uint64_t)vec.at(idx));
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec = *(std::vector<std::string> *)(val.value_.array.data);
      return ValueFactory::GetVarcharValue(vec.at(idx));
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

// Does this value exist in this array?
Value ArrayType::InList(const Value& list, const Value &object) const {
  Value ele = (list.GetElementAt(0));
  ele.CheckComparable(object);
  if (object.IsNull())
    return  ValueFactory::GetNullValueByType(Type::BOOLEAN);
  switch (list.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec = *(std::vector<bool> *)(list.value_.array.data);
      std::vector<bool>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBooleanValue(*it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec = *(std::vector<int8_t> *)(list.value_.array.data);
      std::vector<int8_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = Value(Type::TINYINT, *it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec = *(std::vector<int16_t> *)(list.value_.array.data);
      std::vector<int16_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = Value(Type::SMALLINT, *it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec = *(std::vector<int32_t> *)(list.value_.array.data);
      std::vector<int32_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetIntegerValue(*it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec = *(std::vector<int64_t> *)(list.value_.array.data);
      std::vector<int64_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetBigIntValue(*it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::DECIMAL: {
      std::vector<double> vec = *(std::vector<double> *)(list.value_.array.data);
      std::vector<double>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetDoubleValue(*it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec = *(std::vector<uint64_t> *)(list.value_.array.data);
      std::vector<uint64_t>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = ValueFactory::GetTimestampValue(*it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec = *(std::vector<std::string> *)(list.value_.array.data);
      std::vector<std::string>::iterator it;
      for (it = vec.begin(); it != vec.end(); it++) {
        Value res = Value(Type::VARCHAR, *it).CompareEquals(object);
        if (res.IsTrue())
          return res;

      }
      return ValueFactory::GetBooleanValue(0);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec1 = *(std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> vec2 = *(std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 == vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareNotEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec1 = *(std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> vec2 = *(std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 != vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareLessThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> *vec1 = (std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> *vec2 = (std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 < vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareLessThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec1 = *(std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> vec2 = *(std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 <= vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareGreaterThan(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec1 = *(std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> vec2 = *(std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 > vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CompareGreaterThanEquals(const Value& left, const Value &right) const {
  PL_ASSERT(GetTypeId() == Type::ARRAY);
  left.CheckComparable(right);
  if (right.GetElementType() != left.GetElementType()) {
    std::string msg = Type::GetInstance(
                      right.GetElementType())->ToString()
                      + " mismatch with " +
                      Type::GetInstance(left.GetElementType())->ToString();
    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
  }
  switch (left.GetElementType()) {
    case Type::BOOLEAN: {
      std::vector<bool> vec1 = *(std::vector<bool> *)(left.value_.array.data);
      std::vector<bool> vec2 = *(std::vector<bool> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::TINYINT: {
      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(left.value_.array.data);
      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::SMALLINT: {
      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(left.value_.array.data);
      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::INTEGER: {
      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(left.value_.array.data);
      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::BIGINT: {
      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(left.value_.array.data);
      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::DECIMAL: {
      std::vector<double> vec1 = *(std::vector<double> *)(left.value_.array.data);
      std::vector<double> vec2 = *(std::vector<double> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::TIMESTAMP: {
      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(left.value_.array.data);
      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    case Type::VARCHAR: {
      std::vector<std::string> vec1 = *(std::vector<std::string> *)(left.value_.array.data);
      std::vector<std::string> vec2 = *(std::vector<std::string> *)(right.GetAs<char *>());
      return ValueFactory::GetBooleanValue(vec1 >= vec2);
    }
    default:
      break;
  }
  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
}

Value ArrayType::CastAs(const Value& val UNUSED_ATTRIBUTE, UNUSED_ATTRIBUTE const Type::TypeId type_id) const {
  PL_ASSERT(false);
  throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, "Cannot cast array values.");
}

Type::TypeId ArrayType::GetElementType(const Value& val UNUSED_ATTRIBUTE) const {
  return val.value_.array.array_type;
}

}  // namespace peloton
}  // namespace common
