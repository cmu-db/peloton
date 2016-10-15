////===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// array_value.h
////
//// Identification: src/backend/common/array_value.cpp
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//#include "common/array_value.h"
//#include "common/numeric_value.h"
//#include "common/decimal_value.h"
//#include "common/boolean_value.h"
//#include "common/varlen_value.h"
//#include "common/timestamp_value.h"
//
//namespace peloton {
//namespace common {
//
//// Get the element at a given index in this array
//Value *ArrayValue::GetElementAt(uint64_t idx) const {
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec = *(std::vector<bool> *)(value_.ptr);
//      return new BooleanValue(vec.at(idx));
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec = *(std::vector<int8_t> *)(value_.ptr);
//      return new IntegerValue((int8_t)vec.at(idx));
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec = *(std::vector<int16_t> *)(value_.ptr);
//      return new IntegerValue((int16_t)vec.at(idx));
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec = *(std::vector<int32_t> *)(value_.ptr);
//      return new IntegerValue((int32_t)vec.at(idx));
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec = *(std::vector<int64_t> *)(value_.ptr);
//      return new IntegerValue((int64_t)vec.at(idx));
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec = *(std::vector<double> *)(value_.ptr);
//      return new DecimalValue((double)vec.at(idx));
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec = *(std::vector<uint64_t> *)(value_.ptr);
//      return new TimestampValue((uint64_t)vec.at(idx));
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec = *(std::vector<std::string> *)(value_.ptr);
//      return new VarlenValue(vec.at(idx), false);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//// Does this value exist in this array?
//Value *ArrayValue::InList(const Value &o) const {
//  std::unique_ptr<Value> ele(GetElementAt(0));
//  ele->CheckComparable(o);
//  if (o.IsNull())
//    return new BooleanValue(PELOTON_BOOLEAN_NULL);
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) BooleanValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) IntegerValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) IntegerValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) IntegerValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) IntegerValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec = *(std::vector<double> *)(value_.ptr);
//      std::vector<double>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) DecimalValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) TimestampValue(*it).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string>::iterator it;
//      for (it = vec.begin(); it != vec.end(); it++) {
//        BooleanValue *res = (BooleanValue *) VarlenValue(*it, false).CompareEquals(o);
//        if (res->IsTrue())
//          return res;
//        delete res;
//      }
//      return new BooleanValue(0);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareEquals(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec1 = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> vec2 = *(std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 == vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareNotEquals(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec1 = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> vec2 = *(std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 != vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareLessThan(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> *vec1 = (std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> *vec2 = (std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 < vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareLessThanEquals(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec1 = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> vec2 = *(std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 <= vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareGreaterThan(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec1 = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> vec2 = *(std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 > vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CompareGreaterThanEquals(const Value &o) const {
//  PL_ASSERT(GetTypeId() == Type::ARRAY);
//  CheckComparable(o);
//  if (((ArrayValue &)o).GetElementType() != GetElementType()) {
//    std::string msg = Type::GetInstance((
//                      (ArrayValue &)o).GetElementType()).ToString()
//                      + " mismatch with " +
//                      Type::GetInstance(GetElementType()).ToString();
//    throw Exception(EXCEPTION_TYPE_MISMATCH_TYPE, msg);
//  }
//  switch (GetElementType()) {
//    case Type::BOOLEAN: {
//      std::vector<bool> vec1 = *(std::vector<bool> *)(value_.ptr);
//      std::vector<bool> vec2 = *(std::vector<bool> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::TINYINT: {
//      std::vector<int8_t> vec1 = *(std::vector<int8_t> *)(value_.ptr);
//      std::vector<int8_t> vec2 = *(std::vector<int8_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::SMALLINT: {
//      std::vector<int16_t> vec1 = *(std::vector<int16_t> *)(value_.ptr);
//      std::vector<int16_t> vec2 = *(std::vector<int16_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::INTEGER: {
//      std::vector<int32_t> vec1 = *(std::vector<int32_t> *)(value_.ptr);
//      std::vector<int32_t> vec2 = *(std::vector<int32_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::BIGINT: {
//      std::vector<int64_t> vec1 = *(std::vector<int64_t> *)(value_.ptr);
//      std::vector<int64_t> vec2 = *(std::vector<int64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::DECIMAL: {
//      std::vector<double> vec1 = *(std::vector<double> *)(value_.ptr);
//      std::vector<double> vec2 = *(std::vector<double> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::TIMESTAMP: {
//      std::vector<uint64_t> vec1 = *(std::vector<uint64_t> *)(value_.ptr);
//      std::vector<uint64_t> vec2 = *(std::vector<uint64_t> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    case Type::VARCHAR: {
//      std::vector<std::string> vec1 = *(std::vector<std::string> *)(value_.ptr);
//      std::vector<std::string> vec2 = *(std::vector<std::string> *)(o.GetAs<char *>());
//      return new BooleanValue(vec1 >= vec2);
//    }
//    default:
//      break;
//  }
//  throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Element type is invalid.");
//}
//
//Value *ArrayValue::CastAs(UNUSED_ATTRIBUTE const Type::TypeId type_id) const {
//  PL_ASSERT(false);
//  throw Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, "Cannot cast array values.");
//}
//
//}  // namespace peloton
//}  // namespace common
