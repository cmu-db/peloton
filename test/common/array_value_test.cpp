//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value_test.cpp
//
// Identification: test/common/array_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#define VALUE_TESTS

#include <limits.h>
#include <cmath>
#include <cstdint>
#include <iostream>

#include "type/array_type.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "common/harness.h"
#include "type/numeric_type.h"
#include "type/value_factory.h"
#include "type/varlen_type.h"

namespace peloton {
namespace test {

class ArrayValueTests : public PelotonTest {};

#define RANDOM(a) (rand() % a)  // Generate a random number in [0, a)
#define RANDOM_DECIMAL() ((double)rand() / (double)rand())
#define TEST_NUM 10

using namespace peloton::common;

int8_t RANDOM8() { return ((rand() % (SCHAR_MAX * 2 - 1)) - (SCHAR_MAX - 1)); }

int16_t RANDOM16() { return ((rand() % (SHRT_MAX * 2 - 1)) - (SHRT_MAX - 1)); }

int32_t RANDOM32() {
  int32_t ret = (((size_t)(rand()) << 1) | ((size_t)(rand()) & 0x1));
  if (ret != type::PELOTON_INT32_NULL) return ret;
  return 1;
}

int64_t RANDOM64() {
  int64_t ret = (((size_t)(rand()) << 33) | ((size_t)(rand()) << 2) |
                 ((size_t)(rand()) & 0x3));
  if (ret != type::PELOTON_INT64_NULL) return ret;
  return 1;
}

std::string RANDOM_STRING(size_t size) {
  char *str = new char[size];
  for (size_t i = 0; i < size - 1; i++) str[i] = RANDOM(26) + 'a';
  str[size - 1] = '\0';
  std::string rand_string(str);
  delete[] str;
  return rand_string;
}

TEST_F(ArrayValueTests, GetElementTest) {
  // Create vectors of different types
  // Insert n elements into each vector
  size_t n = 10;
  std::vector<bool> vec_bool;
  for (size_t i = 0; i < n; i++) {
    vec_bool.push_back(RANDOM(2));
  }
  type::Value array_bool = type::Value(type::Type::ARRAY, vec_bool, type::Type::BOOLEAN);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_bool.GetElementAt(i);
    EXPECT_EQ(bool(ele.GetAs<int8_t>()), vec_bool[i]);
  }

  std::vector<int8_t> vec_tinyint;
  for (size_t i = 0; i < n; i++) {
    vec_tinyint.push_back(RANDOM8());
  }
  type::Value array_tinyint = type::Value(type::Type::ARRAY, vec_tinyint, type::Type::TINYINT);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_tinyint.GetElementAt(i);
    EXPECT_EQ(ele.GetAs<int8_t>(), vec_tinyint[i]);
  }

  std::vector<int16_t> vec_smallint;
  for (size_t i = 0; i < n; i++) {
    vec_smallint.push_back(RANDOM16());
  }
  type::Value array_smallint = type::Value(type::Type::ARRAY, vec_smallint, type::Type::SMALLINT);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_smallint.GetElementAt(i);
    EXPECT_EQ(ele.GetAs<int16_t>(), vec_smallint[i]);
  }

  std::vector<int32_t> vec_integer;
  for (size_t i = 0; i < n; i++) {
    vec_integer.push_back(RANDOM32());
  }
  type::Value array_integer = type::Value(type::Type::ARRAY, vec_integer, type::Type::INTEGER);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_integer.GetElementAt(i);
    EXPECT_EQ(ele.GetAs<int32_t>(), vec_integer[i]);
  }

  std::vector<int64_t> vec_bigint;
  for (size_t i = 0; i < n; i++) {
    vec_bigint.push_back(RANDOM64());
  }
  type::Value array_bigint = type::Value(type::Type::ARRAY, vec_bigint, type::Type::BIGINT);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_bigint.GetElementAt(i);
    EXPECT_EQ(ele.GetAs<int64_t>(), vec_bigint[i]);
  }

  std::vector<double> vec_decimal;
  for (size_t i = 0; i < n; i++) {
    vec_decimal.push_back(RANDOM_DECIMAL());
  }
  type::Value array_decimal = type::Value(type::Type::ARRAY, vec_decimal, type::Type::DECIMAL);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_decimal.GetElementAt(i);
    EXPECT_EQ(ele.GetAs<double>(), vec_decimal[i]);
  }

  std::vector<std::string> vec_varchar;
  for (size_t i = 0; i < n; i++) {
    vec_varchar.push_back(RANDOM_STRING(RANDOM(100) + 1));
  }
  type::Value array_varchar = type::Value(type::Type::ARRAY, vec_varchar, type::Type::VARCHAR);
  for (size_t i = 0; i < n; i++) {
    type::Value ele = array_varchar.GetElementAt(i);
    EXPECT_EQ((ele).GetData(), vec_varchar[i]);
  }
}

TEST_F(ArrayValueTests, InListTest) {
  // Create vectors of different types
  // Insert n elements into each vector
  size_t n = 10;
  std::vector<bool> vec_bool;
  for (size_t i = 0; i < n; i++) {
    vec_bool.push_back(RANDOM(2));
  }
  type::Value array_bool = type::Value(type::Type::ARRAY, vec_bool, type::Type::BOOLEAN);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_bool.InList(type::ValueFactory::GetBooleanValue(vec_bool[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  EXPECT_THROW(array_bool.InList(type::ValueFactory::GetIntegerValue(0)),
               peloton::Exception);
  EXPECT_THROW(array_bool.InList(type::ValueFactory::GetDoubleValue(0.0)),
               peloton::Exception);
  EXPECT_THROW(array_bool.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_bool.InList(array_bool), peloton::Exception);

  std::vector<int8_t> vec_tinyint;
  for (size_t i = 0; i < n; i++) {
    vec_tinyint.push_back(RANDOM8());
  }
  type::Value array_tinyint = type::Value(type::Type::ARRAY, vec_tinyint, type::Type::TINYINT);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_tinyint.InList(type::ValueFactory::GetTinyIntValue(vec_tinyint[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    int8_t val = RANDOM8();
    std::vector<int8_t>::iterator it =
        find(vec_tinyint.begin(), vec_tinyint.end(), val);
    if (it == vec_tinyint.end()) {
      type::Value in_list = array_tinyint.InList(type::ValueFactory::GetTinyIntValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_tinyint.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_tinyint.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_tinyint.InList(array_tinyint), peloton::Exception);

  std::vector<int16_t> vec_smallint;
  for (size_t i = 0; i < n; i++) {
    vec_smallint.push_back(RANDOM16());
  }
  type::Value array_smallint = type::Value(type::Type::ARRAY, vec_smallint, type::Type::SMALLINT);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_smallint.InList(type::ValueFactory::GetSmallIntValue(vec_smallint[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    int16_t val = RANDOM16();
    std::vector<int16_t>::iterator it =
        find(vec_smallint.begin(), vec_smallint.end(), val);
    if (it == vec_smallint.end()) {
      type::Value in_list =
          array_smallint.InList(type::ValueFactory::GetSmallIntValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_smallint.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_smallint.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_smallint.InList(array_smallint), peloton::Exception);

  std::vector<int32_t> vec_integer;
  for (size_t i = 0; i < n; i++) {
    vec_integer.push_back(RANDOM32());
  }
  type::Value array_integer = type::Value(type::Type::ARRAY, vec_integer, type::Type::INTEGER);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_integer.InList(type::ValueFactory::GetIntegerValue(vec_integer[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    int32_t val = RANDOM32();
    std::vector<int32_t>::iterator it =
        find(vec_integer.begin(), vec_integer.end(), val);
    if (it == vec_integer.end()) {
      type::Value in_list = array_integer.InList(type::ValueFactory::GetIntegerValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_integer.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_integer.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_integer.InList(array_integer), peloton::Exception);

  std::vector<int64_t> vec_bigint;
  for (size_t i = 0; i < n; i++) {
    vec_bigint.push_back(RANDOM64());
  }
  type::Value array_bigint = type::Value(type::Type::ARRAY, vec_bigint, type::Type::BIGINT);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_bigint.InList(type::ValueFactory::GetBigIntValue(vec_bigint[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    int64_t val = RANDOM64();
    std::vector<int64_t>::iterator it =
        find(vec_bigint.begin(), vec_bigint.end(), val);
    if (it == vec_bigint.end()) {
      type::Value in_list = array_bigint.InList(type::ValueFactory::GetBigIntValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_bigint.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_bigint.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_bigint.InList(array_bigint), peloton::Exception);

  std::vector<double> vec_decimal;
  for (size_t i = 0; i < n; i++) {
    vec_decimal.push_back(RANDOM64());
  }
  type::Value array_decimal = type::Value(type::Type::ARRAY, vec_decimal, type::Type::DECIMAL);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_decimal.InList(type::ValueFactory::GetDoubleValue(vec_decimal[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    double val = RANDOM_DECIMAL();
    std::vector<double>::iterator it =
        find(vec_decimal.begin(), vec_decimal.end(), val);
    if (it == vec_decimal.end()) {
      type::Value in_list = array_decimal.InList(type::ValueFactory::GetDoubleValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_decimal.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_decimal.InList(type::ValueFactory::GetVarcharValue(nullptr, 0)),
               peloton::Exception);
  EXPECT_THROW(array_decimal.InList(array_decimal), peloton::Exception);

  std::vector<std::string> vec_varchar;
  for (size_t i = 0; i < n; i++) {
    vec_varchar.push_back(RANDOM_STRING(RANDOM(100) + 1));
  }
  type::Value array_varchar = type::Value(type::Type::ARRAY, vec_varchar, type::Type::VARCHAR);
  for (size_t i = 0; i < n; i++) {
    type::Value in_list =
        array_varchar.InList(type::ValueFactory::GetVarcharValue(vec_varchar[i]));
    EXPECT_TRUE((in_list).IsTrue());
  }
  for (size_t i = 0; i < n; i++) {
    std::string val = RANDOM_STRING(RANDOM(100) + 1);
    std::vector<std::string>::iterator it =
        find(vec_varchar.begin(), vec_varchar.end(), val);
    if (it == vec_varchar.end()) {
      type::Value in_list = array_varchar.InList(type::ValueFactory::GetVarcharValue(val));
      EXPECT_TRUE((in_list).IsFalse());
    }
  }
  EXPECT_THROW(array_varchar.InList(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(array_varchar.InList(type::ValueFactory::GetIntegerValue(0)),
               peloton::Exception);
  EXPECT_THROW(array_varchar.InList(type::ValueFactory::GetDoubleValue(0.0)),
               peloton::Exception);
  EXPECT_THROW(array_varchar.InList(array_varchar), peloton::Exception);
}

void CheckEqual(type::Value v1, type::Value v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE(result[0].IsTrue());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE(result[1].IsFalse());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE(result[2].IsFalse());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE(result[3].IsTrue());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE(result[4].IsFalse());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE(result[5].IsTrue());
}

void CheckLessThan(type::Value v1, type::Value v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE(result[0].IsFalse());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE(result[1].IsTrue());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE(result[2].IsTrue());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE(result[3].IsTrue());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE(result[4].IsFalse());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE(result[5].IsFalse());
}

void CheckGreaterThan(type::Value v1, type::Value v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE(result[0].IsFalse());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE(result[1].IsTrue());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE(result[2].IsFalse());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE(result[3].IsFalse());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE(result[4].IsTrue());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE(result[5].IsTrue());
}

TEST_F(ArrayValueTests, CompareTest) {
  for (int i = 0; i < TEST_NUM; i++) {
    size_t len = 10;
    std::string str1 = RANDOM_STRING(len);
    std::string str2 = RANDOM_STRING(len);
    type::Value v1 = type::ValueFactory::GetVarcharValue(str1);
    type::Value v2 = type::ValueFactory::GetVarcharValue(str2);
    EXPECT_EQ(len, (v1).GetLength());
    EXPECT_EQ(len, (v2).GetLength());
    if (str1 == str2)
      CheckEqual(v1, v2);
    else if (str1 < str2)
      CheckLessThan(v1, v2);
    else
      CheckGreaterThan(v1, v2);
  }

  // Test type mismatch
  type::Value v = type::ValueFactory::GetVarcharValue("");
  EXPECT_THROW(v.CompareEquals(type::ValueFactory::GetBooleanValue(false)),
               peloton::Exception);
  EXPECT_THROW(v.CompareEquals(type::ValueFactory::GetIntegerValue(0)),
               peloton::Exception);
  EXPECT_THROW(v.CompareEquals(type::ValueFactory::GetDoubleValue(0.0)),
               peloton::Exception);

  // Test null varchar
  type::Value cmp = (v.CompareEquals(type::ValueFactory::GetVarcharValue(nullptr, 0)));
  EXPECT_TRUE(cmp.IsNull());
}
}
}
