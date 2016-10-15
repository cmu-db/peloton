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


#include <limits.h>
#include <iostream>
#include <cstdint>
#include <cmath>

#include "../../src/include/common/array_type.h"
#include "../../src/include/common/boolean_type.h"
#include "../../src/include/common/decimal_type.h"
#include "../../src/include/common/numeric_type.h"
#include "../../src/include/common/varlen_type.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class ArrayValueTests : public PelotonTest {};

#define RANDOM(a) (rand() % a) // Generate a random number in [0, a)
#define RANDOM_DECIMAL() ((double)rand() / (double)rand())
#define TEST_NUM 10

using namespace peloton::common;

int8_t RANDOM8() {
  return ((rand() % (SCHAR_MAX * 2 - 1)) - (SCHAR_MAX - 1));
}

int16_t RANDOM16() {
  return ((rand() % (SHRT_MAX * 2 - 1)) - (SHRT_MAX - 1));
}

int32_t RANDOM32() {
  int32_t ret = (((size_t)(rand()) << 1) | ((size_t)(rand()) & 0x1));
  if (ret != PELOTON_INT32_NULL)
    return ret;
  return 1;
}

int64_t RANDOM64() {
  int64_t ret = (((size_t)(rand()) << 33) | ((size_t)(rand()) << 2) |
                 ((size_t)(rand()) & 0x3));
  if (ret != PELOTON_INT64_NULL)
    return ret;
  return 1;
}

std::string RANDOM_STRING(size_t size) {
  char *str = new char[size];
  for (size_t i = 0; i < size - 1; i++)
    str[i] = RANDOM(26) + 'a';
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
  ArrayValue *array_bool = new ArrayValue(vec_bool,
    Type::GetInstance(Type::BOOLEAN));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_bool->GetElementAt(i);
    EXPECT_EQ(bool(ele->GetAs<int8_t>()), vec_bool[i]);
    delete ele;
  }
  delete array_bool;

  std::vector<int8_t> vec_tinyint;
  for (size_t i = 0; i < n; i++) {
    vec_tinyint.push_back(RANDOM8());
  }
  ArrayValue *array_tinyint = new ArrayValue(vec_tinyint,
    Type::GetInstance(Type::TINYINT));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_tinyint->GetElementAt(i);
    EXPECT_EQ(ele->GetAs<int8_t>(), vec_tinyint[i]);
    delete ele;
  }
  delete array_tinyint;

  std::vector<int16_t> vec_smallint;
  for (size_t i = 0; i < n; i++) {
    vec_smallint.push_back(RANDOM16());
  }
  ArrayValue *array_smallint = new ArrayValue(vec_smallint,
    Type::GetInstance(Type::SMALLINT));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_smallint->GetElementAt(i);
    EXPECT_EQ(ele->GetAs<int16_t>(), vec_smallint[i]);
    delete ele;
  }
  delete array_smallint;

  std::vector<int32_t> vec_integer;
  for (size_t i = 0; i < n; i++) {
    vec_integer.push_back(RANDOM32());
  }
  ArrayValue *array_integer = new ArrayValue(vec_integer,
    Type::GetInstance(Type::INTEGER));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_integer->GetElementAt(i);
    EXPECT_EQ(ele->GetAs<int32_t>(), vec_integer[i]);
    delete ele;
  }
  delete array_integer;

  std::vector<int64_t> vec_bigint;
  for (size_t i = 0; i < n; i++) {
    vec_bigint.push_back(RANDOM64());
  }
  ArrayValue *array_bigint = new ArrayValue(vec_bigint,
    Type::GetInstance(Type::BIGINT));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_bigint->GetElementAt(i);
    EXPECT_EQ(ele->GetAs<int64_t>(), vec_bigint[i]);
    delete ele;
  }
  delete array_bigint;

  std::vector<double> vec_decimal;
  for (size_t i = 0; i < n; i++) {
    vec_decimal.push_back(RANDOM_DECIMAL());
  }
  ArrayValue *array_decimal = new ArrayValue(vec_decimal,
    Type::GetInstance(Type::DECIMAL));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_decimal->GetElementAt(i);
    EXPECT_EQ(ele->GetAs<double>(), vec_decimal[i]);
    delete ele;
  }
  delete array_decimal;

  std::vector<std::string> vec_varchar;
  for (size_t i = 0; i < n; i++) {
    vec_varchar.push_back(RANDOM_STRING(RANDOM(100)));
  }
  ArrayValue *array_varchar = new ArrayValue(vec_varchar,
    Type::GetInstance(Type::VARCHAR));
  for (size_t i = 0; i < n; i++) {
    Value *ele = array_varchar->GetElementAt(i);
    EXPECT_EQ(((VarlenValue *)ele)->GetData(), vec_varchar[i]);
    delete ele;
  }
  delete array_varchar;
}

TEST_F(ArrayValueTests, InListTest) {
  // Create vectors of different types
  // Insert n elements into each vector
  size_t n = 10;
  std::vector<bool> vec_bool;
  for (size_t i = 0; i < n; i++) {
    vec_bool.push_back(RANDOM(2));
  }
  ArrayValue *array_bool = new ArrayValue(vec_bool,
    Type::GetInstance(Type::BOOLEAN));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_bool->InList(BooleanValue(vec_bool[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  EXPECT_THROW(array_bool->InList(IntegerValue(0)), peloton::Exception);
  EXPECT_THROW(array_bool->InList(DecimalValue(0.0)), peloton::Exception);
  EXPECT_THROW(array_bool->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_bool->InList(*array_bool), peloton::Exception);
  delete array_bool;

  std::vector<int8_t> vec_tinyint;
  for (size_t i = 0; i < n; i++) {
    vec_tinyint.push_back(RANDOM8());
  }
  ArrayValue *array_tinyint = new ArrayValue(vec_tinyint,
    Type::GetInstance(Type::TINYINT));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_tinyint->InList(IntegerValue(vec_tinyint[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    int8_t val = RANDOM8();
    std::vector<int8_t>::iterator it = find(vec_tinyint.begin(),
      vec_tinyint.end(), val);
    if (it == vec_tinyint.end()) {
      Value *in_list = array_tinyint->InList(IntegerValue(val));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_tinyint->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_tinyint->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_tinyint->InList(*array_tinyint), peloton::Exception);
  delete array_tinyint;

  std::vector<int16_t> vec_smallint;
  for (size_t i = 0; i < n; i++) {
    vec_smallint.push_back(RANDOM16());
  }
  ArrayValue *array_smallint = new ArrayValue(vec_smallint,
    Type::GetInstance(Type::SMALLINT));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_smallint->InList(IntegerValue(vec_smallint[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    int16_t val = RANDOM16();
    std::vector<int16_t>::iterator it = find(vec_smallint.begin(),
      vec_smallint.end(), val);
    if (it == vec_smallint.end()) {
      Value *in_list = array_smallint->InList(IntegerValue(val));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_smallint->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_smallint->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_smallint->InList(*array_smallint), peloton::Exception);
  delete array_smallint;

  std::vector<int32_t> vec_integer;
  for (size_t i = 0; i < n; i++) {
    vec_integer.push_back(RANDOM32());
  }
  ArrayValue *array_integer = new ArrayValue(vec_integer,
    Type::GetInstance(Type::INTEGER));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_integer->InList(IntegerValue(vec_integer[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    int32_t val = RANDOM32();
    std::vector<int32_t>::iterator it = find(vec_integer.begin(),
      vec_integer.end(), val);
    if (it == vec_integer.end()) {
      Value *in_list = array_integer->InList(IntegerValue(val));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_integer->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_integer->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_integer->InList(*array_integer), peloton::Exception);
  delete array_integer;

  std::vector<int64_t> vec_bigint;
  for (size_t i = 0; i < n; i++) {
    vec_bigint.push_back(RANDOM64());
  }
  ArrayValue *array_bigint = new ArrayValue(vec_bigint,
    Type::GetInstance(Type::BIGINT));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_bigint->InList(IntegerValue(vec_bigint[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    int64_t val = RANDOM64();
    std::vector<int64_t>::iterator it = find(vec_bigint.begin(),
      vec_bigint.end(), val);
    if (it == vec_bigint.end()) {
      Value *in_list = array_bigint->InList(IntegerValue(val));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_bigint->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_bigint->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_bigint->InList(*array_bigint), peloton::Exception);
  delete array_bigint;

  std::vector<double> vec_decimal;
  for (size_t i = 0; i < n; i++) {
    vec_decimal.push_back(RANDOM64());
  }
  ArrayValue *array_decimal = new ArrayValue(vec_decimal,
    Type::GetInstance(Type::DECIMAL));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_decimal->InList(DecimalValue(vec_decimal[i]));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    double val = RANDOM_DECIMAL();
    std::vector<double>::iterator it = find(vec_decimal.begin(),
      vec_decimal.end(), val);
    if (it == vec_decimal.end()) {
      Value *in_list = array_decimal->InList(DecimalValue(val));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_decimal->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_decimal->InList(VarlenValue("", false)), peloton::Exception);
  EXPECT_THROW(array_decimal->InList(*array_decimal), peloton::Exception);
  delete array_decimal;

  std::vector<std::string> vec_varchar;
  for (size_t i = 0; i < n; i++) {
    vec_varchar.push_back(RANDOM_STRING(RANDOM(100)));
  }
  ArrayValue *array_varchar = new ArrayValue(vec_varchar,
    Type::GetInstance(Type::VARCHAR));
  for (size_t i = 0; i < n; i++) {
    Value *in_list = array_varchar->InList(VarlenValue(vec_varchar[i], false));
    EXPECT_TRUE(((BooleanValue *)in_list)->IsTrue());
    delete in_list;
  }
  for (size_t i = 0; i < n; i++) {
    std::string val = RANDOM_STRING(RANDOM(100));
    std::vector<std::string>::iterator it = find(vec_varchar.begin(),
      vec_varchar.end(), val);
    if (it == vec_varchar.end()) {
      Value *in_list = array_varchar->InList(VarlenValue(val, false));
      EXPECT_TRUE(((BooleanValue *)in_list)->IsFalse());
      delete in_list;
    }
  }
  EXPECT_THROW(array_varchar->InList(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(array_varchar->InList(IntegerValue(0)), peloton::Exception);
  EXPECT_THROW(array_varchar->InList(DecimalValue(0.0)), peloton::Exception);
  EXPECT_THROW(array_varchar->InList(*array_varchar), peloton::Exception);
  delete array_varchar;
}

void CheckEqual(Value *v1, Value *v2) {
  BooleanValue *result[6] = { nullptr };
  result[0] = (BooleanValue *)v1->CompareEquals(*v2);
  EXPECT_TRUE(result[0]->IsTrue());
  result[1] = (BooleanValue *)v1->CompareNotEquals(*v2);
  EXPECT_TRUE(result[1]->IsFalse());
  result[2] = (BooleanValue *)v1->CompareLessThan(*v2);
  EXPECT_TRUE(result[2]->IsFalse());
  result[3] = (BooleanValue *)v1->CompareLessThanEquals(*v2);
  EXPECT_TRUE(result[3]->IsTrue());
  result[4] = (BooleanValue *)v1->CompareGreaterThan(*v2);
  EXPECT_TRUE(result[4]->IsFalse());
  result[5] = (BooleanValue *)v1->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(result[5]->IsTrue());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

void CheckLessThan(Value *v1, Value *v2) {
  BooleanValue *result[6] = { nullptr };
  result[0] = (BooleanValue *)v1->CompareEquals(*v2);
  EXPECT_TRUE(result[0]->IsFalse());
  result[1] = (BooleanValue *)v1->CompareNotEquals(*v2);
  EXPECT_TRUE(result[1]->IsTrue());
  result[2] = (BooleanValue *)v1->CompareLessThan(*v2);
  EXPECT_TRUE(result[2]->IsTrue());
  result[3] = (BooleanValue *)v1->CompareLessThanEquals(*v2);
  EXPECT_TRUE(result[3]->IsTrue());
  result[4] = (BooleanValue *)v1->CompareGreaterThan(*v2);
  EXPECT_TRUE(result[4]->IsFalse());
  result[5] = (BooleanValue *)v1->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(result[5]->IsFalse());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

void CheckGreaterThan(Value *v1, Value *v2) {
  BooleanValue *result[6] = { nullptr };
  result[0] = (BooleanValue *)v1->CompareEquals(*v2);
  EXPECT_TRUE(result[0]->IsFalse());
  result[1] = (BooleanValue *)v1->CompareNotEquals(*v2);
  EXPECT_TRUE(result[1]->IsTrue());
  result[2] = (BooleanValue *)v1->CompareLessThan(*v2);
  EXPECT_TRUE(result[2]->IsFalse());
  result[3] = (BooleanValue *)v1->CompareLessThanEquals(*v2);
  EXPECT_TRUE(result[3]->IsFalse());
  result[4] = (BooleanValue *)v1->CompareGreaterThan(*v2);
  EXPECT_TRUE(result[4]->IsTrue());
  result[5] = (BooleanValue *)v1->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(result[5]->IsTrue());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

TEST_F(ArrayValueTests, CompareTest) {
  for (int i = 0; i < TEST_NUM; i++) {
    size_t len = 10;
    std::string str1 = RANDOM_STRING(len);
    std::string str2 = RANDOM_STRING(len);
    Value *v1 = new VarlenValue(str1, false);
    Value *v2 = new VarlenValue(str2, false);
    EXPECT_EQ(len, ((VarlenValue *)v1)->GetLength());
    EXPECT_EQ(len, ((VarlenValue *)v2)->GetLength());
    if (str1 == str2)
      CheckEqual(v1, v2);
    else if (str1 < str2)
      CheckLessThan(v1, v2);
    else
      CheckGreaterThan(v1, v2);
    delete v1;
    delete v2;
  }

  // Test type mismatch
  Value *v = new VarlenValue("", false);
  EXPECT_THROW(v->CompareEquals(BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(v->CompareEquals(IntegerValue(0)), peloton::Exception);
  EXPECT_THROW(v->CompareEquals(DecimalValue(0.0)), peloton::Exception);

  // Test null varchar
  std::unique_ptr<BooleanValue> cmp((BooleanValue *)v->CompareEquals(VarlenValue(nullptr, 0, false)));
  EXPECT_TRUE(cmp->IsNull());
  delete v;
}

}
}
