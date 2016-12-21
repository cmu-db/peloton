//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value_test.cpp
//
// Identification: test/common/numeric_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define VALUE_TESTS

#include <limits.h>
#include <iostream>
#include <cstdint>
#include <cmath>
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/varlen_type.h"
#include "common/harness.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

class NumericValueTests : public PelotonTest {};

#define RANDOM_DECIMAL() ((double)rand() / (double)rand())
#define SEED 233
#define TEST_NUM 1
using namespace peloton::common;

int8_t RANDOM8() {
  return ((rand() % (SCHAR_MAX * 2 - 1)) - (SCHAR_MAX - 1));
}

int16_t RANDOM16() {
  return ((rand() % (SHRT_MAX * 2 - 1)) - (SHRT_MAX - 1));
}

int32_t RANDOM32() {
  int32_t ret = (((size_t)(rand()) << 1) | ((size_t)(rand()) & 0x1));
  if (ret != type::PELOTON_INT32_NULL)
    return ret;
  return 1;
}

int64_t RANDOM64() {
  int64_t ret = (((size_t)(rand()) << 33) | ((size_t)(rand()) << 2) |
                 ((size_t)(rand()) & 0x3));
  if (ret != type::PELOTON_INT64_NULL)
    return ret;
  return 1;
}

void CheckEqual(type::Value &v1, type::Value &v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE((result[0]).IsTrue());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE((result[1]).IsFalse());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE((result[2]).IsFalse());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE((result[3]).IsTrue());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE((result[4]).IsFalse());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE((result[5]).IsTrue());
}

void CheckLessThan(type::Value &v1, type::Value &v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE((result[0]).IsFalse());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE((result[1]).IsTrue());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE((result[2]).IsTrue());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE((result[3]).IsTrue());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE((result[4]).IsFalse());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE((result[5]).IsFalse());
}

void CheckGreaterThan(type::Value &v1, type::Value &v2) {
  type::Value result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_TRUE((result[0]).IsFalse());
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_TRUE((result[1]).IsTrue());
  result[2] = v1.CompareLessThan(v2);
  EXPECT_TRUE((result[2]).IsFalse());
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_TRUE((result[3]).IsFalse());
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_TRUE((result[4]).IsTrue());
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_TRUE((result[5]).IsTrue());
}

// Compare two integers
template<class T1, class T2>
void CheckCompare1(T1 x, T2 y, type::Type::TypeId xtype, type::Type::TypeId ytype) {
  type::Value v1 = type::Value(xtype, x);
  type::Value v2 = type::Value(ytype, y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare integer and decimal
template<class T>
void CheckCompare2(T x, double y, type::Type::TypeId xtype) {
  type::Value v1 = type::Value(xtype, x);
  type::Value v2 = type::ValueFactory::GetDoubleValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare decimal and integer
template<class T>
void CheckCompare3(double x, T y, type::Type::TypeId ytype ) {
  type::Value v1 = type::ValueFactory::GetDoubleValue(x);
  type::Value v2 = type::Value(ytype, y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare two decimals
void CheckCompare4(double x, double y) {
  type::Value v1 = type::ValueFactory::GetDoubleValue(x);
  type::Value v2 = type::ValueFactory::GetDoubleValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Modulo for decimals
inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

// Check the operations of two integers
template<class T1, class T2>
void CheckMath1(T1 x, T2 y, type::Type::TypeId xtype, type::Type::TypeId ytype) {
  type::Type::TypeId maxtype = xtype > ytype? xtype : ytype;
  type::Value v1;
  type::Value v2;
  // Test x + y
  v2 = type::Value(maxtype, x + y);
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);
  // Out of range detection
  if ((x + y) != sum1 && (x + y) != sum2)
    EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0))
      EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
        peloton::Exception);
  }
  else if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0))
    EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Add(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }
  // Test x - y
  v2 = type::Value(maxtype, x - y);
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  // Out of range detection
  if ((x - y) != diff1 && (x - y) != diff2)
    EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0))
      EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
        peloton::Exception);
  }
  else if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0))
    EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Subtract(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x * y
  v2 = type::Value(maxtype, x * y);
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  // Out of range detection
  if ((x * y) != prod1 && (x * y) != prod2)
    EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if (y != 0 && prod1 / y != x)
      EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
        peloton::Exception);
  }
  else if (y != 0 && prod2 / y != x)
    EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Multiply(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x / y
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Divide(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Divide(type::Value(ytype, y));
    v2 = type::Value(maxtype, x / y);
    CheckEqual(v1, v2);
  }

  // Test x % y
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Modulo(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Modulo(type::Value(ytype, y));
    v2 = type::Value(maxtype, x % y);
    CheckEqual(v1, v2);
  }

  // Test sqrt(x)
  if (x < 0)
    EXPECT_THROW(type::Value(xtype, x).Sqrt(),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Sqrt();
    v2 = type::ValueFactory::GetDoubleValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

// Check the operations of an integer and a decimal
template<class T>
void CheckMath2(T x, double y, type::Type::TypeId xtype) {
  type::Value v1, v2;
  v1 = type::Value(xtype, x).Add(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::Value(xtype, x).Subtract(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::Value(xtype, x).Multiply(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Divide(type::ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Divide(type::ValueFactory::GetDoubleValue(y));
    v2 = type::ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Modulo(type::ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Modulo(type::ValueFactory::GetDoubleValue(y));
    v2 = type::ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of a decimal and an integer
template<class T>
void CheckMath3(double x, T y, type::Type::TypeId ytype) {
  type::Value v1, v2;

  v1 = type::ValueFactory::GetDoubleValue(x).Add(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDoubleValue(x).Subtract(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDoubleValue(x).Multiply(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDoubleValue(x).Divide(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDoubleValue(x).Divide(type::Value(ytype, y));
    v2 = type::ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDoubleValue(x).Modulo(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDoubleValue(x).Modulo(type::Value(ytype, y));
    v2 = type::ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of two decimals
void CheckMath4(double x, double y) {
  type::Value v1, v2;

  v1 = type::ValueFactory::GetDoubleValue(x).Add(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDoubleValue(x).Subtract(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDoubleValue(x).Multiply(type::ValueFactory::GetDoubleValue(y));
  v2 = type::ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDoubleValue(x).Divide(type::ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDoubleValue(x).Divide(type::ValueFactory::GetDoubleValue(y));
    v2 = type::ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDoubleValue(x).Modulo(type::ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDoubleValue(x).Modulo(type::ValueFactory::GetDoubleValue(y));
    v2 = type::ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
  
  if (x < 0)
   EXPECT_THROW(type::ValueFactory::GetDoubleValue(x).Sqrt(),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDoubleValue(x).Sqrt();
    v2 = type::ValueFactory::GetDoubleValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

TEST_F(NumericValueTests, ComparisonTest) {
  std::srand(SEED);

  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int8_t, int8_t>(RANDOM8(), RANDOM8(), type::Type::TINYINT, type::Type::TINYINT);
    CheckCompare1<int8_t, int16_t>(RANDOM8(), RANDOM16(), type::Type::TINYINT, type::Type::SMALLINT);
    CheckCompare1<int8_t, int32_t>(RANDOM8(), RANDOM32(), type::Type::TINYINT, type::Type::INTEGER);
    CheckCompare1<int8_t, int64_t>(RANDOM8(), RANDOM64(), type::Type::TINYINT, type::Type::BIGINT);
    CheckCompare2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), type::Type::TINYINT);

    CheckCompare1<int16_t, int8_t>(RANDOM16(), RANDOM8(), type::Type::SMALLINT, type::Type::TINYINT);
    CheckCompare1<int16_t, int16_t>(RANDOM16(), RANDOM16(), type::Type::SMALLINT, type::Type::SMALLINT);
    CheckCompare1<int16_t, int32_t>(RANDOM16(), RANDOM32(), type::Type::SMALLINT, type::Type::INTEGER);
    CheckCompare1<int16_t, int64_t>(RANDOM16(), RANDOM64(), type::Type::SMALLINT, type::Type::BIGINT);
    CheckCompare2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), type::Type::SMALLINT);

    CheckCompare1<int32_t, int8_t>(RANDOM32(), RANDOM8(), type::Type::INTEGER, type::Type::TINYINT);
    CheckCompare1<int32_t, int16_t>(RANDOM32(), RANDOM16(), type::Type::INTEGER, type::Type::SMALLINT);
    CheckCompare1<int32_t, int32_t>(RANDOM32(), RANDOM32(), type::Type::INTEGER, type::Type::INTEGER);
    CheckCompare1<int32_t, int64_t>(RANDOM32(), RANDOM64(), type::Type::INTEGER, type::Type::BIGINT);
    CheckCompare2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), type::Type::INTEGER);

    CheckCompare1<int64_t, int8_t>(RANDOM64(), RANDOM8(), type::Type::BIGINT, type::Type::TINYINT);
    CheckCompare1<int64_t, int16_t>(RANDOM64(), RANDOM16(), type::Type::BIGINT, type::Type::SMALLINT);
    CheckCompare1<int64_t, int32_t>(RANDOM64(), RANDOM32(), type::Type::BIGINT, type::Type::INTEGER);
    CheckCompare1<int64_t, int64_t>(RANDOM64(), RANDOM64(), type::Type::BIGINT, type::Type::BIGINT);
    CheckCompare2<int64_t>(RANDOM64(), RANDOM_DECIMAL(), type::Type::BIGINT);

    CheckCompare3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), type::Type::TINYINT);
    CheckCompare3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), type::Type::SMALLINT);
    CheckCompare3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), type::Type::INTEGER);
    CheckCompare3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), type::Type::BIGINT);
    CheckCompare4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTests, MathTest) {
  std::srand(SEED);

  // Generate two values v1 and v2
  // Check type::Value(v1) op type::Value(v2) == type::Value(v1 op v2);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckMath1<int8_t, int8_t>(RANDOM8(), RANDOM8(), type::Type::TINYINT, type::Type::TINYINT);
    CheckMath1<int8_t, int16_t>(RANDOM8(), RANDOM16(), type::Type::TINYINT, type::Type::SMALLINT);
    CheckMath1<int8_t, int32_t>(RANDOM8(), RANDOM32(), type::Type::TINYINT, type::Type::INTEGER);
    CheckMath1<int8_t, int64_t>(RANDOM8(), RANDOM64(), type::Type::TINYINT, type::Type::BIGINT);
    CheckMath2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), type::Type::TINYINT);

    CheckMath1<int16_t, int8_t>(RANDOM16(), RANDOM8(), type::Type::SMALLINT, type::Type::TINYINT);
    CheckMath1<int16_t, int16_t>(RANDOM16(), RANDOM16(), type::Type::SMALLINT, type::Type::SMALLINT);
    CheckMath1<int16_t, int32_t>(RANDOM16(), RANDOM32(), type::Type::SMALLINT, type::Type::INTEGER);
    CheckMath1<int16_t, int64_t>(RANDOM16(), RANDOM64(), type::Type::SMALLINT, type::Type::BIGINT);
    CheckMath2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), type::Type::SMALLINT);

    CheckMath1<int32_t, int8_t>(RANDOM32(), RANDOM8(), type::Type::INTEGER, type::Type::TINYINT);
    CheckMath1<int32_t, int16_t>(RANDOM32(), RANDOM16(), type::Type::INTEGER, type::Type::SMALLINT);
    CheckMath1<int32_t, int32_t>(RANDOM32(), RANDOM32(), type::Type::INTEGER, type::Type::INTEGER);
    CheckMath1<int32_t, int64_t>(RANDOM32(), RANDOM64(), type::Type::BIGINT, type::Type::BIGINT);
    CheckMath2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), type::Type::INTEGER);

    CheckMath1<int64_t, int8_t>(RANDOM64(), RANDOM8(), type::Type::BIGINT, type::Type::TINYINT);
    CheckMath1<int64_t, int16_t>(RANDOM64(), RANDOM16(), type::Type::BIGINT, type::Type::SMALLINT);
    CheckMath1<int64_t, int32_t>(RANDOM64(), RANDOM32(), type::Type::BIGINT, type::Type::INTEGER);
    CheckMath1<int64_t, int64_t>(RANDOM64(), RANDOM64(), type::Type::BIGINT, type::Type::BIGINT);
    CheckMath2<int64_t>(RANDOM64(), RANDOM_DECIMAL(), type::Type::BIGINT);

    CheckMath3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), type::Type::TINYINT);
    CheckMath3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), type::Type::SMALLINT);
    CheckMath3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), type::Type::INTEGER);
    CheckMath3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), type::Type::BIGINT);
    CheckMath4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTests, DivideByZeroTest) {
  std::srand(SEED);

  CheckMath1<int8_t, int8_t>(RANDOM8(), 0, type::Type::TINYINT, type::Type::TINYINT);
  CheckMath1<int8_t, int16_t>(RANDOM8(), 0, type::Type::TINYINT, type::Type::SMALLINT);
  CheckMath1<int8_t, int32_t>(RANDOM8(), 0, type::Type::TINYINT, type::Type::INTEGER);
  CheckMath1<int8_t, int64_t>(RANDOM8(), 0, type::Type::TINYINT, type::Type::BIGINT);
  CheckMath2<int8_t>(RANDOM8(), 0, type::Type::TINYINT);

  CheckMath1<int16_t, int8_t>(RANDOM16(), 0, type::Type::SMALLINT, type::Type::TINYINT);
  CheckMath1<int16_t, int16_t>(RANDOM16(), 0, type::Type::SMALLINT, type::Type::SMALLINT);
  CheckMath1<int16_t, int32_t>(RANDOM16(), 0, type::Type::SMALLINT, type::Type::INTEGER);
  CheckMath1<int16_t, int64_t>(RANDOM16(), 0, type::Type::SMALLINT, type::Type::BIGINT);
  CheckMath2<int16_t>(RANDOM16(), 0, type::Type::SMALLINT);

  CheckMath1<int32_t, int8_t>(RANDOM32(), 0, type::Type::INTEGER, type::Type::TINYINT);
  CheckMath1<int32_t, int16_t>(RANDOM32(), 0, type::Type::INTEGER, type::Type::SMALLINT);
  CheckMath1<int32_t, int32_t>(RANDOM32(), 0, type::Type::INTEGER, type::Type::INTEGER);
  CheckMath1<int32_t, int64_t>(RANDOM32(), 0, type::Type::BIGINT, type::Type::BIGINT);
  CheckMath2<int32_t>(RANDOM32(), 0, type::Type::INTEGER);

  CheckMath1<int64_t, int8_t>(RANDOM64(), 0, type::Type::BIGINT, type::Type::TINYINT);
  CheckMath1<int64_t, int16_t>(RANDOM64(), 0, type::Type::BIGINT, type::Type::SMALLINT);
  CheckMath1<int64_t, int32_t>(RANDOM64(), 0, type::Type::BIGINT, type::Type::INTEGER);
  CheckMath1<int64_t, int64_t>(RANDOM64(), 0, type::Type::BIGINT, type::Type::BIGINT);
  CheckMath2<int64_t>(RANDOM64(), 0, type::Type::BIGINT);

  CheckMath3<int8_t>(RANDOM_DECIMAL(), 0, type::Type::TINYINT);
  CheckMath3<int16_t>(RANDOM_DECIMAL(), 0, type::Type::SMALLINT);
  CheckMath3<int32_t>(RANDOM_DECIMAL(), 0, type::Type::INTEGER);
  CheckMath3<int64_t>(RANDOM_DECIMAL(), 0, type::Type::BIGINT);
  CheckMath4(RANDOM_DECIMAL(), 0);
}

TEST_F(NumericValueTests, NullValueTest) {
  std::srand(SEED);
  type::Value result[5];

  // Compare null
  result[0] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  // Operate null
  result[0] = type::ValueFactory::GetIntegerValue(rand()).Add(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).Add(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).Add(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).Add(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).Add(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetIntegerValue(rand()).Subtract(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).Subtract(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).Subtract(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).Subtract(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).Subtract(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }



  result[0] = type::ValueFactory::GetIntegerValue(rand()).Multiply(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).Multiply(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).Multiply(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).Multiply(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).Multiply(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetIntegerValue(rand()).Divide(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).Divide(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).Divide(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).Divide(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).Divide(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetIntegerValue(rand()).Modulo(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  result[1] = type::ValueFactory::GetIntegerValue(rand()).Modulo(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  result[2] = type::ValueFactory::GetIntegerValue(rand()).Modulo(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  result[3] = type::ValueFactory::GetIntegerValue(rand()).Modulo(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  result[4] = type::ValueFactory::GetIntegerValue(rand()).Modulo(
    type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Add(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Add(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Add(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Add(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Add(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Subtract(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Subtract(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Subtract(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Subtract(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Subtract(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Multiply(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Multiply(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Multiply(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Multiply(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Multiply(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Divide(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Divide(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Divide(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Divide(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Divide(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }
  
  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }


  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Sqrt();
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Sqrt();
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Sqrt();
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Sqrt();
  result[4] = type::ValueFactory::GetDoubleValue((double)type::PELOTON_DECIMAL_NULL).Sqrt();
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }
}

}
}
