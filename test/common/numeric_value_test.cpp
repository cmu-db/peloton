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
#include "common/boolean_type.h"
#include "common/decimal_type.h"
#include "common/numeric_type.h"
#include "common/varlen_type.h"
#include "common/harness.h"
#include "common/value_factory.h"

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

void CheckEqual(Value &v1, Value &v2) {
  Value result[6];
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

void CheckLessThan(Value &v1, Value &v2) {
  Value result[6];
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

void CheckGreaterThan(Value &v1, Value &v2) {
  Value result[6];
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
void CheckCompare1(T1 x, T2 y, Type::TypeId xtype, Type::TypeId ytype) {
  Value v1 = Value(xtype, x);
  Value v2 = Value(ytype, y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare integer and decimal
template<class T>
void CheckCompare2(T x, double y, Type::TypeId xtype) {
  Value v1 = Value(xtype, x);
  Value v2 = ValueFactory::GetDoubleValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare decimal and integer
template<class T>
void CheckCompare3(double x, T y, Type::TypeId ytype ) {
  Value v1 = ValueFactory::GetDoubleValue(x);
  Value v2 = Value(ytype, y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare two decimals
void CheckCompare4(double x, double y) {
  Value v1 = ValueFactory::GetDoubleValue(x);
  Value v2 = ValueFactory::GetDoubleValue(y);
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
void CheckMath1(T1 x, T2 y, Type::TypeId xtype, Type::TypeId ytype) {
  Type::TypeId maxtype = xtype > ytype? xtype : ytype;
  Value v1;
  Value v2;
  // Test x + y
  v2 = Value(maxtype, x + y);
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);
  // Out of range detection
  if ((x + y) != sum1 && (x + y) != sum2)
    EXPECT_THROW(Value(xtype, x).Add(Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0))
      EXPECT_THROW(Value(xtype, x).Add(Value(ytype, y)),
        peloton::Exception);
  }
  else if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0))
    EXPECT_THROW(Value(xtype, x).Add(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Add(Value(ytype, y));
    CheckEqual(v1, v2);
  }
  // Test x - y
  v2 = Value(maxtype, x - y);
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  // Out of range detection
  if ((x - y) != diff1 && (x - y) != diff2)
    EXPECT_THROW(Value(xtype, x).Subtract(Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0))
      EXPECT_THROW(Value(xtype, x).Subtract(Value(ytype, y)),
        peloton::Exception);
  }
  else if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0))
    EXPECT_THROW(Value(xtype, x).Subtract(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Subtract(Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x * y
  v2 = Value(maxtype, x * y);
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  // Out of range detection
  if ((x * y) != prod1 && (x * y) != prod2)
    EXPECT_THROW(Value(xtype, x).Multiply(Value(ytype, y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if (y != 0 && prod1 / y != x)
      EXPECT_THROW(Value(xtype, x).Multiply(Value(ytype, y)),
        peloton::Exception);
  }
  else if (y != 0 && prod2 / y != x)
    EXPECT_THROW(Value(xtype, x).Multiply(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Multiply(Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x / y
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(Value(xtype, x).Divide(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Divide(Value(ytype, y));
    v2 = Value(maxtype, x / y);
    CheckEqual(v1, v2);
  }

  // Test x % y
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(Value(xtype, x).Modulo(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Modulo(Value(ytype, y));
    v2 = Value(maxtype, x % y);
    CheckEqual(v1, v2);
  }

  // Test sqrt(x)
  if (x < 0)
    EXPECT_THROW(Value(xtype, x).Sqrt(),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Sqrt();
    v2 = ValueFactory::GetDoubleValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

// Check the operations of an integer and a decimal
template<class T>
void CheckMath2(T x, double y, Type::TypeId xtype) {
  Value v1, v2;
  v1 = Value(xtype, x).Add(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = Value(xtype, x).Subtract(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = Value(xtype, x).Multiply(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(Value(xtype, x).Divide(ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Divide(ValueFactory::GetDoubleValue(y));
    v2 = ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(Value(xtype, x).Modulo(ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = Value(xtype, x).Modulo(ValueFactory::GetDoubleValue(y));
    v2 = ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of a decimal and an integer
template<class T>
void CheckMath3(double x, T y, Type::TypeId ytype) {
  Value v1, v2;

  v1 = ValueFactory::GetDoubleValue(x).Add(Value(ytype, y));
  v2 = ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = ValueFactory::GetDoubleValue(x).Subtract(Value(ytype, y));
  v2 = ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = ValueFactory::GetDoubleValue(x).Multiply(Value(ytype, y));
  v2 = ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(ValueFactory::GetDoubleValue(x).Divide(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = ValueFactory::GetDoubleValue(x).Divide(Value(ytype, y));
    v2 = ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(ValueFactory::GetDoubleValue(x).Modulo(Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = ValueFactory::GetDoubleValue(x).Modulo(Value(ytype, y));
    v2 = ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of two decimals
void CheckMath4(double x, double y) {
  Value v1, v2;

  v1 = ValueFactory::GetDoubleValue(x).Add(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x + y);
  CheckEqual(v1, v2);

  v1 = ValueFactory::GetDoubleValue(x).Subtract(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x - y);
  CheckEqual(v1, v2);

  v1 = ValueFactory::GetDoubleValue(x).Multiply(ValueFactory::GetDoubleValue(y));
  v2 = ValueFactory::GetDoubleValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(ValueFactory::GetDoubleValue(x).Divide(ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = ValueFactory::GetDoubleValue(x).Divide(ValueFactory::GetDoubleValue(y));
    v2 = ValueFactory::GetDoubleValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(ValueFactory::GetDoubleValue(x).Modulo(ValueFactory::GetDoubleValue(y)),
      peloton::Exception);
  else {
    v1 = ValueFactory::GetDoubleValue(x).Modulo(ValueFactory::GetDoubleValue(y));
    v2 = ValueFactory::GetDoubleValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
  
  if (x < 0)
   EXPECT_THROW(ValueFactory::GetDoubleValue(x).Sqrt(),
      peloton::Exception);
  else {
    v1 = ValueFactory::GetDoubleValue(x).Sqrt();
    v2 = ValueFactory::GetDoubleValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

TEST_F(NumericValueTests, ComparisonTest) {
  std::srand(SEED);

  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int8_t, int8_t>(RANDOM8(), RANDOM8(), Type::TINYINT, Type::TINYINT);
    CheckCompare1<int8_t, int16_t>(RANDOM8(), RANDOM16(), Type::TINYINT, Type::SMALLINT);
    CheckCompare1<int8_t, int32_t>(RANDOM8(), RANDOM32(), Type::TINYINT, Type::INTEGER);
    CheckCompare1<int8_t, int64_t>(RANDOM8(), RANDOM64(), Type::TINYINT, Type::BIGINT);
    CheckCompare2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), Type::TINYINT);

    CheckCompare1<int16_t, int8_t>(RANDOM16(), RANDOM8(), Type::SMALLINT, Type::TINYINT);
    CheckCompare1<int16_t, int16_t>(RANDOM16(), RANDOM16(), Type::SMALLINT, Type::SMALLINT);
    CheckCompare1<int16_t, int32_t>(RANDOM16(), RANDOM32(), Type::SMALLINT, Type::INTEGER);
    CheckCompare1<int16_t, int64_t>(RANDOM16(), RANDOM64(), Type::SMALLINT, Type::BIGINT);
    CheckCompare2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), Type::SMALLINT);

    CheckCompare1<int32_t, int8_t>(RANDOM32(), RANDOM8(), Type::INTEGER, Type::TINYINT);
    CheckCompare1<int32_t, int16_t>(RANDOM32(), RANDOM16(), Type::INTEGER, Type::SMALLINT);
    CheckCompare1<int32_t, int32_t>(RANDOM32(), RANDOM32(), Type::INTEGER, Type::INTEGER);
    CheckCompare1<int32_t, int64_t>(RANDOM32(), RANDOM64(), Type::INTEGER, Type::BIGINT);
    CheckCompare2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), Type::INTEGER);

    CheckCompare1<int64_t, int8_t>(RANDOM64(), RANDOM8(), Type::BIGINT, Type::TINYINT);
    CheckCompare1<int64_t, int16_t>(RANDOM64(), RANDOM16(), Type::BIGINT, Type::SMALLINT);
    CheckCompare1<int64_t, int32_t>(RANDOM64(), RANDOM32(), Type::BIGINT, Type::INTEGER);
    CheckCompare1<int64_t, int64_t>(RANDOM64(), RANDOM64(), Type::BIGINT, Type::BIGINT);
    CheckCompare2<int64_t>(RANDOM64(), RANDOM_DECIMAL(),Type::BIGINT);

    CheckCompare3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), Type::TINYINT);
    CheckCompare3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), Type::SMALLINT);
    CheckCompare3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), Type::INTEGER);
    CheckCompare3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), Type::BIGINT);
    CheckCompare4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTests, MathTest) {
  std::srand(SEED);

  // Generate two values v1 and v2
  // Check Value(v1) op Value(v2) == Value(v1 op v2);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckMath1<int8_t, int8_t>(RANDOM8(), RANDOM8(), Type::TINYINT, Type::TINYINT);
    CheckMath1<int8_t, int16_t>(RANDOM8(), RANDOM16(), Type::TINYINT, Type::SMALLINT);
    CheckMath1<int8_t, int32_t>(RANDOM8(), RANDOM32(), Type::TINYINT, Type::INTEGER);
    CheckMath1<int8_t, int64_t>(RANDOM8(), RANDOM64(), Type::TINYINT, Type::BIGINT);
    CheckMath2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), Type::TINYINT);

    CheckMath1<int16_t, int8_t>(RANDOM16(), RANDOM8(), Type::SMALLINT, Type::TINYINT);
    CheckMath1<int16_t, int16_t>(RANDOM16(), RANDOM16(), Type::SMALLINT, Type::SMALLINT);
    CheckMath1<int16_t, int32_t>(RANDOM16(), RANDOM32(), Type::SMALLINT, Type::INTEGER);
    CheckMath1<int16_t, int64_t>(RANDOM16(), RANDOM64(), Type::SMALLINT, Type::BIGINT);
    CheckMath2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), Type::SMALLINT);

    CheckMath1<int32_t, int8_t>(RANDOM32(), RANDOM8(), Type::INTEGER, Type::TINYINT);
    CheckMath1<int32_t, int16_t>(RANDOM32(), RANDOM16(), Type::INTEGER, Type::SMALLINT);
    CheckMath1<int32_t, int32_t>(RANDOM32(), RANDOM32(), Type::INTEGER, Type::INTEGER);
    CheckMath1<int32_t, int64_t>(RANDOM32(), RANDOM64(), Type::BIGINT, Type::BIGINT);
    CheckMath2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), Type::INTEGER);

    CheckMath1<int64_t, int8_t>(RANDOM64(), RANDOM8(), Type::BIGINT, Type::TINYINT);
    CheckMath1<int64_t, int16_t>(RANDOM64(), RANDOM16(), Type::BIGINT, Type::SMALLINT);
    CheckMath1<int64_t, int32_t>(RANDOM64(), RANDOM32(), Type::BIGINT, Type::INTEGER);
    CheckMath1<int64_t, int64_t>(RANDOM64(), RANDOM64(), Type::BIGINT, Type::BIGINT);
    CheckMath2<int64_t>(RANDOM64(), RANDOM_DECIMAL(), Type::BIGINT);

    CheckMath3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), Type::TINYINT);
    CheckMath3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), Type::SMALLINT);
    CheckMath3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), Type::INTEGER);
    CheckMath3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), Type::BIGINT);
    CheckMath4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTests, DivideByZeroTest) {
  std::srand(SEED);

  CheckMath1<int8_t, int8_t>(RANDOM8(), 0, Type::TINYINT, Type::TINYINT);
  CheckMath1<int8_t, int16_t>(RANDOM8(), 0, Type::TINYINT, Type::SMALLINT);
  CheckMath1<int8_t, int32_t>(RANDOM8(), 0, Type::TINYINT, Type::INTEGER);
  CheckMath1<int8_t, int64_t>(RANDOM8(), 0, Type::TINYINT, Type::BIGINT);
  CheckMath2<int8_t>(RANDOM8(), 0, Type::TINYINT);

  CheckMath1<int16_t, int8_t>(RANDOM16(), 0, Type::SMALLINT, Type::TINYINT);
  CheckMath1<int16_t, int16_t>(RANDOM16(), 0, Type::SMALLINT, Type::SMALLINT);
  CheckMath1<int16_t, int32_t>(RANDOM16(), 0, Type::SMALLINT, Type::INTEGER);
  CheckMath1<int16_t, int64_t>(RANDOM16(), 0, Type::SMALLINT, Type::BIGINT);
  CheckMath2<int16_t>(RANDOM16(), 0, Type::SMALLINT);

  CheckMath1<int32_t, int8_t>(RANDOM32(), 0, Type::INTEGER, Type::TINYINT);
  CheckMath1<int32_t, int16_t>(RANDOM32(), 0, Type::INTEGER, Type::SMALLINT);
  CheckMath1<int32_t, int32_t>(RANDOM32(), 0, Type::INTEGER, Type::INTEGER);
  CheckMath1<int32_t, int64_t>(RANDOM32(), 0, Type::BIGINT, Type::BIGINT);
  CheckMath2<int32_t>(RANDOM32(), 0, Type::INTEGER);

  CheckMath1<int64_t, int8_t>(RANDOM64(), 0, Type::BIGINT, Type::TINYINT);
  CheckMath1<int64_t, int16_t>(RANDOM64(), 0, Type::BIGINT, Type::SMALLINT);
  CheckMath1<int64_t, int32_t>(RANDOM64(), 0, Type::BIGINT, Type::INTEGER);
  CheckMath1<int64_t, int64_t>(RANDOM64(), 0, Type::BIGINT, Type::BIGINT);
  CheckMath2<int64_t>(RANDOM64(), 0, Type::BIGINT);

  CheckMath3<int8_t>(RANDOM_DECIMAL(), 0, Type::TINYINT);
  CheckMath3<int16_t>(RANDOM_DECIMAL(), 0, Type::SMALLINT);
  CheckMath3<int32_t>(RANDOM_DECIMAL(), 0, Type::INTEGER);
  CheckMath3<int64_t>(RANDOM_DECIMAL(), 0, Type::BIGINT);
  CheckMath4(RANDOM_DECIMAL(), 0);
}

TEST_F(NumericValueTests, NullValueTest) {
  std::srand(SEED);
  Value result[5];

  // Compare null
  result[0] = ValueFactory::GetIntegerValue(rand()).CompareEquals(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).CompareEquals(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).CompareEquals(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).CompareEquals(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).CompareEquals(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).CompareEquals(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).CompareEquals(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).CompareEquals(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).CompareEquals(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).CompareEquals(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  // Operate null
  result[0] = ValueFactory::GetIntegerValue(rand()).Add(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).Add(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).Add(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).Add(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).Add(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetIntegerValue(rand()).Subtract(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).Subtract(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).Subtract(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).Subtract(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).Subtract(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }



  result[0] = ValueFactory::GetIntegerValue(rand()).Multiply(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).Multiply(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).Multiply(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).Multiply(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).Multiply(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetIntegerValue(rand()).Divide(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).Divide(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).Divide(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).Divide(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).Divide(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetIntegerValue(rand()).Modulo(
    ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL));
  result[1] = ValueFactory::GetIntegerValue(rand()).Modulo(
    ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL));
  result[2] = ValueFactory::GetIntegerValue(rand()).Modulo(
    ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = ValueFactory::GetIntegerValue(rand()).Modulo(
    ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL));
  result[4] = ValueFactory::GetIntegerValue(rand()).Modulo(
    ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Add(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Add(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Add(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Add(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Add(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Subtract(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Subtract(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Subtract(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Subtract(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Subtract(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Multiply(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Multiply(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Multiply(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Multiply(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Multiply(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }

  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Divide(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Divide(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Divide(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Divide(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Divide(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }
  
  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Modulo(
    ValueFactory::GetIntegerValue(rand()));
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Modulo(
    ValueFactory::GetIntegerValue(rand()));
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Modulo(
    ValueFactory::GetIntegerValue(rand()));
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Modulo(
    ValueFactory::GetIntegerValue(rand()));
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Modulo(
    ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }


  result[0] = ValueFactory::GetTinyIntValue((int8_t)PELOTON_INT8_NULL).Sqrt();
  result[1] = ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL).Sqrt();
  result[2] = ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL).Sqrt();
  result[3] = ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL).Sqrt();
  result[4] = ValueFactory::GetDoubleValue((double)PELOTON_DECIMAL_NULL).Sqrt();
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }
}

}
}
