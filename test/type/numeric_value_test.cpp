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
#include <cmath>

#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/varlen_type.h"
#include "common/harness.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

class NumericValueTests : public PelotonTest {};

#define RANDOM_DECIMAL() ((double)rand() / (double)rand())
#define SEED 233
#define TEST_NUM 100

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
  CmpBool result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[0]);
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_EQ(CmpBool::FALSE, result[1]);
  result[2] = v1.CompareLessThan(v2);
  EXPECT_EQ(CmpBool::FALSE, result[2]);
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[3]);
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_EQ(CmpBool::FALSE, result[4]);
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[5]);
}

void CheckLessThan(type::Value &v1, type::Value &v2) {
  CmpBool result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_EQ(CmpBool::FALSE, result[0]);
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[1]);
  result[2] = v1.CompareLessThan(v2);
  EXPECT_EQ(CmpBool::TRUE, result[2]);
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[3]);
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_EQ(CmpBool::FALSE, result[4]);
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_EQ(CmpBool::FALSE, result[5]);
}

void CheckGreaterThan(type::Value &v1, type::Value &v2) {
  CmpBool result[6];
  result[0] = v1.CompareEquals(v2);
  EXPECT_EQ(CmpBool::FALSE, result[0]);
  result[1] = v1.CompareNotEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[1]);
  result[2] = v1.CompareLessThan(v2);
  EXPECT_EQ(CmpBool::FALSE, result[2]);
  result[3] = v1.CompareLessThanEquals(v2);
  EXPECT_EQ(CmpBool::FALSE, result[3]);
  result[4] = v1.CompareGreaterThan(v2);
  EXPECT_EQ(CmpBool::TRUE, result[4]);
  result[5] = v1.CompareGreaterThanEquals(v2);
  EXPECT_EQ(CmpBool::TRUE, result[5]);
}

// Compare two integers
template<class T1, class T2>
void CheckCompare1(T1 x, T2 y, type::TypeId xtype, type::TypeId ytype) {
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
void CheckCompare2(T x, double y, type::TypeId xtype) {
  type::Value v1 = type::Value(xtype, x);
  type::Value v2 = type::ValueFactory::GetDecimalValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare decimal and integer
template<class T>
void CheckCompare3(double x, T y, type::TypeId ytype) {
  type::Value v1 = type::ValueFactory::GetDecimalValue(x);
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
  type::Value v1 = type::ValueFactory::GetDecimalValue(x);
  type::Value v2 = type::ValueFactory::GetDecimalValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
}

// Compare number with varchar
template<class T>
void CheckCompare5(T x, T y, type::TypeId xtype) {
  type::Value v1 = type::Value(xtype, x);
  type::Value v2 = type::ValueFactory::GetVarcharValue(type::Value(xtype, y).ToString());
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
void CheckMath1(T1 x, T2 y, type::TypeId xtype, type::TypeId ytype) {
  type::TypeId maxtype = xtype > ytype? xtype : ytype;
  type::Value v1;
  type::Value v2;
  // Test x + y
  v2 = type::Value(maxtype, x + y);
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);
  // Out of range detection
  if ((x + y) != sum1 && (x + y) != sum2) {
    EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0)) {
      EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
                   peloton::Exception);
    }
  }
  else if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0)) {
    EXPECT_THROW(type::Value(xtype, x).Add(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Add(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }
  // Test x - y
  v2 = type::Value(maxtype, x - y);
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  // Out of range detection
  if ((x - y) != diff1 && (x - y) != diff2) {
    EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0)) {
      EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
                   peloton::Exception);
    }
  }
  else if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0)) {
    EXPECT_THROW(type::Value(xtype, x).Subtract(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Subtract(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x * y
  v2 = type::Value(maxtype, x * y);
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  // Out of range detection
  if ((x * y) != prod1 && (x * y) != prod2) {
    EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else if (sizeof(x) >= sizeof(y)) {
    if (y != 0 && prod1 / y != x) {
      EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
                   peloton::Exception);
    }
  }
  else if (y != 0 && prod2 / y != x) {
    EXPECT_THROW(type::Value(xtype, x).Multiply(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Multiply(type::Value(ytype, y));
    CheckEqual(v1, v2);
  }

  // Test x / y
  // Divide by zero detection
  if (y == 0) {
    EXPECT_THROW(type::Value(xtype, x).Divide(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Divide(type::Value(ytype, y));
    v2 = type::Value(maxtype, x / y);
    CheckEqual(v1, v2);
  }

  // Test x % y
  // Divide by zero detection
  if (y == 0) {
    EXPECT_THROW(type::Value(xtype, x).Modulo(type::Value(ytype, y)),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Modulo(type::Value(ytype, y));
    v2 = type::Value(maxtype, x % y);
    CheckEqual(v1, v2);
  }

  // Test sqrt(x)
  if (x < 0) {
    EXPECT_THROW(type::Value(xtype, x).Sqrt(),
                 peloton::Exception);
  }
  else {
    v1 = type::Value(xtype, x).Sqrt();
    v2 = type::ValueFactory::GetDecimalValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

// Check the operations of an integer and a decimal
template<class T>
void CheckMath2(T x, double y, type::TypeId xtype) {
  type::Value v1, v2;
  v1 = type::Value(xtype, x).Add(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::Value(xtype, x).Subtract(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::Value(xtype, x).Multiply(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Divide(type::ValueFactory::GetDecimalValue(y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Divide(type::ValueFactory::GetDecimalValue(y));
    v2 = type::ValueFactory::GetDecimalValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(type::Value(xtype, x).Modulo(type::ValueFactory::GetDecimalValue(y)),
      peloton::Exception);
  else {
    v1 = type::Value(xtype, x).Modulo(type::ValueFactory::GetDecimalValue(y));
    v2 = type::ValueFactory::GetDecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of a decimal and an integer
template<class T>
void CheckMath3(double x, T y, type::TypeId ytype) {
  type::Value v1, v2;

  v1 = type::ValueFactory::GetDecimalValue(x).Add(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDecimalValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDecimalValue(x).Subtract(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDecimalValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDecimalValue(x).Multiply(type::Value(ytype, y));
  v2 = type::ValueFactory::GetDecimalValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDecimalValue(x).Divide(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDecimalValue(x).Divide(type::Value(ytype, y));
    v2 = type::ValueFactory::GetDecimalValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDecimalValue(x).Modulo(type::Value(ytype, y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDecimalValue(x).Modulo(type::Value(ytype, y));
    v2 = type::ValueFactory::GetDecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
}

// Check the operations of two decimals
void CheckMath4(double x, double y) {
  type::Value v1, v2;

  v1 = type::ValueFactory::GetDecimalValue(x).Add(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x + y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDecimalValue(x).Subtract(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x - y);
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetDecimalValue(x).Multiply(type::ValueFactory::GetDecimalValue(y));
  v2 = type::ValueFactory::GetDecimalValue(x * y);
  CheckEqual(v1, v2);

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDecimalValue(x).Divide(type::ValueFactory::GetDecimalValue(y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDecimalValue(x).Divide(type::ValueFactory::GetDecimalValue(y));
    v2 = type::ValueFactory::GetDecimalValue(x / y);
    CheckEqual(v1, v2);
  }

  // Divide by zero detection
  if (y == 0)
   EXPECT_THROW(type::ValueFactory::GetDecimalValue(x).Modulo(type::ValueFactory::GetDecimalValue(y)),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDecimalValue(x).Modulo(type::ValueFactory::GetDecimalValue(y));
    v2 = type::ValueFactory::GetDecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
  }
  
  if (x < 0)
   EXPECT_THROW(type::ValueFactory::GetDecimalValue(x).Sqrt(),
      peloton::Exception);
  else {
    v1 = type::ValueFactory::GetDecimalValue(x).Sqrt();
    v2 = type::ValueFactory::GetDecimalValue(sqrt(x));
    CheckEqual(v1, v2);
  }
}

TEST_F(NumericValueTests, TinyIntComparisonTest) {
  std::srand(SEED);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int8_t, int8_t>(RANDOM8(), RANDOM8(), type::TypeId::TINYINT, type::TypeId::TINYINT);
    CheckCompare1<int8_t, int16_t>(RANDOM8(), RANDOM16(), type::TypeId::TINYINT, type::TypeId::SMALLINT);
    CheckCompare1<int8_t, int32_t>(RANDOM8(), RANDOM32(), type::TypeId::TINYINT, type::TypeId::INTEGER);
    CheckCompare1<int8_t, int64_t>(RANDOM8(), RANDOM64(), type::TypeId::TINYINT, type::TypeId::BIGINT);
    CheckCompare2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), type::TypeId::TINYINT);
    CheckCompare3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), type::TypeId::TINYINT);

    int8_t v0 = RANDOM8();
    int8_t v1 = v0 + 1;
    int8_t v2 = v0 - 1;
    CheckCompare5<int8_t>(v0, v0, type::TypeId::TINYINT);
    CheckCompare5<int8_t>(v0, v1, type::TypeId::TINYINT);
    CheckCompare5<int8_t>(v0, v2, type::TypeId::TINYINT);
  }
}

TEST_F(NumericValueTests, SmallIntComparisonTest) {
  std::srand(SEED);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int16_t, int8_t>(RANDOM16(), RANDOM8(), type::TypeId::SMALLINT, type::TypeId::TINYINT);
    CheckCompare1<int16_t, int16_t>(RANDOM16(), RANDOM16(), type::TypeId::SMALLINT, type::TypeId::SMALLINT);
    CheckCompare1<int16_t, int32_t>(RANDOM16(), RANDOM32(), type::TypeId::SMALLINT, type::TypeId::INTEGER);
    CheckCompare1<int16_t, int64_t>(RANDOM16(), RANDOM64(), type::TypeId::SMALLINT, type::TypeId::BIGINT);
    CheckCompare2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), type::TypeId::SMALLINT);
    CheckCompare3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), type::TypeId::SMALLINT);

    int16_t v0 = RANDOM16();
    int16_t v1 = v0 + 1;
    int16_t v2 = v0 - 1;
    CheckCompare5<int16_t>(v0, v0, type::TypeId::SMALLINT);
    CheckCompare5<int16_t>(v0, v1, type::TypeId::SMALLINT);
    CheckCompare5<int16_t>(v0, v2, type::TypeId::SMALLINT);
  }
}

TEST_F(NumericValueTests, IntComparisonTest) {
  std::srand(SEED);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int32_t, int8_t>(RANDOM32(), RANDOM8(), type::TypeId::INTEGER, type::TypeId::TINYINT);
    CheckCompare1<int32_t, int16_t>(RANDOM32(), RANDOM16(), type::TypeId::INTEGER, type::TypeId::SMALLINT);
    CheckCompare1<int32_t, int32_t>(RANDOM32(), RANDOM32(), type::TypeId::INTEGER, type::TypeId::INTEGER);
    CheckCompare1<int32_t, int64_t>(RANDOM32(), RANDOM64(), type::TypeId::INTEGER, type::TypeId::BIGINT);
    CheckCompare2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), type::TypeId::INTEGER);
    CheckCompare3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), type::TypeId::INTEGER);

    int32_t v0 = RANDOM32();
    int32_t v1 = v0 + 1;
    int32_t v2 = v0 - 1;
    CheckCompare5<int32_t>(v0, v0, type::TypeId::INTEGER);
    CheckCompare5<int32_t>(v0, v1, type::TypeId::INTEGER);
    CheckCompare5<int32_t>(v0, v2, type::TypeId::INTEGER);
  }
}

TEST_F(NumericValueTests, BigIntComparisonTest) {
  std::srand(SEED);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int64_t, int8_t>(RANDOM64(), RANDOM8(), type::TypeId::BIGINT, type::TypeId::TINYINT);
    CheckCompare1<int64_t, int16_t>(RANDOM64(), RANDOM16(), type::TypeId::BIGINT, type::TypeId::SMALLINT);
    CheckCompare1<int64_t, int32_t>(RANDOM64(), RANDOM32(), type::TypeId::BIGINT, type::TypeId::INTEGER);
    CheckCompare1<int64_t, int64_t>(RANDOM64(), RANDOM64(), type::TypeId::BIGINT, type::TypeId::BIGINT);
    CheckCompare2<int64_t>(RANDOM64(), RANDOM_DECIMAL(), type::TypeId::BIGINT);
    CheckCompare3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), type::TypeId::BIGINT);
    CheckCompare4(RANDOM_DECIMAL(), RANDOM_DECIMAL());

    int64_t v0 = RANDOM64();
    int64_t v1 = v0 + 1;
    int64_t v2 = v0 - 1;
    CheckCompare5<int64_t>(v0, v0, type::TypeId::BIGINT);
    CheckCompare5<int64_t>(v0, v1, type::TypeId::BIGINT);
    CheckCompare5<int64_t>(v0, v2, type::TypeId::BIGINT);
  }
}

TEST_F(NumericValueTests, MathTest) {
  std::srand(SEED);

  // Generate two values v1 and v2
  // Check type::Value(v1) op type::Value(v2) == type::Value(v1 op v2);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckMath1<int8_t, int8_t>(RANDOM8(), RANDOM8(), type::TypeId::TINYINT, type::TypeId::TINYINT);
    CheckMath1<int8_t, int16_t>(RANDOM8(), RANDOM16(), type::TypeId::TINYINT, type::TypeId::SMALLINT);
    CheckMath1<int8_t, int32_t>(RANDOM8(), RANDOM32(), type::TypeId::TINYINT, type::TypeId::INTEGER);
    CheckMath1<int8_t, int64_t>(RANDOM8(), RANDOM64(), type::TypeId::TINYINT, type::TypeId::BIGINT);
    CheckMath2<int8_t>(RANDOM8(), RANDOM_DECIMAL(), type::TypeId::TINYINT);

    CheckMath1<int16_t, int8_t>(RANDOM16(), RANDOM8(), type::TypeId::SMALLINT, type::TypeId::TINYINT);
    CheckMath1<int16_t, int16_t>(RANDOM16(), RANDOM16(), type::TypeId::SMALLINT, type::TypeId::SMALLINT);
    CheckMath1<int16_t, int32_t>(RANDOM16(), RANDOM32(), type::TypeId::SMALLINT, type::TypeId::INTEGER);
    CheckMath1<int16_t, int64_t>(RANDOM16(), RANDOM64(), type::TypeId::SMALLINT, type::TypeId::BIGINT);
    CheckMath2<int16_t>(RANDOM16(), RANDOM_DECIMAL(), type::TypeId::SMALLINT);

    CheckMath1<int32_t, int8_t>(RANDOM32(), RANDOM8(), type::TypeId::INTEGER, type::TypeId::TINYINT);
    CheckMath1<int32_t, int16_t>(RANDOM32(), RANDOM16(), type::TypeId::INTEGER, type::TypeId::SMALLINT);
    CheckMath1<int32_t, int32_t>(RANDOM32(), RANDOM32(), type::TypeId::INTEGER, type::TypeId::INTEGER);
    CheckMath1<int32_t, int64_t>(RANDOM32(), RANDOM64(), type::TypeId::BIGINT, type::TypeId::BIGINT);
    CheckMath2<int32_t>(RANDOM32(), RANDOM_DECIMAL(), type::TypeId::INTEGER);

    CheckMath1<int64_t, int8_t>(RANDOM64(), RANDOM8(), type::TypeId::BIGINT, type::TypeId::TINYINT);
    CheckMath1<int64_t, int16_t>(RANDOM64(), RANDOM16(), type::TypeId::BIGINT, type::TypeId::SMALLINT);
    CheckMath1<int64_t, int32_t>(RANDOM64(), RANDOM32(), type::TypeId::BIGINT, type::TypeId::INTEGER);
    CheckMath1<int64_t, int64_t>(RANDOM64(), RANDOM64(), type::TypeId::BIGINT, type::TypeId::BIGINT);
    CheckMath2<int64_t>(RANDOM64(), RANDOM_DECIMAL(), type::TypeId::BIGINT);

    CheckMath3<int8_t>(RANDOM_DECIMAL(), RANDOM8(), type::TypeId::TINYINT);
    CheckMath3<int16_t>(RANDOM_DECIMAL(), RANDOM16(), type::TypeId::SMALLINT);
    CheckMath3<int32_t>(RANDOM_DECIMAL(), RANDOM32(), type::TypeId::INTEGER);
    CheckMath3<int64_t>(RANDOM_DECIMAL(), RANDOM64(), type::TypeId::BIGINT);
    CheckMath4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTests, IsZeroTest) {
  type::Value v1, v2;

  v1 = type::ValueFactory::GetTinyIntValue(0);
  v2 = type::ValueFactory::GetZeroValueByType(type::TypeId::TINYINT);
  EXPECT_TRUE(v1.IsZero());
  EXPECT_FALSE(v1.IsNull());
  EXPECT_TRUE(v2.IsZero());
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetSmallIntValue(0);
  v2 = type::ValueFactory::GetZeroValueByType(type::TypeId::SMALLINT);
  EXPECT_TRUE(v1.IsZero());
  EXPECT_FALSE(v1.IsNull());
  EXPECT_TRUE(v2.IsZero());
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetIntegerValue(0);
  v2 = type::ValueFactory::GetZeroValueByType(type::TypeId::INTEGER);
  EXPECT_TRUE(v1.IsZero());
  EXPECT_FALSE(v1.IsNull());
  EXPECT_TRUE(v2.IsZero());
  CheckEqual(v1, v2);

  v1 = type::ValueFactory::GetBigIntValue(0);
  v2 = type::ValueFactory::GetZeroValueByType(type::TypeId::BIGINT);
  EXPECT_TRUE(v1.IsZero());
  EXPECT_FALSE(v1.IsNull());
  EXPECT_TRUE(v2.IsZero());
  CheckEqual(v1, v2);
}

TEST_F(NumericValueTests, SqrtTest) {

  for (int i = 1; i <= 10; i++) {
    type::Value v1, v2;

    v1 = type::ValueFactory::GetTinyIntValue(i * i).Sqrt();
    v2 = type::ValueFactory::GetTinyIntValue(i);
    CheckEqual(v1, v2);

    v1 = type::ValueFactory::GetSmallIntValue(i * i).Sqrt();
    v2 = type::ValueFactory::GetSmallIntValue(i);
    CheckEqual(v1, v2);

    v1 = type::ValueFactory::GetIntegerValue(i * i).Sqrt();
    v2 = type::ValueFactory::GetIntegerValue(i);
    CheckEqual(v1, v2);

    v1 = type::ValueFactory::GetBigIntValue(i * i).Sqrt();
    v2 = type::ValueFactory::GetBigIntValue(i);
    CheckEqual(v1, v2);
  } // FOR
}

TEST_F(NumericValueTests, CastAsTest) {

  std::vector<type::TypeId> types = {
      type::TypeId::TINYINT,
      type::TypeId::SMALLINT,
      type::TypeId::INTEGER,
      type::TypeId::BIGINT,
      type::TypeId::DECIMAL,
      type::TypeId::VARCHAR,
  };

  for (int i = type::PELOTON_INT8_MIN; i <= type::PELOTON_INT8_MAX; i++) {
    for (auto t1 : types) {
      type::Value v1;

      switch (t1) {
        case type::TypeId::TINYINT:
          v1 = type::ValueFactory::GetTinyIntValue(i);
          break;
        case type::TypeId::SMALLINT:
          v1 = type::ValueFactory::GetSmallIntValue(i);
          break;
        case type::TypeId::INTEGER:
          v1 = type::ValueFactory::GetIntegerValue(i);
          break;
        case type::TypeId::BIGINT:
          v1 = type::ValueFactory::GetBigIntValue(i);
          break;
        case type::TypeId::DECIMAL:
          v1 = type::ValueFactory::GetDecimalValue((double)i);
          break;
        case type::TypeId::VARCHAR:
          v1 = type::ValueFactory::GetVarcharValue(
                    type::ValueFactory::GetSmallIntValue(i).ToString());
          break;
        default:
          LOG_ERROR("Unexpected type!");
          ASSERT_FALSE(true);
      }
      EXPECT_FALSE(v1.IsNull());
      for (auto t2 : types) {
        type::Value v2 = v1.CastAs(t2);
        LOG_TRACE("[%02d] %s -> %s",
                  i, TypeIdToString(t1).c_str(),
                  TypeIdToString(t2).c_str());
        CheckEqual(v1, v2);
      } // FOR
    } // FOR
  } // FOR

}

TEST_F(NumericValueTests, DivideByZeroTest) {
  std::srand(SEED);

  CheckMath1<int8_t, int8_t>(RANDOM8(), 0, type::TypeId::TINYINT, type::TypeId::TINYINT);
  CheckMath1<int8_t, int16_t>(RANDOM8(), 0, type::TypeId::TINYINT, type::TypeId::SMALLINT);
  CheckMath1<int8_t, int32_t>(RANDOM8(), 0, type::TypeId::TINYINT, type::TypeId::INTEGER);
  CheckMath1<int8_t, int64_t>(RANDOM8(), 0, type::TypeId::TINYINT, type::TypeId::BIGINT);
  CheckMath2<int8_t>(RANDOM8(), 0, type::TypeId::TINYINT);

  CheckMath1<int16_t, int8_t>(RANDOM16(), 0, type::TypeId::SMALLINT, type::TypeId::TINYINT);
  CheckMath1<int16_t, int16_t>(RANDOM16(), 0, type::TypeId::SMALLINT, type::TypeId::SMALLINT);
  CheckMath1<int16_t, int32_t>(RANDOM16(), 0, type::TypeId::SMALLINT, type::TypeId::INTEGER);
  CheckMath1<int16_t, int64_t>(RANDOM16(), 0, type::TypeId::SMALLINT, type::TypeId::BIGINT);
  CheckMath2<int16_t>(RANDOM16(), 0, type::TypeId::SMALLINT);

  CheckMath1<int32_t, int8_t>(RANDOM32(), 0, type::TypeId::INTEGER, type::TypeId::TINYINT);
  CheckMath1<int32_t, int16_t>(RANDOM32(), 0, type::TypeId::INTEGER, type::TypeId::SMALLINT);
  CheckMath1<int32_t, int32_t>(RANDOM32(), 0, type::TypeId::INTEGER, type::TypeId::INTEGER);
  CheckMath1<int32_t, int64_t>(RANDOM32(), 0, type::TypeId::BIGINT, type::TypeId::BIGINT);
  CheckMath2<int32_t>(RANDOM32(), 0, type::TypeId::INTEGER);

  CheckMath1<int64_t, int8_t>(RANDOM64(), 0, type::TypeId::BIGINT, type::TypeId::TINYINT);
  CheckMath1<int64_t, int16_t>(RANDOM64(), 0, type::TypeId::BIGINT, type::TypeId::SMALLINT);
  CheckMath1<int64_t, int32_t>(RANDOM64(), 0, type::TypeId::BIGINT, type::TypeId::INTEGER);
  CheckMath1<int64_t, int64_t>(RANDOM64(), 0, type::TypeId::BIGINT, type::TypeId::BIGINT);
  CheckMath2<int64_t>(RANDOM64(), 0, type::TypeId::BIGINT);

  CheckMath3<int8_t>(RANDOM_DECIMAL(), 0, type::TypeId::TINYINT);
  CheckMath3<int16_t>(RANDOM_DECIMAL(), 0, type::TypeId::SMALLINT);
  CheckMath3<int32_t>(RANDOM_DECIMAL(), 0, type::TypeId::INTEGER);
  CheckMath3<int64_t>(RANDOM_DECIMAL(), 0, type::TypeId::BIGINT);
  CheckMath4(RANDOM_DECIMAL(), 0);
}

TEST_F(NumericValueTests, NullValueTest) {
  std::srand(SEED);
  CmpBool bool_result[5];

  // Compare null
  bool_result[0] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL));
  bool_result[1] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL));
  bool_result[2] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL));
  bool_result[3] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL));
  bool_result[4] = type::ValueFactory::GetIntegerValue(rand()).CompareEquals(
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(bool_result[i] == CmpBool::NULL_);
  }

  bool_result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  bool_result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  bool_result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  bool_result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  bool_result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).CompareEquals(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(bool_result[i] == CmpBool::NULL_);
  }

  type::Value result[5];


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
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
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
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
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
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
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
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
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
    type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL));
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
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Add(
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
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Subtract(
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
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Multiply(
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
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Divide(
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
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Modulo(
    type::ValueFactory::GetIntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }


  result[0] = type::ValueFactory::GetTinyIntValue((int8_t)type::PELOTON_INT8_NULL).Sqrt();
  result[1] = type::ValueFactory::GetSmallIntValue((int16_t)type::PELOTON_INT16_NULL).Sqrt();
  result[2] = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_NULL).Sqrt();
  result[3] = type::ValueFactory::GetBigIntValue((int64_t)type::PELOTON_INT64_NULL).Sqrt();
  result[4] = type::ValueFactory::GetDecimalValue((double)type::PELOTON_DECIMAL_NULL).Sqrt();
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i].IsNull());
  }
}

}
}
