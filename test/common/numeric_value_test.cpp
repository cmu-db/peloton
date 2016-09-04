#include <limits.h>
#include <iostream>
#include <cstdint>
#include <cmath>
#include "common/numeric_value.h"
#include "common/boolean_value.h"
#include "common/decimal_value.h"
#include "common/varlen_value.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class NumericValueTest : public PelotonTest {};

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

void CheckEqual(Value *v1, Value *v2) {
  Value *result[6] = { nullptr };
  result[0] = ((IntegerValue *)v1)->CompareEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[0])->IsTrue());
  result[1] = ((IntegerValue *)v1)->CompareNotEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[1])->IsFalse());
  result[2] = ((IntegerValue *)v1)->CompareLessThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[2])->IsFalse());
  result[3] = ((IntegerValue *)v1)->CompareLessThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[3])->IsTrue());
  result[4] = ((IntegerValue *)v1)->CompareGreaterThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[4])->IsFalse());
  result[5] = ((IntegerValue *)v1)->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[5])->IsTrue());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

void CheckLessThan(Value *v1, Value *v2) {
  Value *result[6] = { nullptr };
  result[0] = ((IntegerValue *)v1)->CompareEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[0])->IsFalse());
  result[1] = ((IntegerValue *)v1)->CompareNotEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[1])->IsTrue());
  result[2] = ((IntegerValue *)v1)->CompareLessThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[2])->IsTrue());
  result[3] = ((IntegerValue *)v1)->CompareLessThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[3])->IsTrue());
  result[4] = ((IntegerValue *)v1)->CompareGreaterThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[4])->IsFalse());
  result[5] = ((IntegerValue *)v1)->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[5])->IsFalse());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

void CheckGreaterThan(Value *v1, Value *v2) {
  Value *result[6] = { nullptr };
  result[0] = ((IntegerValue *)v1)->CompareEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[0])->IsFalse());
  result[1] = ((IntegerValue *)v1)->CompareNotEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[1])->IsTrue());
  result[2] = ((IntegerValue *)v1)->CompareLessThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[2])->IsFalse());
  result[3] = ((IntegerValue *)v1)->CompareLessThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[3])->IsFalse());
  result[4] = ((IntegerValue *)v1)->CompareGreaterThan(*v2);
  EXPECT_TRUE(((BooleanValue *)result[4])->IsTrue());
  result[5] = ((IntegerValue *)v1)->CompareGreaterThanEquals(*v2);
  EXPECT_TRUE(((BooleanValue *)result[5])->IsTrue());
  for (int i = 0; i < 6; i++)
    delete result[i];
}

// Compare two integers
template<class T1, class T2>
void CheckCompare1(T1 x, T2 y) {
  Value *v1 = new IntegerValue(x);
  Value *v2 = new IntegerValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
  delete v1;
  delete v2;
}

// Compare integer and decimal
template<class T>
void CheckCompare2(T x, double y) {
  Value *v1 = new IntegerValue(x);
  Value *v2 = new DecimalValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
  delete v1;
  delete v2;
}

// Compare decimal and integer
template<class T>
void CheckCompare3(double x, T y) {
  Value *v1 = new DecimalValue(x);
  Value *v2 = new IntegerValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
  delete v1;
  delete v2;
}

// Compare two decimals
void CheckCompare4(double x, double y) {
  Value *v1 = new DecimalValue(x);
  Value *v2 = new DecimalValue(y);
  if (x == y)
    CheckEqual(v1, v2);
  else if (x < y)
    CheckLessThan(v1, v2);
  else if (x > y)
    CheckGreaterThan(v1, v2);
  delete v1;
  delete v2;
}

// Modulo for decimals
inline double ValMod(double x, double y) {
  return x - trunc((double)x / (double)y) * y;
}

// Check the operations of two integers
template<class T1, class T2>
void CheckMath1(T1 x, T2 y) {
  Value *v1 = nullptr;
  Value *v2 = nullptr;
  // Test x + y
  printf("test x + y:\n");
  v1 = nullptr;
  v2 = new IntegerValue(x + y);
  T1 sum1 = (T1)(x + y);
  T2 sum2 = (T2)(x + y);
  // Out of range detection
  if ((x + y) != sum1 && (x + y) != sum2)
    EXPECT_THROW(IntegerValue(x).Add(IntegerValue(y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0))
      EXPECT_THROW(IntegerValue(x).Add(IntegerValue(y)),
        peloton::Exception);
  }
  else if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0))
    EXPECT_THROW(IntegerValue(x).Add(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Add(IntegerValue(y));
    CheckEqual(v1, v2);
  }
  delete v1;
  delete v2;

  // Test x - y
  printf("test x - y:\n");
  v1 = nullptr;
  v2 = new IntegerValue(x - y);
  T1 diff1 = (T1)(x - y);
  T2 diff2 = (T2)(x - y);
  // Out of range detection
  if ((x - y) != diff1 && (x - y) != diff2)
    EXPECT_THROW(IntegerValue(x).Subtract(IntegerValue(y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0))
      EXPECT_THROW(IntegerValue(x).Subtract(IntegerValue(y)),
        peloton::Exception);
  }
  else if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0))
    EXPECT_THROW(IntegerValue(x).Subtract(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Subtract(IntegerValue(y));
    CheckEqual(v1, v2);
  }
  delete v1;
  delete v2;

  // Test x * y
  printf("test x * y:\n");
  v1 = nullptr;
  v2 = new IntegerValue(x * y);
  T1 prod1 = (T1)(x * y);
  T2 prod2 = (T2)(x * y);
  // Out of range detection
  if ((x * y) != prod1 && (x * y) != prod2)
    EXPECT_THROW(IntegerValue(x).Multiply(IntegerValue(y)),
      peloton::Exception);
  else if (sizeof(x) >= sizeof(y)) {
    if (y != 0 && prod1 / y != x)
      EXPECT_THROW(IntegerValue(x).Multiply(IntegerValue(y)),
        peloton::Exception);
  }
  else if (y != 0 && prod2 / y != x)
    EXPECT_THROW(IntegerValue(x).Multiply(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Multiply(IntegerValue(y));
    CheckEqual(v1, v2);
  }
  delete v1;
  delete v2;

  // Test x / y
  printf("test x / y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(IntegerValue(x).Divide(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Divide(IntegerValue(y));
    v2 = new IntegerValue(x / y);
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;

  // Test x % y
  printf("test x mod y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(IntegerValue(x).Modulo(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Modulo(IntegerValue(y));
    v2 = new IntegerValue(x % y);
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;

  // Test sqrt(x)
  printf("test sqrt(x)\n");
  v1 = nullptr;
  if (x < 0)
    EXPECT_THROW(IntegerValue(x).Sqrt(),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Sqrt();
    v2 = new DecimalValue(sqrt(x));
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;
}

// Check the operations of an integer and a decimal
template<class T>
void CheckMath2(T x, double y) {
  Value *v1, *v2;
  printf("test x + y:\n");
  v1 = IntegerValue(x).Add(DecimalValue(y));
  v2 = new DecimalValue(x + y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x - y:\n");
  v1 = IntegerValue(x).Subtract(DecimalValue(y));
  v2 = new DecimalValue(x - y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x * y:\n");
  v1 = IntegerValue(x).Multiply(DecimalValue(y));
  v2 = new DecimalValue(x * y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x / y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(IntegerValue(x).Divide(DecimalValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Divide(DecimalValue(y));
    v2 = new DecimalValue(x / y);
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;

  printf("test x mod y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(IntegerValue(x).Modulo(DecimalValue(y)),
      peloton::Exception);
  else {
    v1 = IntegerValue(x).Modulo(DecimalValue(y));
    v2 = new DecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;
}

// Check the operations of a decimal and an integer
template<class T>
void CheckMath3(double x, T y) {
  Value *v1, *v2;

  printf("test x + y:\n");
  v1 = DecimalValue(x).Add(IntegerValue(y));
  v2 = new DecimalValue(x + y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x - y:\n");
  v1 = DecimalValue(x).Subtract(IntegerValue(y));
  v2 = new DecimalValue(x - y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x * y:\n");
  v1 = DecimalValue(x).Multiply(IntegerValue(y));
  v2 = new DecimalValue(x * y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x / y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(DecimalValue(x).Divide(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = DecimalValue(x).Divide(IntegerValue(y));
    v2 = new DecimalValue(x / y);
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;

  printf("test x mod y:\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(DecimalValue(x).Modulo(IntegerValue(y)),
      peloton::Exception);
  else {
    v1 = DecimalValue(x).Modulo(IntegerValue(y));
    v2 = new DecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;
}

// Check the operations of two decimals
void CheckMath4(double x, double y) {
  Value *v1, *v2;

  printf("test x + y:\n");
  v1 = DecimalValue(x).Add(DecimalValue(y));
  v2 = new DecimalValue(x + y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x - y:\n");
  v1 = DecimalValue(x).Subtract(DecimalValue(y));
  v2 = new DecimalValue(x - y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x * y:\n");
  v1 = DecimalValue(x).Multiply(DecimalValue(y));
  v2 = new DecimalValue(x * y);
  CheckEqual(v1, v2);
  delete v1;
  delete v2;

  printf("test x / y\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(DecimalValue(x).Divide(DecimalValue(y)),
      peloton::Exception);
  else {
    v1 = DecimalValue(x).Divide(DecimalValue(y));
    v2 = new DecimalValue(x / y);
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;

  printf("test x mod y:\n");
  v1 = nullptr;
  // Divide by zero detection
  if (y == 0)
    EXPECT_THROW(DecimalValue(x).Modulo(DecimalValue(y)),
      peloton::Exception);
  else {
    v1 = DecimalValue(x).Modulo(DecimalValue(y));
    v2 = new DecimalValue(ValMod(x, y));
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;
  
  printf("test sqrt(x)\n");
  v1 = nullptr;
  if (x < 0)
    EXPECT_THROW(DecimalValue(x).Sqrt(),
      peloton::Exception);
  else {
    v1 = DecimalValue(x).Sqrt();
    v2 = new DecimalValue(sqrt(x));
    CheckEqual(v1, v2);
    delete v2;
  }
  delete v1;
}

TEST_F(NumericValueTest, Comstruct) {
  DecimalValue *v = new DecimalValue(0.0);
  delete v;
}

TEST_F(NumericValueTest, Compare) {
  std::srand(SEED);

  for (int i = 0; i < TEST_NUM; i++) {
    CheckCompare1<int8_t, int8_t>(RANDOM8(), RANDOM8());
    CheckCompare1<int8_t, int16_t>(RANDOM8(), RANDOM16());
    CheckCompare1<int8_t, int32_t>(RANDOM8(), RANDOM32());
    CheckCompare1<int8_t, int64_t>(RANDOM8(), RANDOM64());
    CheckCompare2<int8_t>(RANDOM8(), RANDOM_DECIMAL());

    CheckCompare1<int16_t, int8_t>(RANDOM16(), RANDOM8());
    CheckCompare1<int16_t, int16_t>(RANDOM16(), RANDOM16());
    CheckCompare1<int16_t, int32_t>(RANDOM16(), RANDOM32());
    CheckCompare1<int16_t, int64_t>(RANDOM16(), RANDOM64());
    CheckCompare2<int16_t>(RANDOM16(), RANDOM_DECIMAL());

    CheckCompare1<int32_t, int8_t>(RANDOM32(), RANDOM8());
    CheckCompare1<int32_t, int16_t>(RANDOM32(), RANDOM16());
    CheckCompare1<int32_t, int32_t>(RANDOM32(), RANDOM32());
    CheckCompare1<int32_t, int64_t>(RANDOM32(), RANDOM64());
    CheckCompare2<int32_t>(RANDOM32(), RANDOM_DECIMAL());

    CheckCompare1<int64_t, int8_t>(RANDOM64(), RANDOM8());
    CheckCompare1<int64_t, int16_t>(RANDOM64(), RANDOM16());
    CheckCompare1<int64_t, int32_t>(RANDOM64(), RANDOM32());
    CheckCompare1<int64_t, int64_t>(RANDOM64(), RANDOM64());
    CheckCompare2<int64_t>(RANDOM64(), RANDOM_DECIMAL());

    CheckCompare3<int8_t>(RANDOM_DECIMAL(), RANDOM8());
    CheckCompare3<int16_t>(RANDOM_DECIMAL(), RANDOM16());
    CheckCompare3<int32_t>(RANDOM_DECIMAL(), RANDOM32());
    CheckCompare3<int64_t>(RANDOM_DECIMAL(), RANDOM64());
    CheckCompare4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTest, Math) {
  std::srand(SEED);

  // Generate two values v1 and v2
  // Check Value(v1) op Value(v2) == Value(v1 op v2);
  for (int i = 0; i < TEST_NUM; i++) {
    CheckMath1<int8_t, int8_t>(RANDOM8(), RANDOM8());
    CheckMath1<int8_t, int16_t>(RANDOM8(), RANDOM16());
    CheckMath1<int8_t, int32_t>(RANDOM8(), RANDOM32());
    CheckMath1<int8_t, int64_t>(RANDOM8(), RANDOM64());
    CheckMath2<int8_t>(RANDOM8(), RANDOM_DECIMAL());

    CheckMath1<int16_t, int8_t>(RANDOM16(), RANDOM8());
    CheckMath1<int16_t, int16_t>(RANDOM16(), RANDOM16());
    CheckMath1<int16_t, int32_t>(RANDOM16(), RANDOM32());
    CheckMath1<int16_t, int64_t>(RANDOM16(), RANDOM64());
    CheckMath2<int16_t>(RANDOM16(), RANDOM_DECIMAL());

    CheckMath1<int32_t, int8_t>(RANDOM32(), RANDOM8());
    CheckMath1<int32_t, int16_t>(RANDOM32(), RANDOM16());
    CheckMath1<int32_t, int32_t>(RANDOM32(), RANDOM32());
    CheckMath1<int32_t, int64_t>(RANDOM32(), RANDOM64());
    CheckMath2<int32_t>(RANDOM32(), RANDOM_DECIMAL());

    CheckMath1<int64_t, int8_t>(RANDOM64(), RANDOM8());
    CheckMath1<int64_t, int16_t>(RANDOM64(), RANDOM16());
    CheckMath1<int64_t, int32_t>(RANDOM64(), RANDOM32());
    CheckMath1<int64_t, int64_t>(RANDOM64(), RANDOM64());
    CheckMath2<int64_t>(RANDOM64(), RANDOM_DECIMAL());

    CheckMath3<int8_t>(RANDOM_DECIMAL(), RANDOM8());
    CheckMath3<int16_t>(RANDOM_DECIMAL(), RANDOM16());
    CheckMath3<int32_t>(RANDOM_DECIMAL(), RANDOM32());
    CheckMath3<int64_t>(RANDOM_DECIMAL(), RANDOM64());
    CheckMath4(RANDOM_DECIMAL(), RANDOM_DECIMAL());
  }
}

TEST_F(NumericValueTest, DivideByZero) {
  std::srand(SEED);

  CheckMath1<int8_t, int8_t>(RANDOM8(), 0);
  CheckMath1<int8_t, int16_t>(RANDOM8(), 0);
  CheckMath1<int8_t, int32_t>(RANDOM8(), 0);
  CheckMath1<int8_t, int64_t>(RANDOM8(), 0);
  CheckMath2<int8_t>(RANDOM8(), 0);

  CheckMath1<int16_t, int8_t>(RANDOM16(), 0);
  CheckMath1<int16_t, int16_t>(RANDOM16(), 0);
  CheckMath1<int16_t, int32_t>(RANDOM16(), 0);
  CheckMath1<int16_t, int64_t>(RANDOM16(), 0);
  CheckMath2<int16_t>(RANDOM16(), 0);

  CheckMath1<int32_t, int8_t>(RANDOM32(), 0);
  CheckMath1<int32_t, int16_t>(RANDOM32(), 0);
  CheckMath1<int32_t, int32_t>(RANDOM32(), 0);
  CheckMath1<int32_t, int64_t>(RANDOM32(), 0);
  CheckMath2<int32_t>(RANDOM32(), 0);

  CheckMath1<int64_t, int8_t>(RANDOM64(), 0);
  CheckMath1<int64_t, int16_t>(RANDOM64(), 0);
  CheckMath1<int64_t, int32_t>(RANDOM64(), 0);
  CheckMath1<int64_t, int64_t>(RANDOM64(), 0);
  CheckMath2<int64_t>(RANDOM64(), 0);

  CheckMath3<int8_t>(RANDOM_DECIMAL(), 0);
  CheckMath3<int16_t>(RANDOM_DECIMAL(), 0);
  CheckMath3<int32_t>(RANDOM_DECIMAL(), 0);
  CheckMath3<int64_t>(RANDOM_DECIMAL(), 0);
  CheckMath4(RANDOM_DECIMAL(), 0);
}

TEST_F(NumericValueTest, TestNull) {
  std::srand(SEED);
  Value *result[5] = { nullptr };

  // Compare null
  result[0] = IntegerValue(rand()).CompareEquals(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).CompareEquals(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).CompareEquals(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).CompareEquals(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).CompareEquals(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).CompareEquals(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).CompareEquals(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).CompareEquals(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).CompareEquals(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).CompareEquals(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  // Operate null
  printf("test operate null\n");
  result[0] = IntegerValue(rand()).Add(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).Add(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).Add(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).Add(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).Add(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue(rand()).Subtract(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).Subtract(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).Subtract(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).Subtract(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).Subtract(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }



  result[0] = IntegerValue(rand()).Multiply(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).Multiply(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).Multiply(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).Multiply(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).Multiply(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue(rand()).Divide(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).Divide(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).Divide(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).Divide(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).Divide(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue(rand()).Modulo(
    IntegerValue((int8_t)PELOTON_INT8_NULL));
  result[1] = IntegerValue(rand()).Modulo(
    IntegerValue((int16_t)PELOTON_INT16_NULL));
  result[2] = IntegerValue(rand()).Modulo(
    IntegerValue((int32_t)PELOTON_INT32_NULL));
  result[3] = IntegerValue(rand()).Modulo(
    IntegerValue((int64_t)PELOTON_INT64_NULL));
  result[4] = IntegerValue(rand()).Modulo(
    DecimalValue((double)PELOTON_DECIMAL_NULL));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Add(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Add(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Add(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Add(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Add(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Subtract(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Subtract(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Subtract(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Subtract(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Subtract(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Multiply(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Multiply(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Multiply(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Multiply(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Multiply(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }

  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Divide(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Divide(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Divide(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Divide(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Divide(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }
  
  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Modulo(
    IntegerValue(rand()));
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Modulo(
    IntegerValue(rand()));
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Modulo(
    IntegerValue(rand()));
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Modulo(
    IntegerValue(rand()));
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Modulo(
    IntegerValue(rand()));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }


  result[0] = IntegerValue((int8_t)PELOTON_INT8_NULL).Sqrt();
  result[1] = IntegerValue((int16_t)PELOTON_INT16_NULL).Sqrt();
  result[2] = IntegerValue((int32_t)PELOTON_INT32_NULL).Sqrt();
  result[3] = IntegerValue((int64_t)PELOTON_INT64_NULL).Sqrt();
  result[4] = DecimalValue((double)PELOTON_DECIMAL_NULL).Sqrt();
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(result[i]->IsNull());
    delete result[i];
  }
}

}
}
