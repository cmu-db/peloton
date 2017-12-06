//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions_test.cpp
//
// Identification: test/expression/decimal_functions_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>
#include <vector>
#include <cmath>

#include "common/harness.h"

#include "function/decimal_functions.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "util/string_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

static const double MAX_ABS_ERROR = 0.1;

class DecimalFunctionsTests : public PelotonTest {};

// Test DOUBLE square root
TEST_F(DecimalFunctionsTests, SqrtSmallDoubleTest) {
  std::vector<double> inputs = {9.0, 12.0, 16.0};
  std::vector<type::Value> args;

  for (double in : inputs) {
    args = {type::ValueFactory::GetDecimalValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test DOUBLE square root with larger numbers
TEST_F(DecimalFunctionsTests, SqrtLargeDoubleTest) {
  std::vector<double> inputs = {5000.0, 50000.0, 500000.0};
  std::vector<type::Value> args;

  for (double in : inputs) {
    args = {type::ValueFactory::GetDecimalValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test TINYINT square root
TEST_F(DecimalFunctionsTests, SqrtTinyIntTest1) {
  std::vector<int8_t> inputs = {4, 30, 55};
  std::vector<type::Value> args;

  for (int8_t in : inputs) {
    args = {type::ValueFactory::GetTinyIntValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test TINYINT square root
TEST_F(DecimalFunctionsTests, SqrtTinyIntTest2) {
  std::vector<int8_t> inputs = {100, 120, 127};
  std::vector<type::Value> args;

  for (int8_t in : inputs) {
    args = {type::ValueFactory::GetTinyIntValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test SMALLINT square root
TEST_F(DecimalFunctionsTests, SqrtSmallIntTest1) {
  std::vector<int16_t> inputs = {100, 120, 127};
  std::vector<type::Value> args;

  for (int16_t in : inputs) {
    args = {type::ValueFactory::GetSmallIntValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test SMALLINT square root
TEST_F(DecimalFunctionsTests, SqrtSmallIntTest2) {
  std::vector<int16_t> inputs = {10000, 25000, 32767};
  std::vector<type::Value> args;

  for (int16_t in : inputs) {
    args = {type::ValueFactory::GetSmallIntValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test INT square root
TEST_F(DecimalFunctionsTests, SqrtInt32Test1) {
  std::vector<int32_t> inputs = {10000, 25000, 32767};
  std::vector<type::Value> args;

  for (int32_t in : inputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test INT square root
TEST_F(DecimalFunctionsTests, SqrtInt32Test2) {
  std::vector<int32_t> inputs = {100000000, 1073741824, 2147483647};
  std::vector<type::Value> args;

  for (int32_t in : inputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

// Test BIGINT square root
TEST_F(DecimalFunctionsTests, SqrtInt64Test1) {
  std::vector<int64_t> inputs = {100000000, 1073741824, 2147483647};
  std::vector<type::Value> args;

  for (int64_t in : inputs) {
    args = {type::ValueFactory::GetBigIntValue(in)};
    auto result = function::DecimalFunctions::_Sqrt(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_DOUBLE_EQ(sqrt(in), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(DecimalFunctionsTests, FloorTest) {
  // Testing Floor with DecimalTypes
  std::vector<double> inputs = {9.5, 3.3, -4.4, 0.0};
  std::vector<type::Value> args;
  for (double in : inputs) {
    args = {type::ValueFactory::GetDecimalValue(in)};
    auto result = function::DecimalFunctions::_Floor(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(floor(in), result.GetAs<double>());
  }

  // Testing Floor with Integer Types(Should be a no-op)
  int64_t numInt64 = 1;
  args = {type::ValueFactory::GetIntegerValue(numInt64)};
  auto result = function::DecimalFunctions::_Floor(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(numInt64, result.GetAs<double>());

  int32_t numInt32 = 1;
  args = {type::ValueFactory::GetIntegerValue(numInt32)};
  result = function::DecimalFunctions::_Floor(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(numInt32, result.GetAs<double>());

  int16_t numInt16 = 1;
  args = {type::ValueFactory::GetIntegerValue(numInt32)};
  result = function::DecimalFunctions::_Floor(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(numInt16, result.GetAs<double>());

  int16_t numInt8 = 1;
  args = {type::ValueFactory::GetIntegerValue(numInt8)};
  result = function::DecimalFunctions::_Floor(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(numInt8, result.GetAs<double>());

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  result = function::DecimalFunctions::_Floor(args);
  EXPECT_TRUE(result.IsNull());
}

}  // namespace test
}  // namespace peloton
