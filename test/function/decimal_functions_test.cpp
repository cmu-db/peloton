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
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "util/string_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class DecimalFunctionsTests : public PelotonTest {};

TEST_F(DecimalFunctionsTests, SqrtTest) {
  const double column_val = 9.0;
  const double expected = sqrt(9.0);
  std::vector<type::Value> args = {
      type::ValueFactory::GetDecimalValue(column_val)};

  auto result = function::DecimalFunctions::Sqrt(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.GetAs<double>());

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  result = function::DecimalFunctions::Sqrt(args);
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

TEST_F(DecimalFunctionsTests, RoundTest) {
  std::vector<double> column_vals = {9.5, 3.3, -4.4, -5.5, 0.0};
  std::vector<type::Value> args;
  for (double val : column_vals) {
    args = {type::ValueFactory::GetDecimalValue(val)};
    auto result = function::DecimalFunctions::_Round(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(round(val), result.GetAs<double>());
  }

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Round(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(DecimalFunctionsTests, CeilTestDouble) {
  std::vector<double> doubleTestInputs = {-36.0, -35.222, -0.7, -0.5, -0.2,
                                          0.0, 0.2, 0.5, 0.7, 35.2, 36.0,
                                          37.2222};
  std::vector<type::Value> args;
  for (double in: doubleTestInputs) {
    args = {type::ValueFactory::GetDecimalValue(in)};
    auto result = function::DecimalFunctions::_Ceil(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(ceil(in), result.GetAs<double>());
  }

  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  auto result = function::DecimalFunctions::_Ceil(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(DecimalFunctionsTests, CeilTestInt) {
  std::vector<int64_t> bigIntTestInputs = {-20, -15, -10, 0, 10, 20};
  std::vector<int32_t> intTestInputs = {-20, -15, -10, 0, 10, 20};
  std::vector<int16_t> smallIntTestInputs = {-20, -15, -10, 0, 10, 20};
  std::vector<int8_t> tinyIntTestInputs = {-20, -15, -10, 0, 10, 20};

  std::vector<type::Value> args;
  // Testing Ceil with Integer Types
  for (int64_t in: bigIntTestInputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Ceil(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(ceil(in), result.GetAs<double>());
  }

  for (int in: intTestInputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Ceil(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(ceil(in), result.GetAs<double>());
  }

  for (int in: smallIntTestInputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Ceil(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(ceil(in), result.GetAs<double>());
  }

  for (int in: tinyIntTestInputs) {
    args = {type::ValueFactory::GetIntegerValue(in)};
    auto result = function::DecimalFunctions::_Ceil(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(ceil(in), result.GetAs<double>());
  }
}

}  // namespace test
}  // namespace peloton
