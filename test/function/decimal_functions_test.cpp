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
  for(double in: inputs) {
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
