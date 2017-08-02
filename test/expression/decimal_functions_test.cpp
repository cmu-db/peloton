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

#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "include/function/decimal_functions.h"
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

  auto result = expression::DecimalFunctions::Sqrt(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.GetAs<double>());

  // NULL CHECK
  args = {type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL)};
  result = expression::DecimalFunctions::Sqrt(args);
  EXPECT_TRUE(result.IsNull());
}

}  // namespace test
}  // namespace peloton
