//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_test.cpp
//
// Identification: test/expression/date_functions_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"

#include "type/value.h"
#include "type/value_factory.h"
#include "expression/expression_util.h"
#include "expression/function_expression.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

typedef std::unique_ptr<expression::AbstractExpression> ExpPtr;

class DateFunctionsTests : public PelotonTest {
};

//A simple test to make sure function expressions are filled in correctly
TEST_F(DateFunctionsTests, ExtractDateFunctionsTests) {
  // these will be cleaned up by extract_expr
  auto part = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(EXPRESSION_DATE_PART_MILLISECONDS));
  auto timestamp = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::CastAsTimestamp(
          type::ValueFactory::GetVarcharValue(
              "2013-02-16 20:38:40.000000+00")));

  // these need unique ptrs to clean them
  auto extract_expr = ExpPtr(new expression::FunctionExpression("extract", { part, timestamp }));

  expression::ExpressionUtil::TransformExpression(nullptr, extract_expr.get());


  // do a lookup (we pass null schema because there are no tuple value expressions
  EXPECT_TRUE(
      extract_expr->Evaluate(nullptr, nullptr, nullptr).IsNull());
}

}  // namespace test
}  // namespace peloton
