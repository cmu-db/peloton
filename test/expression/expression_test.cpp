//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_test.cpp
//
// Identification: test/expression/expression_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

class ExpressionTests : public PelotonTest {
};

//A simple test to make sure function expressions are filled in correctly
TEST_F(ExpressionTests, FunctionExpressionTest) {
  // these will be gc'd by substr
  auto str = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetVarcharValue("test123"));
  auto from = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(2));
  auto to = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(3));
  // these need unique ptrs to clean them
  auto substr = ExpPtr(new expression::FunctionExpression("substr", { str, from,
      to }));
  auto not_found = ExpPtr(new expression::FunctionExpression("", { }));
  // throw an exception when we cannot find a function
  EXPECT_THROW(
      expression::ExpressionUtil::TransformExpression(nullptr, not_found.get()),
      peloton::Exception);
  expression::ExpressionUtil::TransformExpression(nullptr, substr.get());
  // do a lookup (we pass null schema because there are no tuple value expressions
  EXPECT_TRUE(substr->Evaluate(nullptr, nullptr, nullptr).CompareEquals(type::ValueFactory::GetVarcharValue("est")) == type::CMP_TRUE);

}

}  // namespace test
}  // namespace peloton
