//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_test.cpp
//
// Identification: test/expression/expressionutil_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"
#include "common/logger.h"

#include "type/value.h"
#include "type/value_factory.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

class ExpressionUtilTest : public PelotonTest {};

std::string CONSTANT_VALUE_STRING1 = "ABC";
std::string CONSTANT_VALUE_STRING2 = "XYZ";


expression::AbstractExpression* createExpTree() {
  auto exp1 = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(1));
  auto exp2 = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(1));
  auto exp3 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, exp1, exp2);

  auto exp4 = expression::ExpressionUtil::ConstantValueFactory(
	  type::ValueFactory::GetVarcharValue(CONSTANT_VALUE_STRING1));
  auto exp5 = expression::ExpressionUtil::ConstantValueFactory(
  	  type::ValueFactory::GetVarcharValue(CONSTANT_VALUE_STRING2));
  auto exp6 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_NOTEQUAL, exp4, exp5);

  auto root = expression::ExpressionUtil::ConjunctionFactory(
		  ExpressionType::CONJUNCTION_AND, exp3, exp6);
  return (root);
}

// Make sure that we can traverse a tree
TEST_F(ExpressionUtilTest, GetInfoTest) {
  auto root = createExpTree();
  std::string info = expression::ExpressionUtil::GetInfo(root);

  // Just make sure that it has our constant strings
  EXPECT_TRUE(info.size() > 0);
  EXPECT_NE(std::string::npos, info.find(CONSTANT_VALUE_STRING1));
  EXPECT_NE(std::string::npos, info.find(CONSTANT_VALUE_STRING2));

  delete root;
}

}  // namespace test
}  // namespace peloton
