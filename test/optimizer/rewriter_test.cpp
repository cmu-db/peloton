//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_test.cpp
//
// Identification: test/optimizer/operator_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include "common/harness.h"

#include "optimizer/operators.h"
#include "optimizer/rewriter.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "optimizer/rule_rewrite.h"

namespace peloton {

namespace test {

using namespace optimizer;

class RewriterTests : public PelotonTest {};

TEST_F(RewriterTests, SingleCompareEqualRewritePassFalse) {
  type::Value leftValue = type::ValueFactory::GetIntegerValue(3);
  type::Value rightValue = type::ValueFactory::GetIntegerValue(2);
  auto left = new expression::ConstantValueExpression(leftValue);
  auto right = new expression::ConstantValueExpression(rightValue);
  auto common = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, left, right);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(common);

  delete rewriter;
  delete common;

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == false);

  delete rewrote;
}

TEST_F(RewriterTests, SingleCompareEqualRewritePassTrue) {
  type::Value leftValue = type::ValueFactory::GetIntegerValue(4);
  type::Value rightValue = type::ValueFactory::GetIntegerValue(4);
  auto left = new expression::ConstantValueExpression(leftValue);
  auto right = new expression::ConstantValueExpression(rightValue);
  auto common = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, left, right);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(common);

  delete rewriter;
  delete common;

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == true);

  delete rewrote;
}

TEST_F(RewriterTests, SimpleEqualityTree) {
  //                      [=]
  //                  [=]     [=]
  //                [4] [5] [3] [3]
  type::Value val4 = type::ValueFactory::GetIntegerValue(4);
  type::Value val5 = type::ValueFactory::GetIntegerValue(5);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  auto lb_left_child = new expression::ConstantValueExpression(val4);
  auto lb_right_child = new expression::ConstantValueExpression(val5);
  auto rb_left_child = new expression::ConstantValueExpression(val3);
  auto rb_right_child = new expression::ConstantValueExpression(val3);

  auto lb = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                                 lb_left_child, lb_right_child);
  auto rb = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                                 rb_left_child, rb_right_child);
  auto top = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lb, rb);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(top);

  delete rewriter;
  delete top;

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_TRUE(rewrote->GetChildrenSize() == 0);
  EXPECT_TRUE(rewrote->GetExpressionType() == ExpressionType::VALUE_CONSTANT);

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted->GetValueType() == type::TypeId::BOOLEAN);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == false);

  delete rewrote;
}

TEST_F(RewriterTests, ComparativeOperatorTest) {
  //                       [=]
  //                  [<=]     [>=]
  //                [4] [4]  [5] [3]
  type::Value val4 = type::ValueFactory::GetIntegerValue(4);
  type::Value val5 = type::ValueFactory::GetIntegerValue(5);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  auto lb_left_child = new expression::ConstantValueExpression(val4);
  auto lb_right_child = new expression::ConstantValueExpression(val4);
  auto rb_left_child = new expression::ConstantValueExpression(val5);
  auto rb_right_child = new expression::ConstantValueExpression(val3);

  auto lb = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO,
                                                 lb_left_child, lb_right_child);
  auto rb = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                                 rb_left_child, rb_right_child);
  auto top = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lb, rb);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(top);

  delete rewriter;
  delete top;

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_TRUE(rewrote->GetChildrenSize() == 0);
  EXPECT_TRUE(rewrote->GetExpressionType() == ExpressionType::VALUE_CONSTANT);

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted->GetValueType() == type::TypeId::BOOLEAN);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == true);

  delete rewrote;
}

}  // namespace test
}  // namespace peloton
