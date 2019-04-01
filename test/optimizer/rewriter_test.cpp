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

TEST_F(RewriterTests, ConvertAbsExpr) {
  type::Value leftValue = type::ValueFactory::GetIntegerValue(1);
  type::Value rightValue = type::ValueFactory::GetIntegerValue(2);
  auto left = new expression::ConstantValueExpression(leftValue);
  auto right = new expression::ConstantValueExpression(rightValue);
  auto common = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, left, right);

  Rewriter *rewriter = new Rewriter();

  auto absexpr = rewriter->ConvertToAbsExpr(common);
  EXPECT_TRUE(absexpr != nullptr);
  EXPECT_TRUE(absexpr->Op().GetType() == ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(absexpr->Children().size() == 2);

  auto lefta = absexpr->Children()[0];
  auto righta = absexpr->Children()[1];
  EXPECT_TRUE(lefta != nullptr && righta != nullptr);
  EXPECT_TRUE(lefta->Op().GetType() == righta->Op().GetType());
  EXPECT_TRUE(lefta->Op().GetType() == ExpressionType::VALUE_CONSTANT);

  auto left_cve = static_cast<const expression::ConstantValueExpression*>(lefta->Op().GetExpr());
  auto right_cve = static_cast<const expression::ConstantValueExpression*>(righta->Op().GetExpr());
  EXPECT_TRUE(left_cve == left);
  EXPECT_TRUE(right_cve == right);

  // Try applying the rule
  ComparatorElimination rule;
  EXPECT_TRUE(rule.Check(absexpr, nullptr) == true);

  std::vector<std::shared_ptr<AbsExpr_Expression>> transform;
  rule.Transform(absexpr, transform, nullptr);
  EXPECT_TRUE(transform.size() == 1);
  
  delete rewriter;
  delete common;

  auto tr_expr = transform[0];
  EXPECT_TRUE(tr_expr != nullptr);
  EXPECT_TRUE(tr_expr->Op().GetType() == ExpressionType::VALUE_CONSTANT);
  EXPECT_TRUE(tr_expr->Children().size() == 0);

  auto tr_cve = static_cast<const expression::ConstantValueExpression*>(tr_expr->Op().GetExpr());
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(tr_cve->GetValue()) == false);

  // (TODO): hack to fix the memory leak bubbled from Transform()
  delete tr_cve;
}

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
}  // namespace test
}  // namespace peloton
