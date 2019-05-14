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
#include "expression/operator_expression.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "optimizer/rule_rewrite.h"

namespace peloton {

namespace test {

using namespace optimizer;

class RewriterTests : public PelotonTest {};

TEST_F(RewriterTests, SingleCompareEqualRewritePassFalse) {
  // 3 = 2 ==> FALSE
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
  // 4 = 4 ==> TRUE
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
  //                  [=]     [=]     ==> FALSE
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
  //                  [<=]     [>=]     ==> TRUE
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

TEST_F(RewriterTests, BasicAndShortCircuitTest) {

  // First, build the rewriter and the values that will be used in test cases
  Rewriter *rewriter = new Rewriter();

  type::Value val_false = type::ValueFactory::GetBooleanValue(false);
  type::Value val_true = type::ValueFactory::GetBooleanValue(true);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  //
  //            [AND]
  //     [FALSE]     [=]
  //               [X] [3]
  //
  //  Intended output: [FALSE]
  //

  expression::ConstantValueExpression *lh = new expression::ConstantValueExpression(val_false);
  expression::ConstantValueExpression *rh_right_child = new expression::ConstantValueExpression(val3);
  expression::TupleValueExpression *rh_left_child = new expression::TupleValueExpression("t","x");

  expression::ComparisonExpression *rh = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, rh_left_child, rh_right_child);
  expression::ConjunctionExpression *root = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, lh, rh);

  expression::AbstractExpression *rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 0);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::VALUE_CONSTANT);

  delete rewrote;
  delete root;

  //
  //             [AND]
  //       [TRUE]     [=]
  //                [X] [3]
  //
  //  Intended output: same as input
  //

  lh = new expression::ConstantValueExpression(val_true);
  rh_right_child = new expression::ConstantValueExpression(val3);
  rh_left_child = new expression::TupleValueExpression("t","x");

  rh = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, rh_left_child, rh_right_child);
  root = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, lh, rh);

  rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 2);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::CONJUNCTION_AND);

  delete rewrote;
  delete root;

  delete rewriter;
}


TEST_F(RewriterTests, BasicOrShortCircuitTest) {
  // First, build the rewriter and the values that will be used in test cases
  Rewriter *rewriter = new Rewriter();

  type::Value val_false = type::ValueFactory::GetBooleanValue(false);
  type::Value val_true = type::ValueFactory::GetBooleanValue(true);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  //
  //            [OR]
  //      [TRUE]    [=]
  //              [X] [3]
  //
  //  Intended output: [TRUE]
  //

  expression::ConstantValueExpression *lh = new expression::ConstantValueExpression(val_true);
  expression::ConstantValueExpression *rh_right_child = new expression::ConstantValueExpression(val3);
  expression::TupleValueExpression *rh_left_child = new expression::TupleValueExpression("t","x");

  expression::ComparisonExpression *rh = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, rh_left_child, rh_right_child);
  expression::ConjunctionExpression *root = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_OR, lh, rh);

  expression::AbstractExpression *rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 0);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::VALUE_CONSTANT);

  delete rewrote;
  delete root;

  //
  //              [OR]
  //       [FALSE]    [=]
  //                [X] [3]
  //
  //  Intended output: same as input
  //

  lh = new expression::ConstantValueExpression(val_false);
  rh_right_child = new expression::ConstantValueExpression(val3);
  rh_left_child = new expression::TupleValueExpression("t","x");

  rh = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, rh_left_child, rh_right_child);
  root = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_OR, lh, rh);

  rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 2);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::CONJUNCTION_OR);

  delete rewrote;
  delete root;

  delete rewriter;
}


TEST_F(RewriterTests, AndShortCircuitComparatorEliminationMixTest) {
  //                      [AND]
  //                  [<=]     [=]
  //                [4] [4]  [5] [3]
  //             Intended Output: FALSE
  //
  type::Value val4 = type::ValueFactory::GetIntegerValue(4);
  type::Value val5 = type::ValueFactory::GetIntegerValue(5);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  auto lb_left_child = new expression::ConstantValueExpression(val4);
  auto lb_right_child = new expression::ConstantValueExpression(val4);
  auto rb_left_child = new expression::ConstantValueExpression(val5);
  auto rb_right_child = new expression::ConstantValueExpression(val3);

  auto lb = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO,
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


TEST_F(RewriterTests, OrShortCircuitComparatorEliminationMixTest) {
  //                      [OR]
  //                  [<=]    [=]
  //                [4] [4] [5] [3]
  //             Intended Output: TRUE
  //
  type::Value val4 = type::ValueFactory::GetIntegerValue(4);
  type::Value val5 = type::ValueFactory::GetIntegerValue(5);
  type::Value val3 = type::ValueFactory::GetIntegerValue(3);

  auto lb_left_child = new expression::ConstantValueExpression(val4);
  auto lb_right_child = new expression::ConstantValueExpression(val4);
  auto rb_left_child = new expression::ConstantValueExpression(val5);
  auto rb_right_child = new expression::ConstantValueExpression(val3);

  auto lb = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO,
                                                 lb_left_child, lb_right_child);
  auto rb = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                                 rb_left_child, rb_right_child);
  auto top = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_OR, lb, rb);

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


TEST_F(RewriterTests, NotNullColumnsTest) {

  // First, build rewriter to be used in all test cases
  Rewriter *rewriter = new Rewriter();

  // [T.X IS NULL], where X is a non-NULL column in table T
  //     Intended output: FALSE

  auto child = new expression::TupleValueExpression("t","x");
  child->SetIsNotNull(true);
  auto root = new expression::OperatorExpression(ExpressionType::OPERATOR_IS_NULL, type::TypeId::BOOLEAN, child, nullptr);

  auto rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 0);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::VALUE_CONSTANT);

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_EQ(casted->GetValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(type::ValuePeeker::PeekBoolean(casted->GetValue()), false);

  delete root;
  delete rewrote;

  // [T.X IS NOT NULL], where X is a non-NULL column in table T
  //     Intended output: TRUE

  child = new expression::TupleValueExpression("t","x");
  child->SetIsNotNull(true);
  root = new expression::OperatorExpression(ExpressionType::OPERATOR_IS_NOT_NULL, type::TypeId::BOOLEAN, child, nullptr);

  rewrote = rewriter->RewriteExpression(root);

  EXPECT_TRUE(rewrote != nullptr);
  EXPECT_EQ(rewrote->GetChildrenSize(), 0);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::VALUE_CONSTANT);

  casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_EQ(casted->GetValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(type::ValuePeeker::PeekBoolean(casted->GetValue()), true);

  delete root;
  delete rewrote;

  // [T.Y IS NULL], where Y is a possibly NULL column in table T
  //     Intended output: same as input

  child = new expression::TupleValueExpression("t","y");
  child->SetIsNotNull(false); // is_not_null is false by default, but explicitly setting it is for readability's sake
  root = new expression::OperatorExpression(ExpressionType::OPERATOR_IS_NULL, type::TypeId::BOOLEAN, child, nullptr);

  rewrote = rewriter->RewriteExpression(root);

  EXPECT_EQ(rewrote->GetChildrenSize(), 1);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::OPERATOR_IS_NULL);

  delete root;
  delete rewrote;

  // [T.Y IS NOT NULL], where Y is a possibly NULL column in table T
  //     Intended output: same as input

  child = new expression::TupleValueExpression("t","y");
  child->SetIsNotNull(false); // is_not_null is false by default, but explicitly setting it is for readability's sake
  root = new expression::OperatorExpression(ExpressionType::OPERATOR_IS_NOT_NULL, type::TypeId::BOOLEAN, child, nullptr);

  rewrote = rewriter->RewriteExpression(root);

  EXPECT_EQ(rewrote->GetChildrenSize(), 1);
  EXPECT_EQ(rewrote->GetExpressionType(), ExpressionType::OPERATOR_IS_NOT_NULL);

  delete root;
  delete rewrote;

  delete rewriter;
}


}  // namespace test
}  // namespace peloton
