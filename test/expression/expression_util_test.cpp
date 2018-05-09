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

class ExpressionUtilTests : public PelotonTest {};

std::string CONSTANT_VALUE_STRING1 = "ABC";
std::string CONSTANT_VALUE_STRING2 = "XYZ";

expression::AbstractExpression *createExpTree() {
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
TEST_F(ExpressionUtilTests, OperatorFactoryTest) {
  auto exp1 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(1));
  auto exp2 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(1));
  auto exp_two = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(2));

  auto two = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::OperatorFactory(
          ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, exp1, exp2);
  EXPECT_EQ(CmpBool::CmpTrue,
            exp_two->GetValue().CompareEquals(two->GetValue()));
  delete two;
  delete exp_two;
}

TEST_F(ExpressionUtilTests, ComparisonFactoryTest) {
  auto exp1 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(1));
  auto exp2 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(1));
  auto true_value = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(true));
  auto cmp = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, exp1, exp2);

  EXPECT_EQ(CmpBool::CmpTrue,
            cmp->GetValue().CompareEquals(true_value->GetValue()));
  delete true_value;
  delete cmp;
}

TEST_F(ExpressionUtilTests, ConjunctionFactoryTest) {
  auto exp1 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(true));
  auto exp2 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(false));
  auto cmp = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, exp1->Copy(), exp2->Copy());
  EXPECT_EQ(CmpBool::CmpTrue, cmp->GetValue().CompareEquals(exp2->GetValue()));
  delete cmp;
  delete exp1;
  delete exp2;

  auto exp3 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(true));
  auto exp4 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetBooleanValue(false));
  auto cmp2 = (expression::ConstantValueExpression *)
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_OR, exp3->Copy(), exp4->Copy());
  EXPECT_EQ(CmpBool::CmpFalse,
            cmp2->GetValue().CompareEquals(exp4->GetValue()));
  delete cmp2;
  delete exp3;
  delete exp4;
}

// Make sure that we can traverse a tree
TEST_F(ExpressionUtilTests, GetInfoTest) {
  std::unique_ptr<expression::AbstractExpression> root(createExpTree());
  std::string info = expression::ExpressionUtil::GetInfo(root.get());

  // Just make sure that it has our constant strings
  EXPECT_TRUE(info.size() > 0);
}

TEST_F(ExpressionUtilTests, ExtractJoinColTest) {
  // Table1.a = Table2.b
  auto expr1 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 1);
  auto expr2 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 1, 0);
  auto expr3 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr1, expr2);

  // Table1.c < Table2.d
  auto expr4 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 0);
  auto expr5 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 1, 1);
  auto expr6 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_LESSTHAN, expr4, expr5);

  // Table1.a = 3
  auto expr7 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 1);
  auto expr8 = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(3));
  auto expr9 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr7, expr8);

  // Table1.c = Table2.d
  auto expr10 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 0);
  auto expr11 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 1, 1);
  auto expr12 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr10, expr11);

  std::vector<std::unique_ptr<const expression::AbstractExpression>>
      l_column_ids, r_column_ids;
  // Table1.a = Table2.b -> nullptr
  std::unique_ptr<expression::AbstractExpression> ret_expr1(
      expression::ExpressionUtil::ExtractJoinColumns(l_column_ids, r_column_ids,
                                                     expr3));
  EXPECT_EQ(nullptr, ret_expr1.get());
  EXPECT_EQ(1, l_column_ids.size());

  // (Table1.a = Table2.b) AND (Table1.c < Table2.d) -> (Table1.c < Table2.d)
  std::unique_ptr<expression::AbstractExpression> expr13(
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, expr3, expr6));
  l_column_ids.clear();
  r_column_ids.clear();
  std::unique_ptr<expression::AbstractExpression> ret_expr2(
      expression::ExpressionUtil::ExtractJoinColumns(l_column_ids, r_column_ids,
                                                     expr13.get()));

  EXPECT_EQ(ExpressionType::COMPARE_LESSTHAN, ret_expr2->GetExpressionType());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE,
            ret_expr2->GetChild(0)->GetExpressionType());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE,
            ret_expr2->GetChild(1)->GetExpressionType());

  EXPECT_EQ(1, l_column_ids.size());
  EXPECT_EQ(1, reinterpret_cast<const expression::TupleValueExpression *>(
                   l_column_ids[0].get())->GetColumnId());
  EXPECT_EQ(0, reinterpret_cast<const expression::TupleValueExpression *>(
                   r_column_ids[0].get())->GetColumnId());

  // Table1.a = Table2.b
  auto expr14 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 1);
  auto expr15 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 1, 0);
  auto expr16 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr14, expr15);
  // ((Table1.a = Table2.b AND Table1.c = Table2.d) AND Table1.a = 3) ->
  // Table1.a = 3
  auto expr17 = expression::ExpressionUtil::ConjunctionFactory(
      ExpressionType::CONJUNCTION_AND, expr16, expr12);

  std::unique_ptr<expression::AbstractExpression> expr18(
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, expr17, expr9));

  l_column_ids.clear();
  r_column_ids.clear();
  std::unique_ptr<expression::AbstractExpression> ret_expr3(
      expression::ExpressionUtil::ExtractJoinColumns(l_column_ids, r_column_ids,
                                                     expr18.get()));

  EXPECT_EQ(2, l_column_ids.size());
  EXPECT_EQ(1, reinterpret_cast<const expression::TupleValueExpression *>(
                   l_column_ids[0].get())->GetColumnId());
  EXPECT_EQ(0, reinterpret_cast<const expression::TupleValueExpression *>(
                   r_column_ids[0].get())->GetColumnId());
  EXPECT_EQ(0, reinterpret_cast<const expression::TupleValueExpression *>(
                   l_column_ids[1].get())->GetColumnId());
  EXPECT_EQ(1, reinterpret_cast<const expression::TupleValueExpression *>(
                   r_column_ids[1].get())->GetColumnId());

  EXPECT_EQ(ExpressionType::COMPARE_EQUAL, ret_expr3->GetExpressionType());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE,
            ret_expr3->GetChild(0)->GetExpressionType());
  EXPECT_EQ(ExpressionType::VALUE_CONSTANT,
            ret_expr3->GetChild(1)->GetExpressionType());
}

}  // namespace test
}  // namespace peloton
