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
TEST_F(ExpressionUtilTests, GetInfoTest) {
  auto root = createExpTree();
  std::string info = expression::ExpressionUtil::GetInfo(root);

  // Just make sure that it has our constant strings
  EXPECT_TRUE(info.size() > 0);
  EXPECT_NE(std::string::npos, info.find(CONSTANT_VALUE_STRING1));
  EXPECT_NE(std::string::npos, info.find(CONSTANT_VALUE_STRING2));

  delete root;
}

TEST_F(ExpressionUtilTests, ExtractJoinColTest) {
  // Table1.a = Table2.b
  auto expr1 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0, 1);
  auto expr2 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 1, 0);
  auto expr3 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr1, expr2);

  // Table1.c < Table2.d
  auto expr4 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0, 0);
  auto expr5 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 1, 1);
  auto expr6 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_LESSTHAN, expr4, expr5);

  // Table1.a = 3
  auto expr7 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0, 1);
  auto expr8 = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(3));
  auto expr9 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr7, expr8);

  // Table1.c = Table2.d
  auto expr10 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0, 0);
  auto expr11 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 1, 1);
  auto expr12 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr10, expr11);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> l_column_ids,
      r_column_ids;
  // Table1.a = Table2.b -> nullptr
  auto ret_expr1 = expression::ExpressionUtil::ExtractJoinColumns(
      l_column_ids, r_column_ids, expr3);
  EXPECT_EQ(nullptr, ret_expr1);
  EXPECT_EQ(1, l_column_ids.size());

  // (Table1.a = Table2.b) AND (Table1.c < Table2.d) -> (Table1.c < Table2.d)
  auto expr13 = expression::ExpressionUtil::ConjunctionFactory(
      ExpressionType::CONJUNCTION_AND, expr3, expr6);
  l_column_ids.clear();
  r_column_ids.clear();
  auto ret_expr2 = expression::ExpressionUtil::ExtractJoinColumns(
      l_column_ids, r_column_ids, expr13);

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
  auto expr14 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0, 1);
  auto expr15 =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 1, 0);
  auto expr16 = expression::ExpressionUtil::ComparisonFactory(
      ExpressionType::COMPARE_EQUAL, expr14, expr15);
  // ((Table1.a = Table2.b AND Table1.c = Table2.d) AND Table1.a = 3) ->
  // Table1.a = 3
  auto expr17 = expression::ExpressionUtil::ConjunctionFactory(
      ExpressionType::CONJUNCTION_AND, expr16, expr12);

  auto expr18 = expression::ExpressionUtil::ConjunctionFactory(
      ExpressionType::CONJUNCTION_AND, expr17, expr9);

  l_column_ids.clear();
  r_column_ids.clear();
  auto ret_expr3 = expression::ExpressionUtil::ExtractJoinColumns(
      l_column_ids, r_column_ids, expr18);

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

  if (ret_expr1 != nullptr) delete ret_expr1;
  delete ret_expr2;
  delete ret_expr3;
  delete expr18;
  delete expr13;
}
}  // namespace test
}  // namespace peloton
