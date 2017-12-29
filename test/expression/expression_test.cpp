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

#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "expression/comparison_expression.h"
#include "expression/case_expression.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "storage/tuple.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

class ExpressionTests : public PelotonTest {};

TEST_F(ExpressionTests, EqualityTest) {
  // First tree operator_expr(-) -> (tup_expr(A.a), const_expr(2))
  std::tuple<oid_t, oid_t, oid_t> bound_oid1(1, 1, 1);
  auto left1 = new expression::TupleValueExpression("a", "A");
  left1->SetBoundOid(bound_oid1);
  auto right1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root1(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left1, right1));
  // Second tree operator_expr(-) -> (tup_expr(A.b), const_expr(2))
  std::tuple<oid_t, oid_t, oid_t> bound_oid2(1, 1, 0);
  auto left2 = new expression::TupleValueExpression("b", "A");
  left2->SetBoundOid(bound_oid2);
  auto right2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root2(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left2, right2));
  EXPECT_TRUE(*root1.get() != *root2.get());

  // Third tree operator_expr(-) -> (tup_expr(a.a), const_expr(2))
  auto left3 = new expression::TupleValueExpression("a", "a");
  left3->SetBoundOid(bound_oid1);
  auto right3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root3(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left3, right3));
  EXPECT_TRUE(*root1.get() == *root3.get());
}


TEST_F(ExpressionTests, HashTest) {
  // First tree operator_expr(-) -> (tup_expr(A.a), const_expr(2))
  auto left1 = new expression::TupleValueExpression("a", "A");
  auto oids1 = std::make_tuple<oid_t, oid_t, oid_t>(0,0,0);
  left1->SetBoundOid(oids1);
  auto right1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root1(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left1, right1));
  LOG_INFO("Hash(tree1)=%ld", root1->Hash());

  // Second tree operator_expr(-) -> (tup_expr(A.b), const_expr(2))
  auto left2 = new expression::TupleValueExpression("b", "A");
  auto oids2 = std::make_tuple<oid_t, oid_t, oid_t>(0,0,1);
  left2->SetBoundOid(oids2);
  auto right2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root2(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left2, right2));
  LOG_INFO("Hash(tree2)=%ld", root2->Hash());

  EXPECT_NE(root1->Hash(), root2->Hash());

  // Third tree operator_expr(-) -> (tup_expr(A.a), const_expr(2))
  auto left3 = new expression::TupleValueExpression("a", "A");
  auto oids3 = oids1;
  left3->SetBoundOid(oids3);
  auto right3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  std::unique_ptr<expression::OperatorExpression> root3(
      new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INVALID, left3, right3));
  LOG_INFO("Hash(tree3)=%ld", root3->Hash());

  EXPECT_EQ(root1->Hash(), root3->Hash());
}

TEST_F(ExpressionTests, DistinctFromTest) {
  // Create a table with id column and value column
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "id",
                          true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "value",
                          true);

  columns.push_back(column1);
  columns.push_back(column2);

  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema.get(), true));

  // Create "id IS DISTINCT FROM value" comparison
  auto lexpr = new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto rexpr = new expression::TupleValueExpression(type::TypeId::INTEGER, 1, 1);

  expression::ComparisonExpression expr(
      StringToExpressionType("COMPARE_DISTINCT_FROM"), lexpr, rexpr);

  auto pool = TestingHarness::GetInstance().GetTestingPool();

  // id, value not NULL with the same values, should be false
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(10), pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(10), pool);
  EXPECT_TRUE(expr.Evaluate(tuple.get(), tuple.get(), nullptr).IsFalse());

  // id, value not NULL with different values, should be true
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(5), pool);
  EXPECT_TRUE(expr.Evaluate(tuple.get(), tuple.get(), nullptr).IsTrue());

  // id not NULL, value is NULL, should be true
  tuple->SetValue(1,
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER), pool);
  EXPECT_TRUE(expr.Evaluate(tuple.get(), tuple.get(), nullptr).IsTrue());

  // id is NULL, value not NULL, should be true
  tuple->SetValue(0,
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER), pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(10), pool);
  EXPECT_TRUE(expr.Evaluate(tuple.get(), tuple.get(), nullptr).IsTrue());

  // id is NULL, value is NULL, should be false
  tuple->SetValue(1,
       type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER), pool);
  EXPECT_TRUE(expr.Evaluate(tuple.get(), tuple.get(), nullptr).IsFalse());
}

TEST_F(ExpressionTests, ExtractDateTests) {
  // PAVLO: 2017-01-18
  // This will test whether we can invoke the EXTRACT function
  // correctly. This is different than DateFunctionsTests
  // because it goes through the in our expression system.
  //  This should be uncommented once we get a full implementation

  //  std::string date = "2017-01-01 12:13:14.999999+00";
  //
  //  // <PART> <EXPECTED>
  //  // You can generate the expected value in postgres using this SQL:
  //  // SELECT EXTRACT(MILLISECONDS
  //  //                FROM CAST('2017-01-01 12:13:14.999999+00' AS
  //  TIMESTAMP));
  //  std::vector<std::pair<DatePartType, double>> data = {
  //      std::make_pair(DatePartType::CENTURY, 21),
  //      std::make_pair(DatePartType::DECADE, 201),
  //      std::make_pair(DatePartType::DOW, 0),
  //      std::make_pair(DatePartType::DOY, 1),
  //      std::make_pair(DatePartType::YEAR, 2017),
  //      std::make_pair(DatePartType::MONTH, 1),
  //      std::make_pair(DatePartType::DAY, 2),
  //      std::make_pair(DatePartType::HOUR, 12),
  //      std::make_pair(DatePartType::MINUTE, 13),
  //
  //      // Note that we can support these DatePartTypes with and without
  //      // a trailing 's' at the end.
  //      std::make_pair(DatePartType::SECOND, 14),
  //      std::make_pair(DatePartType::SECONDS, 14),
  //      std::make_pair(DatePartType::MILLISECOND, 14999.999),
  //      std::make_pair(DatePartType::MILLISECONDS, 14999.999),
  //  };
  //
  //  for (auto x : data) {
  //    // these will be cleaned up by extract_expr
  //    auto part = expression::ExpressionUtil::ConstantValueFactory(
  //        type::ValueFactory::GetIntegerValue(static_cast<int>(x.first)));
  //    auto timestamp = expression::ExpressionUtil::ConstantValueFactory(
  //        type::ValueFactory::CastAsTimestamp(
  //            type::ValueFactory::GetVarcharValue(date)));
  //
  //    // these need unique ptrs to clean them
  //    auto extract_expr = ExpPtr(
  //        new expression::FunctionExpression("extract", {part, timestamp}));
  //
  //    expression::ExpressionUtil::TransformExpression(nullptr,
  //                                                    extract_expr.get());
  //
  //    // Perform evaluation and check the result matches
  //    // NOTE: We pass null schema because there are no tuple value
  //    expressions
  //    type::Value expected = type::ValueFactory::GetDecimalValue(x.second);
  //    type::Value result = extract_expr->Evaluate(nullptr, nullptr, nullptr);
  //    EXPECT_FALSE(result.IsNull());
  //    EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));
  //  }
}

TEST_F(ExpressionTests, SimpleCase) {

  // CASE WHEN i=1 THEN 2 ELSE 3 END

  // EXPRESSION
  auto tup_val_exp = new expression::TupleValueExpression(type::TypeId::INTEGER,
      0, 0);
  auto const_val_exp_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto const_val_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  auto const_val_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(3));
  auto *when_cond =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
          tup_val_exp, const_val_exp_1);

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(when_cond),
      expression::CaseExpression::AbsExprPtr(const_val_exp_2)));

  std::unique_ptr<expression::CaseExpression> case_expression(
      new expression::CaseExpression(
      type::TypeId::INTEGER, clauses,
      expression::CaseExpression::AbsExprPtr(const_val_exp_3)));

  // TUPLE
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i1", true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i2", true);
  columns.push_back(column1);
  columns.push_back(column2);
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema.get(), true));

  // Test with A = 1, should get 2
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  type::Value result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  type::Value expected = type::ValueFactory::GetIntegerValue(2);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));

  // Test with A = 2, should get 3
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(2), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  expected = type::ValueFactory::GetIntegerValue(3);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));
}

TEST_F(ExpressionTests, SimpleCaseCopyTest) {

  // CASE WHEN i=1 THEN 2 ELSE 3 END
  // EXPRESSION
  auto tup_val_exp = new expression::TupleValueExpression(type::TypeId::INTEGER,
      0, 0);
  auto const_val_exp_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto const_val_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  auto const_val_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(3));

  auto *when_cond =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
          tup_val_exp, const_val_exp_1);

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(when_cond),
      expression::CaseExpression::AbsExprPtr(const_val_exp_2)));

  std::unique_ptr<expression::CaseExpression> o_case_expression(
      new expression::CaseExpression(type::TypeId::INTEGER, clauses,
          expression::CaseExpression::AbsExprPtr(const_val_exp_3)));

  std::unique_ptr<expression::CaseExpression> case_expression(
      dynamic_cast<expression::CaseExpression *>(o_case_expression->Copy()));

  // TUPLE
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i1", true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i2", true);
  columns.push_back(column1);
  columns.push_back(column2);
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema.get(), true));

  // Test with A = 1, should get 2
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  type::Value result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  type::Value expected = type::ValueFactory::GetIntegerValue(2);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));

  // Test with A = 2, should get 3
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(2), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  expected = type::ValueFactory::GetIntegerValue(3);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));
}

TEST_F(ExpressionTests, SimpleCaseWithDefault) {

  // CASE i WHEN 1 THEN 2 ELSE 3 END

  // EXPRESSION
  auto tup_val_exp = new expression::TupleValueExpression(type::TypeId::INTEGER,
      0, 0);
  auto const_val_exp_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto const_val_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));
  auto const_val_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(3));

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(const_val_exp_1),
      expression::CaseExpression::AbsExprPtr(const_val_exp_2)));

  std::unique_ptr<expression::CaseExpression> case_expression(
      new expression::CaseExpression(type::TypeId::INTEGER,
      expression::CaseExpression::AbsExprPtr(tup_val_exp),
      clauses, expression::CaseExpression::AbsExprPtr(const_val_exp_3)));

  // TUPLE
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i1", true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "i2", true);
  columns.push_back(column1);
  columns.push_back(column2);
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema.get(), true));

  // Test with A = 1, should get 2
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  type::Value result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  type::Value expected = type::ValueFactory::GetIntegerValue(2);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));

  // Test with A = 2, should get 3
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(2), nullptr);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), nullptr);
  result = case_expression->Evaluate(tuple.get(), nullptr, nullptr);
  expected = type::ValueFactory::GetIntegerValue(3);
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));
}

}  // namespace test
}  // namespace peloton
