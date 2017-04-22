//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameterization_test.cpp
//
// Identification: test/codegen/parameterization_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/seq_scan_plan.h"
#include "expression/parameter_value_expression.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of query
// plans with parameters. All the tests use a single table created and loaded
// during SetUp().  The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 64 rows of random values.
//===----------------------------------------------------------------------===//

class ParameterizationTest : public PelotonCodeGenTest {
 public:
  ParameterizationTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

 private:
  uint32_t num_rows_to_insert = 64;
};

// Tests whether parameterization works for varchar type in const value expr
TEST_F(ParameterizationTest, TestConstVarCharParam) {
  //
  // SELECT d FROM table where d != "";
  //

  // 1) Setup the predicate
  auto* d_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::VARCHAR, 0, 3);
  std::string str = "";
  auto* const_str_exp = CodegenTestUtils::ConstVarCharExpression(str);
  auto* d_ne_str = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp, const_str_exp);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), d_ne_str, {0, 1, 2, 3}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

// Tests whether parameterization works for varchar type in param value expr
TEST_F(ParameterizationTest, TestNonConstVarCharParam) {
  //
  // SELECT d FROM table where d != "";
  //

  // 1) Setup the predicate
  auto* d_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::VARCHAR, 0, 3);
  std::string str = "";
  type::Value param_str = type::ValueFactory::GetVarcharValue(str);
  auto* param_str_exp = CodegenTestUtils::ParamExpression(0);
  auto* d_ne_str = new expression::ComparisonExpression(
        ExpressionType::COMPARE_NOTEQUAL, d_col_exp, param_str_exp);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), d_ne_str, {0, 1, 2, 3}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // 4) Collect params
  std::vector<type::Value> params = {param_str};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

// Tests whether parameterization works for conjuction with const value exprs
// (varchar included)
TEST_F(ParameterizationTest, TestConjunctionWithConstParams) {
  //
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  //

  // 1) Construct the components of the predicate

  // a >= 20
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  // b = 21
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_21_exp = CodegenTestUtils::ConstIntExpression(21);
  auto* b_eq_21 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

  // a >= 20 AND b = 21
  auto* conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), conj_eq, {0, 1, 2}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
                                type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(1).CompareEquals(
                                type::ValueFactory::GetIntegerValue(21)));
}

// Tests whether parameterization works for conjuction with param value exprs
TEST_F(ParameterizationTest, TestConjunctionWithNonConstParams) {
  //
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  //

  // 1) Construct the components of the predicate

  // a >= 20
  auto* a_col_exp =
    new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* param_20_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
    ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, param_20_exp);

  // d != ""
  auto* d_col_exp =
    new expression::TupleValueExpression(type::Type::TypeId::VARCHAR, 0, 3);
  std::string str = "";
  type::Value param_str = type::ValueFactory::GetVarcharValue(str);
  auto* param_str_exp = CodegenTestUtils::ParamExpression(1);
  auto* d_ne_str = new expression::ComparisonExpression(
    ExpressionType::COMPARE_NOTEQUAL, d_col_exp, param_str_exp);

  // a >= 20 AND d != ""
  auto* conj_eq = new expression::ConjunctionExpression(
    ExpressionType::CONJUNCTION_AND, a_gt_20, d_ne_str);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), conj_eq, {0, 1, 2, 3}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // 4) Collect params
  std::vector<type::Value> params = {param_a, param_str};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  ASSERT_EQ(NumRowsInTestTable() - 2, results.size());
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
    type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(type::CMP_FALSE, results[0].GetValue(3).CompareEquals(
    type::ValueFactory::GetVarcharValue(str)));
}


TEST_F(ParameterizationTest, TestColWithParamAddition) {
  //
  // SELECT a, b FROM table where b = a + ?;
  // ? = 1
  //

  // Construct the components of the predicate

  // a + ?
  auto *a_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* param_1_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(1);
  auto *a_plus_param = new expression::OperatorExpression(
        ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, a_col_exp,
        param_1_exp);

  // b = a + ?
  auto *b_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, b_col_exp, a_plus_param);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
        &GetTestTable(TestTableId()), b_eq_a_plus_param, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Collect params
  std::vector<type::Value> params = {param_a};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(ParameterizationTest, TestColWithParamSubtraction) {
  //
  // SELECT a, b FROM table where a = b - ?;
  // ? = 1
  //

  // Construct the components of the predicate

  // b - 1
  auto* b_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* param_1_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_b = type::ValueFactory::GetIntegerValue(1);
  auto* b_minus_param = new expression::OperatorExpression(
        ExpressionType::OPERATOR_MINUS, type::Type::TypeId::INTEGER, b_col_exp,
        param_1_exp);

  // a = b - ?
  auto* a_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_b_minus_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, a_col_exp, b_minus_param);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
        &GetTestTable(TestTableId()), a_eq_b_minus_param, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Collect params
  std::vector<type::Value> params = {param_b};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(ParameterizationTest, TestColWithParamDivision) {
  //
  //   SELECT a, b, c FROM table where a = a / ?;
  //   ? = 2
  //

  // Construct the components of the predicate

  // a / ?
  auto* a_rhs_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* param_2_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(2);
  auto* a_div_param = new expression::OperatorExpression(
        ExpressionType::OPERATOR_DIVIDE, type::Type::TypeId::INTEGER,
        a_rhs_col_exp, param_2_exp);

  // a = a / ?
  auto* a_lhs_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_a_div_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, a_lhs_col_exp, a_div_param);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
        &GetTestTable(TestTableId()), a_eq_a_div_param, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Collect params
  std::vector<type::Value> params = {param_a};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results - only one output tuple (with a == 0)
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(ParameterizationTest, TestColWithParamMultiplication) {
  //
  // SELECT a, b, c FROM table where a * ? = a * b;
  // ? = 1
  //

  // Construct the components of the predicate

  // a * b
  auto* a_rhs_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* b_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* a_mul_b = new expression::OperatorExpression(
        ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::BIGINT,
        a_rhs_col_exp, b_col_exp);

  // a * ?
  auto* a_lhs_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* param_1_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(1);
  auto* a_mul_param = new expression::OperatorExpression(
        ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::BIGINT,
        a_lhs_col_exp, param_1_exp);

  // a * ? = a * b
  auto* a_mul_param_eq_a_mul_b = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, a_mul_param, a_mul_b);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
        &GetTestTable(TestTableId()), a_mul_param_eq_a_mul_b, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Collect params
  std::vector<type::Value> params = {param_a};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(ParameterizationTest, TestColWithParamModulo) {
  //
  // SELECT a, b, c FROM table where a = b % ?;
  // ? = 1
  //

  // Construct the components of the predicate

  // b % ?
  auto* b_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* param_1_exp = CodegenTestUtils::ParamExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(1);
  auto* b_mod_param = new expression::OperatorExpression(
        ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, b_col_exp,
        param_1_exp);

  // a = b % ?
  auto* a_col_exp =
        new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_b_mod_param = new expression::ComparisonExpression(
        ExpressionType::COMPARE_EQUAL, a_col_exp, b_mod_param);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
        &GetTestTable(TestTableId()), a_eq_b_mod_param, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Collect params
  std::vector<type::Value> params = {param_a};

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &params);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
        type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(1).CompareEquals(
        type::ValueFactory::GetIntegerValue(1)));
}

}  // namespace test
}  // namespace peloton