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

TEST_F(ParameterizationTest, ScanWithoutParam) {
  //
  // SELECT a, b, c FROM table;
  //

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(ParameterizationTest, ScanWithConstIntParam) {
  //
  // SELECT a, b, c FROM table where a >= 20;
  //

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), a_gt_20, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 2, results.size());
}

TEST_F(ParameterizationTest, ScanWithConstVarCharParam) {
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

TEST_F(ParameterizationTest, ScanWithMultiConstParams) {
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

TEST_F(ParameterizationTest, ScanWithMultiNonConstParams) {
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
}  // namespace test
}  // namespace peloton