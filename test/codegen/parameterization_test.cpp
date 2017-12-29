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
#include "codegen/testing_codegen_util.h"
#include "common/harness.h"
#include "expression/conjunction_expression.h"
#include "expression/comparison_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace test {

class ParameterizationTest : public PelotonCodeGenTest {
 public:
  ParameterizationTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  oid_t TestTableId() { return test_table_oids[0]; }

 private:
  uint32_t num_rows_to_insert = 64;
};

// Tests whether parameterization works for varchar type in Constant Value Expr
// (1) Query with a varchar constant
// (2) Query with a different varchar on the previously cached compiled query
TEST_F(ParameterizationTest, ConstParameterVarchar) {
  // SELECT d FROM table where d != "";
  auto *d_col_exp =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  auto *const_str_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue(""));
  auto *d_ne_str = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp, const_str_exp);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), d_ne_str, {0, 1, 2, 3}}};

  planner::BindingContext context;
  scan->PerformBinding(context);

  codegen::BufferingConsumer buffer{{3}, context};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached);
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_FALSE(cached);

  // SELECT d FROM table where d != "test";
  auto *d_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  auto *const_str_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue("test"));
  auto *d_ne_str_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp_2, const_str_exp_2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), d_ne_str_2, {0, 1, 2, 3}}};

  planner::BindingContext context_2;
  scan_2->PerformBinding(context_2);

  codegen::BufferingConsumer buffer_2{{3}, context_2};

  CompileAndExecuteCache(scan_2, buffer_2, cached);

  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results_2.size());
  EXPECT_TRUE(cached);
}

// Tests whether parameterization works for varchar type in Parameter Value Expr
// (1) Query with a varchar parameter
// (2) Query with a different varchar on the previously cached compiled query
// (3) Query with a const parameter with the same value
// The last query attemts to see whether query with a const paraemeter is
// distinct from (2)
TEST_F(ParameterizationTest, ParamParameterVarchar) {
  // SELECT d FROM table where d != ?, with ? = "";
  auto *d_col_exp =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  type::Value param_str = type::ValueFactory::GetVarcharValue("");
  auto *param_str_exp = new expression::ParameterValueExpression(0);
  auto *d_ne_str = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp, param_str_exp);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), d_ne_str, {0, 1, 2, 3}}};

  planner::BindingContext context;
  scan->PerformBinding(context);

  std::vector<type::Value> params = {param_str};

  codegen::BufferingConsumer buffer{{3}, context};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached, params);

  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_FALSE(cached);

  // SELECT d FROM table where d != ?, with ? = "empty";
  auto *d_col_exp2 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  type::Value param_str2 = type::ValueFactory::GetVarcharValue("empty");
  auto *param_str_exp2 = new expression::ParameterValueExpression(0);
  auto *d_ne_str2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp2, param_str_exp2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), d_ne_str2, {0, 1, 2, 3}}};

  planner::BindingContext context2;
  scan_2->PerformBinding(context);

  std::vector<type::Value> params_2 = {param_str2};

  codegen::BufferingConsumer buffer_2{{3}, context};

  CompileAndExecuteCache(scan_2, buffer_2, cached, params_2);

  const auto &results2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results2.size());
  EXPECT_TRUE(cached);

  // SELECT d FROM table where d != "empty";
  auto *d_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  auto *const_str_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue("empty"));
  auto *d_ne_str_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp_3, const_str_exp_3);

  std::shared_ptr<planner::SeqScanPlan> scan_3{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), d_ne_str_3, {0, 1, 2, 3}}};

  planner::BindingContext context_3;
  scan_3->PerformBinding(context_3);

  codegen::BufferingConsumer buffer_3{{3}, context_3};

  CompileAndExecuteCache(scan_3, buffer_3, cached);

  const auto &results_3 = buffer_3.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results_3.size());
  EXPECT_TRUE(cached);
}

// Tests whether parameterization works for conjuction with Const Value Exprs
// (1) Query with const integer parameters
// (2) Query with differnt consts on the previously cached compiled query
TEST_F(ParameterizationTest, ConstParameterWithConjunction) {
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_20_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(20));
  auto *a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *const_21_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(21));
  auto *b_eq_21 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

  // a >= 20 AND b = 21
  auto *conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq, {0, 1, 2}}};
  planner::BindingContext context;
  scan->PerformBinding(context);
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached);

  const auto &results = buffer.GetOutputTuples();
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(21)));
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetDecimalValue(22)));

  // SELECT a, b, c FROM table where a >= 30 and b = 31;
  auto *a_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_30_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(30));
  auto *a_gt_30_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_2,
      const_30_exp_2);

  auto *b_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *const_31_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(31));
  auto *b_eq_31_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp_2, const_31_exp_2);

  // a >= 30 AND b = 31
  auto *conj_eq_2 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_31_2, a_gt_30_2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq_2, {0, 1, 2}}};
  planner::BindingContext context_2;
  scan_2->PerformBinding(context_2);
  codegen::BufferingConsumer buffer_2{{0, 1, 2}, context_2};

  CompileAndExecuteCache(scan_2, buffer_2, cached);

  const auto &results_2 = buffer_2.GetOutputTuples();
  ASSERT_EQ(1, results_2.size());
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(30)));
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(31)));
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetDecimalValue(32)));
  EXPECT_TRUE(cached);

  // SELECT a, b, c FROM table where a >= 30 and b = null;
  auto *a_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_30_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(30));
  auto *a_gt_30_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_3,
      const_30_exp_3);

  auto *b_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *const_31_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetNullValueByType(peloton::type::TypeId::INTEGER));
  auto *b_eq_31_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp_3, const_31_exp_3);

  // a >= 30 AND b = null
  auto *conj_eq_3 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_31_3, a_gt_30_3);

  std::shared_ptr<planner::SeqScanPlan> scan_3{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq_3, {0, 1, 2}}};
  planner::BindingContext context_3;
  scan_3->PerformBinding(context_3);
  codegen::BufferingConsumer buffer_3{{0, 1, 2}, context_3};

  CompileAndExecuteCache(scan_3, buffer_3, cached);

  const auto &results_3 = buffer_3.GetOutputTuples();
  ASSERT_EQ(0, results_3.size());
  EXPECT_FALSE(cached);
}

// Tests whether parameterization works for conjuction with Param Value Exprs
// (1) Query with param parameters
// (2) Query with different param values on the previously cached compiled query
// (2) Query with the same plan, but with a param value changed to a contant
TEST_F(ParameterizationTest, ParamParameterWithConjunction) {
  // SELECT a, b, c FROM table where a >= 20 and d != "";
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *param_20_exp = new expression::ParameterValueExpression(0);
  type::Value param_a = type::ValueFactory::GetIntegerValue(20);
  auto *a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, param_20_exp);

  auto *d_col_exp =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  type::Value param_str = type::ValueFactory::GetVarcharValue("");
  auto *param_str_exp = new expression::ParameterValueExpression(1);
  auto *d_ne_str = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp, param_str_exp);

  // a >= 20 AND d != ""
  auto *conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, a_gt_20, d_ne_str);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq, {0, 1, 2, 3}}};
  planner::BindingContext context;
  scan->PerformBinding(context);
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  std::vector<type::Value> params = {param_a, param_str};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached, params);

  const auto &results = buffer.GetOutputTuples();
  ASSERT_EQ(NumRowsInTestTable() - 2, results.size());
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(CmpBool::FALSE, results[0].GetValue(3).CompareEquals(
                                      type::ValueFactory::GetVarcharValue("")));
  EXPECT_FALSE(cached);

  // SELECT a, b, c FROM table where a >= 30 and d != "empty";
  auto *a_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *param_30_exp_2 = new expression::ParameterValueExpression(0);
  type::Value param_a_2 = type::ValueFactory::GetIntegerValue(30);
  auto *a_gt_30_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_2,
      param_30_exp_2);

  auto *d_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  type::Value param_str_2 = type::ValueFactory::GetVarcharValue("empty");
  auto *param_str_exp_2 = new expression::ParameterValueExpression(1);
  auto *d_ne_str_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp_2, param_str_exp_2);

  // a >= 30 AND d != "empty"
  auto *conj_eq_2 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, a_gt_30_2, d_ne_str_2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq_2, {0, 1, 2, 3}}};
  planner::BindingContext context_2;
  scan_2->PerformBinding(context_2);
  codegen::BufferingConsumer buffer_2{{0, 1, 2, 3}, context_2};

  std::vector<type::Value> params_2 = {param_a_2, param_str_2};

  CompileAndExecuteCache(scan_2, buffer_2, cached, params_2);

  const auto &results_2 = buffer_2.GetOutputTuples();
  ASSERT_EQ(NumRowsInTestTable() - 3, results_2.size());
  EXPECT_EQ(CmpBool::TRUE, results_2[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(30)));
  EXPECT_EQ(CmpBool::FALSE,
            results_2[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("empty")));
  EXPECT_TRUE(cached);

  // SELECT a, b, c FROM table where a >= 30 and d != "empty";
  auto *a_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_30_exp_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(30));
  auto *a_gt_30_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_3,
      const_30_exp_3);

  auto *d_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  type::Value param_str_3 = type::ValueFactory::GetVarcharValue("empty");
  auto *param_str_exp_3 = new expression::ParameterValueExpression(0);
  auto *d_ne_str_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp_3, param_str_exp_3);

  // a >= 30 AND d != "empty"
  auto *conj_eq_3 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, a_gt_30_3, d_ne_str_3);

  std::shared_ptr<planner::SeqScanPlan> scan_3{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq_3, {0, 1, 2, 3}}};
  planner::BindingContext context_3;
  scan_3->PerformBinding(context_3);
  codegen::BufferingConsumer buffer_3{{0, 1, 2, 3}, context_3};

  std::vector<type::Value> params_3 = {param_str_3};

  CompileAndExecuteCache(scan_3, buffer_3, cached, params_3);

  const auto &results_3 = buffer_3.GetOutputTuples();
  ASSERT_EQ(NumRowsInTestTable() - 3, results_3.size());
  EXPECT_EQ(CmpBool::TRUE, results_3[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(30)));
  EXPECT_EQ(CmpBool::FALSE,
            results_3[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("empty")));
  EXPECT_TRUE(cached);

  // SELECT a, b, c FROM table where a >= ? and d != "empty";
  auto *d_col_exp_4 =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0, 3);
  auto *const_empty_exp_4 = new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue("empty"));
  auto *d_ne_str_4 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, d_col_exp_4, const_empty_exp_4);

  auto *a_col_exp_4 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  type::Value param_int_4 = type::ValueFactory::GetIntegerValue(30);
  auto *param_int_exp_4 = new expression::ParameterValueExpression(0);
  auto *a_gt_30_4 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp_4,
      param_int_exp_4);

  // a >= 30 AND d != "empty"
  auto *conj_eq_4 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, a_gt_30_4, d_ne_str_4);

  std::shared_ptr<planner::SeqScanPlan> scan_4{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), conj_eq_4, {0, 1, 2, 3}}};
  planner::BindingContext context_4;
  scan_4->PerformBinding(context_4);
  codegen::BufferingConsumer buffer_4{{0, 1, 2, 3}, context_4};

  std::vector<type::Value> params_4 = {param_int_4};

  CompileAndExecuteCache(scan_4, buffer_4, cached, params_4);

  const auto &results_4 = buffer_4.GetOutputTuples();
  ASSERT_EQ(NumRowsInTestTable() - 3, results_3.size());
  EXPECT_EQ(CmpBool::TRUE, results_4[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(30)));
  EXPECT_EQ(CmpBool::FALSE,
            results_4[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("empty")));
  EXPECT_TRUE(cached);
}

// (1) Check to use a Parameter Value Expression
// (2) Set a different value on the cached query
// (3) Query a similar one, but with a different operator expression
TEST_F(ParameterizationTest, ParamParameterWithOperators) {
  // (1) Check to use a Parameter Value Expression
  // SELECT a, b FROM table where b = a + ?;
  // b = a + ?
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *param_1_exp = new expression::ParameterValueExpression(0);
  auto *a_plus_param = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, a_col_exp,
      param_1_exp);
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_param = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_plus_param);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), b_eq_a_plus_param, {0, 1}}};

  planner::BindingContext context;
  scan->PerformBinding(context);

  // b = a + 1
  type::Value param_a = type::ValueFactory::GetIntegerValue(1);
  std::vector<type::Value> params = {param_a};

  codegen::BufferingConsumer buffer{{0, 1}, context};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached, params);

  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_FALSE(cached);

  // (2) Set a different value on the cached query
  auto *a_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *param_1_exp_2 = new expression::ParameterValueExpression(0);
  auto *a_plus_param_2 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, a_col_exp_2,
      param_1_exp_2);
  auto *b_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_param_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp_2, a_plus_param_2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), b_eq_a_plus_param_2, {0, 1}}};

  planner::BindingContext context_2;
  scan_2->PerformBinding(context_2);

  // b = a + 2
  type::Value param_a_2 = type::ValueFactory::GetIntegerValue(2);
  std::vector<type::Value> params_2 = {param_a_2};

  codegen::BufferingConsumer buffer_2{{0, 1}, context_2};

  CompileAndExecuteCache(scan_2, buffer_2, cached, params_2);

  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(0, results_2.size());
  EXPECT_TRUE(cached);

  // (3) Query a similar one, but with a different operator expression
  // a = b - 1
  auto *b_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *param_1_exp_3 = new expression::ParameterValueExpression(0);
  type::Value param_b_3 = type::ValueFactory::GetIntegerValue(1);
  auto *b_minus_param_3 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INTEGER, b_col_exp_3,
      param_1_exp_3);
  auto *a_col_exp_3 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *a_eq_b_minus_param_3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp_3, b_minus_param_3);

  std::shared_ptr<planner::SeqScanPlan> scan_3{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), a_eq_b_minus_param_3, {0, 1}}};

  planner::BindingContext context_3;
  scan_3->PerformBinding(context_3);

  codegen::BufferingConsumer buffer_3{{0, 1}, context_3};

  std::vector<type::Value> params_3 = {param_b_3};

  CompileAndExecuteCache(scan_3, buffer_3, cached, params_3);
  const auto &results_3 = buffer_3.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results_3.size());
  EXPECT_FALSE(cached);
}

TEST_F(ParameterizationTest, ParamParameterWithOperatersLeftHand) {
  // SELECT a, b, c FROM table where a * 1 = a * b;
  auto *a_lhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_1_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto *a_mul_param = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::TypeId::BIGINT, a_lhs_col_exp,
      const_1_exp);

  auto *a_rhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *a_mul_b = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::TypeId::BIGINT, a_rhs_col_exp,
      b_col_exp);

  auto *a_mul_param_eq_a_mul_b = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mul_param, a_mul_b);

  std::shared_ptr<planner::SeqScanPlan> scan{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), a_mul_param_eq_a_mul_b, {0, 1, 2}}};
  planner::BindingContext context;
  scan->PerformBinding(context);
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  bool cached;
  CompileAndExecuteCache(scan, buffer, cached);

  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
  EXPECT_FALSE(cached);

  // SELECT a, b, c FROM table where a * ? = a * b; with ? = 1
  auto *a_lhs_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *param_1_exp_2 = new expression::ParameterValueExpression(0);
  type::Value param_a_2 = type::ValueFactory::GetIntegerValue(1);
  auto *a_mul_param_2 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::TypeId::BIGINT, a_lhs_col_exp_2,
      param_1_exp_2);

  auto *a_rhs_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_col_exp_2 =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *a_mul_b_2 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::TypeId::BIGINT, a_rhs_col_exp_2,
      b_col_exp_2);

  auto *a_mul_param_eq_a_mul_b_2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mul_param_2, a_mul_b_2);

  std::shared_ptr<planner::SeqScanPlan> scan_2{new planner::SeqScanPlan{
      &GetTestTable(TestTableId()), a_mul_param_eq_a_mul_b_2, {0, 1, 2}}};
  planner::BindingContext context_2;
  scan->PerformBinding(context_2);
  codegen::BufferingConsumer buffer_2{{0, 1, 2}, context_2};

  std::vector<type::Value> params_2 = {param_a_2};

  CompileAndExecuteCache(scan_2, buffer_2, cached, params_2);

  const auto &results_2 = buffer_2.GetOutputTuples();
  EXPECT_EQ(1, results_2.size());
  EXPECT_TRUE(cached);
}

}  // namespace test
}  // namespace peloton
