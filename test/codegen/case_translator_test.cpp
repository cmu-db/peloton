//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator_test.cpp
//
// Identification: test/codegen/case_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "expression/case_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/seq_scan_plan.h"
#include "planner/projection_plan.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of table
// scan query plans. All the tests use a single table created and loaded during
// SetUp().  The schema of the table is as follows:
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

class CaseTranslatorTest : public PelotonCodeGenTest {
 public:
  CaseTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(CaseTranslatorTest, SimpleCase) {

  //
  // SELECT a, case when a=10 then 1 when else 0 FROM table;
  //

  // Setup the when clause
  auto *const_val_exp_10 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(10));
  auto *const_val_exp_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto *const_val_exp_0 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(0));

  // TVE
  auto *tve = new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);

  // Make one When condition
  auto *when_cond =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
          tve, const_val_exp_10);
  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(when_cond),
      expression::CaseExpression::AbsExprPtr(const_val_exp_1)));

  // Set up CASE with all the When's and the default value
  expression::CaseExpression *case_expr = new expression::CaseExpression(
      type::Type::INTEGER, clauses,
      expression::CaseExpression::AbsExprPtr(const_val_exp_0));

  // Setup a projection
  DirectMapList direct_map_list = {{0, {0, 0}}};
  TargetList target_list;
  planner::DerivedAttribute attribute;
  attribute.expr = case_expr;
  attribute.attribute_info.type = case_expr->GetValueType();
  Target target = std::make_pair(1, attribute);
  target_list.push_back(target);
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list))};

  // Setup the plan nodes
  std::unique_ptr<planner::SeqScanPlan> scan(
    new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0}));

  std::shared_ptr<catalog::Schema> sp_table_schema{
      new catalog::Schema(*GetTestTable(TestTableId()).GetSchema())};
  planner::ProjectionPlan projection {std::move(proj_info), sp_table_schema};
  projection.AddChild(std::move(scan));

  // Bind the context 
  planner::BindingContext context;
  projection.PerformBinding(context);

  // Set up the output consumer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(projection, buffer, 
                    reinterpret_cast<char*>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) == type::CMP_TRUE);
  EXPECT_TRUE(results[0].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) == type::CMP_TRUE);
  EXPECT_TRUE(results[1].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(10)) == type::CMP_TRUE);
  EXPECT_TRUE(results[1].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(1)) == type::CMP_TRUE);
}

TEST_F(CaseTranslatorTest, SimpleCaseMoreWhen) {

  //
  // SELECT a, case when a=10 then 1 when a=20 then 2 else 0 FROM table;
  //

  // Setup the when clause
  auto *const_val_exp_10 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(10));
  auto *const_val_exp_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto *const_val_exp_0 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(0));
  auto *const_val_exp_20 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(20));
  auto *const_val_exp_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(2));

  // TVE
  auto *tve_1 = new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);
  auto *tve_2 = new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);

  std::vector<expression::CaseExpression::WhenClause> clauses;

  // Make one When condition
  auto *when_cond_1 =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                           tve_1, const_val_exp_10);
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(when_cond_1),
      expression::CaseExpression::AbsExprPtr(const_val_exp_1)));

  // Make another When condition
  auto *when_cond_2 =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                           tve_2, const_val_exp_20);
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbsExprPtr(when_cond_2),
      expression::CaseExpression::AbsExprPtr(const_val_exp_2)));

  // Set up CASE with all the When's and the default value
  expression::CaseExpression *case_expr = new expression::CaseExpression(
      type::Type::INTEGER, clauses,
      expression::CaseExpression::AbsExprPtr(const_val_exp_0));

  // Setup a projection
  DirectMapList direct_map_list = {{0, {0, 0}}};
  TargetList target_list;
  planner::DerivedAttribute attribute;
  attribute.expr = case_expr;
  attribute.attribute_info.type = case_expr->GetValueType();
  Target target = std::make_pair(1, attribute);
  target_list.push_back(target);
  std::unique_ptr<planner::ProjectInfo> proj_info{
      new planner::ProjectInfo(std::move(target_list), 
                               std::move(direct_map_list))};

  // Setup the plan nodes
  std::unique_ptr<planner::SeqScanPlan> scan(
    new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0}));

  std::shared_ptr<catalog::Schema> sp_table_schema{
      new catalog::Schema(*GetTestTable(TestTableId()).GetSchema())};
  planner::ProjectionPlan projection {std::move(proj_info), sp_table_schema};
  projection.AddChild(std::move(scan));

  // Bind the context 
  planner::BindingContext context;
  projection.PerformBinding(context);

  // Set up the output consumer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(projection, buffer, 
                    reinterpret_cast<char*>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) == type::CMP_TRUE);
  EXPECT_TRUE(results[0].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) == type::CMP_TRUE);
  EXPECT_TRUE(results[1].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(10)) == type::CMP_TRUE);
  EXPECT_TRUE(results[1].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(1)) == type::CMP_TRUE);
  EXPECT_TRUE(results[2].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(20)) == type::CMP_TRUE);
  EXPECT_TRUE(results[2].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(2)) == type::CMP_TRUE);
}

}  // namespace test
}  // namespace peloton
