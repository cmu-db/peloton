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
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"
#include "planner/projection_plan.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class CaseTranslatorTest : public PelotonCodeGenTest {
 public:
  CaseTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  oid_t TestTableId() { return test_table_oids[0]; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(CaseTranslatorTest, SimpleCase) {
  //
  // SELECT a, case when a=10 then 1 when else 0 FROM table;
  //

  // Make one When condition
  auto when_a_eq_10 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause{
      std::move(when_a_eq_10), ConstIntExpr(1)});

  // Set up CASE with all the When's and the default value
  expression::CaseExpression *case_expr = new expression::CaseExpression(
      type::TypeId::INTEGER, clauses, ConstIntExpr(0));

  // Setup a projection
  DirectMapList direct_map_list = {{0, {0, 0}}};
  TargetList target_list;
  planner::DerivedAttribute attribute{case_expr};
  Target target = std::make_pair(1, attribute);
  target_list.push_back(target);
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list))};

  // Setup the plan nodes
  std::unique_ptr<planner::SeqScanPlan> scan(
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0}));

  std::shared_ptr<catalog::Schema> sp_table_schema{
      new catalog::Schema(*GetTestTable(TestTableId()).GetSchema())};
  planner::ProjectionPlan projection{std::move(proj_info), sp_table_schema};
  projection.AddChild(std::move(scan));

  // Bind the context
  planner::BindingContext context;
  projection.PerformBinding(context);

  // Set up the output consumer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(projection, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[0].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[1].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(10)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[1].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(1)) ==
              CmpBool::TRUE);

  for (uint32_t i = 2; i < NumRowsInTestTable(); i++) {
    EXPECT_TRUE(results[i].GetValue(1).CompareEquals(
                    type::ValueFactory::GetBigIntValue(0)) ==
                CmpBool::TRUE);
  }
}

TEST_F(CaseTranslatorTest, SimpleCaseMoreWhen) {
  //
  // SELECT a, case when a=10 then 1 when a=20 then 2 else 0 FROM table;
  //

  // Make the when conditions
  auto when_a_eq_10 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));
  auto when_a_eq_20 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.emplace_back(std::move(when_a_eq_10), ConstIntExpr(1));
  clauses.emplace_back(std::move(when_a_eq_20), ConstIntExpr(2));

  // Set up CASE with all the When's and the default value
  expression::CaseExpression *case_expr = new expression::CaseExpression(
      type::TypeId::INTEGER, clauses, ConstIntExpr(0));

  // Setup a projection
  DirectMapList direct_map_list = {{0, {0, 0}}};
  TargetList target_list;
  planner::DerivedAttribute attribute{case_expr};
  Target target = std::make_pair(1, attribute);
  target_list.push_back(target);
  std::unique_ptr<planner::ProjectInfo> proj_info{new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list))};

  // Setup the plan nodes
  std::unique_ptr<planner::SeqScanPlan> scan(
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0}));

  std::shared_ptr<catalog::Schema> sp_table_schema{
      new catalog::Schema(*GetTestTable(TestTableId()).GetSchema())};
  planner::ProjectionPlan projection{std::move(proj_info), sp_table_schema};
  projection.AddChild(std::move(scan));

  // Bind the context
  planner::BindingContext context;
  projection.PerformBinding(context);

  // Set up the output consumer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(projection, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
  EXPECT_TRUE(results[0].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[0].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(0)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[1].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(10)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[1].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(1)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[2].GetValue(0).CompareEquals(
                  type::ValueFactory::GetBigIntValue(20)) ==
              CmpBool::TRUE);
  EXPECT_TRUE(results[2].GetValue(1).CompareEquals(
                  type::ValueFactory::GetBigIntValue(2)) ==
              CmpBool::TRUE);
}

}  // namespace test
}  // namespace peloton
