//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator_test.cpp
//
// Identification: test/codegen/update_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "planner/abstract_plan.h"
#include "planner/create_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/plan_util.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace test {

class UpdateTranslatorTest : public PelotonCodeGenTest {
 public:
  UpdateTranslatorTest() : PelotonCodeGenTest() {}

  oid_t TestTableId1() { return test_table_oids[0]; }
  oid_t TestTableId2() { return test_table_oids[1]; }
  oid_t TestTableId5() { return test_table_oids[4]; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

 private:
  uint32_t num_rows_to_insert = 10;
};

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstant) {
  LoadTestTable(TestTableId1(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId1());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), nullptr, {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{0,
            planner::DerivedAttribute{
                expression::ExpressionUtil::ConstantValueFactory(
                    type::ValueFactory::GetIntegerValue(1))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{1, {0, 1}}, {2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId1()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() * 2, table->GetTupleCount());

  // Setup the scan plan node
  std::unique_ptr<planner::SeqScanPlan> scan_plan_1(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), nullptr, {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_1->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_1, buffer_1);

  // Check that we got all the results
  auto &results_1 = buffer_1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(2)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(3).CompareEquals(
                                     type::ValueFactory::GetVarcharValue("3")));
  EXPECT_EQ(CmpBool::TRUE, results_1[9].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_1[9].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(91)));
  EXPECT_EQ(CmpBool::TRUE, results_1[9].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(92)));
  EXPECT_EQ(CmpBool::TRUE,
            results_1[9].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("93")));
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstantAndPredicate) {
  LoadTestTable(TestTableId2(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId2());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  ExpressionPtr b_eq_41 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(41));

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), b_eq_41.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{1,
            planner::DerivedAttribute{
                expression::ExpressionUtil::ConstantValueFactory(
                    type::ValueFactory::GetIntegerValue(49))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{0, {0, 0}}, {2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId2()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 1, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr b_eq_49 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(49));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_2(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), b_eq_49.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_2->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_2, buffer_1);

  // Check that we got all the results
  auto &results_1 = buffer_1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(40)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(49)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(42)));
  EXPECT_EQ(CmpBool::TRUE,
            results_1[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("43")));
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAnOperatorExpression) {
  LoadTestTable(TestTableId2(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId2());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  ExpressionPtr b_eq_41 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(41));

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), b_eq_41.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  auto c_val_9 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(9));
  auto tuple_val_expr = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 0);
  expression::AbstractExpression *op_expr =
      expression::ExpressionUtil::OperatorFactory(ExpressionType::OPERATOR_PLUS,
                                                  type::TypeId::INTEGER,
                                                  tuple_val_expr, c_val_9);
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{1, planner::DerivedAttribute{op_expr}}},
          // expression::ExpressionUtil::ConstantValueFactory(
          //   type::ValueFactory::GetIntegerValue(49))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{0, {0, 0}}, {2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId2()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 1, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr b_eq_49 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(49));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_2(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), b_eq_49.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_2->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_2, buffer_1);

  // Check that we got all the results
  auto &results_1 = buffer_1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(40)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(49)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(42)));
  EXPECT_EQ(CmpBool::TRUE,
            results_1[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("43")));
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAnOperatorExpressionComplex) {
  LoadTestTable(TestTableId2(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId2());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  ExpressionPtr b_eq_41 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(41));

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), b_eq_41.release(), {0, 1, 2, 3}));

  // Transform using a projection
  auto c_val_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto tuple_val_expr = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 0);
  expression::AbstractExpression *op_expr =
      expression::ExpressionUtil::OperatorFactory(ExpressionType::OPERATOR_PLUS,
                                                  type::TypeId::INTEGER,
                                                  tuple_val_expr, c_val_1);

  auto tuple_val_expr_1 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 0);
  auto tuple_val_expr_2 = expression::ExpressionUtil::TupleValueFactory(
      type::TypeId::INTEGER, 0, 1);
  expression::AbstractExpression *op_expr_2 =
      expression::ExpressionUtil::OperatorFactory(
          ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER,
          tuple_val_expr_1, tuple_val_expr_2);

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{0, planner::DerivedAttribute{op_expr}},
           {1, planner::DerivedAttribute{op_expr_2}}},
          // expression::ExpressionUtil::ConstantValueFactory(
          //   type::ValueFactory::GetIntegerValue(49))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId2()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 1, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr a_eq_41 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(41));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_2(new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), a_eq_41.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_2->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_2, buffer_1);

  // Check that we got all the results
  auto &results_1 = buffer_1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(41)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(81)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(42)));
  EXPECT_EQ(CmpBool::TRUE,
            results_1[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("43")));
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstantPrimary) {
  LoadTestTable(TestTableId5(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId5());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  ExpressionPtr a_eq_10 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId5()), a_eq_10.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{0,
            planner::DerivedAttribute{
                expression::ExpressionUtil::ConstantValueFactory(
                    type::ValueFactory::GetIntegerValue(1))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{1, {0, 1}}, {2, {0, 2}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId5()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 2, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr a_eq_1 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(1));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_5(new planner::SeqScanPlan(
      &GetTestTable(TestTableId5()), a_eq_1.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_5->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_5{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_5, buffer_5);

  // Check that we got all the results
  auto &results_5 = buffer_5.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_5[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_5[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(11)));
  EXPECT_EQ(CmpBool::TRUE, results_5[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(12)));
  EXPECT_EQ(CmpBool::TRUE,
            results_5[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("13")));
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithCast) {
  LoadTestTable(TestTableId1(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId1());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  // Get the scan plan without a predicate with four columns
  ExpressionPtr a_eq_10 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), a_eq_10.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{2,
            planner::DerivedAttribute{
                expression::ExpressionUtil::ConstantValueFactory(
                    type::ValueFactory::GetDecimalValue(2.0))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{0, {0, 0}}, {1, {0, 1}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId1()), std::move(project_info)));

  // Add the scan to the update plan
  update_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  update_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{}, context};

  // COMPILE and execute
  CompileAndExecute(*update_plan, buffer);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 1, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr a_eq_10_1 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_1(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), a_eq_10_1.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_1;
  scan_plan_1->PerformBinding(context_1);

  // Printing consumer
  codegen::BufferingConsumer buffer_1{{0, 1, 2, 3}, context_1};

  // COMPILE and execute
  CompileAndExecute(*scan_plan_1, buffer_1);

  // Check that we got all the results
  auto &results_1 = buffer_1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(10)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(11)));
  EXPECT_EQ(CmpBool::TRUE, results_1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(2)));
  EXPECT_EQ(CmpBool::TRUE,
            results_1[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("13")));

  // Get the scan plan without a predicate with four columns
  ExpressionPtr a_eq_10_2 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_2(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), a_eq_10_2.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info_2(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{2,
            planner::DerivedAttribute{
                expression::ExpressionUtil::ConstantValueFactory(
                    type::ValueFactory::GetIntegerValue(3))}}},
          // Direct Map List : [(oid_t, (oid_t, oid_t))]
          // Specify columns that are directly pulled from the original tuple.
          {{0, {0, 0}}, {1, {0, 1}}, {3, {0, 3}}}));

  // Embed the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan_2(new planner::UpdatePlan(
      &GetTestTable(TestTableId1()), std::move(project_info_2)));

  // Add the scan to the update plan
  update_plan_2->AddChild(std::move(scan_plan_2));

  // Do binding
  planner::BindingContext context_2;
  update_plan_2->PerformBinding(context_2);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer_2{{}, context_2};

  // COMPILE and execute
  CompileAndExecute(*update_plan_2, buffer_2);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable() + 2, table->GetTupleCount());

  // Setup the scan plan node
  ExpressionPtr a_eq_10_3 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));
  std::unique_ptr<planner::SeqScanPlan> scan_plan_3(new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), a_eq_10_3.release(), {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context_3;
  scan_plan_3->PerformBinding(context_3);

  // Printing consumer
  codegen::BufferingConsumer buffer_3{{0, 1, 2, 3}, context_3};

  CompileAndExecute(*scan_plan_3, buffer_3);

  // Check that we got all the results
  auto &results_3 = buffer_3.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_3[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(10)));
  EXPECT_EQ(CmpBool::TRUE, results_3[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(11)));
  EXPECT_EQ(CmpBool::TRUE, results_3[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(3)));
  EXPECT_EQ(CmpBool::TRUE,
            results_3[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("13")));
}

}  // namespace test
}  // namespace peloton
