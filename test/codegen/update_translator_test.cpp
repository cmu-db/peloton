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
          {{0, planner::DerivedAttribute{
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
  CompileAndExecute(*update_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable()*2, table->GetTupleCount());
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstantAndPredicate) {
  LoadTestTable(TestTableId2(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId2());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  std::unique_ptr<expression::AbstractExpression> b_eq_41 =
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
          {{1, planner::DerivedAttribute{
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
  CompileAndExecute(*update_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable()+1, table->GetTupleCount());
}

TEST_F(UpdateTranslatorTest, UpdateColumnsWithAConstantPrimary) {
  LoadTestTable(TestTableId5(), NumRowsInTestTable());

  // SET a = 1;
  storage::DataTable *table = &this->GetTestTable(this->TestTableId5());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_DEBUG("%s", table->GetInfo().c_str());

  // Pre-condition
  EXPECT_EQ(NumRowsInTestTable(), table->GetTupleCount());

  std::unique_ptr<expression::AbstractExpression> b_eq_40 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(10));

  // Get the scan plan without a predicate with four columns
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId5()), b_eq_40.release(), {0, 1, 2, 3}));

  // Transform using a projection
  // Column 0 of the updated tuple will have constant value 1
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(
          // Target List : [(oid_t, planner::DerivedAttribute)]
          // Specify columns that are transformed.
          {{0, planner::DerivedAttribute{
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
  CompileAndExecute(*update_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // Post-condition: The table will have twice many, since it also has old ones
  EXPECT_EQ(NumRowsInTestTable()+2, table->GetTupleCount());
}


}  // namespace test
}  // namespace peloton
