//===----------------------------------------------------------------------===//
//
//                         Peloton
//
//
// delete_translator_test.cpp
//
// Identification: test/codegen/delete_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "catalog/catalog.h"
#include "codegen/codegen_test_util.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace test {

class DeleteTranslatorTest : public PelotonCodeGenTest {
 public:
  DeleteTranslatorTest() : PelotonCodeGenTest() {}

  // We need this to get a transactionally consistent idea of the table size
  size_t GetCurrentTableSize(uint32_t table_id) {
    planner::SeqScanPlan scan{&GetTestTable(table_id), nullptr, {0, 1}};
    planner::BindingContext context;
    scan.PerformBinding(context);

    codegen::BufferingConsumer buffer{{0, 1}, context};
    CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));
    return buffer.GetOutputTuples().size();
  }

  uint32_t TestTableId1() { return test_table1_id; }
  uint32_t TestTableId2() { return test_table2_id; }
  uint32_t TestTableId3() { return test_table3_id; }
  uint32_t TestTableId4() { return test_table4_id; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(DeleteTranslatorTest, DeleteAllTuples) {
  //
  // DELETE FROM table;
  //
  LoadTestTable(TestTableId1(), NumRowsInTestTable());

  EXPECT_EQ(NumRowsInTestTable(), GetTestTable(TestTableId1()).GetTupleCount());

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId1()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId1()), nullptr, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  LOG_DEBUG("tile group count %zu",
            GetTestTable(TestTableId1()).GetTileGroupCount());

  planner::BindingContext deleteContext;
  delete_plan->PerformBinding(deleteContext);

  codegen::BufferingConsumer buffer{{0, 1}, deleteContext};
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));
  EXPECT_EQ(0, GetCurrentTableSize(TestTableId1()));
}

TEST_F(DeleteTranslatorTest, DeleteWithSimplePredicate) {
  //
  // DELETE FROM table where a >= 40;
  //

  LoadTestTable(TestTableId2(), NumRowsInTestTable());

  EXPECT_EQ(NumRowsInTestTable(), GetTestTable(TestTableId2()).GetTupleCount());

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_gt_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_40_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId2()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), a_gt_40, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  EXPECT_EQ(4, GetCurrentTableSize(TestTableId2()));
}

TEST_F(DeleteTranslatorTest, DeleteWithCompositePredicate) {
  //
  // DELETE FROM table where a >= 20 and b = 21;
  //

  LoadTestTable(TestTableId3(), NumRowsInTestTable());

  EXPECT_EQ(NumRowsInTestTable(), GetTestTable(TestTableId3()).GetTupleCount());

  // Construct the components of the predicate
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

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId3()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId3()), conj_eq, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  EXPECT_EQ(NumRowsInTestTable() - 1, GetCurrentTableSize(TestTableId3()));
}

TEST_F(DeleteTranslatorTest, DeleteWithModuloPredicate) {
  //
  // DELETE FROM table where a = b % 1;
  //

  LoadTestTable(TestTableId4(), NumRowsInTestTable());

  EXPECT_EQ(NumRowsInTestTable(), GetTestTable(TestTableId4()).GetTupleCount());

  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_1_exp = CodegenTestUtils::ConstIntExpression(1);
  auto* b_mod_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, b_col_exp,
      const_1_exp);

  // a = b % 1
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_b_mod_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, b_mod_1);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId4()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId4()), a_eq_b_mod_1, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  EXPECT_EQ(NumRowsInTestTable() - 1, GetCurrentTableSize(TestTableId4()));
}

}  // namespace test
}  // namespace peloton
