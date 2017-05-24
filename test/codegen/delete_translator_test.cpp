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

#include "catalog/catalog.h"
#include "codegen/codegen_test_util.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "common/harness.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of insert
// plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
//===----------------------------------------------------------------------===//

class DeleteTranslatorTest : public PelotonCodeGenTest {
 public:
  DeleteTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  size_t getCurrentTableSize() {
    planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1}};
    planner::BindingContext context;
    scan.PerformBinding(context);

    codegen::BufferingConsumer buffer{{0, 1}, context};
    CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));
    return buffer.GetOutputTuples().size();
  }

  // delete all entries in table and then repopulate it.
  void reloadTable() {
    std::unique_ptr<planner::DeletePlan> delete_plan{
        new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
    std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
    delete_plan->AddChild(std::move(scan));

    planner::BindingContext deleteContext;
    delete_plan->PerformBinding(deleteContext);

    codegen::BufferingConsumer buffer{{0, 1}, deleteContext};
    CompileAndExecute(*delete_plan, buffer,
                      reinterpret_cast<char*>(buffer.GetState()));
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t TestTableId() { return test_table1_id; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(DeleteTranslatorTest, DeleteAllTuples) {
  //
  // DELETE FROM table;
  //
  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  LOG_DEBUG("tile group count %zu",
            GetTestTable(TestTableId()).GetTileGroupCount());

  planner::BindingContext deleteContext;
  delete_plan->PerformBinding(deleteContext);

  codegen::BufferingConsumer buffer{{0, 1}, deleteContext};
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));
  EXPECT_EQ(0, getCurrentTableSize());

  reloadTable();
}

TEST_F(DeleteTranslatorTest, DeleteWithSimplePredicate) {
  //
  // DELETE FROM table where a >= 40;
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_gt_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_40_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), a_gt_40, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  EXPECT_EQ(4, getCurrentTableSize());
  reloadTable();
}

TEST_F(DeleteTranslatorTest, DeleteWithCompositePredicate) {
  //
  // DELETE FROM table where a >= 40 and b = 21;
  //

  // Construct the components of the predicate

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

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
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), conj_eq, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  ASSERT_EQ(NumRowsInTestTable() - 1, getCurrentTableSize());
  reloadTable();
}

TEST_F(DeleteTranslatorTest, DeleteWithModuloPredicate) {
  //
  // DELETE FROM table where a = b % 1;
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

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
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), a_eq_b_mod_1, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  ASSERT_EQ(NumRowsInTestTable() - 1, getCurrentTableSize());
  reloadTable();
}

}  // namespace test
}  // namespace peloton
