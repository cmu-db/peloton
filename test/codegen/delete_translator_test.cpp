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

#include "codegen/testing_codegen_util.h"

#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace test {

class DeleteTranslatorTest : public PelotonCodeGenTest {
 public:
  DeleteTranslatorTest() : PelotonCodeGenTest() {}

  size_t GetCurrentTableSize(TableId table_id) {
    planner::SeqScanPlan scan{&GetTestTable(table_id), nullptr, {0, 1}};
    planner::BindingContext context;
    scan.PerformBinding(context);

    codegen::BufferingConsumer buffer{{0, 1}, context};
    CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()));
    return buffer.GetOutputTuples().size();
  }

  TableId TestTableId1() { return TableId::_1; }
  TableId TestTableId2() { return TableId::_2; }
  TableId TestTableId3() { return TableId::_3; }
  TableId TestTableId4() { return TableId::_4; }
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
  auto a_gt_40 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(40));

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId2()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId2()), a_gt_40.release(), {0, 1, 2})};
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
  auto a_gt_20 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));

  // b = 21
  auto b_eq_21 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(21));

  // a >= 20 AND b = 21
  auto* conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21.release(), a_gt_20.release());

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

  auto b_col_exp = ColRefExpr(type::TypeId::INTEGER, 1);
  auto const_1_exp = ConstIntExpr(1);
  auto b_mod_1 = std::unique_ptr<expression::AbstractExpression>{
      new expression::OperatorExpression(
          ExpressionType::OPERATOR_MOD, type::TypeId::DECIMAL,
          b_col_exp.release(), const_1_exp.release())};

  // a = b % 1
  auto a_eq_b_mod_1 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), std::move(b_mod_1));

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId4()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId4()), a_eq_b_mod_1.release(), {0, 1, 2})};
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
