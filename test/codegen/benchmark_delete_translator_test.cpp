//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark_delete_translator_test.cpp
//
// Identification: test/codegen/benchmark_delete_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/harness.h"
#include "expression/conjunction_expression.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "codegen/codegen_test_util.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace test {

// static const uint32_t NUM_OF_TEST_ROWS = 1000000;
static const uint32_t NUM_OF_TEST_ROWS = 1000;

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of insert
// plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
//===----------------------------------------------------------------------===//

class BenchmarkDeleteTranslatorTest : public PelotonCodeGenTest {
 public:
  BenchmarkDeleteTranslatorTest()
      : PelotonCodeGenTest(), num_rows_to_insert(NUM_OF_TEST_ROWS) {
    LOG_INFO("Constructor");
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t TestTableId() { return test_table1_id; }

  void TestDeleteExecutor(expression::AbstractExpression *predicate) {
    std::unique_ptr<planner::DeletePlan> delete_plan{
        new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)
    };

    std::unique_ptr<planner::AbstractPlan> scan{
        new planner::SeqScanPlan(
            &GetTestTable(TestTableId()), predicate, {0, 1, 2}
        )
    };

    delete_plan->AddChild(std::move(scan));

    auto &txn_manager =
        concurrency::TransactionManagerFactory::GetInstance();

    concurrency::Transaction *txn = txn_manager.BeginTransaction();

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn)
    );

    std::unique_ptr<executor::DeleteExecutor> delete_executor =
        std::make_unique<executor::DeleteExecutor>(
            delete_plan.get(), context.get()
        );

    std::unique_ptr<executor::SeqScanExecutor> scan_executor =
        std::make_unique<executor::SeqScanExecutor>(
            delete_plan->GetChild(0), context.get()
        );

    delete_executor->AddChild(scan_executor.get());

    EXPECT_TRUE(delete_executor->Init());

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    while (delete_executor->Execute()) {}

    timer.Stop();

    txn_manager.CommitTransaction(txn);

    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());
  }

  void TestDeleteTranslator(expression::AbstractExpression *predicate) {
    std::unique_ptr<planner::DeletePlan> delete_plan{
        new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)
    };

    std::unique_ptr<planner::AbstractPlan> scan{
        new planner::SeqScanPlan(
            &GetTestTable(TestTableId()), predicate, {0, 1, 2}
        )
    };

    delete_plan->AddChild(std::move(scan));

    planner::BindingContext deleteContext;
    delete_plan->PerformBinding(deleteContext);

    codegen::BufferingConsumer buffer{{0, 1}, deleteContext};

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    CompileAndExecute(*delete_plan, buffer,
                      reinterpret_cast<char *>(buffer.GetState()));

    timer.Stop();
    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());
  }

 private:
  uint32_t num_rows_to_insert = NUM_OF_TEST_ROWS;
};

TEST_F(BenchmarkDeleteTranslatorTest, DeleteAllExecutor) {
  this->TestDeleteExecutor(nullptr);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteAllTranslator) {
  this->TestDeleteTranslator(nullptr);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteOneTranslator) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestDeleteTranslator(a_equal_40);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteOneExecutor) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestDeleteExecutor(a_equal_40);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteMinorityExecutor) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestDeleteExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteMinorityTranslator) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestDeleteTranslator(a_mod_40_neq_0);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteHalfExecutor) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto *a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestDeleteExecutor(a_mod_20_eq_0);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteHalfTranslator) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto *a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestDeleteTranslator(a_mod_20_eq_0);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteMajorityExecutor) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestDeleteExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkDeleteTranslatorTest, DeleteMajorityTranslator) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestDeleteTranslator(a_mod_40_neq_0);
}

}  // namespace test
}  // namespace peloton