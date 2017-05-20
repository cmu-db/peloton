//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark_insert_translator_test.cpp
//
// Identification: test/codegen/benchmark_insert_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/codegen_test_util.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "common/harness.h"
#include "executor/plan_executor.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace test {

// static const int NUM_OF_INSERT_ROWS = 1000000;
static const int NUM_OF_INSERT_ROWS = 1000;

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of insert
// plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
//===----------------------------------------------------------------------===//

class BenchmarkInsertTranslatorTest : public PelotonCodeGenTest {
 public:
  BenchmarkInsertTranslatorTest() : PelotonCodeGenTest() {
    LoadTestTable(TestTable4Id(), NUM_OF_INSERT_ROWS);
  }

  uint32_t TestTable1Id() { return test_table1_id; }

  uint32_t TestTable2Id() { return test_table2_id; }

  uint32_t TestTable3Id() { return test_table3_id; }

  uint32_t TestTable4Id() { return test_table4_id; }

  void TestInsertScanExecutor(expression::AbstractExpression *predicate) {
    auto table3 = &this->GetTestTable(this->TestTable3Id());
    auto table4 = &this->GetTestTable(this->TestTable4Id());

    // Insert into table3
    std::unique_ptr<planner::InsertPlan> insert_plan(
        new planner::InsertPlan(table3));

    // Scan from table2
    std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
        new planner::SeqScanPlan(table4, predicate, {0, 1, 2, 3}));

    insert_plan->AddChild(std::move(seq_scan_plan));

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    concurrency::Transaction *txn = txn_manager.BeginTransaction();

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    std::unique_ptr<executor::InsertExecutor> insert_executor =
        std::make_unique<executor::InsertExecutor>(insert_plan.get(),
                                                   context.get());

    std::unique_ptr<executor::SeqScanExecutor> scan_executor =
        std::make_unique<executor::SeqScanExecutor>(insert_plan->GetChild(0),
                                                    context.get());

    insert_executor->AddChild(scan_executor.get());

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    EXPECT_TRUE(insert_executor->Init());
    while (insert_executor->Execute())
      ;

    timer.Stop();
    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());

    txn_manager.CommitTransaction(txn);

    LOG_INFO("Table 3 has %zu tuples", table3->GetTupleCount());
    LOG_INFO("Table 4 has %zu tuples", table4->GetTupleCount());
  }

  void TestInsertScanTranslator(expression::AbstractExpression *predicate) {
    auto table3 = &this->GetTestTable(this->TestTable3Id());
    auto table4 = &this->GetTestTable(this->TestTable4Id());

    LOG_INFO("Table 3 has %zu tuples", table3->GetTupleCount());
    LOG_INFO("Table 4 has %zu tuples", table4->GetTupleCount());

    // Insert into table3
    std::unique_ptr<planner::InsertPlan> insert_plan(
        new planner::InsertPlan(table3));

    // Scan from table4
    std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
        new planner::SeqScanPlan(table4, predicate, {0, 1, 2, 3}));

    insert_plan->AddChild(std::move(seq_scan_plan));

    // Do binding
    planner::BindingContext context;
    insert_plan->PerformBinding(context);

    // We collect the results of the query into an in-memory buffer
    codegen::BufferingConsumer buffer{{0, 1}, context};

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();
    // COMPILE and execute
    CompileAndExecute(*insert_plan, buffer,
                      reinterpret_cast<char *>(buffer.GetState()));
    timer.Stop();
    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());
    // The results should be sorted in ascending order
    auto &results = buffer.GetOutputTuples();
    (void)results;

    LOG_INFO("Table 3 has %zu tuples", table3->GetTupleCount());
    LOG_INFO("Table 4 has %zu tuples", table4->GetTupleCount());
  }
};

TEST_F(BenchmarkInsertTranslatorTest, InsertScanExecutorAll) {
  this->TestInsertScanExecutor(nullptr);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanTranslatorAll) {
  this->TestInsertScanTranslator(nullptr);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanExecutorOne) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestInsertScanExecutor(a_equal_40);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanTranslatorOne) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestInsertScanTranslator(a_equal_40);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanExecutorMinority) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestInsertScanExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanTranslatorMinority) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestInsertScanTranslator(a_mod_40_neq_0);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanExecutorHalf) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto *a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestInsertScanExecutor(a_mod_20_eq_0);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanTranslatorHalf) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto *a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestInsertScanTranslator(a_mod_20_eq_0);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanExecutorMajority) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestInsertScanExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkInsertTranslatorTest, InsertScanTranslatorMajority) {
  auto *a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto *const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto *const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto *a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto *a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestInsertScanTranslator(a_mod_40_neq_0);
}

}  // namespace test
}  // namespace peloton