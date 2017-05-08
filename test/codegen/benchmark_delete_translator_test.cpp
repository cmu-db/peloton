//===----------------------------------------------------------------------===//
//
//                         Peloton
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


#define NUM_OF_TEST_ROWS 100000

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
  DeleteTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(NUM_OF_TEST_ROWS){
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
    std::unique_ptr<planner::AbstractPlan> scan{
        new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
    delete_plan->AddChild(std::move(scan));

    planner::BindingContext deleteContext;
    delete_plan->PerformBinding(deleteContext);

    codegen::BufferingConsumer buffer{{0, 1}, deleteContext};
    CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t TestTableId() { return test_table1_id; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }
 private:
  uint32_t num_rows_to_insert = NUM_OF_TEST_ROWS;
};

TEST_F(DeleteTranslatorTest, DeleteAllTuples) {

  //
  // DELETE FROM table;
  //
  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  planner::BindingContext deleteContext;
  delete_plan->PerformBinding(deleteContext);

  codegen::BufferingConsumer buffer{{0, 1}, deleteContext};

  Timer<std::ratio<1, 1000>> timer;

  LOG_INFO("starting benchmarking empty table");
  timer.Start();

  CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  timer.Stop();
  LOG_INFO("compiled empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(0, getCurrentTableSize());

  timer.Reset();

  reloadTable();

  // =========================
  //  Benchmark Executor mode
  // =========================

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

  timer.Start();
  while (delete_executor->Execute()) {}

  txn_manager.CommitTransaction(txn);

  timer.Stop();
  LOG_INFO("executor empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(0, getCurrentTableSize());

  reloadTable();
}


TEST_F(DeleteTranslatorTest, DeleteWithSimplePredicate) {
  //
  // DELETE FROM table where a == 10000; only one record shall be deleted
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());


  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_equal_40, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  Timer<std::ratio<1, 1000>> timer;

  LOG_INFO("starting benchmarking empty table");
  timer.Start();

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  timer.Stop();
  LOG_INFO("compiled empty table finished in: %.2f ms\n", timer.GetDuration());

  EXPECT_EQ(NUM_OF_TEST_ROWS - 1, getCurrentTableSize());
  reloadTable();

  // =========================
  //  Benchmark Executor mode
  // =========================
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::DeleteExecutor> delete_executor =
      std::make_unique<executor::DeleteExecutor>(
          delete_plan.get(), executor_context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          delete_plan->GetChild(0), executor_context.get()
      );

  delete_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(delete_executor->Init());

  timer.Reset();
  timer.Start();
  while (delete_executor->Execute()) {}
  txn_manager.CommitTransaction(txn);

  timer.Stop();
  LOG_INFO("executor empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(99999, getCurrentTableSize());

  reloadTable();
}

TEST_F(DeleteTranslatorTest, DeleteHalfRecords) {
  //
  // DELETE FROM table where a % 20 == 0; half records will be deleted
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto* a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_mod_20_eq_0, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  Timer<std::ratio<1, 1000>> timer;

  LOG_INFO("starting benchmarking empty table");
  timer.Start();

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  timer.Stop();
  LOG_INFO("compiled empty table finished in: %.2f ms\n", timer.GetDuration());

  EXPECT_EQ(50000, getCurrentTableSize());
  reloadTable();

  // =========================
  //  Benchmark Executor mode
  // =========================
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::DeleteExecutor> delete_executor =
      std::make_unique<executor::DeleteExecutor>(
          delete_plan.get(), executor_context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          delete_plan->GetChild(0), executor_context.get()
      );

  delete_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(delete_executor->Init());
  timer.Reset();
  timer.Start();
  while (delete_executor->Execute()) {}

  txn_manager.CommitTransaction(txn);

  timer.Stop();
  LOG_INFO("executor empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(50000, getCurrentTableSize());
  reloadTable();
}


// delete 75% of the rows
TEST_F(DeleteTranslatorTest, DeleteMajorityRecords) {
  //
  // DELETE FROM table where a % 40 != 0; 75% records will be deleted
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_mod_40_neq_0, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  Timer<std::ratio<1, 1000>> timer;

  LOG_INFO("starting benchmarking empty table");
  timer.Start();

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  timer.Stop();
  LOG_INFO("compiled empty table finished in: %.2f ms\n", timer.GetDuration());

  EXPECT_EQ(25000, getCurrentTableSize());
  reloadTable();

  // =========================
  //  Benchmark Executor mode
  // =========================
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::DeleteExecutor> delete_executor =
      std::make_unique<executor::DeleteExecutor>(
          delete_plan.get(), executor_context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          delete_plan->GetChild(0), executor_context.get()
      );

  delete_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(delete_executor->Init());
  timer.Reset();
  timer.Start();
  while (delete_executor->Execute()) {}

  txn_manager.CommitTransaction(txn);

  timer.Stop();
  LOG_INFO("executor empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(25000, getCurrentTableSize());
  reloadTable();
}


// delete 25% of the rows
TEST_F(DeleteTranslatorTest, DeleteMinorityRecords) {
  //
  // DELETE FROM table where a % 40 == 0; 25% records will be deleted
  //

  EXPECT_EQ(NumRowsInTestTable(), getCurrentTableSize());

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);

  std::unique_ptr<planner::DeletePlan> delete_plan{
      new planner::DeletePlan(&GetTestTable(TestTableId()), nullptr)};
  std::unique_ptr<planner::AbstractPlan> scan{
      new planner::SeqScanPlan(&GetTestTable(TestTableId()), a_mod_40_neq_0, {0, 1, 2})};
  delete_plan->AddChild(std::move(scan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  Timer<std::ratio<1, 1000>> timer;

  LOG_INFO("starting benchmarking empty table");
  timer.Start();

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

  timer.Stop();
  LOG_INFO("compiled empty table finished in: %.2f ms\n", timer.GetDuration());

  EXPECT_EQ(75000, getCurrentTableSize());
  reloadTable();

  // =========================
  //  Benchmark Executor mode
  // =========================
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::DeleteExecutor> delete_executor =
      std::make_unique<executor::DeleteExecutor>(
          delete_plan.get(), executor_context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          delete_plan->GetChild(0), executor_context.get()
      );

  delete_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(delete_executor->Init());
  timer.Reset();
  timer.Start();
  while (delete_executor->Execute()) {}

  txn_manager.CommitTransaction(txn);

  timer.Stop();
  LOG_INFO("executor empty table finished in: %.2f ms\n", timer.GetDuration());
  EXPECT_EQ(75000, getCurrentTableSize());
  reloadTable();
}

}  // namespace test
}  // namespace peloton
