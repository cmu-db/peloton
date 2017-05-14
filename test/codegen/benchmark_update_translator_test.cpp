//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark_update_translator_test.cpp
//
// Identification: test/codegen/benchmark_update_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "executor/update_executor.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "optimizer/simple_optimizer.h"
#include "tcop/tcop.h"
#include "planner/create_plan.h"
#include "executor/create_executor.h"
#include "common/statement.h"
#include "parser/postgresparser.h"
#include "planner/plan_util.h"
#include "planner/abstract_plan.h"
#include "parser/sql_statement.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

// static const uint32_t NUM_ROWS = 1000000;
static const uint32_t NUM_ROWS = 1000;

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

class BenchmarkUpdateTranslatorTest : public PelotonCodeGenTest {
 public:
  BenchmarkUpdateTranslatorTest()
      : PelotonCodeGenTest(), num_rows_to_insert(NUM_ROWS) {

    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

  void TestUpdateExecutor(expression::AbstractExpression *predicate) {
    // UPDATE table
    // SET a = 1;

    storage::DataTable *table = &this->GetTestTable(this->TestTableId());
    (void)table;
    LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());

    // =============
    //  Create plan
    // =============

    // Each update query needs to first scan the table.
    std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), /* table */
        predicate,                    /* predicate */
        {0, 1, 2, 3}                  /* columns */
    ));

    // Then it transforms each tuple scanned.
    // Weirdly, the transformation is represented by a "projection".
    std::unique_ptr<planner::ProjectInfo> project_info(new planner::ProjectInfo(
        // target list : [(oid_t, planner::DerivedAttribute)]
        // These specify columns that are transformed.
        {
            // Column 0 of the updated tuple will have constant value 1
            {
                0,
                planner::DerivedAttribute{
                    planner::AttributeInfo{},
                    expression::ExpressionUtil::ConstantValueFactory(
                        type::ValueFactory::GetIntegerValue(1)
                    )
                }
            }
        },

        // direct map list : [(oid_t, (oid_t, oid_t))]
        // These specify columns that are directly pulled from the original tuple.
        {
            { 1, { 0, 1 } },
            { 2, { 0, 2 } },
            { 3, { 0, 3 } },
        }
    ));

    // Now embed the scan and the transformation to build up an update plan.
    std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
        &GetTestTable(TestTableId()), /* table */
        std::move(project_info)       /* projection info */
    ));

    update_plan->AddChild(std::move(scan_plan));

    // ==============
    //  Execute plan
    // ==============

    auto &txn_manager =
        concurrency::TransactionManagerFactory::GetInstance();

    concurrency::Transaction *txn = txn_manager.BeginTransaction();

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn)
    );

    std::unique_ptr<executor::UpdateExecutor> update_executor =
        std::make_unique<executor::UpdateExecutor>(
            update_plan.get(), context.get()
        );

    std::unique_ptr<executor::SeqScanExecutor> scan_executor =
        std::make_unique<executor::SeqScanExecutor>(
            update_plan->GetChild(0), context.get()
        );

    update_executor->AddChild(scan_executor.get());

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    EXPECT_TRUE(update_executor->Init());

    // The weird peloton behavior: each time we call Execute, only one tile group
    // is operated on.
    while (update_executor->Execute()) {}

    txn_manager.CommitTransaction(txn);

    timer.Stop();
    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());

    LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  }

  void TestUpdateTranslator(expression::AbstractExpression *predicate) {
    // UPDATE table
    // SET a = 1;

    storage::DataTable *table = &this->GetTestTable(this->TestTableId());
    (void)table;
    LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());

    // =============
    //  Create plan
    // =============

    // Each update query needs to first scan the table.
    std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), /* table */
        predicate,                    /* predicate */
        {0, 1, 2, 3}                  /* columns */
    ));

    // Then it transforms each tuple scanned.
    // Weirdly, the transformation is represented by a "projection".
    std::unique_ptr<const planner::ProjectInfo> project_info(new planner::ProjectInfo(
        // target list : [(oid_t, planner::DerivedAttribute)]
        // These specify columns that are transformed.
        {
            // Column 0 of the updated tuple will have constant value 1
            {
                0,
                planner::DerivedAttribute{
                    // I haven't figured out what this should be
                    planner::AttributeInfo{},

                    expression::ExpressionUtil::ConstantValueFactory(
                        type::ValueFactory::GetIntegerValue(1)
                    )
                }
            }
        },

        // direct map list : [(oid_t, (oid_t, oid_t))]
        // These specify columns that are directly pulled from the original tuple.
        {
            { 1, { 0, 1 } },
            { 2, { 0, 2 } },
            { 3, { 0, 3 } },
        }
    ));

    // Now embed the scan and the transformation to build up an update plan.
    std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
        &GetTestTable(TestTableId()), /* table */
        std::move(project_info)       /* projection info */
    ));

    update_plan->AddChild(std::move(scan_plan));

    // Do binding
    planner::BindingContext context;
    update_plan->PerformBinding(context);

    // We collect the results of the query into an in-memory buffer
    codegen::BufferingConsumer buffer{{}, context};

    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    // COMPILE and execute
    CompileAndExecute(*update_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));

    timer.Stop();
    LOG_INFO("Time: %.2f ms\n", timer.GetDuration());

    LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  }

 private:
  uint32_t num_rows_to_insert;
};

TEST_F(BenchmarkUpdateTranslatorTest, UpdateAllExecutor) {
  this->TestUpdateExecutor(nullptr);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateAllTranslator) {
  this->TestUpdateTranslator(nullptr);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateOneExecutor) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestUpdateExecutor(a_equal_40);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateOneTranslator) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_equal_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, const_40_exp);
  this->TestUpdateTranslator(a_equal_40);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateMinorityExecutor) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestUpdateExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateMinorityTranslator) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_40, const_0_exp);
  this->TestUpdateTranslator(a_mod_40_neq_0);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateHalfExecutor) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto* a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestUpdateExecutor(a_mod_20_eq_0);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateHalfTranslator) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_20 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_20_exp);
  auto* a_mod_20_eq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_mod_20, const_0_exp);
  this->TestUpdateTranslator(a_mod_20_eq_0);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateMajorityExecutor) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestUpdateExecutor(a_mod_40_neq_0);
}

TEST_F(BenchmarkUpdateTranslatorTest, UpdateMajorityTranslator) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);
  auto* a_mod_40 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, a_col_exp,
      const_40_exp);
  auto* a_mod_40_neq_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_NOTEQUAL, a_mod_40, const_0_exp);
  this->TestUpdateTranslator(a_mod_40_neq_0);
}

}  // namespace test
}  // namespace peloton