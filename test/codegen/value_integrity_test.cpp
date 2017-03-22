//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_integrity_test.cpp
//
// Identification: test/codegen/value_integrity_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation for
// undefined values that are the result of a divide by zero error or integer
// overflow. All the tests use a single table created and loaded during
// SetUp().  The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 10 rows of random values.
//===----------------------------------------------------------------------===//

class ValueIntegrityTest : public PelotonCodeGenTest {
 public:
  ValueIntegrityTest() : PelotonCodeGenTest() {
    LoadTestTable(test_table1_id, 10);
  }
};

void DivideByZeroTest(ExpressionType exp_type, storage::DataTable* table) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);

  expression::AbstractExpression* a_op_0 = nullptr;
  switch (exp_type) {
    case ExpressionType::OPERATOR_DIVIDE:
      // a / 0
      a_op_0 = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::DECIMAL, a_col_exp, const_0_exp);
      break;
    case ExpressionType::OPERATOR_MOD:
      // a % 0
      a_op_0 = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::DECIMAL, a_col_exp, const_0_exp);
      break;
    default: {}
  }

  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_eq_a_op_0 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_op_0);

  // Setup the scan plan node
  planner::SeqScanPlan scan{table, b_eq_a_op_0, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto compiled_query = compiler.Compile(scan, buffer);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Run
  EXPECT_THROW(
      compiled_query->Execute(*txn, reinterpret_cast<char*>(buffer.GetState())),
      DivideByZeroException);

  txn_manager.CommitTransaction(txn);
}

void OverflowTest(ExpressionType exp_type, storage::DataTable* table) {
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);

  expression::AbstractExpression* a_op_lim = nullptr;
  switch (exp_type) {
    case ExpressionType::OPERATOR_PLUS: {
      // a + INT32_MAX
      auto* const_max_exp = CodegenTestUtils::ConstIntExpression(INT32_MAX);
      a_op_lim = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::INTEGER, a_col_exp, const_max_exp);
      break;
    }
    case ExpressionType::OPERATOR_MINUS: {
      // a - INT32_MIN
      auto* const_min_exp = CodegenTestUtils::ConstIntExpression(INT32_MIN);
      a_op_lim = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::INTEGER, a_col_exp, const_min_exp);
      break;
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      // a * INT32_MAX
      auto* const_max_exp = CodegenTestUtils::ConstIntExpression(INT32_MAX);
      a_op_lim = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::INTEGER, a_col_exp, const_max_exp);
      break;
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      // Overflow when: lhs == INT_MIN && rhs == -1
      // (a - 1) --> this makes a[0] = -1
      // INT64_MIN / (a - 1)
      auto* const_min_exp = CodegenTestUtils::ConstIntExpression(INT32_MIN);
      auto* const_one_exp = CodegenTestUtils::ConstIntExpression(1);
      auto* a_sub_one = new expression::OperatorExpression(
          ExpressionType::OPERATOR_MINUS, type::Type::TypeId::INTEGER,
          a_col_exp, const_one_exp);
      a_op_lim = new expression::OperatorExpression(
          exp_type, type::Type::TypeId::INTEGER, const_min_exp, a_sub_one);
      break;
    }
    default: {}
  }

  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_eq_a_op_lim = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_op_lim);

  // Setup the scan plan node
  planner::SeqScanPlan scan{table, b_eq_a_op_lim, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto compiled_query = compiler.Compile(scan, buffer);

  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto* txn = txn_manager.BeginTransaction();

  // Run
  EXPECT_THROW(
      compiled_query->Execute(*txn, reinterpret_cast<char*>(buffer.GetState())),
      std::overflow_error);

  txn_manager.CommitTransaction(txn);
}

// Commented out until we fix the new type system ...
/*
TEST_F(ValueIntegrityTest, DivideByZeroModuloOp) {
  //
  // SELECT a, b, c FROM table where b = a % 0;
  //
  LaunchParallelTest(1, DivideByZeroTest, ExpressionType::OPERATOR_MOD,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, DivideByZeroDivideOp) {
  //
  // SELECT a, b, c FROM table where b = a / 0;
  //
  LaunchParallelTest(1, DivideByZeroTest, ExpressionType::OPERATOR_DIVIDE,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowAddOp) {
  //
  // SELECT a, b, c FROM table where b = a + INT32_MAX;
  //
  LaunchParallelTest(1, OverflowTest, ExpressionType::OPERATOR_PLUS,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowSubOp) {
  //
  // SELECT a, b, c FROM table where b = a - INT32_MIN;
  //
  LaunchParallelTest(1, OverflowTest, ExpressionType::OPERATOR_MINUS,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowMulOp) {
  //
  // SELECT a, b, c FROM table where b = a * INT32_MAX;
  //
  LaunchParallelTest(1, OverflowTest, ExpressionType::OPERATOR_MULTIPLY,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowDivOp) {
  //
  // Overflow when: lhs == INT_MIN && rhs == -1
  // (a - 1) --> this makes a[0] = -1
  //
  // SELECT a, b, c FROM table where b =INT64_MIN / (a - 1);
  //
  LaunchParallelTest(1, OverflowTest, ExpressionType::OPERATOR_DIVIDE,
                     &GetTestTable());
}
*/

}  // namespace test
}  // namespace peloton