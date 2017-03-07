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

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/executor_tests_util.h"

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

class ValueIntegrityTest : public PelotonTest {
 public:
  ValueIntegrityTest() {
    CreateDatabase();
    CreateTestTable();
    LoadTestTable();
  }

  ~ValueIntegrityTest() override {
    catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
  }

  void CreateDatabase() {
    database = new storage::Database(INVALID_OID);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void CreateTestTable() {
    const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
    auto* data_table = ExecutorTestsUtil::CreateTable(tuple_count, false);
    GetDatabase().AddTable(data_table);
  }

  void LoadTestTable(uint32_t num_rows = 10) {
    auto& data_table = GetTestTable();
    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.BeginTransaction();
    ExecutorTestsUtil::PopulateTable(&data_table, num_rows, false, false,
                                     false);
    txn_manager.CommitTransaction();
  }

  storage::Database& GetDatabase() { return *database; }

  storage::DataTable& GetTestTable() {
    return *GetDatabase().GetTableWithOid(INVALID_OID);
  }

 private:
  storage::Database* database;
};

void DivideByZeroTest(ExpressionType exp_type, storage::DataTable* table) {
  auto& manager = catalog::Manager::GetInstance();

  auto* a_col_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  auto* const_0_exp = CodegenTestUtils::ConstIntExpression(0);

  expression::AbstractExpression* a_op_0 = nullptr;
  switch (exp_type) {
    case EXPRESSION_TYPE_OPERATOR_DIVIDE:
      // a / 0
      a_op_0 = new expression::OperatorExpression<expression::OpDivide>(
          exp_type, a_col_exp, const_0_exp);
      break;
    case EXPRESSION_TYPE_OPERATOR_MOD:
      // a % 0
      a_op_0 = new expression::OperatorExpression<expression::OpMod>(
          exp_type, a_col_exp, const_0_exp);
      break;
    default: {}
  }

  auto* b_col_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  auto* b_eq_a_op_0 = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, b_col_exp, a_op_0);

  // Setup the scan plan node
  planner::SeqScanPlan scan{table, b_eq_a_op_0, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);

  bool caught_exception = false;
  try {
    // Should throw div0 exception
    query_statement->Execute(manager,
                             reinterpret_cast<char*>(buffer.GetState()));
  } catch (DivideByZeroException& e) {
    // Caught it!
    caught_exception = true;
  }
  EXPECT_TRUE(caught_exception);
}

void OverflowTest(ExpressionType exp_type, storage::DataTable* table) {
  auto& manager = catalog::Manager::GetInstance();

  auto* a_col_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);

  expression::AbstractExpression* a_op_lim = nullptr;
  switch (exp_type) {
    case EXPRESSION_TYPE_OPERATOR_PLUS: {
      // a + INT32_MAX
      auto* const_max_exp =
          CodegenTestUtils::ConstIntExpression(INT32_MAX);
      a_op_lim = new expression::OperatorExpression<expression::OpPlus>(
          exp_type, a_col_exp, const_max_exp);
      break;
    }
    case EXPRESSION_TYPE_OPERATOR_MINUS: {
      // a - INT32_MIN
      auto* const_min_exp =
          CodegenTestUtils::ConstIntExpression(INT32_MIN);
      a_op_lim = new expression::OperatorExpression<expression::OpMinus>(
          exp_type, a_col_exp, const_min_exp);
      break;
    }
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY: {
      // a * INT32_MAX
      auto* const_max_exp =
          CodegenTestUtils::ConstIntExpression(INT32_MAX);
      a_op_lim = new expression::OperatorExpression<expression::OpMultiply>(
          exp_type, a_col_exp, const_max_exp);
      break;
    }
    case EXPRESSION_TYPE_OPERATOR_DIVIDE: {
      // Overflow when: lhs == INT_MIN && rhs == -1
      // (a - 1) --> this makes a[0] = -1
      // INT64_MIN / (a - 1)
      auto* const_min_exp =
          CodegenTestUtils::ConstIntExpression(INT32_MIN);
      auto* const_one_exp = CodegenTestUtils::ConstIntExpression(1);
      auto* a_sub_one = new expression::OperatorExpression<expression::OpMinus>(
          EXPRESSION_TYPE_OPERATOR_MINUS, a_col_exp, const_one_exp);
      a_op_lim = new expression::OperatorExpression<expression::OpDivide>(
          exp_type, const_min_exp, a_sub_one);
      break;
    }
    default: {}
  }

  auto* b_col_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
  auto* b_eq_a_op_lim = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, b_col_exp, a_op_lim);

  // Setup the scan plan node
  planner::SeqScanPlan scan{table, b_eq_a_op_lim, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);

  bool caught_exception = false;
  try {
    // Should throw overflow exception
    query_statement->Execute(manager,
                             reinterpret_cast<char*>(buffer.GetState()));
  } catch (std::overflow_error& e) {
    // Caught it!
    caught_exception = true;
  }
  EXPECT_TRUE(caught_exception);
}

TEST_F(ValueIntegrityTest, DivideByZeroModuloOp) {
  //
  // SELECT a, b, c FROM table where b = a % 0;
  //
  LaunchParallelTest(1, DivideByZeroTest, EXPRESSION_TYPE_OPERATOR_MOD,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, DivideByZeroDivideOp) {
  //
  // SELECT a, b, c FROM table where b = a / 0;
  //
  LaunchParallelTest(1, DivideByZeroTest, EXPRESSION_TYPE_OPERATOR_DIVIDE,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowAddOp) {
  //
  // SELECT a, b, c FROM table where b = a + INT32_MAX;
  //
  LaunchParallelTest(1, OverflowTest, EXPRESSION_TYPE_OPERATOR_PLUS,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowSubOp) {
  //
  // SELECT a, b, c FROM table where b = a - INT32_MIN;
  //
  LaunchParallelTest(1, OverflowTest, EXPRESSION_TYPE_OPERATOR_MINUS,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowMulOp) {
  //
  // SELECT a, b, c FROM table where b = a * INT32_MAX;
  //
  LaunchParallelTest(1, OverflowTest, EXPRESSION_TYPE_OPERATOR_MULTIPLY,
                     &GetTestTable());
}

TEST_F(ValueIntegrityTest, IntegerOverflowDivOp) {
  //
  // Overflow when: lhs == INT_MIN && rhs == -1
  // (a - 1) --> this makes a[0] = -1
  //
  // SELECT a, b, c FROM table where b =INT64_MIN / (a - 1);
  //
  LaunchParallelTest(1, OverflowTest, EXPRESSION_TYPE_OPERATOR_DIVIDE,
                     &GetTestTable());
}

}  // namespace test
}  // namespace peloton
