//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_codegen_util.h
//
// Identification: test/include/codegen/testing_codegen_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.h"
#include "codegen/buffering_consumer.h"
#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/binding_context.h"
#include "storage/data_table.h"
#include "storage/database.h"

#include "common/harness.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

using ExpressionPtr = std::unique_ptr<expression::AbstractExpression>;
using ConstExpressionPtr =
    std::unique_ptr<const expression::AbstractExpression>;

using PlanPtr = std::unique_ptr<planner::AbstractPlan>;
using ConstPlanPtr = std::unique_ptr<const planner::AbstractPlan>;

//===----------------------------------------------------------------------===//
// Common base class for all codegen tests. This class four test tables that all
// the codegen components use. Their ID's are available through the oid_t
// enumeration.
//===----------------------------------------------------------------------===//
class PelotonCodeGenTest : public PelotonTest {
 public:
  std::string test_db_name = "PELOTON_CODEGEN";
  std::vector<std::string> test_table_names = {"table1", "table2", "table3",
                                               "table4", "table5"};
  std::vector<oid_t> test_table_oids;

  PelotonCodeGenTest(oid_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP);

  virtual ~PelotonCodeGenTest();

  // Get the test database
  storage::Database &GetDatabase() const { return *test_db; }

  // Get the test table with the given ID
  storage::DataTable &GetTestTable(oid_t table_id) const {
    return *GetDatabase().GetTableWithOid(static_cast<uint32_t>(table_id));
  }

  // Create the schema (common among all tables)
  catalog::Column GetTestColumn(uint32_t col_id) const;

  std::unique_ptr<catalog::Schema> CreateTestSchema(
      bool add_primary = false) const;

  // Create the test tables
  void CreateTestTables(
      concurrency::TransactionContext *txn,
      oid_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP);

  // Load the given table with the given number of rows
  void LoadTestTable(oid_t table_id, uint32_t num_rows,
                     bool insert_nulls = false);

  static void ExecuteSync(
      codegen::Query &query,
      std::unique_ptr<executor::ExecutorContext> executor_context,
      codegen::QueryResultConsumer &consumer);

  // Compile and execute the given plan
  codegen::QueryCompiler::CompileStats CompileAndExecute(
      planner::AbstractPlan &plan, codegen::QueryResultConsumer &consumer);

  codegen::QueryCompiler::CompileStats CompileAndExecuteCache(
      std::shared_ptr<planner::AbstractPlan> plan,
      codegen::QueryResultConsumer &consumer, bool &cached,
      std::vector<type::Value> params = {});

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  ExpressionPtr ConstIntExpr(int64_t val);

  ExpressionPtr ConstDecimalExpr(double val);

  ExpressionPtr ColRefExpr(type::TypeId type, uint32_t col_id);
  ExpressionPtr ColRefExpr(type::TypeId type, bool left, uint32_t col_id);

  ExpressionPtr CmpExpr(ExpressionType cmp_type, ExpressionPtr &&left,
                        ExpressionPtr &&right);

  ExpressionPtr CmpLtExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpLteExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpGtExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpGteExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpEqExpr(ExpressionPtr &&left, ExpressionPtr &&right);

  ExpressionPtr OpExpr(ExpressionType op_type, type::TypeId type,
                       ExpressionPtr &&left, ExpressionPtr &&right);

 private:
  storage::Database *test_db;
};

//===----------------------------------------------------------------------===//
// A query consumer that prints the tuples to standard out
//===----------------------------------------------------------------------===//
class Printer : public codegen::QueryResultConsumer {
 public:
  Printer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
  }

  // None of these are used, except ConsumeResult()
  void Prepare(codegen::CompilationContext &) override {}
  void InitializeState(codegen::CompilationContext &) override {}
  void TearDownState(codegen::CompilationContext &) override {}
  // Use
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &) const override;

 private:
  std::vector<const planner::AttributeInfo *> ais_;
};

}  // namespace test
}  // namespace peloton
