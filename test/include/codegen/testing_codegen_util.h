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
#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/buffering_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"
#include "expression/constant_value_expression.h"
#include "planner/binding_context.h"

#include "common/harness.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// Common base class for all codegen tests. This class four test tables that all
// the codegen components use. Their ID's are available through the TableId
// enumeration.
//===----------------------------------------------------------------------===//
class PelotonCodeGenTest : public PelotonTest {
 public:
  enum class TableId : uint32_t { _1 = 44, _2 = 45, _3 = 46, _4 = 47 };

  const uint32_t test_db_id = INVALID_OID;

  PelotonCodeGenTest();

  virtual ~PelotonCodeGenTest();

  // Get the test database
  storage::Database &GetDatabase() const { return *test_db; }

  // Get the test table with the given ID
  storage::DataTable &GetTestTable(TableId table_id) const {
    return *GetDatabase().GetTableWithOid(static_cast<uint32_t>(table_id));
  }

  // Create the schema (common among all tables)
  std::unique_ptr<catalog::Schema> CreateTestSchema() const;

  // Create the test tables
  void CreateTestTables();

  // Load the given table with the given number of rows
  void LoadTestTable(TableId table_id, uint32_t num_rows,
                     bool insert_nulls = false);

  // Compile and execute the given plan
  codegen::QueryCompiler::CompileStats CompileAndExecute(
      const planner::AbstractPlan &plan, codegen::QueryResultConsumer &consumer,
      char *consumer_state);

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  std::unique_ptr<expression::AbstractExpression> ConstIntExpr(int64_t val);

  std::unique_ptr<expression::AbstractExpression> ConstDecimalExpr(double val);

  std::unique_ptr<expression::AbstractExpression> ColRefExpr(
      type::TypeId type, uint32_t col_id);

  std::unique_ptr<expression::AbstractExpression> CmpExpr(
      ExpressionType cmp_type,
      std::unique_ptr<expression::AbstractExpression> &&left,
      std::unique_ptr<expression::AbstractExpression> &&right);

  std::unique_ptr<expression::AbstractExpression> CmpLtExpr(
      std::unique_ptr<expression::AbstractExpression> &&left,
      std::unique_ptr<expression::AbstractExpression> &&right);

  std::unique_ptr<expression::AbstractExpression> CmpGtExpr(
      std::unique_ptr<expression::AbstractExpression> &&left,
      std::unique_ptr<expression::AbstractExpression> &&right);
  std::unique_ptr<expression::AbstractExpression> CmpGteExpr(
      std::unique_ptr<expression::AbstractExpression> &&left,
      std::unique_ptr<expression::AbstractExpression> &&right);

  std::unique_ptr<expression::AbstractExpression> CmpEqExpr(
      std::unique_ptr<expression::AbstractExpression> &&left,
      std::unique_ptr<expression::AbstractExpression> &&right);

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

//===----------------------------------------------------------------------===//
// A consumer that just counts the number of results
//===----------------------------------------------------------------------===//
class CountingConsumer : public codegen::QueryResultConsumer {
 public:
  void Prepare(codegen::CompilationContext &compilation_context) override;
  void InitializeState(codegen::CompilationContext &context) override;
  void ConsumeResult(codegen::ConsumerContext &context,
                     codegen::RowBatch::Row &row) const override;
  void TearDownState(codegen::CompilationContext &) override {}

  uint64_t GetCount() const { return counter_; }

 private:
  llvm::Value *GetCounter(codegen::CodeGen &codegen,
                          codegen::RuntimeState &runtime_state) const {
    return runtime_state.LoadStateValue(codegen, counter_state_id_);
  }

  llvm::Value *GetCounter(codegen::ConsumerContext &ctx) const {
    return GetCounter(ctx.GetCodeGen(), ctx.GetRuntimeState());
  }

 private:
  uint64_t counter_;
  // The slot in the runtime state to find our state context
  codegen::RuntimeState::StateID counter_state_id_;
};

}  // namespace test
}  // namespace peloton
