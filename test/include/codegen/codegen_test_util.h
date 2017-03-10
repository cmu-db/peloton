//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen_test_utils.h
//
// Identification: test/include/codegen/codegen_test_utils.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.h"
#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"
#include "expression/constant_value_expression.h"
#include "planner/binding_context.h"

#include "common/harness.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// Common utilities
//===----------------------------------------------------------------------===//
class CodegenTestUtils {
 public:
  static expression::ConstantValueExpression *ConstIntExpression(int64_t val);
};

//===----------------------------------------------------------------------===//
// Common base class for all codegen tests
//===----------------------------------------------------------------------===//
class PelotonCodeGenTest : public PelotonTest {
 public:
  const uint32_t test_db_id = INVALID_OID;
  const uint32_t test_table1_id = 44;
  const uint32_t test_table2_id = 45;
  const uint32_t test_table3_id = 46;
  const uint32_t test_table4_id = 47;

  PelotonCodeGenTest();

  virtual ~PelotonCodeGenTest();

  storage::Database &GetDatabase() const { return *test_db; }
  storage::DataTable &GetTestTable(uint32_t table_id) const {
    PL_ASSERT(table_id >= test_table1_id && table_id <= test_table4_id);
    return *GetDatabase().GetTableWithOid(table_id);
  }

  // Create the test tables
  void CreateTestTables();

  // Load the given table with the given number of rows
  void LoadTestTable(uint32_t table_id, uint32_t num_rows);

  // Compile and execute the given plan
  codegen::QueryCompiler::CompileStats CompileAndExecute(
      const planner::AbstractPlan &plan, codegen::QueryResultConsumer &consumer,
      char *consumer_state);

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
// A wrapper around a vector of values that looks like a proper tuple
//===----------------------------------------------------------------------===//
class WrappedTuple
    : public expression::ContainerTuple<std::vector<type::Value>> {
 public:
  // Basic
  WrappedTuple(type::Value *vals, uint32_t num_vals)
      : ContainerTuple(&tuple_), tuple_(vals, vals + num_vals) {}

  // Copy
  WrappedTuple(const WrappedTuple &o)
      : ContainerTuple(&tuple_), tuple_(o.tuple_) {}
  WrappedTuple &operator=(const WrappedTuple &o) {
    expression::ContainerTuple<std::vector<type::Value>>::operator=(o);
    tuple_ = o.tuple_;
    return *this;
  }

 private:
  // The tuple
  std::vector<type::Value> tuple_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local buffer
//===----------------------------------------------------------------------===//
class BufferingConsumer : public codegen::QueryResultConsumer {
 public:
  struct BufferingState {
    std::vector<WrappedTuple> *output;
  };

  // Constructor
  BufferingConsumer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
    state.output = &tuples_;
  }

  void Prepare(codegen::CompilationContext &compilation_context) override;
  void InitializeState(codegen::CompilationContext &) override {}
  void TearDownState(codegen::CompilationContext &) override {}
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &row) const override;

  llvm::Value *GetStateValue(codegen::ConsumerContext &ctx,
                             const codegen::RuntimeState::StateID &id) const {
    auto &runtime_state = ctx.GetRuntimeState();
    return runtime_state.GetStateValue(ctx.GetCodeGen(), id);
  }

  struct _BufferTupleProxy {
    static llvm::Function *GetFunction(codegen::CodeGen &codegen);
  };

  // Called from query plan to buffer the tuple
  static void BufferTuple(char *state, type::Value *vals, uint32_t num_vals);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  BufferingState *GetState() { return &state; }

  const std::vector<WrappedTuple> &GetOutputTuples() const { return tuples_; }

 private:
  //
  std::vector<const planner::AttributeInfo *> ais_;
  // Buffered output tuples
  std::vector<WrappedTuple> tuples_;
  // Running buffering state
  BufferingState state;
  // The ID of our consumer state
  codegen::RuntimeState::StateID consumer_state_id_;
  // The ID of our output tuple buffer state
  codegen::RuntimeState::StateID tuple_output_state_id_;
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
    return runtime_state.GetStateValue(codegen, counter_state_id_);
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