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
      char *consumer_state, codegen::Query::RuntimeStats *runtime_stats = nullptr);

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

struct Stats {

  // NOTE: for g++ > 4.8
  // codegen::QueryCompiler::CompileStats compile_stats{0.0, 0.0, 0.0};
  // codegen::Query::RuntimeStats runtime_stats{0.0, 0.0, 0.0};

  // NOTE: for g++ 4.8
  codegen::QueryCompiler::CompileStats compile_stats;
  codegen::Query::RuntimeStats runtime_stats;

  double num_samples = 0.0;
  int32_t tuple_result_size = -1;

  void Merge(codegen::QueryCompiler::CompileStats &o_compile_stats,
             codegen::Query::RuntimeStats &o_runtime_stats,
             int32_t o_tuple_result_size) {
    compile_stats.ir_gen_ms += o_compile_stats.ir_gen_ms;
    compile_stats.jit_ms += o_compile_stats.jit_ms;
    compile_stats.setup_ms += o_compile_stats.setup_ms;

    runtime_stats.init_ms += o_runtime_stats.init_ms;
    runtime_stats.plan_ms += o_runtime_stats.plan_ms;
    runtime_stats.tear_down_ms += o_runtime_stats.tear_down_ms;

    if (tuple_result_size < 0) {
      tuple_result_size = o_tuple_result_size;
    } else if (tuple_result_size != o_tuple_result_size) {
      throw Exception{"ERROR: tuple result size should not"
        " vary for the same test!"};
    }

    num_samples++;
  }

  void Finalize() {
    compile_stats.ir_gen_ms /= num_samples;
    compile_stats.jit_ms /= num_samples;
    compile_stats.setup_ms /= num_samples;

    runtime_stats.init_ms /= num_samples;
    runtime_stats.plan_ms /= num_samples;
    runtime_stats.tear_down_ms /= num_samples;
  }

  void PrintStats() {
    fprintf(
        stderr,
        "Setup time: %.2f ms, IR Gen time: %.2f ms, Compile time: %.2f ms\n",
        compile_stats.setup_ms, compile_stats.ir_gen_ms, compile_stats.jit_ms);
    fprintf(stderr,
            "Initialization time: %.2f ms, execution time: %.2f ms, Tear down "
            "time: %.2f ms\n",
            runtime_stats.init_ms, runtime_stats.plan_ms,
            runtime_stats.tear_down_ms);
    fprintf(stderr,
            "Tuple result size: %d\n",
            tuple_result_size);
  }
};

}  // namespace test
}  // namespace peloton
