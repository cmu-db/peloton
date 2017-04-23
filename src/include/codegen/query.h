//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_statement.h
//
// Identification: src/include/codegen/query_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "codegen/runtime_state.h"
#include "codegen/parameter.h"
#include "type/value.h"
#include "executor/executor_context.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A query statement that can be compiled
//===----------------------------------------------------------------------===//
class Query {
 public:
  struct RuntimeStats {
    double init_ms = 0.0;
    double plan_ms = 0.0;
    double tear_down_ms = 0.0;
  };

  struct QueryFunctions {
    llvm::Function *init_func;
    llvm::Function *plan_func;
    llvm::Function *tear_down_func;
  };

  // Setup this query statement with the given LLVM function components. The
  // provided functions perform initialization, execution and tear down of
  // this query.
  bool Prepare(const QueryFunctions &funcs);

  // Execute th e query given the catalog manager and runtime/consumer state
  // that is passed along to the query execution code.
  void Execute(concurrency::Transaction &txn, char *consumer_arg,
               RuntimeStats *stats = nullptr,
               executor::ExecutorContext *exec_context = nullptr);

  // Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  // Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  // The class tracking all the state needed by this query
  RuntimeState &GetRuntimeState() { return runtime_state_; }

  uint32_t StoreParam(Parameter param);

 private:
  friend class QueryCompiler;

  // Constructor
  Query(const planner::AbstractPlan &query_plan);

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  RuntimeState runtime_state_;

  // The init(), plan() and tearDown() functions
  typedef void (*compiled_function_t)(char *);
  compiled_function_t init_func_;
  compiled_function_t plan_func_;
  compiled_function_t tear_down_func_;

  std::vector<Parameter> params_;

  uint32_t int_8_type_cnt_;
  uint32_t int_16_type_cnt_;
  uint32_t int_32_type_cnt_;
  uint32_t int_64_type_cnt_;
  uint32_t double_type_cnt_;
  uint32_t varchar_type_cnt_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);
};

}  // namespace codegen
}  // namespace peloton