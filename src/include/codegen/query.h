//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query.h
//
// Identification: src/include/codegen/query.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "codegen/runtime_state.h"
#include "codegen/query_parameters.h"
#include "codegen/parameter_cache.h"
#include "executor/executor_context.h"

namespace peloton {

namespace executor {
class ExecutorContext;
struct ExecutionResult;
}  // namespace executor

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

class QueryResultConsumer;

//===----------------------------------------------------------------------===//
// A compiled query. An instance of this class can be created either by
// providing a plan and its compiled function components through the constructor
// of by codegen::QueryCompiler::Compile(). The former method is purely for
// testing purposes. The system uses QueryCompiler to generate compiled query
// objects.
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

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);

  /**
   * @brief Setup this query with the given JITed function components
   *
   * @param funcs The compiled functions that implement the logic of the query
   */
  bool Prepare(const QueryFunctions &funcs);

  /**
   * @brief Executes the compiled query.
   *
   * This function is **asynchronous** - it returns before execution completes,
   * and invokes a user-provided callback on completion. It is the user's
   * responsibility that the result consumer object has a lifetime as far as
   * the return of the callback.
   *
   * @param executor_context Stores transaction and parameters.
   * @param consumer Stores the result.
   * @param on_complete The callback to be invoked when execution completes.
   */
  void Execute(std::unique_ptr<executor::ExecutorContext> executor_context,
               QueryResultConsumer &consumer,
               std::function<void(executor::ExecutionResult)> on_complete,
               RuntimeStats *stats = nullptr);

  /// Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  /// Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  /// The class tracking all the state needed by this query
  RuntimeState &GetRuntimeState() { return runtime_state_; }

 private:
  friend class QueryCompiler;

  /// Constructor
  explicit Query(const planner::AbstractPlan &query_plan);

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
};

}  // namespace codegen
}  // namespace peloton
