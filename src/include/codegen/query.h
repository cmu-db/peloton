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
#include "codegen/query_parameters.h"
#include "codegen/query_state.h"
#include "codegen/parameter_cache.h"

namespace peloton {

namespace executor {
class ExecutorContext;
struct ExecutionResult;
}  // namespace executor

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

class ExecutionConsumer;

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
   * @param executor_context Stores transaction and parameters.
   * @param consumer Stores the result.
   * @param stats Handy struct to collect various runtime timing statistics
   */
  void Execute(executor::ExecutorContext &executor_context,
               ExecutionConsumer &consumer, RuntimeStats *stats = nullptr);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  /// Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  /// The class tracking all the state needed by this query
  QueryState &GetQueryState() { return query_state_; }

 private:
  friend class QueryCompiler;

  /// Constructor. Private so callers use the QueryCompiler class.
  explicit Query(const planner::AbstractPlan &query_plan);

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  QueryState query_state_;

  // The init(), plan() and tearDown() functions
  typedef void (*compiled_function_t)(char *);
  compiled_function_t init_func_;
  compiled_function_t plan_func_;
  compiled_function_t tear_down_func_;
};

}  // namespace codegen
}  // namespace peloton
