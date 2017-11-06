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
#include "codegen/parameter_storage.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace planner {
class AbstractPlan;
class Parameter;
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
  void Execute(concurrency::Transaction &txn,
               executor::ExecutorContext *executor_context,
               QueryParameters *parameters, char *consumer_arg,
               RuntimeStats *stats = nullptr);

  // Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  // Return the query parameters
  codegen::QueryParameters &GetQueryParameters() const {
    return parameters_;
  }

  // Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  // The class tracking all the state needed by this query
  RuntimeState &GetRuntimeState() { return runtime_state_; }

  size_t GetParameterIdx(const expression::AbstractExpression *expression) {
    return parameters_.GetParameterIdx(expression);
  }

  ParameterStorage &GetParameterStorage() {
    return parameters_storage_;
  }

 private:
  friend class QueryCompiler;

  // Constructor
  Query(const planner::AbstractPlan &query_plan, QueryParameters &parameters);

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The parameters and mapping for expression and parameter ids to
  QueryParameters &parameters_;

  // The parameters and mapping for expression and parameter ids to
  ParameterStorage parameters_storage_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  RuntimeState runtime_state_;

  // The init(), plan() and tearDown() functions
  typedef void (*compiled_function_t)(char *);
  compiled_function_t init_func_;
  compiled_function_t plan_func_;
  compiled_function_t tear_down_func_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);
};

}  // namespace codegen
}  // namespace peloton
