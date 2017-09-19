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
#include "planner/abstract_plan.h"

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
               executor::ExecutorContext *executor_context, char *consumer_arg,
               RuntimeStats *stats = nullptr);

  // Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  // Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  // The class tracking all the state needed by this query
  RuntimeState &GetRuntimeState() { return runtime_state_; }

  // Reset the parameters so that this query can be reused
  void ReplaceParameters(const planner::AbstractPlan &plan) {
    parameters_.clear();
    parameters_index_.clear();
    plan.ExtractParameters(parameters_, parameters_index_);
  };

  size_t GetParameterIdx(const expression::AbstractExpression *expression) {
    auto param = parameters_index_.find(expression);
    PL_ASSERT(param != parameters_index_.end());
    return param->second;
  }

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

  // The parameters and mapping for expression and parameter ids to 
  std::vector<planner::Parameter> parameters_;
  std::unordered_map<const expression::AbstractExpression *, size_t>
      parameters_index_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);
};

}  // namespace codegen
}  // namespace peloton
