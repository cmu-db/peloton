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
class QueryStatement {
 private:
  // The JITed function type
  typedef void (*compiled_function_t)(char *);

 public:
  struct RuntimeStats {
    double init_ms;
    double plan_ms;
    double tear_down_ms;
  };

  // Constructor
  QueryStatement(const planner::AbstractPlan &query_plan);

  // Setup this query statement with the given LLVM function components. The
  // provided functions perform initialization, execution and tear down of
  // this query.
  bool Setup(uint64_t param_size, llvm::Function *init_func,
             llvm::Function *plan_func, llvm::Function *tear_down_func);

  // Execute th e query given the catalog manager and runtime/consumer state
  // that is passed along to the query execution code.
  void Execute(concurrency::Transaction &txn, char *consumer_arg,
               RuntimeStats *stats = nullptr) const;

  // Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  // Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  uint64_t param_size_;

  // The init(), plan() and tearDown() functions
  compiled_function_t init_func_;
  compiled_function_t plan_func_;
  compiled_function_t tear_down_func_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(QueryStatement);
};

}  // namespace codegen
}  // namespace peloton