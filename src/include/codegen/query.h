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
#include "planner/project_info.h"

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

  uint32_t StoreParam(Parameter param, int idx = -1);

  void StoreTargetList(TargetList &target_list) {
    update_direct_list_.clear();
    for (uint32_t i = 0; i < target_list.size(); i ++) {
      update_target_list_.emplace_back(target_list[i]);
    }
  }

  void StoreDirectList(DirectMapList &direct_list) {
    update_direct_list_.clear();
    for (uint32_t i = 0; i < direct_list.size(); i ++) {
      update_direct_list_.emplace_back(direct_list[i]);
    }
  }

 private:
  friend class QueryCompiler;

  // Constructor
  Query(const planner::AbstractPlan &query_plan);

  // Function for finalize parameters (for parameterization)
  void PrepareParams(executor::ExecutorContext *exec_context);
  // Function for loading serialized parameters into runtime state
  void LoadParams(std::vector<std::unique_ptr<char[]>> &params,
                  char **char_ptr_params,
                  int32_t *char_len_params);

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
  TargetList update_target_list_;
  DirectMapList update_direct_list_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);
};

}  // namespace codegen
}  // namespace peloton