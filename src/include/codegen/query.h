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
#include "codegen/parameter_cache.h"
#include "codegen/query_parameters.h"
#include "codegen/query_state.h"

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
  struct CompileStats {
    double compile_ms = 0.0;
  };

  struct RuntimeStats {
    double interpreter_prepare_ms = 0.0;
    double init_ms = 0.0;
    double plan_ms = 0.0;
    double tear_down_ms = 0.0;
  };

  // We use this handy class for the parameters to the llvm functions
  // to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    executor::ExecutorContext *executor_context;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  struct LLVMFunctions {
    llvm::Function *init_func;
    llvm::Function *plan_func;
    llvm::Function *tear_down_func;
  };

  using compiled_function_t = void (*)(FunctionArguments *);

  struct CompiledFunctions {
    compiled_function_t init_func;
    compiled_function_t plan_func;
    compiled_function_t tear_down_func;
  };

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);

  /**
   * @brief Setup this query with the given JITed function components
   *
   * @param funcs The compiled functions that implement the logic of the query
   */
  void Prepare(const LLVMFunctions &funcs);

  // Compiles the function in this query to native code
  void Compile(CompileStats *stats = nullptr);

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

  // Execute the query as native code (must already be compiled)
  void ExecuteNative(FunctionArguments *function_arguments,
                     RuntimeStats *stats);

  // Execute the query using the interpreter
  void ExecuteInterpreter(FunctionArguments *function_arguments,
                          RuntimeStats *stats);

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  QueryState query_state_;

  // LLVM IR of the query functions
  LLVMFunctions llvm_functions_;

  // Pointers to the compiled query functions
  CompiledFunctions compiled_functions_;

  // Shows if the query has been compiled to native code
  bool is_compiled_;
};

}  // namespace codegen
}  // namespace peloton
