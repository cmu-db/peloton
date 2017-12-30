//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query.cpp
//
// Identification: src/codegen/query.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query.h"
#include "codegen/query_result_consumer.h"
#include "common/timer.h"
#include "executor/plan_executor.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

void Query::Execute(std::unique_ptr<executor::ExecutorContext> executor_context,
                    QueryResultConsumer &consumer,
                    std::function<void(executor::ExecutionResult)> on_complete,
                    RuntimeStats *stats) {
  CodeGen codegen{GetCodeContext()};

  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  size_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT((parameter_size % 8 == 0) && "parameter size not multiple of 8");

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  char *param = param_data.get();
  PL_MEMSET(param, 0, parameter_size);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    storage::StorageManager *storage_manager;
    executor::ExecutorContext *executor_context;
    QueryParameters *query_parameters;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->storage_manager = storage::StorageManager::GetInstance();
  func_args->executor_context = executor_context.get();
  func_args->query_parameters = &executor_context->GetParams();
  func_args->consumer_arg = consumer.GetConsumerState();

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    init_func_(param);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    plan_func_(param);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param);
    throw;
  }

  // Timer plan execution
  if (stats != nullptr) {
    timer.Stop();
    stats->plan_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Clean up
  LOG_TRACE("Calling query's tearDown() ...");
  tear_down_func_(param);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }

  executor::ExecutionResult result;
  result.m_result = ResultType::SUCCESS;
  result.m_processed = executor_context->num_processed;
  on_complete(result);
}

bool Query::Prepare(const QueryFunctions &query_funcs) {
  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up Query ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetRawFunctionPointer(
      query_funcs.init_func);
  PL_ASSERT(init_func_ != nullptr);

  plan_func_ = (compiled_function_t)code_context_.GetRawFunctionPointer(
      query_funcs.plan_func);
  PL_ASSERT(plan_func_ != nullptr);

  tear_down_func_ = (compiled_function_t)code_context_.GetRawFunctionPointer(
      query_funcs.tear_down_func);
  PL_ASSERT(tear_down_func_ != nullptr);

  LOG_TRACE("Query has been setup ...");

  // All is well
  return true;
}

}  // namespace codegen
}  // namespace peloton
