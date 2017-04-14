//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_statement.cpp
//
// Identification: src/codegen/query_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query.h"

#include "catalog/catalog.h"
#include "executor/executor_context.h"
#include "common/macros.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

// Execute the query on the given database (and within the provided transaction)
// This really involves calling the init(), plan() and tearDown() functions, in
// that order. We also need to correctly handle cases where _any_ of those
// functions throw exceptions.
void Query::Execute(concurrency::Transaction &txn, char *consumer_arg,
                    RuntimeStats *stats, executor::ExecutorContext *exec_context) {
  CodeGen codegen{GetCodeContext()};

  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  uint64_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT(parameter_size % 8 == 0);

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  type::Value values[params_.size()];
  GetParams(values, exec_context);

  // Grab an non-owning pointer to the space
  char *param = param_data.get();

  // Clean the space
  PL_MEMSET(param, 0, parameter_size);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    concurrency::Transaction *txn;
    catalog::Catalog *catalog;
    type::Value *values;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->txn = &txn;
  func_args->catalog = catalog::Catalog::GetInstance();
  func_args->values = values;
  func_args->consumer_arg = consumer_arg;

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // Call init
  LOG_DEBUG("Calling query's init() ...");
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
  LOG_DEBUG("Calling query's plan() ...");
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
  LOG_DEBUG("Calling query's tearDown() ...");
  tear_down_func_(param);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }
}

bool Query::Prepare(const QueryFunctions &query_funcs) {
  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up Query ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.init_func);
  PL_ASSERT(init_func_ != nullptr);

  plan_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.plan_func);
  PL_ASSERT(plan_func_ != nullptr);

  tear_down_func_ = (compiled_function_t)code_context_.GetFunctionPointer(
      query_funcs.tear_down_func);
  PL_ASSERT(tear_down_func_ != nullptr);

  LOG_TRACE("Query has been setup ...");

  // All is well
  return true;
}

uint32_t Query::StoreParam(Parameter param) {
  uint32_t offset = params_.size();
  params_.emplace_back(param);
  return offset;
}

void Query::GetParams(type::Value *values,
           executor::ExecutorContext *exec_context) {
  for (uint32_t i = 0; i < params_.size(); i ++) {
    switch (params_[i].GetType()) {
      case Parameter::ParamType::Const:
      case Parameter::ParamType::Tuple:
        values[i] = params_[i].GetValue();
        break;
      case Parameter::ParamType::Param:
        PL_ASSERT(exec_context != nullptr);
        values[i] = exec_context->GetParams().at(
                params_[i].GetParamIdx());
        break;
      default: {
        throw Exception{"No Such Param Type"};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton