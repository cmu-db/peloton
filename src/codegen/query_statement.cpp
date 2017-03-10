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

#include "codegen/query_statement.h"

#include "catalog/catalog.h"
#include "common/macros.h"

namespace peloton {
namespace codegen {

// Constructor
QueryStatement::QueryStatement(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

// Execute the query on the given database (and within the provided transaction)
// This really involves calling the init(), plan() and tearDown() functions, in
// that order. We also need to correctly handle cases where _any_ of those
// functions throw exceptions.
void QueryStatement::Execute(concurrency::Transaction &txn, char *consumer_arg,
                             RuntimeStats *stats) const {
  // Allocate some space for the function arguments
  std::unique_ptr<char> param_data{new char[param_size_]};

  // Grab an non-owning pointer to the space
  char *param = param_data.get();

  // Clean the space
  memset(param, 0, param_size_);

  // We use this handy class to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    concurrency::Transaction *txn;
    catalog::Catalog *catalog;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments*>(param_data.get());
  func_args->txn = &txn;
  func_args->catalog = catalog::Catalog::GetInstance();
  func_args->consumer_arg = consumer_arg;

  // Timer
  Timer<std::ratio<1, 1000>> timer;

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

bool QueryStatement::Setup(uint64_t param_size, llvm::Function *init_func,
                           llvm::Function *plan_func,
                           llvm::Function *tear_down_func) {
  PL_ASSERT(param_size % 8 == 0);
  param_size_ = param_size;

  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up QueryStatement ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetFunctionPointer(init_func);
  PL_ASSERT(init_func_ != nullptr);

  plan_func_ = (compiled_function_t)code_context_.GetFunctionPointer(plan_func);
  PL_ASSERT(plan_func_ != nullptr);

  tear_down_func_ =
      (compiled_function_t)code_context_.GetFunctionPointer(tear_down_func);
  PL_ASSERT(tear_down_func_ != nullptr);

  LOG_TRACE("QueryStatement has been setup ...");

  // All is well
  return true;
}

}  // namespace codegen
}  // namespace peloton