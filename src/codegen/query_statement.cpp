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

namespace peloton {
namespace codegen {

// Constructor
QueryStatement::QueryStatement(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

// Execute the query on the given database (and within the provided transaction)
// This really involves calling the init(), plan() and tearDown() functions, in
// that order. We also need to correctly handle cases where _any_ of those
// functions throw exceptions.
void QueryStatement::Execute(catalog::Catalog &catalog, char *consumer_arg,
                             RuntimeStats *stats) const {
  // Create clean memory space for the parameters
  char param_data[param_size_];
  memset(param_data, 0, param_size_);

  // Set the first parameter as the database pointer
  *reinterpret_cast<catalog::Catalog **>(param_data) = &catalog;

  // Set the second parameter as the runtime state pointer
  char *state_pos = param_data + sizeof(char *);
  *reinterpret_cast<char **>(state_pos) = consumer_arg;

  // Time
  Timer<std::ratio<1, 1000>> timer;

  // Call init
  LOG_DEBUG("Calling query's init() ...");
  try {
    init_func_(param_data);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param_data);
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
    plan_func_(param_data);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_(param_data);
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
  tear_down_func_(param_data);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }
}

bool QueryStatement::Setup(uint64_t param_size, llvm::Function *init_func,
                           llvm::Function *plan_func,
                           llvm::Function *tear_down_func) {
  assert(param_size % 8 == 0);
  param_size_ = param_size;

  LOG_TRACE("Going to JIT the query ...");

  // Compile the code
  if (!code_context_.Compile()) {
    return false;
  }

  LOG_TRACE("Setting up QueryStatement ...");

  // Get pointers to the JITed functions
  init_func_ = (compiled_function_t)code_context_.GetFunctionPointer(init_func);
  assert(init_func_ != nullptr);

  plan_func_ = (compiled_function_t)code_context_.GetFunctionPointer(plan_func);
  assert(plan_func_ != nullptr);

  tear_down_func_ =
      (compiled_function_t)code_context_.GetFunctionPointer(tear_down_func);
  assert(tear_down_func_ != nullptr);

  LOG_TRACE("QueryStatement has been setup ...");

  // All is well
  return true;
}

}  // namespace codegen
}  // namespace peloton