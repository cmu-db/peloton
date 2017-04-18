//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_executor.cpp
//
// Identification: src/executor/create_function_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/create_function_executor.h"
#include "executor/executor_context.h"
#include "common/logger.h"
#include "catalog/catalog.h"

#include <vector>

namespace peloton {
namespace executor {

// Constructor for drop executor
CreateFunctionExecutor::CreateFunctionExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context = executor_context;
}

// Initialize executer
// Nothing to initialize now
bool CreateFunctionExecutor::DInit() {
  LOG_TRACE("Initializing Create Function Executer...");
  LOG_TRACE("Create Function Executer initialized!");
  return true;
}

bool CreateFunctionExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  UNUSED_ATTRIBUTE const planner::CreateFunctionPlan &node = GetPlanNode<planner::CreateFunctionPlan>();
  UNUSED_ATTRIBUTE auto current_txn = context->GetTransaction();

  //Insert into the catalog

  // Look at create executor for reference

  return false;
}
}
}
