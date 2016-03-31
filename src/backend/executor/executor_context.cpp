//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// executor_context.cpp
//
// Identification: src/backend/executor/executor_context.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/value.h"
#include "backend/executor/executor_context.h"

namespace peloton {
namespace executor {

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction)
    : transaction_(transaction), params_exec_flag_(INVALID) {}

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction,
                                 const std::vector<Value> &params)
    : transaction_(transaction), params_(params), params_exec_flag_(INVALID) {}

ExecutorContext::~ExecutorContext() {
  // params will be freed automatically
}

VarlenPool *ExecutorContext::GetExecutorContextPool() {
  // construct pool if needed
  if (pool_.get() == nullptr) pool_.reset(new VarlenPool(BACKEND_TYPE_MM));

  // return pool
  return pool_.get();
}

}  // namespace executor
}  // namespace peloton
