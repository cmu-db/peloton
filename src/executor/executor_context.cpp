//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.cpp
//
// Identification: src/executor/executor_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/value.h"
#include "executor/executor_context.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace executor {

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction)
    : transaction_(transaction) {}

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction,
                                 const std::vector<Value> &params)
    : transaction_(transaction),
      params_(params) {}

ExecutorContext::~ExecutorContext() {
  // params will be freed automatically
}

concurrency::Transaction *ExecutorContext::GetTransaction() const {
  return transaction_;
}

const std::vector<Value> &ExecutorContext::GetParams() const {
  return params_;
}

void ExecutorContext::SetParams(Value value) {
  params_.push_back(value);
}

void ExecutorContext::ClearParams() {
  params_.clear();
}

VarlenPool *ExecutorContext::GetExecutorContextPool() {
  // construct pool if needed
  if (pool_.get() == nullptr) pool_.reset(new VarlenPool(BACKEND_TYPE_MM));

  // return pool
  return pool_.get();
}

}  // namespace executor
}  // namespace peloton
