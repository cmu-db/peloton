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

#include "type/value.h"
#include "executor/executor_context.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace executor {

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction)
    : transaction_(transaction) {}

ExecutorContext::ExecutorContext(concurrency::Transaction *transaction,
                                 const std::vector<type::Value> &params)
    : transaction_(transaction), params_(params) {}

ExecutorContext::~ExecutorContext() {
  // params will be freed automatically
}

concurrency::Transaction *ExecutorContext::GetTransaction() const {
  return transaction_;
}

const std::vector<type::Value> &ExecutorContext::GetParams() const {
  return params_;
}

void ExecutorContext::SetParams(type::Value &value) {
  params_.push_back(value);
}

void ExecutorContext::ClearParams() { params_.clear(); }

type::EphemeralPool *ExecutorContext::GetPool() {
  // construct pool if needed
  if (pool_.get() == nullptr) {
    pool_.reset(new type::EphemeralPool());
  }

  // return pool
  return pool_.get();
}

}  // namespace executor
}  // namespace peloton
