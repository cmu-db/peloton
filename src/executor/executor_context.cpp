//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.cpp
//
// Identification: src/executor/executor_context.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>

#include "type/value.h"
#include "executor/executor_context.h"
#include "concurrency/transaction_context.h"

namespace peloton {
namespace executor {

ExecutorContext::ExecutorContext(concurrency::TransactionContext *transaction,
                                 codegen::QueryParameters parameters)
    : transaction_(transaction), parameters_(std::move(parameters)) {}

concurrency::TransactionContext *ExecutorContext::GetTransaction() const {
  return transaction_;
}

const std::vector<type::Value> &ExecutorContext::GetParamValues() const {
  return parameters_.GetParameterValues();
}

codegen::QueryParameters &ExecutorContext::GetParams() { return parameters_; }

type::EphemeralPool *ExecutorContext::GetPool() {
  // construct pool if needed
  if (pool_ == nullptr) {
    pool_.reset(new type::EphemeralPool());
  }

  // return pool
  return pool_.get();
}

}  // namespace executor
}  // namespace peloton
