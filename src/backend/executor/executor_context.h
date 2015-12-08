//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// executor_context.h
//
// Identification: src/backend/executor/executor_context.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction.h"
#include "backend/common/pool.h"

namespace peloton {
namespace executor {

//===--------------------------------------------------------------------===//
// Executor Context
//===--------------------------------------------------------------------===//

class ExecutorContext {
 public:
  ExecutorContext(const ExecutorContext &) = delete;
  ExecutorContext &operator=(const ExecutorContext &) = delete;
  ExecutorContext(ExecutorContext &&) = delete;
  ExecutorContext &operator=(ExecutorContext &&) = delete;

  ExecutorContext(concurrency::Transaction *transaction);

  ExecutorContext(concurrency::Transaction *transaction,
                  const std::vector<Value> &params);

  ~ExecutorContext();

  concurrency::Transaction *GetTransaction() const { return transaction_; }

  const std::vector<Value> &GetParams() const { return params_; }

  // Get a varlen pool (will construct the pool only if needed)
  VarlenPool *GetExecutorContextPool();

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // transaction
  concurrency::Transaction *transaction_;

  // params
  std::vector<Value> params_;

  // pool
  std::unique_ptr<VarlenPool> pool_;
};

}  // namespace executor
}  // namespace peloton
