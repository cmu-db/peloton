//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/include/executor/executor_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/pool.h"

namespace peloton {

class Value;

namespace concurrency{
class Transaction;
}

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

  concurrency::Transaction *GetTransaction() const;

  const std::vector<Value> &GetParams() const;

  void SetParams(Value value);

  void ClearParams();

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
