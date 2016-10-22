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

#include "common/varlen_pool.h"
#include "common/value.h"

namespace peloton {

//class Value;

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
                  const std::vector<common::Value> &params);

  ~ExecutorContext();

  concurrency::Transaction *GetTransaction() const;

  const std::vector<common::Value> &GetParams() const;

  void SetParams(common::Value &value);

  void ClearParams();

  // Get a varlen pool (will construct the pool only if needed)
  common::VarlenPool *GetExecutorContextPool();

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // transaction
  concurrency::Transaction *transaction_;

  // params
  std::vector<common::Value> params_;

  // pool
  std::unique_ptr<common::VarlenPool> pool_;

};

}  // namespace executor
}  // namespace peloton
